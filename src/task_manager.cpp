#include "task_manager.hpp"

#include "protocol.hpp"
#include "tcp_connection_master.hpp"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <unistd.h>

namespace {
constexpr uint64_t MATRIX_ELEMENTS_SIZE = 18ULL;  // "0." + 15 digits + ' '

uint64_t ceil_div(uint64_t a, uint64_t b) {
    return (a + b - 1) / b;
}
}  // namespace

task_manager::task_manager(const std::string& matrixA_path,
                           const std::string& matrixB_path,
                           uint16_t port,
                           uint64_t expected_processors,
                           uint64_t step_k,
                           const std::string& output_path)
    : matrixA_file_path(matrixA_path),
      matrixB_file_path(matrixB_path),
      output_file_path(output_path),
      port(port),
      expected_processors(expected_processors),
      step_k(step_k) {
    create_processor_grid(expected_processors);
}

void task_manager::start() {
    try {
        open_and_validate_inputs();

        std::cout << "[task_manager] GRID: " << processor_grid.first << "x" << processor_grid.second << "\n";
        std::cout << "[task_manager] MATRIX A: " << m_rows_ << "x" << inner_k_ << "\n";
        std::cout << "[task_manager] MATRIX B: " << inner_k_ << "x" << n_cols_ << "\n";

        build_tiles();
        prefetch_budget_bytes_ = detect_prefetch_budget_bytes();
        prefetch_enabled_ = prefetch_budget_bytes_ > 0;
        if (prefetch_enabled_) {
            // I THOUGHT IT WAS A GOOD IDEA ddx
            std::cout << "[task_manager] PREFETCH ENABLED, budget=" << prefetch_budget_bytes_ / (1024 * 1024)
                      << " MB\n";
        } else {
            std::cout << "[task_manager] PREFETCH DISABLED\n";
        }

        accept_workers();
        start_prefetcher();
        run_scheduler();
        write_result_to_file();
        terminate();
    } catch (...) {
        terminate();
        throw;
    }
}

void task_manager::terminate() {
    stop_prefetcher();

    for (auto& worker : workers_) {
        if (!worker.connection) {
            continue;
        }

        try {
            worker.connection->send_stop();
        } catch (const std::exception& e) {
            std::cerr << "[task_manager] STOP send failed????: " << worker.worker_id << ": " << e.what() << "\n";
        }

        boost::system::error_code ec;
        worker.connection->socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
        worker.connection->socket().close(ec);
    }

    close_mapped_inputs();
}

void task_manager::open_and_validate_inputs() {
    auto open_matrix = [](const std::string& path) -> MappedMatrix {
        MappedMatrix matrix;
        matrix.path = path;

        matrix.fd = open(path.c_str(), O_RDONLY);
        if (matrix.fd < 0) {
            throw std::runtime_error("[task_manager] COULD NOT OPEN " + path);
        }

        struct stat st {};
        if (fstat(matrix.fd, &st) != 0) {
            close(matrix.fd);
            throw std::runtime_error("[task_manager] STAT FAILED " + path);
        }

        matrix.file_size = static_cast<size_t>(st.st_size);
        matrix.mapped = static_cast<const char*>(
            mmap(nullptr, matrix.file_size, PROT_READ, MAP_PRIVATE, matrix.fd, 0));
        if (matrix.mapped == MAP_FAILED) {
            close(matrix.fd);
            throw std::runtime_error("[task_manager] MMAP FAILED " + path);
        }

        uint64_t data_offset = 0;
        auto dims = parse_header(matrix.mapped, matrix.file_size, data_offset);
        matrix.rows = dims.first;
        matrix.cols = dims.second;
        matrix.data_offset = data_offset;

        // =============================SLOP=============================
        // CHECKS ROW ENDS FOR ALIGNMENT REASON
        const uint64_t packed_row_bytes = matrix.cols * MATRIX_ELEMENTS_SIZE;
        matrix.row_stride = packed_row_bytes;

        const uint64_t probe = matrix.data_offset + packed_row_bytes;
        if (matrix.rows > 1 && probe < matrix.file_size) {
            const char next = matrix.mapped[probe];
            if (next == '\n') {
                matrix.row_stride = packed_row_bytes + 1;
            } else if (next == '\r' && probe + 1 < matrix.file_size && matrix.mapped[probe + 1] == '\n') {
                matrix.row_stride = packed_row_bytes + 2;
            }
        }
        // =============================================================

        return matrix;
    };

    matrix_a_ = open_matrix(matrixA_file_path);
    matrix_b_ = open_matrix(matrixB_file_path);

    if (!verify_matmul_possibility({matrix_a_.rows, matrix_a_.cols}, {matrix_b_.rows, matrix_b_.cols})) {
        throw std::runtime_error("[task_manager] MATMUL NOT POSSIBLE\n");
    }

    m_rows_ = matrix_a_.rows;
    inner_k_ = matrix_a_.cols;
    n_cols_ = matrix_b_.cols;
}

void task_manager::close_mapped_inputs() {
    auto close_matrix = [](MappedMatrix& matrix) {
        if (matrix.mapped && matrix.mapped != MAP_FAILED) {
            munmap(const_cast<char*>(matrix.mapped), matrix.file_size);
            matrix.mapped = nullptr;
        }
        if (matrix.fd >= 0) {
            close(matrix.fd);
            matrix.fd = -1;
        }
    };

    close_matrix(matrix_a_);
    close_matrix(matrix_b_);
}

void task_manager::create_processor_grid(uint64_t num_processors) {
    uint64_t rows = 1;
    uint64_t cols = std::max<uint64_t>(1, num_processors);

    int64_t min_diff = static_cast<int64_t>(cols);

    for (uint64_t i = 1; i * i <= std::max<uint64_t>(1, num_processors); ++i) {
        if (num_processors % i == 0) {
            const uint64_t j = num_processors / i;
            const int64_t diff = std::llabs(static_cast<int64_t>(i) - static_cast<int64_t>(j));
            if (diff < min_diff) {
                min_diff = diff;
                rows = i;
                cols = j;
            }
        }
    }

    processor_grid = {rows, cols};
}

// DIVIDE RESULT MATRIX PER WORKER
void task_manager::build_tiles() {
    row_block_ = ceil_div(m_rows_, processor_grid.first);
    col_block_ = ceil_div(n_cols_, processor_grid.second);
    total_steps_ = ceil_div(inner_k_, step_k);

    tiles_.clear();
    tiles_.reserve(processor_grid.first * processor_grid.second);

    uint64_t tile_id = 0;
    for (uint64_t pr = 0; pr < processor_grid.first; ++pr) {
        const uint64_t row_start = pr * row_block_;
        const uint64_t row_end = std::min(row_start + row_block_, m_rows_);
        if (row_start >= row_end) {
            continue;
        }

        for (uint64_t pc = 0; pc < processor_grid.second; ++pc) {
            const uint64_t col_start = pc * col_block_;
            const uint64_t col_end = std::min(col_start + col_block_, n_cols_);
            if (col_start >= col_end) {
                continue;
            }

            TileMeta tile;
            tile.tile_id = tile_id++;
            tile.row_start = row_start;
            tile.row_count = row_end - row_start;
            tile.col_start = col_start;
            tile.col_count = col_end - col_start;
            tiles_.push_back(tile);
        }
    }

    result_rows_.assign(m_rows_, std::vector<double>(n_cols_, 0.0));

    std::cout << "[task_manager] TILES READY: " << tiles_.size() << ", steps/tile=" << total_steps_ << "\n";
}

std::pair<uint64_t, uint64_t> task_manager::parse_header(const char* mapped,
                                                         size_t file_size,
                                                         uint64_t& data_offset) {
    const char* header_end = static_cast<const char*>(memchr(mapped, '\n', std::min<size_t>(file_size, 256)));
    if (!header_end) {
        throw std::runtime_error("[task_manager] HEADER NOT FOUND");
    }

    std::string header(mapped, header_end - mapped);
    const auto split = header.find(' ');
    if (split == std::string::npos) {
        throw std::runtime_error("[task_manager] INVALID HEADER");
    }

    const uint64_t rows = std::stoull(header.substr(0, split));
    const uint64_t cols = std::stoull(header.substr(split + 1));

    data_offset = static_cast<uint64_t>((header_end - mapped) + 1);

    const uint64_t expected_bytes = rows * cols * MATRIX_ELEMENTS_SIZE;
    if (data_offset + expected_bytes > file_size) {
        throw std::runtime_error("[task_manager] FILE PAYLOAD IS SHORTER THAN EXPECTED BY HEADER");
    }

    return {rows, cols};
}

bool task_manager::verify_matmul_possibility(const std::pair<uint64_t, uint64_t>& dimA,
                                             const std::pair<uint64_t, uint64_t>& dimB) {
    return dimA.second == dimB.first;
}

double task_manager::parse_cell_value(const MappedMatrix& matrix, uint64_t row, uint64_t col) const {
    const uint64_t offset = matrix.data_offset + row * matrix.row_stride + col * MATRIX_ELEMENTS_SIZE;

    char cell[MATRIX_ELEMENTS_SIZE + 1]{};
    std::memcpy(cell, matrix.mapped + offset, MATRIX_ELEMENTS_SIZE);
    cell[MATRIX_ELEMENTS_SIZE] = '\0';

    return std::strtod(cell, nullptr);
}

std::vector<double> task_manager::read_a_block(uint64_t row_start,
                                               uint64_t row_count,
                                               uint64_t k_start,
                                               uint64_t k_count) const {
    std::vector<double> out;
    out.reserve(row_count * k_count);

    for (uint64_t r = 0; r < row_count; ++r) {
        const uint64_t global_r = row_start + r;
        for (uint64_t k = 0; k < k_count; ++k) {
            out.push_back(parse_cell_value(matrix_a_, global_r, k_start + k));
        }
    }

    return out;
}

std::vector<double> task_manager::read_b_block(uint64_t k_start,
                                               uint64_t k_count,
                                               uint64_t col_start,
                                               uint64_t col_count) const {
    std::vector<double> out;
    out.reserve(k_count * col_count);

    for (uint64_t k = 0; k < k_count; ++k) {
        const uint64_t global_k = k_start + k;
        for (uint64_t c = 0; c < col_count; ++c) {
            out.push_back(parse_cell_value(matrix_b_, global_k, col_start + c));
        }
    }

    return out;
}

wire::TaskMessage task_manager::build_task_message(uint64_t tile_index, uint64_t step_index) const {
    const TileMeta& tile = tiles_.at(tile_index);
    const uint64_t k_start = step_index * step_k;
    const uint64_t k_count = std::min(step_k, inner_k_ - k_start);

    wire::TaskMessage msg;
    msg.tile_id = tile.tile_id;
    msg.row_start = tile.row_start;
    msg.row_count = tile.row_count;
    msg.col_start = tile.col_start;
    msg.col_count = tile.col_count;
    msg.k_start = k_start;
    msg.k_count = k_count;
    msg.step_index = step_index;
    msg.total_steps = total_steps_;
    msg.tile_begin = (step_index == 0) ? 1 : 0;
    msg.tile_end = (step_index + 1 == total_steps_) ? 1 : 0;
    return msg;
}

uint64_t task_manager::batch_bytes_for(uint64_t tile_index, uint64_t step_index) const {
    const auto msg = build_task_message(tile_index, step_index);
    return (msg.row_count * msg.k_count + msg.k_count * msg.col_count) * sizeof(double);
}

// =================SLOP==================
uint64_t task_manager::detect_prefetch_budget_bytes() const {
    struct sysinfo info {};
    if (sysinfo(&info) != 0) {
        return 512ULL << 20;  // 512 MB
    }

    const uint64_t free_bytes = static_cast<uint64_t>(info.freeram) * info.mem_unit;
    if (free_bytes < (512ULL << 20)) {
        return 0;
    }

    const uint64_t min_budget = 256ULL << 20;   // 256 MB
    const uint64_t max_budget = 4ULL << 30;     // 4 GB
    uint64_t budget = free_bytes / 4;           // use at most 25% of currently free RAM
    budget = std::min(max_budget, budget);
    budget = std::max(min_budget, budget);
    return budget;
}
// ======================================

void task_manager::start_prefetcher() {
    if (!prefetch_enabled_) {
        return;
    }

    prefetch_stop_ = false;
    prefetch_thread_ = std::thread(&task_manager::prefetch_loop, this);
}

void task_manager::stop_prefetcher() {
    {
        std::lock_guard<std::mutex> lk(prefetch_mutex_);
        prefetch_stop_ = true;
    }
    prefetch_cv_.notify_all();

    if (prefetch_thread_.joinable()) {
        prefetch_thread_.join();
    }

    std::lock_guard<std::mutex> lk(prefetch_mutex_);
    prefetched_batches_.clear();
    prefetch_queue_.clear();
    prefetched_bytes_in_use_ = 0;
}

void task_manager::prefetch_loop() {
    while (true) {
        TaskKey key;
        std::shared_ptr<PrefetchedBatch> batch;

        {
            std::unique_lock<std::mutex> lk(prefetch_mutex_);
            prefetch_cv_.wait(lk, [&]() { return prefetch_stop_ || !prefetch_queue_.empty(); });

            if (prefetch_stop_ && prefetch_queue_.empty()) {
                return;
            }

            key = prefetch_queue_.front();
            prefetch_queue_.pop_front();

            auto it = prefetched_batches_.find(key);
            if (it == prefetched_batches_.end()) {
                continue;
            }

            batch = it->second;
            batch->queued = false;
            batch->loading = true;
        }

        try {
            batch->task = build_task_message(key.tile_index, key.step_index);
            batch->a_block = read_a_block(batch->task.row_start, batch->task.row_count, batch->task.k_start, batch->task.k_count);
            batch->b_block = read_b_block(batch->task.k_start, batch->task.k_count, batch->task.col_start, batch->task.col_count);

            {
                std::lock_guard<std::mutex> lk(prefetch_mutex_);
                batch->ready = true;
                batch->loading = false;
            }
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lk(prefetch_mutex_);
            batch->failed = true;
            batch->loading = false;
            batch->error = e.what();
        }

        prefetch_cv_.notify_all();
    }
}

void task_manager::schedule_prefetch(uint64_t tile_index, uint64_t step_index) {
    if (!prefetch_enabled_ || step_index >= total_steps_ || tile_index >= tiles_.size()) {
        return;
    }

    const TaskKey key{tile_index, step_index};
    const uint64_t reserve_bytes = batch_bytes_for(tile_index, step_index);

    std::lock_guard<std::mutex> lk(prefetch_mutex_);
    if (prefetched_batches_.find(key) != prefetched_batches_.end()) {
        return;
    }

    if (prefetched_bytes_in_use_ + reserve_bytes > prefetch_budget_bytes_) {
        return;
    }

    auto batch = std::make_shared<PrefetchedBatch>();
    batch->reserved_bytes = reserve_bytes;
    batch->queued = true;

    prefetched_bytes_in_use_ += reserve_bytes;
    prefetched_batches_[key] = batch;
    prefetch_queue_.push_back(key);
    prefetch_cv_.notify_one();
}

std::shared_ptr<task_manager::PrefetchedBatch> task_manager::acquire_batch(uint64_t tile_index, uint64_t step_index) {
    const TaskKey key{tile_index, step_index};

    if (prefetch_enabled_) {
        std::unique_lock<std::mutex> lk(prefetch_mutex_);
        auto it = prefetched_batches_.find(key);
        if (it != prefetched_batches_.end()) {
            auto batch = it->second;
            prefetch_cv_.wait(lk, [&]() { return batch->ready || batch->failed || prefetch_stop_; });

            if (batch->failed) {
                if (prefetched_bytes_in_use_ >= batch->reserved_bytes) {
                    prefetched_bytes_in_use_ -= batch->reserved_bytes;
                } else {
                    prefetched_bytes_in_use_ = 0;
                }
                prefetched_batches_.erase(it);
                throw std::runtime_error("[task_manager] PREFETCH FAILED: " + batch->error);
            }
            if (prefetch_stop_ && !batch->ready) {
                if (prefetched_bytes_in_use_ >= batch->reserved_bytes) {
                    prefetched_bytes_in_use_ -= batch->reserved_bytes;
                } else {
                    prefetched_bytes_in_use_ = 0;
                }
                prefetched_batches_.erase(it);
                throw std::runtime_error("[task_manager] PREFETCHER STOPPED BEFORE BATCH BECAME READY");
            }

            return batch;
        }
    }

    auto batch = std::make_shared<PrefetchedBatch>();
    batch->task = build_task_message(tile_index, step_index);
    batch->a_block = read_a_block(batch->task.row_start, batch->task.row_count, batch->task.k_start, batch->task.k_count);
    batch->b_block = read_b_block(batch->task.k_start, batch->task.k_count, batch->task.col_start, batch->task.col_count);
    batch->ready = true;
    return batch;
}

void task_manager::release_batch(uint64_t tile_index, uint64_t step_index) {
    if (!prefetch_enabled_) {
        return;
    }

    const TaskKey key{tile_index, step_index};
    std::lock_guard<std::mutex> lk(prefetch_mutex_);
    auto it = prefetched_batches_.find(key);
    if (it == prefetched_batches_.end()) {
        return;
    }

    if (prefetched_bytes_in_use_ >= it->second->reserved_bytes) {
        prefetched_bytes_in_use_ -= it->second->reserved_bytes;
    } else {
        prefetched_bytes_in_use_ = 0;
    }
    prefetched_batches_.erase(it);
}

void task_manager::accept_workers() {
    using boost::asio::ip::tcp;

    tcp::acceptor acceptor(io_ctx, tcp::endpoint(tcp::v4(), port));
    workers_.clear();
    workers_.reserve(expected_processors);

    std::cout << "[task_manager] WAAITING FOR WORKERS " << expected_processors << " at " << port << "\n";

    for (uint64_t id = 1; id <= expected_processors; ++id) {
        tcp::socket socket(io_ctx);
        acceptor.accept(socket);

        auto worker_conn = master_connection::create(std::move(socket), shared_from_this());
        worker_conn->set_id(id);
        worker_conn->start();

        WorkerState state;
        state.connection = worker_conn;
        state.worker_id = id;
        workers_.push_back(std::move(state));

        std::cout << "[task_manager] WORKER " << id << " CONNECTED, threads=" << worker_conn->get_threads() << "\n";
    }

    std::cout << "[task_manager] ALL WORKERS CONNECTED\n";
}

void task_manager::run_scheduler() {
    if (workers_.empty()) {
        throw std::runtime_error("[task_manager] NO WORKERS CONNECTED");
    }

    size_t assigned_tiles = 0;
    size_t completed_tiles = 0;

    const size_t seed_count = std::min<size_t>(workers_.size(), tiles_.size());
    for (size_t w = 0; w < seed_count; ++w) {
        workers_[w].current_tile_index = assigned_tiles;
        workers_[w].current_step = 0;
        workers_[w].busy = true;
        send_task_for_worker(workers_[w]);
        schedule_prefetch(workers_[w].current_tile_index, workers_[w].current_step + 1);
        ++assigned_tiles;
    }

    while (completed_tiles < tiles_.size()) {
        bool made_progress = false;

        for (size_t w = 0; w < workers_.size(); ++w) {
            auto& worker = workers_[w];
            if (!worker.busy) {
                continue;
            }

            handle_worker_result(worker);
            made_progress = true;

            if (worker.current_step + 1 < total_steps_) {
                worker.current_step += 1;
                send_task_for_worker(worker);
                schedule_prefetch(worker.current_tile_index, worker.current_step + 1);
                continue;
            }

            ++completed_tiles;
            worker.busy = false;

            if (assigned_tiles < tiles_.size()) {
                worker.current_tile_index = assigned_tiles;
                worker.current_step = 0;
                worker.busy = true;
                send_task_for_worker(worker);
                schedule_prefetch(worker.current_tile_index, worker.current_step + 1);
                ++assigned_tiles;
            }

            if (assigned_tiles < tiles_.size()) {
                // THIS SHOULD BE DISABLED IF LOW RAM
                // 30k x 30k LITERALLY DIES ON 16GB
                schedule_prefetch(assigned_tiles, 0);
            }
        }

        if (!made_progress) {
            throw std::runtime_error("[task_manager] SCHEDULER MADE NO PROGRESS; WORKER STATE DEADLOCK");
        }

        if ((completed_tiles % 4) == 0 || completed_tiles == tiles_.size()) {
            std::cout << "[task_manager] TILES COMPLETED: " << completed_tiles << "/" << tiles_.size() << "\n";
        }
    }
}

void task_manager::send_task_for_worker(WorkerState& worker) {
    const uint64_t tile_index = worker.current_tile_index;
    const uint64_t step_index = worker.current_step;
    auto batch = acquire_batch(tile_index, step_index);
    worker.connection->send_task(batch->task, batch->a_block, batch->b_block);
    release_batch(tile_index, step_index);
}

void task_manager::handle_worker_result(WorkerState& worker) {
    const auto result_header = worker.connection->read_result_header();

    if (result_header.tile_id != tiles_.at(worker.current_tile_index).tile_id) {
        throw std::runtime_error("[task_manager] WORKER DID WHATEVER\n");
    }

    if (!result_header.tile_end) {
        return;
    }

    const size_t count = static_cast<size_t>(result_header.row_count * result_header.col_count);
    std::vector<double> values;
    worker.connection->read_result_payload(values, count);

    const TileMeta& tile = tiles_.at(worker.current_tile_index);
    if (result_header.row_count != tile.row_count || result_header.col_count != tile.col_count) {
        throw std::runtime_error("[task_manager] WORKER RETURNED UNEXPECTED TILE DIMENSIONS");
    }

    for (uint64_t r = 0; r < tile.row_count; ++r) {
        for (uint64_t c = 0; c < tile.col_count; ++c) {
            result_rows_[tile.row_start + r][tile.col_start + c] =
                values[r * tile.col_count + c];
        }
    }
}

void task_manager::write_result_to_file() const {
    std::ofstream out(output_file_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        throw std::runtime_error("[task_manager] FAILED TO OPEN OUTPUT FILE: " + output_file_path);
    }

    out << m_rows_ << " " << n_cols_ << "\n";
    out.setf(std::ios::fixed);
    out.precision(15);

    for (uint64_t r = 0; r < m_rows_; ++r) {
        for (uint64_t c = 0; c < n_cols_; ++c) {
            out << result_rows_[r][c];
            if (c + 1 < n_cols_) {
                out << ' ';
            }
        }
        out << '\n';
    }

    std::cout << "[task_manager] RESULT HERE: " << output_file_path << "\n";
}
