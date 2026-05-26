#pragma once

#ifndef TASK_MANAGER_HPP
#define TASK_MANAGER_HPP

#include <boost/asio.hpp>

#include "protocol.hpp"

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class master_connection;
class tcp_server;

class task_manager : public std::enable_shared_from_this<task_manager> {
   public:
    task_manager(const std::string& matrixA_path,
                 const std::string& matrixB_path,
                 uint16_t port,
                 uint64_t expected_processors,
                 uint64_t step_k = 256,
                 const std::string& output_path = "result_matrix.txt");

    void start();
    void terminate();

   private:
    struct TileMeta {
        uint64_t tile_id = 0;
        uint64_t row_start = 0;
        uint64_t row_count = 0;
        uint64_t col_start = 0;
        uint64_t col_count = 0;
    };

    struct WorkerState {
        std::shared_ptr<master_connection> connection;
        uint64_t worker_id = 0;
        uint64_t current_tile_index = 0;
        uint64_t current_step = 0;
        bool busy = false;
    };

    struct TaskKey {
        uint64_t tile_index = 0;
        uint64_t step_index = 0;

        bool operator==(const TaskKey& other) const {
            return tile_index == other.tile_index && step_index == other.step_index;
        }
    };

    struct TaskKeyHash {
        size_t operator()(const TaskKey& key) const {
            return std::hash<uint64_t>{}(key.tile_index) ^ (std::hash<uint64_t>{}(key.step_index) << 1U);
        }
    };

    struct PrefetchedBatch {
        wire::TaskMessage task;
        std::vector<double> a_block;
        std::vector<double> b_block;
        uint64_t reserved_bytes = 0;
        bool queued = false;
        bool loading = false;
        bool ready = false;
        bool failed = false;
        std::string error;
    };

    struct MappedMatrix {
        std::string path;
        int fd = -1;
        size_t file_size = 0;
        const char* mapped = nullptr;
        uint64_t rows = 0;
        uint64_t cols = 0;
        uint64_t data_offset = 0;
        uint64_t row_stride = 0;
    };

    const std::string matrixA_file_path;
    const std::string matrixB_file_path;
    const std::string output_file_path;
    const uint16_t port;
    const uint64_t expected_processors;
    const uint64_t step_k;

    boost::asio::io_context io_ctx;
    std::vector<WorkerState> workers_;
    std::pair<uint64_t, uint64_t> processor_grid{1, 1};

    MappedMatrix matrix_a_;
    MappedMatrix matrix_b_;

    uint64_t m_rows_ = 0;
    uint64_t inner_k_ = 0;
    uint64_t n_cols_ = 0;

    uint64_t row_block_ = 0;
    uint64_t col_block_ = 0;
    uint64_t total_steps_ = 0;

    std::vector<TileMeta> tiles_;
    std::vector<std::vector<double>> result_rows_;
    uint64_t prefetch_budget_bytes_ = 0;
    uint64_t prefetched_bytes_in_use_ = 0;
    std::unordered_map<TaskKey, std::shared_ptr<PrefetchedBatch>, TaskKeyHash> prefetched_batches_;
    std::deque<TaskKey> prefetch_queue_;
    std::mutex prefetch_mutex_;
    std::condition_variable prefetch_cv_;
    std::thread prefetch_thread_;
    bool prefetch_stop_ = false;
    bool prefetch_enabled_ = false;

    void open_and_validate_inputs();
    void close_mapped_inputs();
    void create_processor_grid(uint64_t num_processors);
    void build_tiles();

    static std::pair<uint64_t, uint64_t> parse_header(const char* mapped, size_t file_size, uint64_t& data_offset);
    static bool verify_matmul_possibility(const std::pair<uint64_t, uint64_t>& dimA,
                                          const std::pair<uint64_t, uint64_t>& dimB);

    std::vector<double> read_a_block(uint64_t row_start, uint64_t row_count, uint64_t k_start, uint64_t k_count) const;
    std::vector<double> read_b_block(uint64_t k_start, uint64_t k_count, uint64_t col_start, uint64_t col_count) const;

    double parse_cell_value(const MappedMatrix& matrix, uint64_t row, uint64_t col) const;

    void accept_workers();
    void run_scheduler();
    void send_task_for_worker(WorkerState& worker);
    void handle_worker_result(WorkerState& worker);
    wire::TaskMessage build_task_message(uint64_t tile_index, uint64_t step_index) const;
    uint64_t batch_bytes_for(uint64_t tile_index, uint64_t step_index) const;
    uint64_t detect_prefetch_budget_bytes() const;
    void start_prefetcher();
    void stop_prefetcher();
    void prefetch_loop();
    void schedule_prefetch(uint64_t tile_index, uint64_t step_index);
    std::shared_ptr<PrefetchedBatch> acquire_batch(uint64_t tile_index, uint64_t step_index);
    void release_batch(uint64_t tile_index, uint64_t step_index);

    void write_result_to_file() const;
};

#endif  // TASK_MANAGER_HPP
