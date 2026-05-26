#include "tcp_connection_slave.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <thread>

using boost::asio::ip::tcp;

slave_connection::slave_connection(tcp::socket&& socket,
                                   uint64_t threads,
                                   tcp::endpoint endpoint,
                                   boost::asio::io_context& io_ctx)
    : tcp_connection(std::move(socket), threads), endpoint_(endpoint), io_context_(io_ctx) {}

void slave_connection::start() {
    connect();
    run_loop();
}

uint64_t slave_connection::get_threads() const { return threads_; }
tcp::socket& slave_connection::socket() { return socket_; }

void slave_connection::connect() {
    tcp::resolver resolver(io_context_);

    while (true) {
        try {
            boost::asio::connect(socket_, resolver.resolve(endpoint_.address().to_string(), std::to_string(endpoint_.port())));
            break;
        } catch (const boost::system::system_error& e) {
            if (e.code() == boost::asio::error::connection_refused) {
                std::cerr << "[slave] CONN REFUSED, 2SEC SLEEP\n";
                std::this_thread::sleep_for(std::chrono::seconds(2));
                continue;
            }

            throw;
        }
    }

    wire::write_type(socket_, wire::MessageType::HELLO);
    wire::HelloMessage hello{};
    hello.threads = threads_;
    wire::write_pod(socket_, hello);

    std::cout << "[tcp_connection] HELLO sent (threads=" << threads_ << ")\n";
}

void slave_connection::run_loop() {
    while (true) {
        const auto type = wire::read_type(socket_);

        if (type == wire::MessageType::STOP) {
            std::cout << "[tcp_connection] STOP received\n";
            return;
        }

        if (type != wire::MessageType::TASK) {
            throw std::runtime_error("[tcp_connection] RECEIVED UNKNOWN MESSAGE\n");
        }

        wire::TaskMessage task;
        wire::read_pod(socket_, task);

        std::vector<double> a_block;
        std::vector<double> b_block;
        wire::read_doubles(socket_, a_block, static_cast<size_t>(task.row_count * task.k_count));
        wire::read_doubles(socket_, b_block, static_cast<size_t>(task.k_count * task.col_count));

        if (task.tile_begin) {
            current_tile_id_ = task.tile_id;
            current_rows_ = task.row_count;
            current_cols_ = task.col_count;
            accum_c_.assign(static_cast<size_t>(current_rows_ * current_cols_), 0.0);
            has_open_tile_ = true;
        }

        if (!has_open_tile_ || current_tile_id_ != task.tile_id || current_rows_ != task.row_count ||
            current_cols_ != task.col_count) {
            throw std::runtime_error("[tcp_connection] INVALID STATE\n");
        }

        multiply_accumulate(a_block, b_block, task.row_count, task.k_count, task.col_count);

        wire::write_type(socket_, wire::MessageType::RESULT);

        wire::ResultMessage result{};
        result.tile_id = task.tile_id;
        result.row_count = task.row_count;
        result.col_count = task.col_count;
        result.tile_end = task.tile_end ? 1 : 0;
        wire::write_pod(socket_, result);

        if (task.tile_end) {
            wire::write_doubles(socket_, accum_c_);
            has_open_tile_ = false;
            accum_c_.clear();
        }
    }
}

void slave_connection::multiply_accumulate(const std::vector<double>& a_block,
                                           const std::vector<double>& b_block,
                                           uint64_t rows,
                                           uint64_t k_count,
                                           uint64_t cols) {
    const size_t thread_count = static_cast<size_t>(std::max<uint64_t>(1, threads_));
    const uint64_t rows_per_thread = (rows + thread_count - 1) / thread_count;

    std::vector<std::thread> pool;
    pool.reserve(thread_count);

    for (size_t t = 0; t < thread_count; ++t) {
        const uint64_t r0 = t * rows_per_thread;
        const uint64_t r1 = std::min<uint64_t>(rows, r0 + rows_per_thread);
        if (r0 >= r1) {
            break;
        }

        pool.emplace_back([&, r0, r1]() {
            for (uint64_t r = r0; r < r1; ++r) {
                for (uint64_t k = 0; k < k_count; ++k) {
                    const double a = a_block[r * k_count + k];
                    const double* b_row = &b_block[k * cols];
                    double* c_row = &accum_c_[r * cols];

                    for (uint64_t c = 0; c < cols; ++c) {
                        c_row[c] += a * b_row[c];
                    }
                }
            }
        });
    }

    for (auto& th : pool) {
        th.join();
    }
}
