#pragma once

#include "protocol.hpp"
#include "tcp_connection.hpp"

#include <cstdint>
#include <memory>
#include <vector>

class slave_connection : public std::enable_shared_from_this<slave_connection>, public tcp_connection {
   public:
    static std::shared_ptr<slave_connection> create(tcp::socket&& socket,
                                                    uint64_t threads,
                                                    tcp::endpoint endpoint,
                                                    boost::asio::io_context& io_ctx) {
        return std::shared_ptr<slave_connection>(
            new slave_connection(std::move(socket), threads, endpoint, io_ctx));
    }

    void start() override;
    uint64_t get_threads() const override;
    tcp::socket& socket() override;

   private:
    tcp::endpoint endpoint_;
    boost::asio::io_context& io_context_;

    uint64_t current_tile_id_ = 0;
    uint64_t current_rows_ = 0;
    uint64_t current_cols_ = 0;
    bool has_open_tile_ = false;
    std::vector<double> accum_c_;

    slave_connection(tcp::socket&& socket,
                     uint64_t threads,
                     tcp::endpoint endpoint,
                     boost::asio::io_context& io_ctx);

    void connect();
    void run_loop();
    void multiply_accumulate(const std::vector<double>& a_block,
                             const std::vector<double>& b_block,
                             uint64_t rows,
                             uint64_t k_count,
                             uint64_t cols);
};
