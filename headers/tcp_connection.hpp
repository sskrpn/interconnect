#pragma once

#include <boost/asio.hpp>

#include <cstdint>
#include <memory>
#include <string>

using boost::asio::ip::tcp;

class task_manager;

class tcp_connection {
   public:
    virtual ~tcp_connection() = default;
    virtual void start() = 0;

    virtual uint64_t get_threads() const = 0;
    virtual tcp::socket& socket() = 0;

   protected:
    explicit tcp_connection(tcp::socket&& socket, uint64_t threads = 0)
        : socket_(std::move(socket)), threads_(threads) {}

    tcp::socket socket_;
    uint64_t threads_ = 0;
};
