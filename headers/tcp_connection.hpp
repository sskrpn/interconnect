#pragma once

#include <iostream>
#include <cstdint>
#include <string>
#include <memory>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class task_manager;

class tcp_connection{
public:
    virtual void start() = 0;
    virtual ~tcp_connection() = default;

    virtual void set_message(std::string& msg) = 0;
    virtual uint64_t get_threads() const = 0;
    virtual std::string& get_message() = 0;
    virtual tcp::socket& socket() = 0;

protected:
    tcp::socket socket_;
    uint64_t threads_;
    std::string message_;
    bool first_connect = true;

    tcp_connection(tcp::socket&& socket, uint64_t threads = 0)
                   : socket_(std::move(socket)), threads_(threads) {}

    virtual void do_read() = 0;
    virtual void respond() = 0;
};