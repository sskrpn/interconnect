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

    virtual void set_message(std::string& msg);
    virtual uint64_t get_threads() const;
    virtual std::string& get_message();

   protected:
    std::weak_ptr<task_manager> manager_;
    tcp::socket socket_;
    uint64_t threads_;
    std::string message_;
    bool first_connect = true;

    tcp_connection(tcp::socket&& socket, 
                   std::shared_ptr<task_manager> task_manager);
    virtual void do_read();
    virtual void respond();
    virtual void read_threads();
    virtual void read_results();
};