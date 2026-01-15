#pragma once

#include "task_manager.hpp"
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class task_manager;

class tcp_connection : public std::enable_shared_from_this<tcp_connection> {
   public:
    using pointer = std::shared_ptr<tcp_connection>;

    static pointer create(boost::asio::io_context& io_context,
                          std::shared_ptr<task_manager> manager) {
        return pointer(new tcp_connection(io_context, manager));
    }

    static pointer create(tcp::socket&& socket,
                          std::shared_ptr<task_manager> manager) {
        return pointer(new tcp_connection(std::move(socket), manager));
    }

    tcp::socket& socket();
    void start();
    void set_id(uint64_t& id);
    void set_message(std::string& msg);
    uint64_t get_id() const;
    uint64_t get_threads() const;
    std::string& get_message();

   private:
    tcp::socket socket_;
    uint64_t    threads_;
    uint64_t    id_;
    std::string message_;
    bool first_connect = true;
    std::shared_ptr<task_manager> manager_;

    tcp_connection(boost::asio::io_context& io_context,
                   std::shared_ptr<task_manager> manager);

    tcp_connection(tcp::socket socket,
                   std::shared_ptr<task_manager> manager);

    void do_read();
    void respond();
    void read_threads();
    void read_results();
};