#pragma once

#include "tcp_connectionv2.hpp"

using boost::asio::ip::tcp;

class master_connection : public std::enable_shared_from_this<master_connection>,
                                 tcp_connection {
public:
    static std::shared_ptr<master_connection> create(tcp::socket&& socket,
                                                     std::shared_ptr<task_manager> task_manager) {
        return std::shared_ptr<master_connection>(new master_connection(std::move(socket), task_manager));
    }

    void         start()               override;
    uint64_t     get_threads()   const override;
    uint64_t     get_id()                 const;
    std::string& get_message()         override;
    void set_message(std::string& msg) override;
    void set_id();

protected:
    uint64_t    id_;

    master_connection(tcp::socket&& socket,
                      std::shared_ptr<task_manager> task_manager);
    void do_read()      override;
    void respond()      override;
    void read_threads() override;
    void read_results() override;
};