#pragma once

#include "task_manager.hpp"
#include <cstdint>
#include <memory>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class tcp_server {
   public:
    tcp_server(boost::asio::io_context& io_context,
               std::shared_ptr<task_manager> manager);

   private:
    tcp::acceptor acceptor_;
    std::shared_ptr<task_manager> manager_;

    void start_accept();
};