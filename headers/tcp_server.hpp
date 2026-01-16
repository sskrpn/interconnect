#pragma once

#ifndef TCP_SERVER_HPP
#define TCP_SERVER_HPP

#include "task_manager.hpp"
#include <iostream>
#include <cstdint>
#include <memory>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class tcp_server {
   public:
    tcp_server(boost::asio::io_context& io_context,
               std::shared_ptr<task_manager> manager,
               uint16_t port);

    void stop_accept();

   private:
    tcp::acceptor acceptor_;
    std::shared_ptr<task_manager> manager_;

    void start_accept();
};

#endif  // TCP_SERVER_HPP