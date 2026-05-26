#include "headers/tcp_connection_slave.hpp"

#include <iostream>
#include <string>
#include <thread>

namespace {
constexpr uint16_t DEFAULT_SERVER_PORT = 52524;
}

/*
USAGE: ./slave [server_ip] [threads]
*/
int main(int argc, char** argv) {
    std::string server_ip = "127.0.0.1";
    uint64_t threads = std::thread::hardware_concurrency();
    if (threads == 0) {
        threads = 4;
    }

    if (argc >= 2) {
        server_ip = argv[1];
    }
    if (argc >= 3) {
        threads = std::stoull(argv[2]);
    }

    try {
        boost::asio::io_context io_context;
        auto connection = slave_connection::create(
            boost::asio::ip::tcp::socket(io_context),
            threads,
            boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address(server_ip), DEFAULT_SERVER_PORT),
            io_context);

        connection->start();
    } catch (const std::exception& e) {
        std::cerr << "[slave] FATAL ERROR: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
