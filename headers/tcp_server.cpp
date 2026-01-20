#include "tcp_server.hpp"

using boost::asio::ip::tcp;

tcp_server::tcp_server(boost::asio::io_context& io_context,
                       std::shared_ptr<task_manager> manager,
                       uint16_t port)
                            : acceptor_(io_context,
                              tcp::endpoint(tcp::v4(), port)),
                              manager_(manager) {
    start_accept();
}

void tcp_server::start_accept() {
    acceptor_.async_accept(
        [this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) {
                std::shared_ptr<tcp_connection> connection =
                    tcp_connection::create(std::move(socket), manager_);
                manager_->add_connection(connection);
                std::cout << "[tcp_server] ";
                std::cout << "NEW CONNECTION ACCEPTED" << '\n';
                connection->start();
            } else {
                std::cerr << "[tcp_server] ";
                std::cerr << "ACCEPT FAILED: " << ec.message() << '\n';
            }

            start_accept();
        });
}

void tcp_server::stop_accept() {
    acceptor_.close();
    std::cout << "[tcp_server] ";
    std::cout << "SERVER STOPPED LISTENING" << '\n';
}