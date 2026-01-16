#include "tcp_connection.hpp"
#include "task_manager.hpp"
#include <iostream>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

tcp::socket& tcp_connection::socket() {
    return socket_;
}

void tcp_connection::start()                 { do_read();}
void tcp_connection::set_id(uint64_t& id)    { id_ = id; }
uint64_t tcp_connection::get_id() const      { return id_; }
uint64_t tcp_connection::get_threads() const { return threads_; }
std::string& tcp_connection::get_message()   { return message_; }
void tcp_connection::set_message(std::string& msg) { message_ = msg; }

tcp_connection::tcp_connection(boost::asio::io_context& io_context,
                               std::shared_ptr<task_manager> manager)
                                : socket_(io_context),
                                  manager_(manager) {}

tcp_connection::tcp_connection(tcp::socket socket,
                               std::shared_ptr<task_manager> manager)
                                : socket_(std::move(socket)),
                                  manager_(manager) {}

void tcp_connection::do_read() {
    if (first_connect) {
        first_connect = false;
        std::cout << "[tcp_connection] ";
        std::cout << "READING THREADS" << '\n';
        read_threads();
        respond();
    } else {
        read_results();
        respond();
    }
}

void tcp_connection::respond() {
    auto manager_ptr = manager_.lock();
    if (!manager_ptr) {
        std::cerr << "[tcp_connection] ";
        std::cerr << "MANAGER DEAD (on respond)" << '\n';
        return;
    }
    manager_ptr->generate_response(id_);
    message_ += '\n';
    boost::system::error_code error;

    std::cout << "[tcp_connection] ";
    std::cout << "SENDING TASK" << '\n';
    size_t n = boost::asio::write(socket(), boost::asio::buffer(message_),
                                  boost::asio::transfer_all(), error);
    if (error) {
        std::cerr << "[tcp_connection] ";
        std::cerr << "WRITE FAILED: " << error.message() << '\n';
    } else {
        std::cout << "[tcp_connection] ";
        std::cout << "SENT TASK:\n"
                  << message_;
        message_.clear();
        do_read();
    }
}

void tcp_connection::read_threads() {
    std::string cores_str;
    boost::asio::read_until(socket(), boost::asio::dynamic_buffer(cores_str), '\n');
    threads_ = std::stoull(cores_str);
    std::cout << "[tcp_connection] ";
    std::cout << "CORES READ ";
    std::cout << threads_ << '\n';
}

void tcp_connection::read_results() {
    auto manager_ptr = manager_.lock();
    if (!manager_ptr) {
        std::cerr << "[tcp_connection] ";
        std::cerr << "MANAGER DEAD (on result read)" << '\n';
        return;
    }
    uint64_t reserved = manager_ptr->get_reserved(id_);

    std::vector<uint64_t> slave_res(reserved);
    boost::system::error_code error;

    std::cout << "[tcp_connection] ";
    std::cout << "READING RESULTS" << '\n';
    size_t n = boost::asio::read(socket(), boost::asio::buffer(slave_res),
                                 boost::asio::transfer_exactly(reserved * 8), error);
    if (error) {
        std::cerr << "[tcp_connection] ";
        std::cerr << "READ FAILED: " << error.message() << '\n';
    } else {
        std::cout << "[tcp_connection] ";
        std::cout << "ACCEPTED: " << slave_res.size() << " RESULTS" << '\n';

        manager_ptr->complete_awaiting(id_);
        manager_ptr->record_results(slave_res);
    }
}