#include "tcp_connection_master.hpp"

#include "protocol.hpp"
#include "task_manager.hpp"

#include <iostream>
#include <stdexcept>

using boost::asio::ip::tcp;

master_connection::master_connection(tcp::socket&& socket, std::shared_ptr<task_manager> task_manager)
    : tcp_connection(std::move(socket)), manager_(std::move(task_manager)) {}

uint64_t     master_connection::get_threads() const { return threads_; }
tcp::socket& master_connection::socket()            { return socket_; }
uint64_t     master_connection::get_id()      const { return id_; }
void         master_connection::set_id(uint64_t id) { id_ = id; }

void master_connection::start() {
    const auto type = wire::read_type(socket_);
    if (type != wire::MessageType::HELLO) {
        throw std::runtime_error("[tcp_connection] Expected HELLO\n");
    }

    wire::HelloMessage hello;
    wire::read_pod(socket_, hello);
    threads_ = hello.threads;
}

void master_connection::send_task(const wire::TaskMessage& task,
                                  const std::vector<double>& a_block,
                                  const std::vector<double>& b_block) {
    wire::write_type(socket_, wire::MessageType::TASK);
    wire::write_pod(socket_, task);
    wire::write_doubles(socket_, a_block);
    wire::write_doubles(socket_, b_block);
}

wire::ResultMessage master_connection::read_result_header() {
    const auto type = wire::read_type(socket_);
    if (type != wire::MessageType::RESULT) {
        throw std::runtime_error("[tcp_connection] Expected RESULT \n");
    }

    wire::ResultMessage result;
    wire::read_pod(socket_, result);
    return result;
}

void master_connection::read_result_payload(std::vector<double>& out_values, size_t count) {
    wire::read_doubles(socket_, out_values, count);
}

void master_connection::send_stop() {
    wire::write_type(socket_, wire::MessageType::STOP);
}
