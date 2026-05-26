#pragma once

#include "protocol.hpp"
#include "tcp_connection.hpp"

#include <memory>
#include <vector>

class task_manager;

class master_connection : public std::enable_shared_from_this<master_connection>, public tcp_connection {
   public:
    static std::shared_ptr<master_connection> create(tcp::socket&& socket,
                                                     std::shared_ptr<task_manager> task_manager) {
        return std::shared_ptr<master_connection>(new master_connection(std::move(socket), std::move(task_manager)));
    }

    void start() override;
    uint64_t get_threads() const override;
    tcp::socket& socket() override;

    uint64_t get_id() const;
    void set_id(uint64_t id);

    void send_task(const wire::TaskMessage& task,
                   const std::vector<double>& a_block,
                   const std::vector<double>& b_block);

    wire::ResultMessage read_result_header();
    void read_result_payload(std::vector<double>& out_values, size_t count);

    void send_stop();

   private:
    std::weak_ptr<task_manager> manager_;
    uint64_t id_ = 0;

    master_connection(tcp::socket&& socket, std::shared_ptr<task_manager> task_manager);
};
