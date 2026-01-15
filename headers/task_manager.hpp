#pragma once

#include "tcp_connection.hpp"
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <boost/asio.hpp>

enum TASK_STATE {
    NOT_STARTED,
    EXECUTING,
    AWAIT_ONLY,
    COMPLETED
};

class tcp_connection;

class task_manager : public std::enable_shared_from_this<task_manager> {
   private:
    TASK_STATE state = NOT_STARTED;
    std::unordered_map<uint64_t, std::shared_ptr<tcp_connection>> connections;
    std::unordered_map<uint64_t, uint64_t> comps;
    std::unordered_map<uint64_t, uint64_t> awaiting;
    std::vector<std::vector<uint64_t>> total_res;
    const uint64_t M;
    const uint64_t CHUNK_DIVIDER;
    uint64_t chunks_left;
    uint64_t total_done = 0;
    uint64_t chunk_size;
    uint64_t leap = 0;
    uint64_t reserved_last = 0;

   public:
    task_manager(uint64_t& matrix_size, uint64_t chunk_div);

    void start();
    void add_connection(std::shared_ptr<tcp_connection> connection);
    void add_comp(uint64_t& id);
    void generate_response(uint64_t& id);
    uint64_t get_reserved(uint64_t& id);
    void complete_awaiting(uint64_t& id);
    void record_results(std::vector<uint64_t>& results);
    void terminate();
};