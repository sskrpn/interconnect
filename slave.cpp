#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <vector>
#include <boost/asio.hpp>
#include "headers/tcp_connection_slave.hpp"

#define SERVER_PORT 52524
const uint64_t CORES_SELF = 11;

using boost::asio::ip::tcp;

std::mutex m;
std::vector<uint64_t> res;

void calculate_partials(uint64_t start, uint64_t end, uint64_t R);

void manage_threads(std::vector<std::vector<uint64_t>>& tasks) {
    std::vector<std::thread> local_threads;
    for (size_t t = 0; t < CORES_SELF; t++) {
        local_threads.emplace_back(calculate_partials, std::ref(tasks[t][0]),
                                   std::ref(tasks[t][1]), std::ref(tasks[t][2]));
    }

    for (auto& th : local_threads) {
        th.join();
    }
}

void calculate_partials(uint64_t start, uint64_t end, uint64_t R) {
    std::cout << "[slave] ";
    std::cout << "TASK STARTED WITH: " << start << ' ' << end << ' ' << R << '\n';
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937 generator(seed);
    uint64_t iter = 0;
    for (size_t i = start; i < end; i++) {
        iter++;
        uint64_t sumR = 0;
        uint64_t rnd;
        for (size_t j = 0; j < R; j++) {
             rnd = static_cast<uint64_t>(generator() % 3 + 1);
             sumR += (rnd * rnd);
        }
        m.lock();
        res.push_back(sumR);
        m.unlock();
    }
    std::cout << "[slave] ";
    std::cout << "THREAD DONE" << '\n';
    std::cout << iter << '\n';
}

std::vector<uint64_t>& get_res() { return res; }

int main() {
    const std::string SERVER_IP = "127.0.0.1";
    //const std::string SERVER_IP = "192.168.0.3";
    std::cout << "[slave] ";
    std::cout << "STARTING SLAVE CONNECTION" << '\n';

    boost::asio::io_context io_context;
    auto con = std::make_shared<slave_connection>(
        tcp::socket(io_context), 
        tcp::endpoint(boost::asio::ip::make_address(SERVER_IP), SERVER_PORT),
        CORES_SELF
    );

    con->start();
}