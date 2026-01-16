#include "headers/task_manager.hpp"
#include "headers/tcp_server.hpp"
#include <boost/asio.hpp>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#define SERVER_PORT 52524

using boost::asio::ip::tcp;

std::mutex mtx;
uint64_t counter = 1;
std::vector<std::vector<uint64_t>> final_res;

class task_manager;

class Timer {
   private:
    using Clock = std::chrono::steady_clock;
    using Second = std::chrono::duration<double, std::ratio<1>>;

    std::chrono::time_point<Clock> m_beg{Clock::now()};

   public:
    void reset() {
        m_beg = Clock::now();
    }

    double elapsed() const {
        return std::chrono::duration_cast<Second>(Clock::now() - m_beg).count();
    }
};

int main() {
    uint64_t M;
    uint64_t div;
    std::cout << "MATRIX DIMENSIONS: ";
    std::cin >> M;
    std::cout << '\n';

    std::cout << "CHUNK DIVIDER (default 100): ";
    std::cin >> div;
    std::cout << '\n';

    std::cout << "[main] ";
    std::cout << "STARTING TASK MANAGER" << '\n';
    auto manager = std::make_shared<task_manager>(M, div, SERVER_PORT);
    manager->start();

    for (auto& i : final_res) {
        for (auto& j : i) {
            std::cout << j << ' ';
        }
    }
}