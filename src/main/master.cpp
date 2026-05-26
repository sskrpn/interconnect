#include "headers/task_manager.hpp"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace {
constexpr uint16_t DEFAULT_SERVER_PORT = 52524;
constexpr uint64_t DEFAULT_WORKERS = 4;
constexpr uint64_t DEFAULT_STEP_K = 256;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "USAGE:./master <matrixA_path> <matrixB_path> [workers] [k_step] [output_path]\n";
        return 1;
    }

    const std::string matrixA_path = argv[1];
    const std::string matrixB_path = argv[2];

    uint64_t workers = DEFAULT_WORKERS;
    uint64_t k_step = DEFAULT_STEP_K;
    std::string output_path = "result_matrix.txt";

    if (argc >= 4) {
        workers = std::stoull(argv[3]);
    }
    if (argc >= 5) {
        k_step = std::stoull(argv[4]);
    }
    if (argc >= 6) {
        output_path = argv[5];
    }

    if (workers == 0) {
        std::cerr << "Workers must be > 0 (4 DEFAULT)\n";
        return 1;
    }
    if (k_step == 0) {
        std::cerr << "k_step must be > 0 (256 DEFAULT)\n";
        return 1;
    }

    try {
        auto manager = std::make_shared<task_manager>(
            matrixA_path,
            matrixB_path,
            DEFAULT_SERVER_PORT,
            workers,
            k_step,
            output_path);

        manager->start();
    } catch (const std::exception& e) {
        std::cerr << "[master] FATAL ERROR: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
