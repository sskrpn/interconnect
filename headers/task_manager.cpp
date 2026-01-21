#include "task_manager.hpp"

using boost::asio::ip::tcp;

extern uint64_t counter;

task_manager::task_manager(uint64_t& matrix_size, uint64_t chunk_div = 100, uint16_t port = 52524)
                            : M(matrix_size),
                              CHUNK_DIVIDER(chunk_div),
                              chunks_left(chunk_div),
                              port(port) {
    chunk_size = M / CHUNK_DIVIDER;
}

void task_manager::start() {
    try {
        auto server = std::make_shared<tcp_server>(io_ctx, shared_from_this(), port);
        state = EXECUTING;

        std::cout << "[task_manager] ";
        std::cout << "TASK MANAGER STARTED" << '\n';
        io_ctx.run();
    } catch (std::exception& e) {
        std::cerr << "[task_manager] ";
        std::cerr << "EXCEPTION ON START: " << e.what() << '\n';
    }
}

void task_manager::add_connection(std::shared_ptr<master_connection> connection) {
    connection->set_id(counter);
    connections[counter] = connection;
    counter++;
}

void task_manager::generate_response(uint64_t& id) {
    auto pointer = connections[id].lock();
    if (!pointer) {
        std::cerr << "[task_manager] ";
        std::cerr << "FAILED TO GENERATE RESPONSE: CONNECTION EXPIRED" << '\n';
        return;
    }
    uint64_t threads = pointer->get_threads();
    std::string message;

    if ((state == COMPLETED) || (state == AWAIT_ONLY)) {
        message = "end\n";
        pointer->set_message(message);
        return;
    }

    uint64_t prev_startR = 0;
    if (chunks_left <= threads) {
        std::cout << "[task_manager] ";
        std::cout << "FINAL TASK IS BEING GENERATED" << '\n';

        prev_startR = leap * chunk_size;
        chunk_size = chunks_left * chunk_size / threads;
        reserved_last = chunk_size * threads +
                        (M - prev_startR) % chunk_size;

        std::cout << "[task_manager] ";
        std::cout << "RESERVED FOR LAST: " << reserved_last << '\n';
        state = AWAIT_ONLY;
    } else if (chunks_left == 0) {
        std::cout << "[task_manager] ";
        std::cout << "ENDING" << '\n';
        state = AWAIT_ONLY;
        message = "end";
        pointer->set_message(message);
        return;
    }

    std::cout << "[task_manager] ";
    std::cout << "GENERATING MESSAGE" << '\n';
    for (size_t th = 0; th < threads; th++) {
        if (state == EXECUTING) {
            uint64_t startR = (th + leap) * chunk_size;
            uint64_t endR = startR + chunk_size;

            message += std::to_string(startR) + ' ' +
                       std::to_string(endR) + ' ' +
                       std::to_string(M) + '!';
        } else if (state == AWAIT_ONLY) {
            uint64_t startR = th * chunk_size + prev_startR;
            uint64_t endR = startR + chunk_size;
            if (th == threads - 1) {
                endR = M;
            }

            message += std::to_string(startR) + ' ' +
                       std::to_string(endR) + ' ' +
                       std::to_string(M) + '!';
        }
    }
    message += '\n';
    pointer->set_message(message);

    awaiting[id] = (state == AWAIT_ONLY) ? reserved_last : (chunk_size * threads);
    leap += threads;
    chunks_left -= threads;
    std::cout << "[task_manager] ";
    std::cout << "CHUNKS LEFT: " << chunks_left << '\n';
}

uint64_t task_manager::get_reserved(uint64_t& id) {
    return awaiting[id];
}

void task_manager::complete_awaiting(uint64_t& id) {
    awaiting.erase(id);
}

void task_manager::record_results(std::vector<uint64_t>& results) {
    total_res.push_back(results);
    total_done += results.size();

    std::cout << "[task_manager] ";
    std::cout << "CURRENTLY RECEIVED: " << total_done << '\n';

    if (total_done == M) {
        state = COMPLETED;
        std::cout << "[task_manager] ";
        std::cout << "ALL RESULTS RECEIVED" << '\n';
        terminate();
        return;
    }
}

void task_manager::terminate() {
    std::cout << "[task_manager] ";
    std::cout << "TERMINATING TASK MANAGER" << '\n';
    server->stop_accept();

    for(auto& [id, conn] : connections) {
        auto conn_ptr = conn.lock();
        if (conn_ptr) {
            boost::system::error_code ec;
            conn_ptr->socket().shutdown(tcp::socket::shutdown_both, ec);
            conn_ptr->socket().close(ec);
        }
    }

    io_ctx.stop();
    std::cout << "[task_manager] ";
    std::cout << "TASK MANAGER TERMINATED" << '\n';
}