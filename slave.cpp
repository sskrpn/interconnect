#include <boost/asio.hpp>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <vector>

using boost::asio::ip::tcp;

const uint64_t SERVER_PORT = 52524;
const std::string SERVER_IP = "127.0.0.1";

const uint64_t CORES_SELF_i = 4;
const std::string CORES_SELF_s = "4\n";

std::mutex m;
std::vector<uint64_t> res;
bool first_connect = true;

void manage_threads(std::vector<std::vector<uint64_t>>& tasks);
void calculate_partials(uint64_t start, uint64_t end, uint64_t R);

class TCP_Connection : public std::enable_shared_from_this<TCP_Connection> {
   public:
    static std::shared_ptr<TCP_Connection> create(tcp::socket&& socket) {
        std::cout << "CONNECTION CREATED" << '\n';
        return std::shared_ptr<TCP_Connection>(new TCP_Connection(std::move(socket)));
    }

    tcp::socket& socket() { return socket_; }

    void write_cores() {
        boost::system::error_code ec;
        boost::asio::write(socket_, boost::asio::buffer(CORES_SELF_s), ec);
        std::cout << "CORES SENT" << '\n';
        first_connect = false;
        read();
    }
    void write_result() {
        boost::system::error_code ec;
        boost::asio::write(socket_, boost::asio::buffer(res), ec);
        std::cout << res.size() << '\n';
        std::cout << "WRITE DONE" << '\n';
        res.clear();
        read();
    }

   private:
    tcp::socket socket_;
    std::string message_;

    TCP_Connection(tcp::socket&& socket)
        : socket_(std::move(socket)) {}

    void read() {
        std::vector<uint64_t> tasks;
        std::vector<std::vector<uint64_t>> tasks_to_send;

        try {
            std::cout << "READING STUFF..." << '\n';
            boost::asio::read_until(socket_,
                                    boost::asio::dynamic_buffer(message_), "\n");

            std::cout << "SERVAK SAYS: " << message_ << '\n';

            std::string task;
            std::istringstream ss(message_);

            while (std::getline(ss, task, '!')) {
                std::string shard;
                std::istringstream sss(task);

                while (std::getline(sss, shard, ' ')) {
                    try {
                        tasks.push_back(std::stoi(shard));
                    } catch (const std::invalid_argument& e) {
                        break;
                    }
                }
            }

            for (size_t i = 0; i < CORES_SELF_i; i++) {
                std::vector<uint64_t> individual_task(tasks.end() - 3, tasks.end());
                tasks_to_send.push_back(individual_task);
                for (size_t p = 0; p < 3; p++) {
                    tasks.pop_back();
                }
            }

            manage_threads(tasks_to_send);
        } catch (std::exception& e) {
            std::cerr << e.what() << " IN TCP READ!" << '\n';
        }
    }
};

class TCP_Client {
   public:
    TCP_Client(boost::asio::io_context& io_context)
        : io_context_(io_context) {
        std::cout << "CLIENT CREATED" << '\n';
    }

    void connect(tcp::endpoint& endpoint) {
        // std::cout << "CONNECTING TO MASTER" << '\n';
        try {
            auto socket = std::make_shared<tcp::socket>(io_context_);
            socket->connect(endpoint);
            std::cout << "CONNECTED TO MASTER" << '\n';

            connection = TCP_Connection::create(std::move(*socket));

            if (first_connect) {
                send_cores();
            } else {
                send_result();
            }

        } catch (boost::system::system_error& ec) {
            std::cout << ec.what() << '\n';
            // connect(endpoint);
        }
    }

   private:
    boost::asio::io_context& io_context_;
    std::shared_ptr<TCP_Connection> connection;

    void send_cores() {
        if (!connection) {
            std::cerr << "CORES MESSAGE FAILED" << '\n';
        }
        connection->write_cores();
    }

    void send_result() {
        if (!connection) {
            std::cerr << "RESULT MESSAGE FAILED" << '\n';
        }
        connection->write_result();
    }
};

void manage_threads(std::vector<std::vector<uint64_t>>& tasks) {
    std::vector<std::thread> local_threads;
    for (size_t t = 0; t < 4; t++) {
        local_threads.emplace_back(calculate_partials, std::ref(tasks[t][0]),
                                   std::ref(tasks[t][1]), std::ref(tasks[t][2]));
    }

    for (auto& th : local_threads) {
        th.join();
    }

    boost::asio::io_context io_context;
    TCP_Client next(io_context);
    tcp::endpoint endpoint(boost::asio::ip::make_address(SERVER_IP), SERVER_PORT);
    next.connect(endpoint);
}

void calculate_partials(uint64_t start, uint64_t end, uint64_t R) {
    std::cout << "TASK STARTED WITH: " << start << ' ' << end << ' ' << R << '\n';
    // unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    // std::mt19937 generator(seed);
    uint64_t iter = 0;
    for (size_t i = start; i < end; i++) {
        iter++;
        uint64_t sumR = 0;
        for (size_t j = 0; j < R; j++) {
            sumR += i + j;
            // sumR += 1;
        }
        m.lock();
        res.push_back(sumR);
        m.unlock();
    }
    std::cout << "THREAD DONE" << '\n';
    std::cout << iter << '\n';
}

int main() {
    boost::asio::io_context io_context;
    TCP_Client client(io_context);
    tcp::endpoint endpoint(boost::asio::ip::make_address(SERVER_IP), SERVER_PORT);

    client.connect(endpoint);
}