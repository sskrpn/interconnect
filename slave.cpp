#include <boost/asio.hpp>
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <random>
#include <mutex>

using boost::asio::ip::tcp;

const uint64_t SERVER_PORT = 52524;
const std::string CORES_SELF = "12\n";

std::mutex m;
std::vector<uint64_t> res;
bool first_connect = true;

int manage_threads(std::vector<std::vector<uint64_t>>& tasks);
void send_results();

class TCP_Connection : public std::enable_shared_from_this<TCP_Connection>{
public:
    static std::shared_ptr<TCP_Connection> create(tcp::socket &&socket){
        std::cout << "CONNECTION CREATED" << '\n';
        return std::shared_ptr<TCP_Connection>(new TCP_Connection(std::move(socket)));
    }

    tcp::socket& socket(){ return socket_; }

    void write_cores(){
        boost::system::error_code ec;
        boost::asio::write(socket_, boost::asio::buffer(CORES_SELF), ec);
        std::cout << "CORES SENT" << '\n';
        first_connect = false;
        read();
    }

    void write_result(){
        boost::system::error_code ec;
        boost::asio::write(socket_, boost::asio::buffer(res), ec);
        std::cout << res.size() << '\n';
        std::cout << "WRITE DONE" << '\n';
        read();
    }

    void read(){
        std::string data;
        std::vector<uint64_t> tasks;
        std::vector<std::vector<uint64_t>> tasks_to_send;

        boost::system::error_code error;

        boost::asio::read_until(socket_, 
            boost::asio::dynamic_buffer(data), "\n");

        std::cout << "SERVAK SAYS: " << data << '\n';

        std::string task;
        std::istringstream ss(data);

        while (std::getline(ss, task, '!')) {
            std::string shard;
            std::istringstream sss(task);

            while (std::getline(sss, shard, ' ')) {
                try{
                    tasks.push_back(std::stoi(shard));
                } catch (const std::invalid_argument& e) {
                    break;
                }
            }
        }
    
        for (size_t i = 0; i < 12; i++) {
            std::vector<uint64_t> individual_task(tasks.end() - 3, tasks.end());
            tasks_to_send.push_back(individual_task);
            for (size_t p = 0; p < 3; p++) {
                tasks.pop_back();
            }
        }
        // for(auto& i : tasks_to_send){
        //     for(auto& j : i){
        //         std::cout << j << '\n';
        //     }
        // }

        manage_threads(tasks_to_send);
        send_results();
    }

private:
    tcp::socket socket_;
    std::string message_;

    TCP_Connection(tcp::socket &&socket)
    : socket_(std::move(socket)){}
};

class TCP_Client{
public:
    TCP_Client(boost::asio::io_context& io_context)
    : io_context_(io_context){
        std::cout << "CLIENT CREATED" << '\n';
    }

    void connect(tcp::endpoint& endpoint){
        std::cout << "TRYING TO CONNECT..." << '\n';
        auto socket = std::make_shared<tcp::socket>(io_context_);
        socket->connect(endpoint);
        connection = TCP_Connection::create(std::move(*socket));
        if(first_connect){
            send_cores();
        } else {
            send_result();
        }
        
    }

    void send_cores(){
        if(!connection){
            std::cerr << "NO CONNECTION (cores)" << '\n';
        }
        connection->write_cores();
    }

    void send_result(){
        if(!connection){
            std::cerr << "NO CONNECTION (results)" << '\n';
        }
        connection->write_result();
    }

private:
      boost::asio::io_context& io_context_;
      std::shared_ptr<TCP_Connection> connection;

      
      
};

int calculate_partials(uint64_t start, uint64_t end, uint64_t R) {
    std::cout << "TASK STARTED WITH: " <<
        start << ' ' << end << ' ' << R << '\n';
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937 generator(seed);
    uint64_t iter = 0;
    for (size_t i = start; i < end; i++) {
        iter++;
        uint64_t sumR = 0;
        for (size_t j = 0; j < R; j++) {
            sumR += i + 1;
        }
        m.lock();
        res.push_back(sumR);
        m.unlock();

    }
    std::cout << "THREAD DONE" << '\n';
    std::cout << iter << '\n';
    return 0;
}

int manage_threads(std::vector<std::vector<uint64_t>>& tasks) {
    std::vector<std::thread> threads;
    for (size_t t = 0; t < 12; t++) {
        threads.emplace_back(calculate_partials, std::ref(tasks[t][0]),
            std::ref(tasks[t][1]), std::ref(tasks[t][2]));
    }

    for (auto& th : threads) {
        th.join();
    }

    return 0;
}

void send_results(){
    boost::asio::io_context io_context;
    TCP_Client client(io_context);
    const std::string master_ip = "127.0.0.1";
    tcp::endpoint endpoint(boost::asio::ip::make_address(master_ip), 52524);

    client.connect(endpoint);
}

int main(){
    boost::asio::io_context io_context;
    TCP_Client first(io_context);
    const std::string master_ip = "127.0.0.1";
    tcp::endpoint endpoint(boost::asio::ip::make_address(master_ip), 52524);

    first.connect(endpoint);

    return 0;
}