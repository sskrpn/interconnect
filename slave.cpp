#include <iostream>
#include <vector>
#include <cmath>
#include <thread>
#include <sstream>
#include <mutex>
#include <random>
#include <boost/asio.hpp>

#define CORES_SLAVE 6

using boost::asio::ip::tcp;

std::mutex m;
std::vector<uint64_t> res;
uint64_t reserved;

int calculate_partials(uint64_t start, uint64_t end, uint64_t R){
    std::cout << "TASK STARTED WITH: " << 
                start << ' ' << end << ' ' << R << '\n';
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
	std::mt19937 generator(seed);
    uint64_t iter = 0;
    for(size_t i = start; i < end; i++){
        iter++;
        uint64_t sumR = 0;
        for(size_t j = 0; j < R; j++){
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

int manage_threads(std::vector<std::vector<uint64_t>>& tasks){
    std::vector<std::thread> threads;
    for(size_t t = 0; t < CORES_SLAVE; t++){
        threads.emplace_back(calculate_partials, std::ref(tasks[t][0]), 
                            std::ref(tasks[t][1]), std::ref(tasks[t][2]));
    }

    for (auto& th : threads) {
		th.join();
    }

    return 0;
}

int recieve_packet(tcp::socket socket){
    try {
        uint8_t counter = 0;
        char data[1024];

        std::vector<uint64_t> tasks;
        std::vector<std::vector<uint64_t>> tasks_to_send;

        while (true){
            boost::system::error_code error;

            std::memset(data, 0, sizeof(data));
            size_t length = socket.read_some(boost::asio::buffer(data), error);
            if (error == boost::asio::error::eof) break;
            if (error) throw boost::system::system_error(error);

            std::cout << "SERVAK SAYS: " << data << '\n';

            std::string task;
            std::stringstream ss;
            ss << data;

            while(getline(ss, task, '!')){
                std::string shard;
                std::stringstream sss(task);

                while (getline(sss, shard, ' ')){
                    tasks.push_back(std::stoi(shard));
                }
            }

            counter++;
            if(counter == CORES_SLAVE){ break; }
        }

        for (size_t i = 0; i < CORES_SLAVE; i++){
            std::vector<uint64_t> individual_task(tasks.end() - 3, tasks.end());
            tasks_to_send.push_back(individual_task);
            for(size_t p = 0; p < 3; p++){
                tasks.pop_back();
            }
        }
        
        manage_threads(tasks_to_send);

    } catch (std::exception& e) {
        std::cerr << e.what() << '\n';
    }

    return 0;
}

int send_result(boost::asio::io_context& io_context, std::string& master_ip){
    try {
    tcp::socket socket(io_context);
    tcp::resolver resolver(io_context);
    tcp::endpoint endpoint(boost::asio::ip::make_address("127.0.0.1"), 52526);

    //std::cout << "CONNECTING BACK TO MASTER to " << master_ip << '\n';
    //boost::asio::connect(socket, resolver.resolve(master_ip, std::to_string(52526)));

    socket.connect(endpoint);
    std::cout << "MASTER CONNECTED" << '\n';

    boost::system::error_code error;
    boost::asio::write(socket, boost::asio::buffer(res), error);
    std::cout << "WRITE DONE" << '\n';
    } catch (std::exception& e) {
        //std::cerr << e.what() << '\n';
        send_result(io_context, master_ip);
    }

    return 0;
}

int main(){
    boost::asio::io_context io_context;
    tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), 52525));
    tcp::socket socket(io_context);
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
        work_guard = boost::asio::make_work_guard(io_context);
        
    std::cout << "WAITING FOR INCOMING CONNECTION..." << '\n';

    acceptor.accept(socket);

    std::cout << "CONNECTED TO SERVAK" << '\n';

    recieve_packet(std::move(socket));
    //std::string master_ip = "192.168.0.3";
    std::string master_ip = "127.0.0.1";
    boost::asio::io_context io_context_send;
    send_result(io_context_send, master_ip);

    return 0;
}
