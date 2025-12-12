#include <iostream>
#include <cstdint>
#include <string>
#include <vector>
#include <thread>
#include <cmath>
#include <mutex>
#include <random>
#include <boost/asio.hpp>

#define SELF_ 0xFF
#define SELF_CORES 5

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

int send_task(tcp::socket& socket, uint64_t start, uint64_t end, uint64_t M){
    try {
        std::string message = std::to_string(start) + ' ' + 
                            std::to_string(end) + ' ' +
                            std::to_string(M) + '!';
        
        boost::system::error_code error;
        boost::asio::write(socket, boost::asio::buffer(message), error);

        if(error) {
            std::cerr << "Error sending to SLAVE " << error.message() << '\n';
            return -1;
        }

        std::cout << "Task sent to slave: " << message << '\n';

    } catch (std::exception& e) {
        std::cerr << "EXCEPTION WITH SLAVE: " << e.what() << '\n';
    }

    return 0;
}

std::vector<uint64_t>  recieve_result(boost::asio::io_context& io_context, uint64_t M){
    try {

        while (true){
            tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), 52526));
            tcp::socket socket(io_context);

            acceptor.accept(socket);

            std::cout << "CONNECTED TO SLAVE" << '\n';

            std::vector<uint64_t> res1(reserved);
            boost::system::error_code error;

            size_t n = boost::asio::read(socket, boost::asio::buffer(res1),
                                        boost::asio::transfer_exactly(reserved * 8), error);
            

            if (error == boost::asio::error::eof) break;
            else if (error) throw boost::system::system_error(error);

        return res1;
        }

    } catch (std::exception& e) {
        std::cerr << e.what() << '\n';
    }
}

int distribute_tasks(std::vector<uint64_t>& threads, std::vector<std::string>& ips, uint64_t& M,
                     boost::asio::io_context& io_context, uint64_t& chunk_size){
    try{
        uint64_t leap = 0;

        for(size_t c = 0; c < threads.size(); c++){
            std::vector<std::thread> slave_threads;
            std::string ip = ips[c];

            tcp::socket socket(io_context);
            tcp::resolver resolver(io_context);

            std::cout << "Connecting to slave at " << ip << '\n';
            boost::asio::connect(socket, resolver.resolve(ip, std::to_string(52525)));

            uint64_t startR, endR;
            for(size_t t = 0; t < threads[c]; t++){
                uint64_t startR = (t + leap) * chunk_size;
                uint64_t endR   = (t + 1 + leap) * chunk_size ;
                send_task(socket, startR, endR, M);
                }

            leap += threads[c];
            socket.close();

        }
    } catch (std::exception& e) {
            std::cerr << "EXCEPTION FROM SLAVE: " << e.what() << '\n';
    }

    return 0;
}



int main() {
    try{
        std::vector<uint64_t> comps = {6};
        //std::vector<std::string> ips = {"192.168.0.2", "self_"};
        std::vector<std::string> ips = {"127.0.0.1"};
        uint64_t M;

        std::cout << "MATRIX DIMENSIONS: ";
        std::cin >> M;

        res.clear();

        //----------TASKS------------------

        uint64_t total_threads = SELF_CORES;
        for(size_t c = 0; c < comps.size(); c++){
            total_threads += comps[c];
        }

        uint64_t chunk_size = M / total_threads;
        uint64_t remainder_ = M % total_threads;
        std::cout << total_threads << ' ' << chunk_size << ' ' << remainder_ << '\n';

        std::vector<std::thread> master_threads;
        std::uint64_t leap = total_threads - SELF_CORES;

        reserved = M - (chunk_size * SELF_CORES + remainder_);
        std::cout << reserved << '\n';

        for(size_t t = 0; t < SELF_CORES; t++){
            uint64_t startR = (t + leap) * chunk_size;
            uint64_t endR   = (t == SELF_CORES - 1) ? 
                              (t + 1 + leap) * chunk_size + remainder_ :
                              (t + 1 + leap) * chunk_size;

            master_threads.emplace_back([startR, endR, M](){
                    calculate_partials(startR, endR, M);
                });
        }
        //----------TASKS-------------------

        boost::asio::io_context io_context;
        tcp::socket socket(io_context);
        tcp::resolver resolver(io_context);

        std::thread control([&comps, &ips, &M, &io_context, &chunk_size](){
                            distribute_tasks(comps, ips, M, io_context, chunk_size);
        });

        control.join();
        std::vector<uint64_t> res1 = recieve_result(io_context, M);

        for(auto& t : master_threads){
            t.join();
        }

        res.insert(res.end(), res1.begin(), res1.end());
        for(auto& i : res){
            std::cout << i << ' ';
        }
        std::cout << '\n' << res.size() << '\n';

    } catch (std::exception& e){
        std::cerr << "EXCEPTION: " << e.what() << '\n';
    }

    return 0;
}
