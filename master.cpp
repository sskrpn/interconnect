#include <iostream>
#include <cstdint>
#include <string>
#include <chrono>
#include <vector>
#include <thread>
#include <cmath>
#include <mutex>
#include <random>
#include <boost/asio.hpp>

#define SELF_ 0xFF
#define SELF_CORES 11

using boost::asio::ip::tcp;


std::mutex m;
std::vector<uint64_t> res;
std::vector<std::vector<uint64_t>> total_res;
uint64_t reserved;
std::vector<uint64_t> reserved_slave;

std::vector<uint64_t> comps = {8, 16, 12};
std::vector<std::string> ips = {"192.168.0.4", "192.168.0.5", "192.168.0.6"};
std::vector<std::string> ports = {"52524", "52525", "52525"};

class Timer
{
private:
	using Clock = std::chrono::steady_clock;
	using Second = std::chrono::duration<double, std::ratio<1> >;

	std::chrono::time_point<Clock> m_beg{ Clock::now() };

public:
	void reset()
	{
		m_beg = Clock::now();
	}

	double elapsed() const
	{
		return std::chrono::duration_cast<Second>(Clock::now() - m_beg).count();
	}
};

int calculate_partials(uint64_t start, uint64_t end, uint64_t R){
    std::cout << "TASK STARTED WITH: " << 
                start << ' ' << end << ' ' << R << '\n';
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
	std::mt19937 generator(seed);
    uint64_t iter = 0;
    for(size_t i = start; i < end; i++){
        iter++;
        uint64_t sumR = 0;
        uint64_t rnd;
        for(size_t j = 0; j < R; j++){
            rnd = static_cast<uint64_t>(generator() % 3 + 1);
            sumR += (rnd * rnd);
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

void recieve_result(boost::asio::io_context& io_context, uint64_t M){
    try {
        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), 52526));
        acceptor.set_option(tcp::acceptor::reuse_address(true));

        for(size_t c = 0; c < comps.size(); c++){
            
            tcp::socket socket(io_context);
            

            acceptor.accept(socket);

            std::cout << "CONNECTED TO SLAVE" << '\n';

            std::vector<uint64_t> slave_res(reserved_slave[c]);
            boost::system::error_code error;

            size_t n = boost::asio::read(socket, boost::asio::buffer(slave_res),
                                        boost::asio::transfer_exactly(reserved_slave[c] * 8), error);
            std::cout << "ACCEPTED " << slave_res.size() << '\n';
            total_res.push_back(slave_res);

            if (error == boost::asio::error::eof) break;
            else if (error) throw boost::system::system_error(error);

            socket.shutdown(tcp::socket::shutdown_both);
            socket.close();
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
            boost::asio::connect(socket, resolver.resolve(ip, ports[c]));

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
        uint64_t M;

        

        std::cout << "MATRIX DIMENSIONS: ";
        std::cin >> M;
        
        Timer t;
        
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
        for(auto& core : comps){
            reserved_slave.push_back(chunk_size * core);
            std::cout << chunk_size * core << '\n';
        }

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

        std::thread control([&M, &io_context, &chunk_size](){
                            distribute_tasks(comps, ips, M, io_context, chunk_size);
        });

        control.join();
        recieve_result(io_context, M);

        for(auto& t : master_threads){
            t.join();
        }

        // total_res.push_back(res);
        // for(auto& i : total_res){
        //     for(auto& j : i){
        //         std::cout << j << ' ';
        //     }
        // }

        uint64_t total_size = 0;
        total_res.push_back(res);
        for(auto& i : total_res){total_size += i.size();}
        std::cout << '\n' << total_size << '\n';

        double timestamp = t.elapsed();
        std::cout << timestamp << " seconds have passed from cin to cout result" << '\n';

    } catch (std::exception& e){
        std::cerr << "EXCEPTION: " << e.what() << '\n';
    }

    return 0;
}
