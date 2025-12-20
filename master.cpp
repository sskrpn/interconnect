#include <iostream>
#include <cstdint>
#include <string>
#include <chrono>
#include <vector>
#include <thread>
#include <cmath>
#include <mutex>
#include <random>
#include <unordered_map>
#include <boost/asio.hpp>

#define SELF_ 0xFF
#define SELF_CORES 11
#define SERVER_PORT 52524
#define SLAVE_PORT 52525

using boost::asio::ip::tcp;

const uint64_t CHUNK_DIVISION = 10000;
const uint64_t EXPECTED_COMPS = 3;
uint64_t chunks_left = CHUNK_DIVISION;

std::mutex m;
std::vector<uint64_t> res;
std::vector<std::vector<uint64_t>> total_res;
uint64_t reserved;
std::unordered_map<std::string, uint64_t> reserved_slave;
std::unordered_map<std::string, uint64_t> comps;
std::vector<std::string> ips_previously_connected;

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

class TCP_Connection : public std::enable_shared_from_this<TCP_Connection>{
public:
    typedef std::shared_ptr<TCP_Connection> pointer;

    static pointer pointer_create(boost::asio::io_context& io_context){
        return pointer(new TCP_Connection(io_context));
    }

    void start(){
        std::string ip_connected = socket().remote_endpoint().address().to_string();

        auto it = std::find(ips_previously_connected.begin(),
                                    ips_previously_connected.end(),
                                        ip_connected);

        bool first = (it != ips_previously_connected.end());

        if(first){
            ips_previously_connected.push_back(ip_connected);
            read_cores(ip_connected);
        } else {
            read_result(ip_connected);
        }

        boost::asio::async_write(socket_, boost::asio::buffer(message_),
            std::bind(&TCP_Connection::handle_write, shared_from_this()));
    }

    void read_cores(std::string& ip){
        std::string cores_str;
        boost::asio::read_until(socket_, boost::asio::dynamic_buffer(cores_str), "\n");
        comps[ip] = static_cast<uint64_t>(stoi(cores_str));

        std::cout << "CORES READ" << '\n';
    }

    void read_result(std::string& ip){
        std::vector<uint64_t> slave_res(reserved_slave[ip]);
        boost::system::error_code error;

        size_t n = boost::asio::read(socket(), boost::asio::buffer(slave_res),
                                        boost::asio::transfer_exactly(reserved_slave[ip] * 8), error);
        std::cout << "ACCEPTED " << slave_res.size() << '\n';
        total_res.push_back(slave_res);

        if (error == boost::asio::error::eof){}
        else if (error) throw boost::system::system_error(error);
    }

    tcp::socket& socket(){ return socket_; }
private:
    tcp::socket socket_;
    std::string message_;

    TCP_Connection(boost::asio::io_context& io_context)
    : socket_(io_context){}

    void handle_write(){
        
    }
};

class TCP_Server{
public:
    TCP_Server(boost::asio::io_context& io_context)
    : io_context_(io_context),
      acceptor_(io_context, tcp::endpoint(tcp::v4(), SERVER_PORT)){

        start_accept();
      }

private:
      boost::asio::io_context& io_context_;
      tcp::acceptor acceptor_;

      void start_accept(){
        TCP_Connection::pointer new_connection = 
            TCP_Connection::pointer_create(io_context_);
        
        acceptor_.async_accept(new_connection->socket(),
            std::bind(&TCP_Server::handle_accept, this, new_connection,
                boost::asio::placeholders::error));
      }

      void handle_accept(TCP_Connection::pointer new_connection,
        const boost::system::error_code& ec){

        if(!ec){
            std::cout << "CONNECTED TO SLAVE" << '\n';
            new_connection->start();
        }

        start_accept();
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

int distribute(std::vector<uint64_t>& threads, std::vector<std::string>& ips, uint64_t& M,
                     boost::asio::io_context& io_context, uint64_t& chunk_size){
    try{
        uint64_t leap = 0;
        tcp::socket socket(io_context);
        tcp::resolver resolver(io_context);

        for(size_t c = 0; c < threads.size(); c++){
            std::vector<std::thread> slave_threads;
            std::string ip = ips[c];

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

    } catch (std::exception& e){
        std::cerr << "EXCEPTION FROM SLAVE: " << e.what() << '\n';
    }
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
        //----------DISTRIBUTION CONTROL-----
        uint64_t total_threads = SELF_CORES;
        for(size_t c = 0; c < comps.size(); c++){
            total_threads += comps[c];
        }

        uint64_t chunk_size = M / CHUNK_DIVISION;
        uint64_t remainder_ = M / CHUNK_DIVISION;

        boost::asio::io_context io_context;
        tcp::socket socket(io_context);
        auto socket_ptr = std::make_unique<tcp::socket>(socket);

        std::thread control([&M, &io_context, &chunk_size](){
                            distribute(comps, ips, M, io_context, chunk_size);
        });

        //----------DISTRIBUTION CONTROL-----

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
