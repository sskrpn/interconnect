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
#define SELF_CORES 4
#define SERVER_PORT 52524
#define SLAVE_PORT 52525

using boost::asio::ip::tcp;

enum TASK_STATE {
    WAITING,
    EXECUTING,      // BOTH ACCEPTING AND SENDING
    AWAIT_ONLY,     // ONLY ACCEPTING RESULTS
    DONE
};

const uint64_t CHUNK_DIVISION = 10;
uint64_t chunks_left = CHUNK_DIVISION;

uint64_t leap = 0;
uint64_t chunk_size = 0;
bool last_batch = false;
uint64_t reserved_last = 0;
TASK_STATE curr_state = WAITING;

std::mutex m;
uint64_t M;
std::vector<uint64_t> res;
std::vector<std::vector<uint64_t>> total_res;
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
        std::cout << ip_connected << '\n';

        switch(curr_state){
        case AWAIT_ONLY: {
            read_result(ip_connected);
            boost::asio::async_write(socket_, boost::asio::buffer(message_),
                                     std::bind(&TCP_Connection::handle_write, shared_from_this()));
            std::cout << "WROTE: " << message_ << '\n';
            break;
        }
        case EXECUTING: {
            auto it = std::find(ips_previously_connected.begin(),
                                ips_previously_connected.end(),
                                ip_connected);
            bool first = (it == ips_previously_connected.end());
            if (first) {
                ips_previously_connected.push_back(ip_connected);
                std::cout << "I SEE " << ip_connected << " THE FIRST TIME!" << '\n';
                read_cores(ip_connected);
            }
            else {
                std::cout << "I SEE " << ip_connected << " NOT THE FIRST TIME!" << '\n';
                read_result(ip_connected);
            }
            message_ += "\n";
            boost::asio::async_write(socket_, boost::asio::buffer(message_),
                                     std::bind(&TCP_Connection::handle_write, shared_from_this()));
            std::cout << "WROTE: " << message_ << '\n';
            break;
        }
        }
    }

    void write_end(){
        message_ = "end";
    }

    void read_cores(std::string& ip){
        std::string cores_str;
        boost::asio::read_until(socket_, boost::asio::dynamic_buffer(cores_str), "\n");
        comps[ip] = static_cast<uint64_t>(stoi(cores_str));
        cores_    = static_cast<uint64_t>(stoi(cores_str));

        std::cout << "CORES READ ";
        std::cout << comps[ip] << '\n';

        for(size_t th = 0; th < cores_; th++){
            uint64_t startR = (th + leap) * chunk_size;
            uint64_t endR   = (th + leap + 1) * chunk_size;
            message_ += std::to_string(startR) + ' ' + 
                        std::to_string(endR) + ' ' +
                        std::to_string(M) + '!';
        }
        leap += comps[ip];
        chunks_left -= comps[ip];
        std::cout << "CHUNKS LEFT: " << chunks_left << '\n';
    }

    void read_result(std::string& ip){
        uint64_t reserved = (last_batch ? reserved_last : 
                    M / CHUNK_DIVISION * comps[ip]);
        
        std::vector<uint64_t> slave_res(reserved);
        boost::system::error_code error;

        std::cout << "ACCEPTING " << reserved << '\n';
        size_t n = boost::asio::read(socket(), boost::asio::buffer(slave_res),
                                        boost::asio::transfer_exactly(reserved * 8), error);
        std::cout << "ACCEPTED " << slave_res.size() << '\n';
        total_res.push_back(slave_res);
        uint64_t curr_res = 0;
        for(auto& i : total_res){
            curr_res += i.size();
        }
        if (curr_res == M){
            curr_state = DONE;
            write_end();
            return;
        }

        if (last_batch){
            write_end();
            return;
        } else {
            uint64_t prev_startR = 0;
            if (chunks_left <= comps[ip]){
                std::cout << "LAST_BATCH FLAG SET" << '\n';
                prev_startR = (leap - comps[ip]) * chunk_size;
                chunk_size = chunks_left * chunk_size / comps[ip];
                reserved_last = chunk_size * comps[ip] + M % CHUNK_DIVISION;
                std::cout << "RESERVED LAST: " << reserved_last << '\n';
                last_batch = true;
                curr_state = AWAIT_ONLY;
            } else if (chunks_left == 0) {
                std::cout << "ENDING" << '\n';
                curr_state = AWAIT_ONLY;
                write_end();
                return;
            }
            std::cout << "GENERATING MESSAGE" << '\n';
            for(size_t th = 0; th < comps[ip]; th++){
                uint64_t startR = (th + leap) * chunk_size + prev_startR;
                uint64_t endR   = (th + leap + 1) * chunk_size + prev_startR;
                if (last_batch && th == comps[ip] - 1){
                    endR += M % CHUNK_DIVISION;
                }
                message_ += std::to_string(startR) + ' ' + 
                            std::to_string(endR) + ' ' +
                            std::to_string(M) + '!';
            }
            leap += comps[ip];
            std::cout << "LEAP: " << leap << '\n';
            chunks_left -= comps[ip];
            std::cout << "CHUNKS LEFT: " << chunks_left << '\n';
        }
    }

    tcp::socket& socket(){ return socket_; }
private:
    tcp::socket socket_;
    std::string message_;
    uint64_t    cores_;

    TCP_Connection(boost::asio::io_context& io_context)
    : socket_(io_context){}

    void handle_write(){
        uint64_t chunk_size = M / CHUNK_DIVISION;
        uint64_t remainder_ = M % CHUNK_DIVISION;
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
            if (curr_state == WAITING) {
                curr_state = EXECUTING;
            }
            switch (curr_state){
            case EXECUTING:{
                TCP_Connection::pointer new_connection = 
                    TCP_Connection::pointer_create(io_context_);

                boost::system::error_code ec;
                std::cout << "WAITING FOR CONNECTIONS..." << '\n';
                acceptor_.async_accept(new_connection->socket(),
                           std::bind(&TCP_Server::handle_accept, this, new_connection,
                                     ec));
                break;
            }
            case AWAIT_ONLY:{
                TCP_Connection::pointer new_connection = 
                    TCP_Connection::pointer_create(io_context_);

                boost::system::error_code ec;
                std::cout << "AWAITING FINAL CONNECTION..." << '\n';
                acceptor_.async_accept(new_connection->socket(),
                           std::bind(&TCP_Server::handle_accept, this, new_connection,
                                     ec));
                break;
            }
            case DONE:{
                std::cout << "ALL TASKS DONE!" << '\n';
                break;
            }
        }
}

void handle_accept(TCP_Connection::pointer new_connection,
              const boost::system::error_code &ec){

        if(!ec){
            std::cout << "CONNECTED TO SLAVE" << '\n';
            new_connection->start();
        }

        start_accept();
    }
};

// int calculate_partials(uint64_t start, uint64_t end, uint64_t R){
//     std::cout << "TASK STARTED WITH: " << 
//                 start << ' ' << end << ' ' << R << '\n';
//     unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
// 	std::mt19937 generator(seed);
//     uint64_t iter = 0;
//     for(size_t i = start; i < end; i++){
//         iter++;
//         uint64_t sumR = 0;
//         uint64_t rnd;
//         for(size_t j = 0; j < R; j++){
//             rnd = static_cast<uint64_t>(generator() % 3 + 1);
//             sumR += (rnd * rnd);
//         }
//         m.lock();
//         res.push_back(sumR);
//         m.unlock();

//     }
//     std::cout << "THREAD DONE" << '\n';
//     std::cout << iter << '\n';
//     return 0;
// }

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

void launch(){
    try {
        boost::asio::io_context io_context;
        TCP_Server server(io_context);
        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "EXCEPTION " << e.what() << '\n';
    }
}


int main() {
    std::cout << "MATRIX DIMENSIONS: ";
    std::cin >> M;
    chunk_size = M / CHUNK_DIVISION;

    std::thread control(launch);

    uint64_t total_threads = SELF_CORES;
    for(auto& [ip, cores] : comps){
        total_threads += cores;
    }
    control.join();

    for(auto& i : total_res){
        for(auto& j : i){
            std::cout << j << ' ';
        }
    }
}