#pragma once

#include <boost/asio.hpp>

#include <cstdint>
#include <stdexcept>
#include <vector>

namespace wire {

enum class MessageType : uint8_t {
    HELLO = 1,
    TASK = 2,
    RESULT = 3,
    STOP = 4,
};

struct HelloMessage {
    uint64_t threads = 0;
};

struct TaskMessage {
    uint64_t tile_id = 0;
    uint64_t row_start = 0;
    uint64_t row_count = 0;
    uint64_t col_start = 0;
    uint64_t col_count = 0;
    uint64_t k_start = 0;
    uint64_t k_count = 0;
    uint64_t step_index = 0;
    uint64_t total_steps = 0;
    uint8_t tile_begin = 0;
    uint8_t tile_end = 0;
    uint8_t reserved[6]{};
};

struct ResultMessage {
    uint64_t tile_id = 0;
    uint64_t row_count = 0;
    uint64_t col_count = 0;
    uint8_t tile_end = 0;
    uint8_t reserved[7]{};
};

inline void write_all(boost::asio::ip::tcp::socket& socket, const void* data, size_t bytes) {
    boost::asio::write(socket, boost::asio::buffer(data, bytes), boost::asio::transfer_exactly(bytes));
}

inline void read_all(boost::asio::ip::tcp::socket& socket, void* data, size_t bytes) {
    boost::asio::read(socket, boost::asio::buffer(data, bytes), boost::asio::transfer_exactly(bytes));
}

// ==================SLOP===================
// я так понимаю, что, если мы будем отправлять бустом не POD и что-то сломается, то я это не задебажу
template <typename T>
inline void write_pod(boost::asio::ip::tcp::socket& socket, const T& value) {
    static_assert(std::is_trivially_copyable<T>::value, "T must be POD-like");
    write_all(socket, &value, sizeof(T));
}

template <typename T>
inline void read_pod(boost::asio::ip::tcp::socket& socket, T& value) {
    static_assert(std::is_trivially_copyable<T>::value, "T must be POD-like");
    read_all(socket, &value, sizeof(T));
}

inline void write_type(boost::asio::ip::tcp::socket& socket, MessageType type) {
    const uint8_t raw = static_cast<uint8_t>(type);
    write_pod(socket, raw);
}

inline MessageType read_type(boost::asio::ip::tcp::socket& socket) {
    uint8_t raw = 0;
    read_pod(socket, raw);
    return static_cast<MessageType>(raw);
}
// =========================================

inline void write_doubles(boost::asio::ip::tcp::socket& socket, const std::vector<double>& values) {
    if (values.empty()) {
        return;
    }
    write_all(socket, values.data(), values.size() * sizeof(double));
}

inline void read_doubles(boost::asio::ip::tcp::socket& socket, std::vector<double>& values, size_t count) {
    values.assign(count, 0.0);
    if (count == 0) {
        return;
    }
    read_all(socket, values.data(), count * sizeof(double));
}

}  // namespace wire
