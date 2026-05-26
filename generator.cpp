#include <array>
#include <charconv>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>

namespace {
constexpr std::uint64_t DEFAULT_N = 30000;
constexpr int DIGITS_AFTER_DOT = 15;
constexpr int ELEMENT_WIDTH = 2 + DIGITS_AFTER_DOT; // "0." + digits
constexpr int BYTES_PER_ELEMENT = ELEMENT_WIDTH + 1; // trailing space

std::uint64_t parse_size(const char* text) {
    std::uint64_t value = 0;
    const char* end = text + std::char_traits<char>::length(text);
    auto [ptr, ec] = std::from_chars(text, end, value);
    if (ec != std::errc{} || ptr != end || value == 0) {
        throw std::invalid_argument("matrix size must be a positive integer");
    }
    return value;
}

void append_fixed_random(std::string& row, std::mt19937_64& rng) {
    static constexpr std::uint64_t SCALE = 1'000'000'000'000'000ULL;
    std::uniform_int_distribution<std::uint64_t> dist(0, SCALE - 1);

    std::uint64_t value = dist(rng);
    std::array<char, DIGITS_AFTER_DOT> digits{};

    for (int i = DIGITS_AFTER_DOT - 1; i >= 0; --i) {
        digits[static_cast<std::size_t>(i)] = static_cast<char>('0' + value % 10);
        value /= 10;
    }

    row.push_back('0');
    row.push_back('.');
    row.append(digits.data(), digits.size());
    row.push_back(' ');
}
} // namespace

int main(int argc, char* argv[]) {
    try {
        const std::uint64_t n = argc > 1 ? parse_size(argv[1]) : DEFAULT_N;
        const std::string path = argc > 2 ? argv[2] : "matrix_A.txt";

        std::ofstream out(path, std::ios::binary);
        if (!out) {
            std::cerr << "failed to open " << path << '\n';
            return 1;
        }

        std::cout << "generating " << n << "x" << n << " matrix at " << path << '\n';
        out<< n << ' ' << n << '\n';

        std::mt19937_64 rng(std::random_device{}());
        std::string row;
        row.reserve(static_cast<std::size_t>(n) * BYTES_PER_ELEMENT + 1);

        for (std::uint64_t i = 0; i < n; ++i) {
            row.clear();
            for (std::uint64_t j = 0; j < n; ++j) {
                append_fixed_random(row, rng);
            }
            row.push_back('\n');
            out.write(row.data(), static_cast<std::streamsize>(row.size()));

            if (!out) {
                std::cerr << "write failed, possibly out of disk space\n";
                return 1;
            }
        }

        std::cout << "generated.\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "generator error: " << e.what() << '\n';
        return 1;
    }
}
