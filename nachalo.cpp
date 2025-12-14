#include <iostream>
#include <cmath>
#include <chrono>
#include <thread>
#include <vector>
#include <mutex>

using namespace std;


mutex mtx;
double pr_stlb = 1.0;

void calculatePartial(int start, int end, int M) {
    double local_product = 1.0;

    for (int i = start; i < end; i++) {
        double sum_str = 0;
        for (int j = 0; j < M; j++) {
            sum_str += i +1;
        }
        local_product *= sum_str;
    }

    lock_guard<mutex> lock(mtx);
    pr_stlb *= local_product;
}

int main() {
    setlocale(LC_ALL, "Russian");

    int M;
    int num_threads;

    cout << "Введите порядок матрицы (M): ";
    cin >> M;

    cout << "Введите количество потоков: ";
    cin >> num_threads;

    num_threads = min(num_threads, M);
    if (num_threads <= 0) num_threads = 1;

    auto start = chrono::high_resolution_clock::now();

    vector<thread> threads;
    int chunk_size = M / num_threads;
    int remainder = M % num_threads;
    int start_index = 0;

    for (int t = 0; t < num_threads; t++) {
        int end_index = start_index + chunk_size + (t < remainder ? 1 : 0);
        threads.emplace_back(calculatePartial, start_index, end_index, M);
        start_index = end_index;
    }

    for (auto& th : threads) {
        th.join();
    }

    auto end = chrono::high_resolution_clock::now();
    auto elapsed = chrono::duration_cast<chrono::milliseconds>(end - start);

    cout << "Время выполнения: " << elapsed.count() << " миллисекунд\n";
    cout << "Результат: " << pr_stlb << "\n";

    return 0;
}