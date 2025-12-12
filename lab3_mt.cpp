#include <iostream>
#include <cmath>
#include <chrono>
#include <thread>
#include <vector>
#include <mutex>

void calculatePartial(int start, int end, int M) {
    double local_product = 1.0;
    
    for (int i = start; i < end; i++) {
        double sum_str = 0;
        for (int j = 0; j < M; j++) {
            sum_str += sqrt(9 * double(i + 1) / 100);
        }
        local_product *= sum_str;
    }
    
    // Блокируем доступ к общей переменной
    lock_guard<mutex> lock(mtx);
    pr_stlb *= local_product;
}

int main() {
    uint8_t num_threads;
    uint64_t = M;

    num_threads = call_threads();

    cout << "Введите порядок матрицы (M): ";
    cin >> M;

    vector<thread> threads;
    int chunk_size = M / num_threads;
    int remainder = M % num_threads;
    int start_index = 0;

    for (int t = 0; t < num_threads; t++) {
        int end_index = start_index + chunk_size + (t < remainder ? 1 : 0);
        threads.emplace_back(calculatePartial, start_index, end_index, M);
        start_index = end_index;
    }

    // Ожидаем завершения всех потоков
    for (auto& th : threads) {
        th.join();
    }

    auto end = chrono::high_resolution_clock::now();
    auto elapsed = chrono::duration_cast<chrono::milliseconds>(end - start);

    cout << "Время выполнения: " << elapsed.count() << " миллисекунд\n";
    cout << "Результат: " << pr_stlb << "\n";

    return 0;
}