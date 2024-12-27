#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"

#include <atomic>
#include <deque>
#include <iomanip>
#include <thread>
#include <vector>

template<typename T>
class Orchestrator {
    std::vector<std::unique_ptr<T[]>> write_out_locations{};
    std::atomic<bool> running = std::atomic(true);
    size_t written_tuples = 0;
    size_t index_within_block = 0;
    BatchedTupleGenerator<T> generator;
    const size_t block_size;

public:
    explicit Orchestrator(const size_t block_size) : generator(SIZE_MAX), block_size(block_size) {
        write_out_locations.emplace_back(std::make_unique<T[]>(block_size));
    }
    void run() {
        while (running) {
            auto [ptr, size_of_batch] = generator.getBatchOfTuples();
            if (ptr == nullptr) {
                break;
            }
            for (size_t i = 0; i < size_of_batch && running; i++) {
                write_out_locations.back()[index_within_block++] = ptr[i];
                if (index_within_block == block_size) {
                    write_out_locations.emplace_back(std::make_unique<T[]>(block_size));
                    index_within_block = 0;
                }
                written_tuples++;
            }
        }
    }

    void stop() {
        running = false;
    }

    [[nodiscard]] size_t get_written_tuples() const {
        return written_tuples;
    }
};

template<typename TupleType>
void benchmark_write_out(const std::chrono::milliseconds time_to_write_out, const size_t block_size) {
    for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
        std::vector<std::jthread> threads_vector;
        std::deque<Orchestrator<TupleType>> orchestrators;
        threads_vector.reserve(threads);
        for (unsigned j = 0; j < threads; ++j) {
            orchestrators.emplace_back(block_size);
        }
        for (unsigned j = 0; j < threads; ++j) {
            threads_vector.emplace_back([&orchestrator = orchestrators[j]] {
                orchestrator.run();
            });
        }

        std::this_thread::sleep_for(time_to_write_out);
        for (auto &orchestrator: orchestrators) {
            orchestrator.stop();
        }

        for (auto &thread: threads_vector) {
            thread.join();
        }

        size_t written_tuples = 0;
        for (auto &orchestrator: orchestrators) {
            written_tuples += orchestrator.get_written_tuples();
        }
        std::cout << "Using " << threads << " Threads: "
                  << "written " << sizeof(TupleType) << "B tuples: " << std::fixed << std::setprecision(2) << written_tuples / 1e6 << " Mio"
                  << " (" << static_cast<double>(sizeof(TupleType)) * written_tuples / (1024.0 * 1024.0 * 1024.0) << " GiB"
                                                                                                                     ", avg: "
                  << static_cast<double>(written_tuples) / (threads * 1e6) << " Mio/thread)"
                  << " within " << time_to_write_out.count() << " ms\n";
        if (threads == 8 && std::thread::hardware_concurrency() >= 20) {
            threads = 5;
        }
    }
    std::cout << std::endl;
}
int main() {
    constexpr auto time_to_write_out = std::chrono::milliseconds(1000);
    constexpr size_t block_size = 500 * 1024 * 1024;

    benchmark_write_out<Tuple4>(time_to_write_out, block_size / 4);
    benchmark_write_out<Tuple16>(time_to_write_out, block_size / 16);
    benchmark_write_out<Tuple100>(time_to_write_out, block_size / 100);
}