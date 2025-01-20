#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "slotted-page/page-manager/OnDemandSingleThreadPageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"

#include <atomic>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

bool hasMoreThan100GiBOfRAM() {
    constexpr uint64_t GB = 1024ULL * 1024 * 1024;
    constexpr uint64_t MEMORY_THRESHOLD = 100 * GB;
    std::ifstream meminfo("/proc/meminfo");
    if (!meminfo) {
        std::cerr << "Could not open /proc/meminfo" << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(meminfo, line)) {
        if (line.starts_with("MemTotal:")) {
            const char *start = line.c_str() + 9;// Skip "MemTotal:"
            while (*start == ' ') { ++start; }   // Skip leading spaces


            char *end;
            uint64_t mem_kb = std::strtoull(start, &end, 10);
            if (end > start) {
                return mem_kb * 1024 > MEMORY_THRESHOLD;
            }
        }
    }

    std::cerr << "Could not find MemTotal in /proc/meminfo" << std::endl;
    return false;
}


template<typename T, size_t partitions>
class OrchestratorWithSeparatePageManagers {
    std::atomic<bool> running = std::atomic(true);
    size_t written_tuples = 0;
    BatchedTupleGenerator<T> generator;
    OnDemandSingleThreadPageManager<T, partitions> page_manager{};
    unsigned num_threads;

public:
    explicit OrchestratorWithSeparatePageManagers(unsigned num_threads) : generator(SIZE_MAX), num_threads(num_threads) {
    }
    void run() {
        static constexpr auto total_buffer_size = 1024 * 1024;
        static constexpr auto max_tuples_in_buffer = total_buffer_size / sizeof(T);
        auto buffer_index = 0u;
        std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);

        auto partition = 0ull;
        while (running.load()) {
            auto [ptr, size_of_batch] = generator.getBatchOfTuples();
            if (ptr == nullptr) {
                break;
            }

            for (size_t i = 0; i < size_of_batch; i++) {
                const auto &tuple = ptr[i];
                auto &index = buffer_index;
                if (index == max_tuples_in_buffer) {
                    page_manager.insert_buffer_of_tuples_batched(buffer.get(), max_tuples_in_buffer, partition);
                    index = 0;
                    written_tuples += max_tuples_in_buffer;
                    partition = (partition + 1) & (partitions - 1);
                }
                buffer[index] = tuple;
                ++buffer_index;
            }
        }
    }

    void stop() {
        running.store(false);
    }

    [[nodiscard]] size_t get_written_tuples() const {
        return written_tuples;
    }
};

template<typename TupleType, size_t partitions>
void print_benchmark_info(const std::chrono::milliseconds time_to_write_out, unsigned threads, size_t written_tuples, bool synchronised) {
    std::cout << "Benchmarking " << (synchronised ? "(synchronised)" : "(not-synchronised)") << " using " << partitions << " Partitions and " << threads << " Thread(s): "
              << "written " << sizeof(TupleType) << "B tuples: " << std::fixed << std::setprecision(2) << written_tuples / 1e6 << " Mio"
              << " (tuple-data: " << static_cast<double>(sizeof(TupleType)) * written_tuples / (1024.0 * 1024.0 * 1024.0) << " GiB"
              << ", slotted-page-data: " << static_cast<double>(sizeof(SlotInfo<TupleType>) + TupleType::get_size_of_variable_data()) * written_tuples / (1024.0 * 1024.0 * 1024.0) << " GiB"
              << ", avg: "
              << static_cast<double>(written_tuples) / (threads * (time_to_write_out.count() / 1e3) * 1e6) << " Mio/(thread+sec))"
              << " within " << time_to_write_out.count() << " ms" << std::endl;
}

template<typename TupleType, size_t partitions>
void benchmark_non_synchronised_write_out(const std::chrono::milliseconds time_to_write_out) {
    for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
        std::vector<std::jthread> threads_vector;
        std::deque<OrchestratorWithSeparatePageManagers<TupleType, partitions>> orchestrators;
        threads_vector.reserve(threads);
        for (unsigned j = 0; j < threads; ++j) {
            orchestrators.emplace_back(threads);
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
        print_benchmark_info<TupleType, partitions>(time_to_write_out, threads, written_tuples, false);
        if (threads == 8 && std::thread::hardware_concurrency() >= 20) {
            threads = 5;
        }
    }
    std::cout << std::endl;
}

template<typename T, size_t partitions>
class OrchestratorSinglePageManager {
    std::atomic<bool> running = std::atomic(true);
    size_t written_tuples = 0;
    BatchedTupleGenerator<T> generator;
    OnDemandPageManager<T, partitions> &page_manager{};
    unsigned num_threads;

public:
    explicit OrchestratorSinglePageManager(OnDemandPageManager<T, partitions> &page_manager, unsigned num_threads) : generator(SIZE_MAX), page_manager(page_manager), num_threads(num_threads) {
    }
    void run() {
        static constexpr auto total_buffer_size = 1024 * 1024;
        static constexpr auto max_tuples_in_buffer = total_buffer_size / sizeof(T);
        auto buffer_index = 0u;
        std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);

        auto partition = 0ull;
        while (running.load()) {
            auto [ptr, size_of_batch] = generator.getBatchOfTuples();
            if (ptr == nullptr) {
                break;
            }

            for (size_t i = 0; i < size_of_batch; i++) {
                const auto &tuple = ptr[i];
                auto &index = buffer_index;
                if (index == max_tuples_in_buffer) {
                    page_manager.insert_buffer_of_tuples_batched(buffer.get(), max_tuples_in_buffer, partition);
                    index = 0;
                    written_tuples += max_tuples_in_buffer;
                    partition = (partition + 1) & (partitions - 1);
                }
                buffer[index] = tuple;
                ++buffer_index;
            }
        }
    }

    void stop() {
        running.store(false);
    }

    [[nodiscard]] size_t get_written_tuples() const {
        return written_tuples;
    }
};

template<typename TupleType, size_t partitions>
void benchmark_synchronised_write_out(const std::chrono::milliseconds time_to_write_out) {
    for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
        std::vector<std::jthread> threads_vector;
        OnDemandPageManager<TupleType, partitions> page_manager{};
        std::deque<OrchestratorSinglePageManager<TupleType, partitions>> orchestrators;
        threads_vector.reserve(threads);
        for (unsigned j = 0; j < threads; ++j) {
            orchestrators.emplace_back(page_manager, threads);
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
        print_benchmark_info<TupleType, partitions>(time_to_write_out, threads, written_tuples, true);
        if (threads == 8 && std::thread::hardware_concurrency() >= 20) {
            threads = 5;
        }
    }
    std::cout << std::endl;
}


int main() {
    constexpr auto time_to_write_out = std::chrono::milliseconds(1000);

    benchmark_non_synchronised_write_out<Tuple4, 32>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple16, 32>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple100, 32>(time_to_write_out);

    if (hasMoreThan100GiBOfRAM()) {
        benchmark_non_synchronised_write_out<Tuple4, 1024>(time_to_write_out);
        benchmark_non_synchronised_write_out<Tuple16, 1024>(time_to_write_out);
        benchmark_non_synchronised_write_out<Tuple100, 1024>(time_to_write_out);
    }

    benchmark_synchronised_write_out<Tuple4, 32>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple16, 32>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple100, 32>(time_to_write_out);

    if (hasMoreThan100GiBOfRAM()) {
        benchmark_synchronised_write_out<Tuple4, 1024>(time_to_write_out);
        benchmark_synchronised_write_out<Tuple16, 1024>(time_to_write_out);
        benchmark_synchronised_write_out<Tuple100, 1024>(time_to_write_out);
    }
}