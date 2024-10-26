#pragma once

#include "radix/materialization/ContiniousMaterialization.hpp"
#include "radix/worker/process_radix_chunk_selectively.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"

template<typename T, size_t partitions, size_t k>
class RadixSelectiveOrchestrator {
    ContinuousMaterialization<T> materialization;
    RadixPageManager<T, partitions> page_manager;
    std::vector<std::thread> threads;
    size_t num_threads;
    size_t num_tuples;

public:
    explicit RadixSelectiveOrchestrator(size_t num_tuples, size_t num_threads) : materialization(num_tuples), page_manager(num_threads), num_threads(num_threads), num_tuples(num_tuples) {
    }

    void run() {
        // auto time_start = std::chrono::high_resolution_clock::now();
        materialization.materialize();
        // auto time_end = std::chrono::high_resolution_clock::now();
        //std::cout << "Materialization time: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << "ms" << std::endl;
        auto data = materialization.get_data();
        for (size_t i = 0; i < num_threads; ++i) {
            size_t chunk_size = num_tuples / num_threads;
            //handle remainder
            if (size_t remainder = num_tuples % num_threads; i < remainder) {
                ++chunk_size;
            }
            const auto raw_pointer = data.get();
            threads.emplace_back([this, raw_pointer, i, chunk_size]() {
                process_radix_chunk_selectively<T,partitions, k>(page_manager, raw_pointer + i * chunk_size, chunk_size);
            });
        }
        for (auto &thread: threads) {
            thread.join();
        }
    }

    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};