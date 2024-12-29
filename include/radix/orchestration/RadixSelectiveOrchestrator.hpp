#pragma once

#include "radix/materialization/ContiniousMaterialization.hpp"
#include "radix/worker/process_radix_chunk_selectively.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024, size_t k = 32>
class RadixSelectiveOrchestrator {
    ContinuousMaterialization<T> materialization;
    RadixPageManager<T, partitions, page_size> page_manager;
    size_t num_threads;
    size_t num_tuples;

public:
    explicit RadixSelectiveOrchestrator(const size_t num_tuples, const size_t num_threads) : materialization(num_tuples, num_threads), page_manager(num_threads), num_threads(num_threads), num_tuples(num_tuples) {
    }

    void run() {
        materialization.materialize();
        const auto data = materialization.get_data();
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        auto current_index = 0;
        for (size_t i = 0; i < num_threads; ++i) {
            const size_t chunk_size = num_tuples / num_threads + (i < num_tuples % num_threads);
            const auto raw_pointer = data.get();
            threads.emplace_back([this, raw_pointer, current_index, chunk_size] {
                process_radix_chunk_selectively<T, partitions, k>(page_manager, raw_pointer + current_index, chunk_size);
            });
            current_index += chunk_size;
        }

        for (auto &thread: threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};