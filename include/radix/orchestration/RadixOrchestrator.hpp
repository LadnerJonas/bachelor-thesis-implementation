#pragma once

#include "radix/materialization/ContiniousMaterialization.hpp"
#include "radix/worker/process_radix_chunk.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class RadixOrchestrator {
    ContinuousMaterialization<T> materialization;
    RadixPageManager<T, partitions, page_size> page_manager;
    size_t num_threads;
    size_t num_tuples;

public:
    explicit RadixOrchestrator(size_t num_tuples, size_t num_threads) : materialization(num_tuples), page_manager(num_threads), num_threads(num_threads), num_tuples(num_tuples) {
    }

    void run() {
        materialization.materialize();
        const auto data = materialization.get_data();
        std::vector<std::jthread> threads;
        threads.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            const size_t chunk_size = (num_tuples + num_threads - 1) / num_threads;
            const auto raw_pointer = data.get();
            threads.emplace_back([this, raw_pointer, i, chunk_size]() {
                process_radix_chunk<T, partitions>(page_manager, raw_pointer + i * chunk_size, chunk_size);
            });
        }
    }

    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};