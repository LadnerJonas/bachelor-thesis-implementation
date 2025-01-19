#pragma once

#include "hybrid/chunk-creation/ChunkCreator.hpp"
#include "hybrid/worker/request_and_process_chunk.hpp"
#include "slotted-page/page-manager/HybridPageManager.hpp"

#include <deque>
#include <thread>
#include <vector>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class HybridOrchestrator {
    HybridPageManager<T, partitions, page_size> page_manager;
    size_t num_tuples;
    size_t num_threads;

public:
    explicit HybridOrchestrator(const size_t num_tuples, const size_t num_threads)
        : page_manager(), num_tuples(num_tuples), num_threads(num_threads) {
    }

    void run() {
        std::deque<BatchedTupleGenerator<T, 10 * 2048>> generators;
        std::vector<std::jthread> threads;

        for (size_t i = 0; i < num_threads; ++i) {
            const auto tuple_to_generate = num_tuples / num_threads + (i < num_tuples % num_threads);
            generators.emplace_back(tuple_to_generate);
            auto &generator = generators.back();

            threads.emplace_back([this, &generator] {
                request_and_process_chunk<T, partitions>(page_manager, generator, num_threads);
            });
        }
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};