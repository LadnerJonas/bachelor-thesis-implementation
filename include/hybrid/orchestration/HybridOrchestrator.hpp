#pragma once

#include "hybrid/chunk-creation/ChunkCreator.hpp"
#include "hybrid/worker/request_and_process_chunk.hpp"
#include "slotted-page/page-manager/HybridPageManager.hpp"

#include <thread>
#include <vector>
#include <deque>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class HybridOrchestrator {
    HybridPageManager<T, partitions, page_size> page_manager;
    size_t num_tuples;
    size_t num_threads;

public:
    explicit HybridOrchestrator(const size_t num_tuples, const size_t num_threads)
        : page_manager(num_threads), num_tuples(num_tuples), num_threads(num_threads) {
    }

    void run() {
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        std::deque<BatchedTupleGenerator<T, 10 * 2048>> generators;
        for (size_t i = 0; i < num_threads; ++i) {
            const auto tuple_to_generate = num_tuples / num_threads + (i < num_tuples % num_threads);
            generators.emplace_back(tuple_to_generate);

            threads.emplace_back([this, i, &generators] {
                request_and_process_chunk<T, partitions>(page_manager, generators[i]);
            });
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