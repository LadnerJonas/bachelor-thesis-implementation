#pragma once

#include "hybrid/chunk-creation/ChunkCreator.hpp"
#include "hybrid/worker/request_and_process_chunk.hpp"
#include "slotted-page/page-manager/HybridPageManager.hpp"
#include <thread>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class HybridOrchestrator {
    ChunkCreator<T> chunk_creator;
    HybridPageManager<T, partitions, page_size> page_manager;
    std::vector<std::thread> threads;
    size_t num_threads;
    size_t num_tuples;

public:
    explicit HybridOrchestrator(size_t num_tuples, size_t num_threads)
        : chunk_creator(num_tuples), page_manager(num_threads, num_tuples), num_threads(num_threads), num_tuples(num_tuples) {
    }

    void run() {
        for (size_t i = 0; i < num_threads; ++i) {
            size_t chunk_size = (num_tuples + num_threads - 1) / num_threads;
            //handle remainder
            if (const size_t remainder = num_tuples % num_threads; i < remainder) {
                ++chunk_size;
            }
            threads.emplace_back([this, chunk_size] {
                request_and_process_chunk<T, partitions>(page_manager, chunk_creator, chunk_size);
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