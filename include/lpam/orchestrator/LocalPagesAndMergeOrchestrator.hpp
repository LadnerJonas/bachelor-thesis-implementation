#pragma once

#include "lpam/worker/process_morsel_lpam.hpp"
#include "slotted-page/page-manager/LocalPagesAndMergePageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"

#include <thread>
#include <vector>
#include <deque>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class LocalPagesAndMergeOrchestrator {
    LocalPagesAndMergePageManager<T, partitions, page_size> page_manager;
    size_t num_tuples;
    size_t num_threads;

public:
    explicit LocalPagesAndMergeOrchestrator(const size_t num_tuples, const size_t num_threads) : page_manager(num_threads), num_tuples(num_tuples), num_threads(num_threads) {
    }

    void run() {
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        std::deque<BatchedTupleGenerator<T>> generators;
        for (unsigned i = 0; i < num_threads; i++) {
            const auto tuple_to_generate = num_tuples / num_threads + (i < num_tuples % num_threads);
            generators.emplace_back(tuple_to_generate);
            threads.emplace_back([this, i, &generators] {
                process_morsel_lpam<T, partitions, page_size>(generators[i], page_manager);
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