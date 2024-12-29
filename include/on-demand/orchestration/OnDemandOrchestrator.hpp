#pragma once

#include <thread>
#include <vector>

#include "on-demand/worker/process_morsel.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include <deque>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class OnDemandOrchestrator {
    OnDemandPageManager<T, partitions, page_size> page_manager;
    size_t num_tuples;
    size_t num_threads;

public:
    explicit OnDemandOrchestrator(size_t num_tuples, size_t num_threads) : num_tuples(num_tuples), num_threads(num_threads) {
    }

    void run() {
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        std::deque<BatchedTupleGenerator<T>> generators;
        for (unsigned i = 0; i < num_threads; i++) {
            const auto tuple_to_generate = num_tuples / num_threads + (i < num_tuples % num_threads);
            generators.emplace_back(tuple_to_generate);

            threads.emplace_back([this, i, &generators] {
                process_morsel<T, partitions, page_size>(generators[i], page_manager);
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