#pragma once

#include "cmp/thread-pool/CmpThreadPool.hpp"
#include "slotted-page/page-manager/OnDemandSingleThreadPageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CollaborativeMorselProcessingThreadPoolOrchestrator {
    BatchedTupleGenerator<T, 10 * 2048> tuple_generator;
    OnDemandSingleThreadPageManager<T, partitions, page_size> page_manager;
    size_t num_threads;

public:
    explicit CollaborativeMorselProcessingThreadPoolOrchestrator(size_t num_tuples, size_t num_threads) : tuple_generator(num_tuples), page_manager(), num_threads(num_threads) {
    }

    void run() {
        num_threads = std::min(std::max(num_threads - 1ul, 1ul), partitions);
        CmpThreadPool<T, partitions, page_size> thread_pool(num_threads, page_manager);
        {
            constexpr auto fetch_threads = 1;
            std::vector<std::jthread> threads;
            threads.reserve(fetch_threads);
            for (int i = 0; i < fetch_threads; i++) {
                threads.emplace_back([this, &thread_pool] {
                    for (auto [batch, batch_size] = tuple_generator.getBatchOfTuples(); batch != nullptr; std::tie(batch, batch_size) = tuple_generator.getBatchOfTuples()) {
                        thread_pool.dispatchTask(std::move(batch), batch_size);
                    }
                });
            }
        }
        thread_pool.stop();
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};