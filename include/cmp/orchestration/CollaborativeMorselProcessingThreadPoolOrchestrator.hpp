#pragma once

#include "cmp/morsel-creation/CollaborativeMorselCreator.hpp"
#include "cmp/morsel-creation/CollaborativeMorselCreator2.hpp"
#include "cmp/thread-pool/CmpThreadPool.hpp"
#include "slotted-page/page-manager/OnDemandSingleThreadPageManager.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CollaborativeMorselProcessingThreadPoolOrchestrator {
    BatchedTupleGenerator<T> morsel_creator;
    OnDemandSingleThreadPageManager<T, partitions, page_size> page_manager;
    size_t num_threads;

public:
    explicit CollaborativeMorselProcessingThreadPoolOrchestrator(size_t num_tuples, size_t num_threads) : morsel_creator(num_tuples), page_manager(), num_threads(num_threads) {
    }

    void run() {
        CmpThreadPool<T, partitions, page_size> thread_pool(num_threads, page_manager);

        for (auto [batch, batch_size] = morsel_creator.getBatchOfTuples(); batch != nullptr; std::tie(batch, batch_size) = morsel_creator.getBatchOfTuples()) {
            thread_pool.dispatchTask(std::move(batch), batch_size);
        }
        thread_pool.stop();
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};