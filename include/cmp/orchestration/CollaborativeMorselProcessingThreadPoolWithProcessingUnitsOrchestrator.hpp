#pragma once

#include "cmp/thread-pool/CmpThreadPoolWithProcessingUnits.hpp"
#include "common/morsel-creation/MorselCreator.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CollaborativeMorselProcessingThreadPoolWithProcessingUnitsOrchestrator {
    BatchedTupleGenerator<T, 10 * 2048> tuple_creator;
    LockFreePageManager<T, partitions, page_size> page_manager;
    size_t num_threads;

public:
    explicit CollaborativeMorselProcessingThreadPoolWithProcessingUnitsOrchestrator(size_t num_tuples, size_t num_threads) : tuple_creator(num_tuples), page_manager(), num_threads(num_threads) {
    }

    void run() {
        const unsigned worker_threads = std::max(num_threads - 1, 1ul);
        auto numProcessingUnits = 1u;
        if (worker_threads >= 9) {
            numProcessingUnits = 3;
        } else if (worker_threads >= 4) {
            numProcessingUnits = 2;
        }

        CmpThreadPoolWithProcessingUnits<T, partitions, page_size> thread_pool(numProcessingUnits, worker_threads, page_manager);

        constexpr auto fetch_threads = 1;
        std::vector<std::jthread> threads;
        threads.reserve(fetch_threads);
        for (int i = 0; i < fetch_threads; i++) {
            threads.emplace_back([this, &thread_pool, numProcessingUnits] {
                int current_processing_unit = 0;
                for (auto [batch, batch_size] = tuple_creator.getBatchOfTuples(); batch != nullptr; std::tie(batch, batch_size) = tuple_creator.getBatchOfTuples()) {
                    thread_pool.dispatchTask(current_processing_unit, std::move(batch), batch_size);
                    current_processing_unit = (current_processing_unit + 1) % numProcessingUnits;
                }

                for (unsigned pu = 0; pu < numProcessingUnits; pu++) {
                    thread_pool.stop(pu);
                }
            });
        }
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};