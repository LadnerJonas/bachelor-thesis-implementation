#pragma once

#include "cmp/thread-pool/CmpThreadPoolWithProcessingUnits.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CollaborativeMorselProcessingThreadPoolWithProcessingUnitsOrchestrator {
    LockFreePageManager<T, partitions, page_size> page_manager;
    size_t num_tuples;
    size_t num_threads;

public:
    explicit CollaborativeMorselProcessingThreadPoolWithProcessingUnitsOrchestrator(const size_t num_tuples, const size_t num_threads) : page_manager(), num_tuples(num_tuples), num_threads(num_threads) {
    }

    void run() {
        const unsigned generator_thread_count = std::max(num_threads / 4, 1ul);
        const unsigned partition_thread_count = std::max(num_threads - generator_thread_count, 1ul);
        auto numProcessingUnits = 1u;
        if (partition_thread_count >= 8) {
            numProcessingUnits = 3;
        } else if (partition_thread_count >= 4) {
            numProcessingUnits = 2;
        }

        CmpThreadPoolWithProcessingUnits<T, partitions, page_size> thread_pool(numProcessingUnits, partition_thread_count, page_manager);
        {
            std::vector<std::jthread> generator_threads;
            generator_threads.reserve(generator_thread_count);
            for (unsigned i = 0; i < generator_thread_count; i++) {
                generator_threads.emplace_back([&, i] {
                    BatchedTupleGenerator<T, 10 * 2048> tuple_creator(num_tuples / generator_thread_count + (num_tuples % generator_thread_count > i ? 1 : 0));
                    unsigned current_processing_unit = i;
                    for (auto [batch, batch_size] = tuple_creator.getBatchOfTuples(); batch != nullptr; std::tie(batch, batch_size) = tuple_creator.getBatchOfTuples()) {
                        thread_pool.dispatchTask(current_processing_unit, std::move(batch), batch_size);
                        current_processing_unit = (current_processing_unit + 1) % numProcessingUnits;
                    }
                });
            }
        }// generator_threads are joined here
        for (unsigned pu = 0; pu < numProcessingUnits; pu++) {
            thread_pool.stop(pu);
        }
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};