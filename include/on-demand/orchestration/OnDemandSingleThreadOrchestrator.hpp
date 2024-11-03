#pragma once

#include "slotted-page/page-manager/OnDemandSingleThreadPageManager.hpp"

template<typename T, size_t partitions>
class OnDemandSingleThreadOrchestrator {
    BatchedTupleGenerator<T> generator;
    OnDemandSingleThreadPageManager<T, partitions> page_manager;
    size_t num_tuples;

public:
    explicit OnDemandSingleThreadOrchestrator(size_t num_tuples) : generator(num_tuples), num_tuples(num_tuples) {
    }

    void run() {
        for (auto [batch, batch_size] = generator.getBatchOfTuples(); batch;
             std::tie(batch, batch_size) = generator.getBatchOfTuples()) {
            for (size_t i = 0; i < batch_size; ++i) {
                page_manager.insert_tuple(batch[i], partition_function<T, partitions>(batch[i]));
            }
        }
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};