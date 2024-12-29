#pragma once

#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "util/partitioning_function.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_morsel(BatchedTupleGenerator<T> &tuple_generator, OnDemandPageManager<T, partitions, page_size> &page_manager) {
    for (auto [batch, batch_size] = tuple_generator.getBatchOfTuples(); batch;
         std::tie(batch, batch_size) = tuple_generator.getBatchOfTuples()) {
        for (size_t i = 0; i < batch_size; ++i) {
            page_manager.insert_tuple(batch[i], partition_function<T, partitions>(batch[i]));
        }
    }
}