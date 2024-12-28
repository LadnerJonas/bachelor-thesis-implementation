#pragma once

#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_morsel_smb(BatchedTupleGenerator<T> &tuple_generator, OnDemandPageManager<T, partitions, page_size> &page_manager, const size_t num_threads) {
    static constexpr unsigned buffer_base_value = partitions <= 32 ? 512 : 4 * 1024;
    const static auto total_buffer_size = buffer_base_value * 1024 / (sizeof(T) * num_threads);
    const static auto buffer_size_per_partition = total_buffer_size / partitions;
    std::array<unsigned, partitions> buffer_index = {};
    std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);

    for (auto [batch, batch_size] = tuple_generator.getBatchOfTuples(); batch; std::tie(batch, batch_size) = tuple_generator.getBatchOfTuples()) {
        for (size_t i = 0; i < batch_size; ++i) {
            const auto &tuple = batch[i];
            const auto partition = partition_function<T, partitions>(tuple);
            auto &index = buffer_index[partition];
            const auto partition_offset = partition * buffer_size_per_partition;

            if (index == buffer_size_per_partition) {
                page_manager.insert_buffer_of_tuples(buffer.get() + partition_offset, buffer_size_per_partition, partition);
                index = 0;
            }

            buffer[partition_offset + index] = tuple;
            ++index;
        }
    }

    for (unsigned i = 0; i < partitions; ++i) {
        if (buffer_index[i] > 0) {
            const auto partition_offset = i * buffer_size_per_partition;
            page_manager.insert_buffer_of_tuples(buffer.get() + partition_offset, buffer_index[i], i);
        }
    }
}