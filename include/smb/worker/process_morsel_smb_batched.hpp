#pragma once
#include "common/morsel-creation/MorselCreator.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_morsel_smb_batched(MorselCreator<T> &morsel_creator, OnDemandPageManager<T, partitions, page_size> &page_manager, const size_t num_threads) {
    const static auto total_buffer_size = 8192 * 1024 / (sizeof(T) * num_threads);
    std::array<unsigned, partitions> buffer_index = {};
    const auto buffer_size_per_partition = total_buffer_size / partitions;
    std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);

    for (auto [batch, batch_size] = morsel_creator.getBatchOfTuples(); batch; std::tie(batch, batch_size) = morsel_creator.getBatchOfTuples()) {
        for (size_t i = 0; i < batch_size; ++i) {
            auto tuple = batch[i];
            auto partition = partition_function<T, partitions>(tuple);
            auto &index = buffer_index[partition];
            auto partition_offset = partition * buffer_size_per_partition;

            if (index == buffer_size_per_partition) {
                page_manager.insert_buffer_of_tuples_batched(buffer.get() + partition_offset, buffer_size_per_partition, partition);
                index = 0;
            }

            buffer[partition_offset + index] = tuple;
            ++index;
        }
    }

    for (unsigned i = 0; i < partitions; ++i) {
        auto partition_offset = i * buffer_size_per_partition;
        if (buffer_index[i] > 0) {
            page_manager.insert_buffer_of_tuples_batched(buffer.get() + partition_offset, buffer_index[i], i);
        }
    }
}