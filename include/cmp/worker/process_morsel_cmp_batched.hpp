#pragma once
#include "cmp/morsel-creation/CollaborativeMorselCreator.hpp"
#include "slotted-page/page-manager/OnDemandSingleThreadPageManager.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_morsel_cmp_batched(const unsigned thread_id, const unsigned total_thread_count, CollaborativeMorselCreator<T> &morsel_creator, OnDemandSingleThreadPageManager<T, partitions, page_size> &page_manager) {
    auto partitions_per_thread = partitions / total_thread_count;
    auto remainder_partitions = partitions % total_thread_count;
    auto start_partition = thread_id * partitions_per_thread + std::min(thread_id, static_cast<unsigned>(remainder_partitions));
    auto end_partition = start_partition + partitions_per_thread + (thread_id < remainder_partitions ? 1 : 0);
    auto partitions_to_consider = end_partition - start_partition;

    if (thread_id == total_thread_count - 1) {
        assert(end_partition == partitions);
    }


    std::array<unsigned, partitions> buffer_index = {};
    const auto total_buffer_size = 1 * 1024 * 1024 / sizeof(T);
    std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);
    const auto buffer_size_per_partition = total_buffer_size / partitions_to_consider;
    auto batch_to_process = 0u;

    for (auto [batch, batch_size] = morsel_creator.requestBatchCollaboratively(batch_to_process++); batch != nullptr; std::tie(batch, batch_size) = morsel_creator.requestBatchCollaboratively(batch_to_process++)) {
        for (size_t i = 0; i < batch_size; ++i) {
            auto tuple = batch[i];
            auto partition = partition_function<T, partitions>(tuple);
            if (partition < start_partition || partition >= end_partition) {
                continue;
            }

            auto &index = buffer_index[partition];
            auto partition_offset = (partition - start_partition) * buffer_size_per_partition;

            if (index == buffer_size_per_partition) {
                page_manager.insert_buffer_of_tuples(buffer.get() + partition_offset, buffer_size_per_partition, partition);
                index = 0;
            }

            buffer[partition_offset + index] = tuple;
            ++index;
        }
    }

    for (unsigned i = start_partition; i < end_partition; ++i) {
        auto partition_offset = (i-start_partition) * buffer_size_per_partition;
        if (buffer_index[i] > 0) {
            page_manager.insert_buffer_of_tuples(buffer.get() + partition_offset, buffer_index[i], i);
        }
    }
}