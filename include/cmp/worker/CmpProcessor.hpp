#pragma once
#include "slotted-page/page-manager/OnDemandSingleThreadPageManager.hpp"
#include "util/partitioning_function.hpp"
#include <array>
#include <memory>


template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CmpProcessor {
    size_t start_partition;
    size_t end_partition;
    size_t buffer_size_per_partition;

    std::array<unsigned, partitions> buffer_index = {};

    std::unique_ptr<T[]> buffer;
    OnDemandSingleThreadPageManager<T, partitions, page_size> &page_manager;

public:
    CmpProcessor(const unsigned thread_id, const unsigned total_thread_count, OnDemandSingleThreadPageManager<T, partitions, page_size> &page_manager) : page_manager(page_manager) {
        auto partitions_per_thread = partitions / total_thread_count;
        auto remainder_partitions = partitions % total_thread_count;
        start_partition = thread_id * partitions_per_thread + std::min(thread_id, static_cast<unsigned>(remainder_partitions));
        end_partition = start_partition + partitions_per_thread + (thread_id < remainder_partitions ? 1 : 0);
        auto partitions_to_consider = end_partition - start_partition;
        auto total_buffer_size = 256 * 1024 / (sizeof(T) * total_thread_count);
        buffer = std::make_unique<T[]>(total_buffer_size);
        buffer_size_per_partition = total_buffer_size / partitions_to_consider;
    }

    void process(T *batch_ptr, size_t batch_size) {
        for (size_t i = 0; i < batch_size; ++i) {
            const auto &tuple = batch_ptr[i];
            auto partition = partition_function<T, partitions>(tuple);
            if (partition < start_partition || partition >= end_partition) {
                continue;
            }

            auto &index = buffer_index[partition];
            auto partition_offset = (partition - start_partition) * buffer_size_per_partition;

            if (index == buffer_size_per_partition) {
                page_manager.insert_buffer_of_tuples_batched(buffer.get() + partition_offset, buffer_size_per_partition, partition);
                index = 0;
            }

            buffer[partition_offset + index] = tuple;
            ++index;
        }
        if (batch_size == 0) {
            for (unsigned i = start_partition; i < end_partition; ++i) {
                auto partition_offset = (i - start_partition) * buffer_size_per_partition;
                if (buffer_index[i] > 0) {
                    page_manager.insert_buffer_of_tuples_batched(buffer.get() + partition_offset, buffer_index[i], i);
                }
            }
        }
    }
};