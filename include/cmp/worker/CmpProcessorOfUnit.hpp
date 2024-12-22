#pragma once
#include "slotted-page/page-manager/LockFreePageManager.hpp"
#include "util/partitioning_function.hpp"
#include <array>
#include <memory>


template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CmpProcessorOfUnit {
    struct BufferPartitionInfo {
        unsigned buffer_offset = 0;
        unsigned write_out_index = 0;
        BufferPartitionInfo() = default;
    };
    static constexpr unsigned buffer_base_value = partitions <= 32 ? 256 : 4 * 1024;
    unsigned start_partition;
    unsigned end_partition;
    unsigned buffer_size_per_partition;

    std::array<BufferPartitionInfo, partitions> buffer_partition_info;
    std::unique_ptr<T[]> buffer;
    LockFreePageManager<T, partitions, page_size> &page_manager;

public:
    CmpProcessorOfUnit(const unsigned thread_id, const unsigned thread_count_per_processing_unit, const unsigned total_count_of_threads, LockFreePageManager<T, partitions, page_size> &page_manager) : page_manager(page_manager) {
        const auto partitions_per_thread = partitions / thread_count_per_processing_unit;
        const auto remainder_partitions = partitions % thread_count_per_processing_unit;
        start_partition = thread_id * partitions_per_thread + std::min(thread_id, static_cast<unsigned>(remainder_partitions));
        end_partition = start_partition + partitions_per_thread + (thread_id < remainder_partitions ? 1 : 0);
        const auto partitions_to_consider = end_partition - start_partition;
        const unsigned total_buffer_size = buffer_base_value * 1024 / (sizeof(T) * total_count_of_threads);
        buffer = std::make_unique<T[]>(total_buffer_size);
        buffer_size_per_partition = total_buffer_size / partitions_to_consider;

        for (unsigned i = start_partition; i < end_partition; ++i) {
            buffer_partition_info[i].buffer_offset = (i - start_partition) * buffer_size_per_partition;
        }
    }

    void process(T *batch_ptr, const size_t batch_size) {
        for (size_t i = 0; i < batch_size; ++i) {
            const auto &tuple = batch_ptr[i];
            auto partition = partition_function<T, partitions>(tuple);
            if (partition < start_partition || partition >= end_partition) {
                continue;
            }

            auto &index = buffer_partition_info[partition].write_out_index;
            if (index == buffer_size_per_partition) {
                page_manager.insert_buffer_of_tuples_batched(buffer.get() + buffer_partition_info[partition].buffer_offset, buffer_size_per_partition, partition);
                index = 0;
            }

            buffer[buffer_partition_info[partition].buffer_offset + index] = tuple;
            ++index;
        }
        if (batch_size == 0) {
            for (unsigned i = start_partition; i < end_partition; ++i) {
                if (buffer_partition_info[i].write_out_index > 0) {
                    page_manager.insert_buffer_of_tuples_batched(buffer.get() + buffer_partition_info[i].buffer_offset, buffer_partition_info[i].write_out_index, i);
                }
            }
        }
    }
};