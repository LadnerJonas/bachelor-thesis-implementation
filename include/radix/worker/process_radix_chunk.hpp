#pragma once

#include "slotted-page/page-manager/PageWriteInfo.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"
#include "util/partitioning_function.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_radix_chunk(RadixPageManager<T, partitions, page_size> &page_manager, T *chunk, const size_t chunk_size, const size_t num_threads) {
    std::array<unsigned, partitions> histogram = {};
    for (size_t i = 0; i < chunk_size; ++i) {
        const size_t partition = partition_function<T, partitions>(chunk[i]);
        ++histogram[partition];
    }
    std::array<std::vector<PageWriteInfo<T>>, partitions> write_info = page_manager.add_histogram_chunk(histogram);

    static constexpr unsigned buffer_base_value = 2 * 1024;
    const static auto total_buffer_size = buffer_base_value * 1024 / (sizeof(T) * num_threads);
    const static auto buffer_size_per_partition = total_buffer_size / partitions;
    std::array<unsigned, partitions> buffer_index = {};
    std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);


    for (size_t i = 0; i < chunk_size; ++i) {
        const auto &tuple = chunk[i];
        const size_t partition = partition_function<T, partitions>(tuple);
        auto &index = buffer_index[partition];
        const auto partition_offset = partition * buffer_size_per_partition;

        if (index == buffer_size_per_partition) {
            write_out_buffer_of_partition<T, partitions, page_size>(buffer.get(), write_info, partition, partition_offset, buffer_size_per_partition);
            index = 0;
        }

        buffer[partition_offset + index] = tuple;
        ++index;
    }

    for (size_t i = 0; i < partitions; ++i) {
        if (buffer_index[i] > 0) {
            const auto partition_offset = i * buffer_size_per_partition;
            write_out_buffer_of_partition<T, partitions, page_size>(buffer.get(), write_info, i, partition_offset, buffer_index[i]);
            buffer_index[i] = 0;
        }
        if (write_info[i].size() > 0) {
            if (const auto info = write_info[i].back(); info.written_tuples > 0) {
                RawSlottedPage<T>::increase_tuple_count(info.page_data, info.written_tuples);
            }
        }
    }
}