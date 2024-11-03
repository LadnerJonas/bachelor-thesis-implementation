#pragma once

#include "radix/worker/PartitionInfo.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"
#include "util/partitioning_function.hpp"


template<typename T, size_t partitions, size_t k, size_t page_size = 5 * 1024 * 1024>
void process_radix_chunk_selectively(RadixPageManager<T, partitions, page_size> &page_manager, T *chunk, size_t chunk_size) {
    //auto time_start = std::chrono::high_resolution_clock::now();
    std::array<unsigned, partitions> histogram = {};
    histogram.fill(0u);

    // First pass: build histogram
    for (size_t i = 0; i < chunk_size; ++i) {
        const size_t partition = partition_function<T, partitions>(chunk[i]);
        ++histogram[partition];
    }

    page_manager.add_histogram_chunk(histogram);
    const auto write_info = page_manager.get_write_info(histogram);
    //auto time_to_start_writing = std::chrono::high_resolution_clock::now();

    std::array<PartitionInfo, partitions> partition_info = {};

    constexpr size_t inner_loop_size = 4096;

    // Process k partitions at a time
    for (size_t start_offset = 0; start_offset < chunk_size; start_offset += inner_loop_size) {
        for (size_t block = 0; block < (partitions + k - 1) / k; ++block) {
            const size_t start_partition = block * k;
            const size_t end_partition = std::min(start_partition + k, partitions);

            for (unsigned i = 0; i < std::min(chunk_size - start_offset ,inner_loop_size); ++i) {
                const size_t partition = partition_function<T, partitions>(chunk[start_offset+i]);

                // Check if current partition is within the k-block we're processing
                if (partition >= start_partition && partition < end_partition) {
                    auto info = write_info[partition][partition_info[partition].entry_index];

                    if (info.tuples_to_write == partition_info[partition].written_tuples) {
                        RawSlottedPage<T>::increase_tuple_count(info.page_data, partition_info[partition].written_tuples);
                        ++partition_info[partition].entry_index;
                        partition_info[partition].written_tuples = 0;
                        info = write_info[partition][partition_info[partition].entry_index];
                    }

                    const size_t entry_num = info.start_num + partition_info[partition].written_tuples;
                    RawSlottedPage<T>::write_tuple(info.page_data, page_size, chunk[start_offset], entry_num);

                    ++partition_info[partition].written_tuples;
                }
            }
        }
    }

    for (size_t i = 0; i < partitions; ++i) {
        const auto [entry_index, written_tuples] = partition_info[i];
        if (written_tuples > 0) {
            assert(write_info[i].size() > entry_index);
            RawSlottedPage<T>::increase_tuple_count(write_info[i][entry_index].page_data, written_tuples);
        }
    }
}
