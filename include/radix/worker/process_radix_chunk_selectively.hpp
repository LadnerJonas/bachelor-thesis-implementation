#pragma once

#include "radix/worker/PartitionInfo.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"
#include "util/partitioning_function.hpp"


template<typename T, size_t partitions, size_t k, size_t page_size = 5 * 1024 * 1024>
void process_radix_chunk_selectively(RadixPageManager<T, partitions, page_size> &page_manager, T *chunk, size_t chunk_size) {
    auto time_start = std::chrono::high_resolution_clock::now();
    std::vector<unsigned> histogram(partitions, 0);

    // First pass: build histogram
    for (size_t i = 0; i < chunk_size; ++i) {
        size_t partition = partition_function<T, partitions>(chunk[i]);
        ++histogram[partition];
    }

    page_manager.add_histogram_chunk(histogram);
    auto write_info = page_manager.get_write_info(histogram);
    auto time_to_start_writing = std::chrono::high_resolution_clock::now();

    std::array<PartitionInfo, partitions> partition_info = {};

    // Process k partitions at a time
    for (size_t block = 0; block < (partitions + k - 1) / k; ++block) {
        size_t start_partition = block * k;
        size_t end_partition = std::min(start_partition + k, partitions);

        for (size_t i = 0; i < chunk_size; ++i) {
            const size_t partition = partition_function<T, partitions>(chunk[i]);

            // Check if current partition is within the k-block we're processing
            if (partition >= start_partition && partition < end_partition) {
                auto &info = write_info[partition][partition_info[partition].entry_index];

                if (info.tuples_to_write == partition_info[partition].written_tuples) {
                    info.page->update_tuple_count(partition_info[partition].written_tuples);
                    ++partition_info[partition].entry_index;
                    partition_info[partition].written_tuples = 0;
                    info = write_info[partition][partition_info[partition].entry_index];
                }

                size_t data_offset = info.start_offset + partition_info[partition].written_tuples;
                info.page->add_tuple_at_offset(chunk[i], data_offset, partition_info[partition].written_tuples);

                ++partition_info[partition].written_tuples;
            }
        }
    }

    // Final update for any remaining entries
    for (size_t i = 0; i < partitions; ++i) {
        auto [entry_index, written_tuples] = partition_info[i];
        if (written_tuples > 0) {
            assert(write_info[i].size() > entry_index);
            write_info[i][entry_index].page->update_tuple_count(written_tuples);
        }
    }
}
