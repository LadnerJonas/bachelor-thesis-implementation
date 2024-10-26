#pragma once

#include "radix/worker/PartitionInfo.hpp"
#include "slotted-page/page-manager/PageWriteInfo.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"
#include "util/partitioning_function.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_radix_chunk(RadixPageManager<T, partitions, page_size> &page_manager, T *chunk, size_t chunk_size) {
    // auto time_start = std::chrono::high_resolution_clock::now();
    std::vector<unsigned> histogram(partitions, 0);
    for (size_t i = 0; i < chunk_size; ++i) {
        size_t partition = partition_function<T, partitions>(chunk[i]);
        ++histogram[partition];
    }
    //std::cout << "Processed histogram in " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - time_start).count() << "ms" << std::endl;
    page_manager.add_histogram_chunk(histogram);
    auto write_info = page_manager.get_write_info(histogram);
    // auto time_to_start_writing = std::chrono::high_resolution_clock::now();
    std::array<PartitionInfo, partitions> partition_info = {};
    for (size_t i = 0; i < chunk_size; ++i) {
        const size_t partition = partition_function<T, partitions>(chunk[i]);
        auto info = write_info[partition][partition_info[partition].entry_index];

        if (info.tuples_to_write == partition_info[partition].written_tuples) {
            RawSlottedPage<T>::increase_tuple_count(info.page_data, partition_info[partition].written_tuples);
            ++partition_info[partition].entry_index;
            partition_info[partition].written_tuples = 0;
            info = write_info[partition][partition_info[partition].entry_index];
        }

        size_t entry_num = info.start_num + partition_info[partition].written_tuples;
        RawSlottedPage<T>::write_tuple(info.page_data, page_size, chunk[i], entry_num);

        ++partition_info[partition].written_tuples;
    }

    for (size_t i = 0; i < partitions; ++i) {
        auto [entry_index, written_tuples] = partition_info[i];
        if (written_tuples > 0) {
            assert(write_info[i].size() > entry_index);
            RawSlottedPage<T>::increase_tuple_count(write_info[i][entry_index].page_data, written_tuples);
        }
    }
    //std::cout << "Processed chunk in " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - time_to_start_writing).count() << "ms" << std::endl;
}