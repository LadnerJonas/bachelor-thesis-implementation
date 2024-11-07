#pragma once

#include "radix/worker/PartitionInfo.hpp"
#include "slotted-page/page-manager/PageWriteInfo.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"
#include "util/partitioning_function.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_radix_chunk(RadixPageManager<T, partitions, page_size> &page_manager, T *chunk, size_t chunk_size) {
    std::array<unsigned, partitions> histogram = {};
    for (size_t i = 0; i < chunk_size; ++i) {
        const size_t partition = partition_function<T, partitions>(chunk[i]);
        ++histogram[partition];
    }
    page_manager.add_histogram_chunk(histogram);
    auto write_info = page_manager.get_write_info(histogram);
    for (size_t i = 0; i < chunk_size; ++i) {
        const size_t partition = partition_function<T, partitions>(chunk[i]);
        auto &info = write_info[partition].back();

        if (info.tuples_to_write == info.written_tuples) {
            RawSlottedPage<T>::increase_tuple_count(info.page_data, info.written_tuples);
            write_info[partition].pop_back();
            i--;
            continue;
        }

        const size_t entry_num = info.start_num + info.written_tuples;
        RawSlottedPage<T>::write_tuple(info.page_data, page_size, chunk[i], entry_num);

        ++info.written_tuples;
    }

    for (size_t i = 0; i < partitions; ++i) {
        if (write_info[i].size() > 0) {
            if (const auto info = write_info[i].back(); info.written_tuples > 0) {
                RawSlottedPage<T>::increase_tuple_count(info.page_data, info.written_tuples);
            }
        }
    }
}