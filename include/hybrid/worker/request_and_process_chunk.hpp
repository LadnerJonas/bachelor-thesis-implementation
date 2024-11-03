#pragma once
#include "hybrid/chunk-creation/ChunkCreator.hpp"
#include "slotted-page/page-manager/HybridPageManager.hpp"
#include "util/partitioning_function.hpp"


struct PartitionInfo;
template<typename T, size_t partitions, size_t page_size>
void handle_sub_chunk(HybridPageManager<T, partitions, page_size> &page_manager, std::unique_ptr<T[]> &chunk, const size_t chunk_size) {
    std::vector<unsigned> histogram(partitions, 0);
    for (size_t i = 0; i < chunk_size; ++i) {
        const size_t partition = partition_function<T, partitions>(chunk[i]);
        ++histogram[partition];
    }
    auto write_info = page_manager.get_write_info(histogram);
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
}
template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void request_and_process_chunk(HybridPageManager<T, partitions, page_size> &page_manager,
                               ChunkCreator<T> &chunk_creator, size_t proposed_chunk_size) {
    auto sub_chunks = 4u;
    auto sub_chunk_size = (proposed_chunk_size + sub_chunks - 1) / sub_chunks;
    for (size_t i = 0; i < sub_chunks; ++i) {
        auto current_chunk_size = std::min(sub_chunk_size, proposed_chunk_size - i * sub_chunk_size);
        auto [chunk, chunk_size] = chunk_creator.getChunkOfTuples(current_chunk_size);
        handle_sub_chunk<T, partitions, page_size>(page_manager, chunk, chunk_size);//time_start
    }
}