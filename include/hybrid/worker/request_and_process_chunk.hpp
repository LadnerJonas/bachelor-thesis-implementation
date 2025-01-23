#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include "slotted-page/page-manager/HybridPageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "util/partitioning_function.hpp"

#include <array>
#include <memory>
#include <ranges>
#include <vector>

template<typename T, size_t partitions, size_t page_size>
void write_out_buffer_of_partition(T *buffer, std::array<std::vector<PageWriteInfo<T>>, partitions> &write_info, const size_t partition, const unsigned long partition_offset, const unsigned num_tuples) {
    unsigned tuples_written = 0;

    while (tuples_written < num_tuples) {
        auto &info = write_info[partition].back();

        unsigned remaining_tuples_in_page = info.tuples_to_write - info.written_tuples;
        unsigned tuples_to_write = std::min(remaining_tuples_in_page, num_tuples - tuples_written);
        RawSlottedPage<T>::write_tuple_batch(info.page_data, page_size, buffer + partition_offset + tuples_written, info.start_num + info.written_tuples,
                                             tuples_to_write);

        info.written_tuples += tuples_to_write;
        tuples_written += tuples_to_write;

        if (info.written_tuples == info.tuples_to_write) {
            RawSlottedPage<T>::increase_tuple_count(info.page_data, info.written_tuples);
            write_info[partition].pop_back();
        }
    }
}


template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void request_and_process_chunk(HybridPageManager<T, partitions, page_size> &page_manager, BatchedTupleGenerator<T, 10 * 2048> &tuple_generator, const size_t num_threads) {
    static constexpr unsigned buffer_base_value = 2 * 1024;
    const static auto total_buffer_size = buffer_base_value * 1024 / (sizeof(T) * num_threads);
    const static auto buffer_size_per_partition = total_buffer_size / partitions;
    std::array<unsigned, partitions> buffer_index = {};
    std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);

    std::array<unsigned, partitions> histogram = {};
    std::array<std::vector<PageWriteInfo<T>>, partitions> write_info = page_manager.get_write_info(histogram);
    for (auto [chunk, chunk_size] = tuple_generator.getBatchOfTuples(); chunk; std::tie(chunk, chunk_size) = tuple_generator.getBatchOfTuples()) {
        histogram.fill(0);
        for (size_t i = 0; i < chunk_size; ++i) {
            const size_t partition = partition_function<T, partitions>(chunk[i]);
            ++histogram[partition];
        }
        std::array<std::vector<PageWriteInfo<T>>, partitions> new_write_info = page_manager.get_write_info(histogram);
        for (size_t i = 0; i < partitions; ++i) {
            write_info[i].insert(write_info[i].end(), new_write_info[i].begin(), new_write_info[i].end());
        }

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
