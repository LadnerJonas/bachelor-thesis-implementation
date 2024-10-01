#pragma once


#include "radix/output_data_storage/RadixPartitionManager.hpp"
#include <vector>

template<typename T, size_t num_partitions, size_t write_out_batch_size>
void process_radix_chunk(T *chunk, size_t chunk_size, RadixPartitionManager<T, num_partitions> &global_partition_manager) {
    std::array<std::array<T, write_out_batch_size>, num_partitions> batched_storage;
    std::array<size_t, num_partitions> batch_index = {};


    std::array<size_t, num_partitions> histogram = {};
    for (int i = 0; i < chunk_size; i++) {
        const auto partition_id = partition_function(chunk[i], num_partitions);
        ++histogram[partition_id];
    }
    global_partition_manager.register_thread_histogram(histogram);
    std::vector<std::pair<T *, size_t>> storage_locations;

    for (size_t i = 0; i < chunk_size; i++) {
        const auto partition_id = partition_function(chunk[i], num_partitions);

        if (batch_index[partition_id] == write_out_batch_size) {
            if (storage_locations.empty())
                storage_locations = global_partition_manager.request_thread_storage_locations(histogram);
            // write out full batch
            auto &[partition_storage, index] = storage_locations[partition_id];
            std::memcpy(&partition_storage[index], batched_storage[partition_id].data(), write_out_batch_size * sizeof(T));
            index += write_out_batch_size;
            batch_index[partition_id] = 0;
        }
        batched_storage[partition_id][batch_index[partition_id]++] = chunk[i];
    }

    if (storage_locations.empty())
        storage_locations = global_partition_manager.request_thread_storage_locations(histogram);
    // write out non-full batches
    for (size_t i = 0; i < num_partitions; ++i) {
        if (batch_index[i] > 0) {
            auto &[partition_storage, index] = storage_locations[i];
            std::memcpy(&partition_storage[index], batched_storage[i].data(), batch_index[i] * sizeof(T));
            index += batch_index[i];
        }
    }
}
