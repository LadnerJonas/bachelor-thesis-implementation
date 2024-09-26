#pragma once


#include "radix/output_data_storage/RadixPartitionManager.hpp"
#include <vector>

template<typename T, size_t num_partitions>
void process_radix_chunk(T *chunk, size_t chunk_size, RadixPartitionManager<T, num_partitions> &global_partition_manager) {
    std::vector<size_t> histogram(num_partitions, 0);

    for (int i = 0; i < chunk_size; i++) {
        auto partition_id = partition_function(chunk[i], num_partitions);
        ++histogram[partition_id];
    }

    global_partition_manager.register_thread_histogram(histogram);
    auto storage_locations = global_partition_manager.request_thread_storage_locations(histogram);

    for (int i = 0; i < chunk_size; i++) {
        auto partition_id = partition_function(chunk[i], num_partitions);

        auto &[partition_storage, index] = storage_locations[partition_id];
        partition_storage[index++] = chunk[i];
    }
}
