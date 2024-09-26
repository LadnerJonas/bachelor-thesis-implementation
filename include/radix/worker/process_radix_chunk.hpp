#pragma once


#include "radix/output_data_storage/RadixPartitionManager.hpp"
#include <vector>

template<typename T, size_t num_partitions, size_t batch_size>
void process_radix_chunk(T *chunk, size_t chunk_size, RadixPartitionManager<T, num_partitions> &global_partition_manager) {
    T batched_storage[num_partitions][batch_size];
    std::array<size_t, num_partitions> batch_index{0};


    std::vector<size_t> histogram(num_partitions, 0);
    for (int i = 0; i < chunk_size; i++) {
        auto partition_id = partition_function(chunk[i], num_partitions);
        ++histogram[partition_id];
    }

    global_partition_manager.register_thread_histogram(histogram);
    auto storage_locations = global_partition_manager.request_thread_storage_locations(histogram);

    for (int i = 0; i < chunk_size; i++) {
        auto partition_id = partition_function(chunk[i], num_partitions);

        if(batch_index[partition_id] == batch_size) {
            // write out full batch
            auto &[partition_storage, index] = storage_locations[partition_id];
            std::memcpy(&partition_storage[index], batched_storage[partition_id], batch_size * sizeof(T));
            index += batch_size;
            batch_index[partition_id] = 0;
        }
        else {
            batched_storage[partition_id][batch_index[partition_id]++] = chunk[i];
        }

        //auto &[partition_storage, index] = storage_locations[partition_id];
        //partition_storage[index++] = chunk[i];
    }

    // write out non-full batches
    for (size_t i = 0; i < num_partitions; ++i) {
        if (batch_index[i] > 0) {
            auto &[partition_storage, index] = storage_locations[i];
            std::memcpy(&partition_storage[index], batched_storage[i], batch_index[i] * sizeof(T));
            index += batch_index[i];
        }
    }
}
