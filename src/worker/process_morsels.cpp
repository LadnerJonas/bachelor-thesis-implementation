#include "worker/process_morsels.hpp"
#include "../output_data_storage/StdVectorBasedPartitionManager.cpp"
#include "../output_data_storage/LockFreeListBasedPartitionManager.cpp"

// Worker function to process morsels and store partitions
template<typename PM, typename T>
void process_morsels(MorselManager<T> &morsel_manager, PartitionManagerBase<PM, T> &global_partition_manager) {
    size_t num_partitions = global_partition_manager.num_partitions();
    std::vector<std::vector<T>> thread_partitions(num_partitions);

    while (auto morsel = morsel_manager.get_next_morsel()) {
        std::shared_ptr<T[]> start = morsel->first;
        size_t size = morsel->second;

        for (size_t i = 0; i < size; ++i) {
            partition_function(std::move(start[i]), thread_partitions, num_partitions);
        }

        // Flush the thread-local partitions to the global partition manager if necessary
        /*
        for (size_t i = 0; i < num_partitions; ++i) {
            if (thread_partitions[i].size() >= size / num_partitions) {
                global_partition_manager.store_partition(i, std::move(thread_partitions[i]));
                thread_partitions[i].clear();
            }
        }
        */
    }

    // Final flush to global partition manager
    for (size_t i = 0; i < num_partitions; ++i) {
        if (!thread_partitions[i].empty()) {
            global_partition_manager.store_partition(i, thread_partitions[i]);
        }
    }
}

// Explicit template instantiation (if needed)
template void process_morsels<StdVectorBasedPartitionManager<int>, int>(MorselManager<int> &morsel_manager,
                                                                        PartitionManagerBase<StdVectorBasedPartitionManager<int>, int> &global_partition_manager);
template void process_morsels<LockFreeListBasedPartitionManager<int>, int>(MorselManager<int> &morsel_manager,
                                                                        PartitionManagerBase<LockFreeListBasedPartitionManager<int>, int> &global_partition_manager);
