#include <iostream>
#include <vector>
#include <thread>
#include <filesystem>
#include "output_data_storage/PartitionManager.hpp"
#include "input_data_storage/MorselManager.hpp"
#include "worker/process_morsels.hpp"
#include "input_preparation/BinaryRelation.hpp"
#include "output_data_storage/StdVectorBasedPartitionManager.cpp"

int main() {
    constexpr size_t morsel_size = 1'000'000;  // 1 million elements per morsel
    auto num_threads = std::thread::hardware_concurrency();        // Number of threads
    constexpr size_t num_partitions = 16;      // Number of partitions

    std::cout << "current directory: " << std::filesystem::current_path() << "\n";

    BinaryRelation<int> relation("../../input_data/data/relation_int.bin");
    auto relation_data = relation.get_data();
    auto relation_size = relation.get_size();

    auto time_start = std::chrono::high_resolution_clock::now();

    MorselManager<int> global_morsel_manager(relation_data, relation_size, morsel_size);
    StdVectorBasedPartitionManager<int> global_partition_manager(num_partitions);


    // Create threads and process data
    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(process_morsels<StdVectorBasedPartitionManager<int>, int>, std::ref(global_morsel_manager),
                             std::ref(global_partition_manager));
    }

    // Join all threads
    for (auto &thread: threads) {
        thread.join();
    }

    auto time_end = std::chrono::high_resolution_clock::now();

    std::cout << "Partitioning complete. Number of partitions: " << global_partition_manager.num_partitions() << "\n";

    // Example of querying and iterating over partitions
    int total_elements = 0;

    for (size_t i = 0; i < global_partition_manager.num_partitions(); ++i) {
        auto [data, size] = global_partition_manager.get_partition(i);
        std::cout << "Partition " << i << " has " << size << " elements.\n";
        total_elements += size;
    }

    std::cout << "Total elements: " << total_elements << "/" << relation_size << "\n";
    std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count()
              << "ms\n";


    return 0;
}
