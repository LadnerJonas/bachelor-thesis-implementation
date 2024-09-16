
#include <iostream>
#include <vector>
#include <thread>
#include "input_data_storage/MorselManager.hpp"
#include "worker/process_morsels.hpp"
#include "input_preparation/BinaryRelation.hpp"
#include "external/perfevent/PerfEvent.hpp"
#include "../src/output_data_storage/StdVectorBasedPartitionManager.cpp"
#include "../src/output_data_storage/LockFreeListBasedPartitionManager.cpp"

template<typename PM>
void run_partitioning(size_t morsel_size, size_t num_partitions, int num_threads, const std::string &relation_path) {
    BinaryRelation<int> relation(relation_path);
    auto relation_data = relation.get_data();
    auto relation_size = relation.get_size();

    MorselManager<int> global_morsel_manager(relation_data, relation_size, morsel_size);
    PM global_partition_manager(num_partitions);

    // Create threads and process data
    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(process_morsels<PM, int>, std::ref(global_morsel_manager),
                             std::ref(global_partition_manager));
    }

    // Join all threads
    for (auto &thread: threads) {
        thread.join();
    }

    // Optionally print the number of elements processed in each partition
    int total_elements = 0;
    for (size_t i = 0; i < global_partition_manager.num_partitions(); ++i) {
        auto [data, size] = global_partition_manager.get_partition(i);
        total_elements += size;
    }

    if (total_elements != relation_size)
        std::cerr << "Error: Total elements processed does not match relation size\n";
}

int main() {
    constexpr size_t morsel_size = 4096;  // 1 million elements per morsel
    constexpr size_t num_partitions = 16;      // Number of partitions
    auto max_threads = std::thread::hardware_concurrency();  // Maximum number of threads to benchmark
    std::string relation_path = "../../input_data/data/relation_int.bin"; // Data path

    // Global benchmark parameters
    BenchmarkParameters params;
    params.setParam("name", "Partitioning Benchmark");

    // Loop over different thread counts
    for (int num_threads = 1; num_threads <= max_threads; ++num_threads) {
        // Set local benchmark parameters for this iteration
        params.setParam("threads", num_threads);

        // Only print header for the first iteration
        bool printHeader = (num_threads == 1);

        {
            params.setParam("impl", "StdVectorBasedPartitionManager");
            // Start performance event block with n events (set n to a reasonable value)
            PerfEventBlock e(1'000'000, params, printHeader);  // Change '1' to number of events

            // Run your partitioning benchmark
            run_partitioning<StdVectorBasedPartitionManager<int>>(morsel_size, num_partitions, num_threads,
                                                                  relation_path);
        }
        {
            params.setParam("impl", "LockFreeListBasedPartitionManager");
            PerfEventBlock e(1'000'000, params, false);  // Change '1' to number of events

            // Run your partitioning benchmark
            run_partitioning<LockFreeListBasedPartitionManager<int>>(morsel_size, num_partitions, num_threads,
                                                                     relation_path);
        }
    }

    return 0;
}
