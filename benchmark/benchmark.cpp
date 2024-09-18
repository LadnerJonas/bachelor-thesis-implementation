#include <execution>
#include <iostream>
#include <thread>
#include <vector>

#include "external/perfevent/PerfEvent.hpp"
#include "input_preparation/BinaryRelation.hpp"
#include "morsel-driven/input_data_storage/MorselManager.hpp"
#include "morsel-driven/worker/process_morsels.hpp"

#include "../src/morsel-driven/output_data_storage/LockFreeListBasedPartitionManager.cpp"
#include "../src/morsel-driven/output_data_storage/StdVectorBasedPartitionManager.cpp"
#include "in-place_sort/sort_partition.hpp"

template<typename PM>
void run_partitioning(
        const std::string &relation_path, const size_t morsel_size, const size_t num_partitions,
        const int num_threads) {
    BinaryRelation<int> relation(relation_path);
    auto relation_data = relation.get_data();
    auto relation_size = relation.get_size();

    MorselManager<int> global_morsel_manager(relation_data, relation_size, morsel_size);
    PM global_partition_manager(num_partitions);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(
                process_morsels<PM, int>, std::ref(global_morsel_manager), std::ref(global_partition_manager));
    }

    for (auto &thread: threads) {
        thread.join();
    }

    int total_elements = 0;
    for (size_t i = 0; i < global_partition_manager.num_partitions(); ++i) {
        auto [data, size] = global_partition_manager.get_partition(i);
        total_elements += size;
    }

    if (total_elements != relation_size) std::cerr << "Error: Total elements processed does not match relation size\n";
}


int main() {
    bool run_binary_relation_benchmark = false;
    bool run_sort_based_benchmark = true;
    bool run_morsel_based_benchmark = true;

    constexpr size_t morsel_size = 4096;
    constexpr size_t num_partitions = 1024;
    auto max_threads = std::thread::hardware_concurrency();
    std::string relation_path = "../../input_data/data/relation_int.bin";

    // Global benchmark parameters
    BenchmarkParameters params;
    if (run_binary_relation_benchmark) {
        auto start = std::chrono::high_resolution_clock::now();
        PerfEventBlock e(1'000'000, params, true);
        BinaryRelation<int> relation(relation_path);
        auto relation_data = relation.get_data();
        auto relation_size = relation.get_size();
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Time taken to read relation: "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms\n";
        std::cout << "relation_size: " << relation_size << std::endl;
        std::cout << "relation_data[0]: " << relation_data[0] << std::endl;
    }
    params.setParam("benchmark", "Partitioning Benchmark 1.9GB ints");
    params.setParam("num_partitions", num_partitions);

    if (run_sort_based_benchmark) {
        std::cout << "Sort based partitioning (in-place)\n";
        {
            params.setParam("impl", "UnsequencedUnstableSortBasedPartitioning");
            PerfEventBlock e(1'000'000, params, true);
            BinaryRelation<int> relation(relation_path);
            auto relation_data = relation.get_data();
            auto relation_size = relation.get_size();

            std::vector<int> data = std::vector<int>(relation_data.get(), relation_data.get() + relation_size);
            auto partition_boundaries = sort_partition<int>(data, num_partitions, std::execution::seq);
            auto total_elements = std::accumulate(partition_boundaries.begin(), partition_boundaries.end(), 0,
                                                  [](int sum, const std::pair<int, int> &p) { return sum + p.second - p.first; });
            if (total_elements != relation_size) std::cerr << "Error: Total elements processed does not match relation size\n";
        }
        {
            params.setParam("impl", "ParallelUnstableSortBasedPartitioning   ");
            PerfEventBlock e(1'000'000, params, false);
            BinaryRelation<int> relation(relation_path);
            auto relation_data = relation.get_data();
            auto relation_size = relation.get_size();

            std::vector<int> data = std::vector<int>(relation_data.get(), relation_data.get() + relation_size);
            auto partition_boundaries = sort_partition<int>(data, num_partitions, std::execution::par);
            auto total_elements = std::accumulate(partition_boundaries.begin(), partition_boundaries.end(), 0,
                                                  [](int sum, const std::pair<int, int> &p) { return sum + p.second - p.first; });
            if (total_elements != relation_size) std::cerr << "Error: Total elements processed does not match relation size\n";
        }
    }

    if (run_morsel_based_benchmark) {
        std::cout << "\nMorsel-driven partitioning\n";
        params.setParam("morsel_size", morsel_size);
        // Loop over different thread counts
        for (auto num_threads = 1; num_threads <= max_threads; ++num_threads) {
            // for (auto num_threads = max_threads; num_threads >= 1; --num_threads) {
            params.setParam("threads", num_threads);

            // Only print header for the first iteration
            bool printHeader = (num_threads == 1);
            {
                params.setParam("impl", "StdVectorBasedPartitionManager   ");
                PerfEventBlock e(1'000'000, params, printHeader);
                run_partitioning<StdVectorBasedPartitionManager<int>>(
                        relation_path, morsel_size, num_partitions, num_threads);
            }
            {
                params.setParam("impl", "LockFreeListBasedPartitionManager");
                PerfEventBlock e(1'000'000, params, false);
                run_partitioning<LockFreeListBasedPartitionManager<int>>(
                        relation_path, morsel_size, num_partitions, num_threads);
            }
        }
    }

    return 0;
}
