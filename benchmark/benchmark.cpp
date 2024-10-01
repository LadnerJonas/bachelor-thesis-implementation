#include <execution>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "external/perfevent/PerfEvent.hpp"
#include "input_preparation/BinaryRelation.hpp"
#include "morsel-driven/input_data_storage/MorselManager.hpp"
#include "morsel-driven/worker/process_morsels.hpp"

#include "../src/morsel-driven/output_data_storage/LockFreeListBasedPartitionManager.cpp"
#include "../src/morsel-driven/output_data_storage/StdVectorBasedPartitionManager.cpp"
#include "in-place_sort/sort_partition.hpp"
#include "radix/output_data_storage/RadixPartitionManager.hpp"
#include "radix/worker/process_radix_chunk.hpp"
#include "radix/worker/process_radix_chunk_futures.hpp"

template<typename T, typename PM>
void run_morsel_driven_partitioning(
        const std::shared_ptr<T[]> relation_data, const size_t relation_size, const size_t morsel_size, const size_t num_partitions,
        const int num_threads) {
    MorselManager<T> global_morsel_manager(relation_data, relation_size, morsel_size);
    PM global_partition_manager(num_partitions);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(
                process_morsels<PM, int>, std::ref(global_morsel_manager), std::ref(global_partition_manager));
    }

    for (auto &thread: threads) {
        thread.join();
    }

    size_t total_elements = 0;
    for (size_t i = 0; i < global_partition_manager.num_partitions(); ++i) {
        auto [data, size] = global_partition_manager.get_partition(i);
        total_elements += size;
    }

    if (total_elements != relation_size) std::cerr << "Error: Total elements processed does not match relation size\n";
}

template<typename T, size_t num_partitions, typename FUNC_PROCESS_RADIX_CHUNK>
void run_radix_partitioning(FUNC_PROCESS_RADIX_CHUNK process_radix_chunk, const std::shared_ptr<T[]> relation_data, const size_t relation_size,
                            const int num_threads) {

    RadixPartitionManager<T, num_partitions> global_partition_manager(num_threads);

    auto chunk_size = relation_size / num_threads;
    std::vector<std::thread> threads;
    auto start = relation_data.get();
    size_t current = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        auto chunk = start + current;
        auto thread_chunk_size = std::min(chunk_size, relation_size - current);
        if (i == num_threads - 1) {
            // this is necessary in case relation_size is not divisible by num_threads
            // the last thread will additionally process the remaining elements
            thread_chunk_size = relation_size - current;
        }
        threads.emplace_back([=, &global_partition_manager] {
            process_radix_chunk(chunk, thread_chunk_size, global_partition_manager);
        });
        current += thread_chunk_size;
    }

    for (auto &thread: threads) {
        thread.join();
    }

    int total_elements = 0;
    for (size_t i = 0; i < global_partition_manager.num_partitions(); ++i) {
        auto [data, size] = global_partition_manager.get_partition(i);
        total_elements += size;
    }

    if (total_elements != relation_size) std::cerr << "Error: Total elements processed does not match relation size " << total_elements << " " << relation_size << std::endl;
}


constexpr size_t num_partitions = 8;
constexpr size_t radix_write_out_batch_size = 8 * 9096 / num_partitions;

int main() {
    bool run_binary_relation_benchmark = false;
    bool run_sort_based_benchmark = false;
    bool run_morsel_based_benchmark = true;
    bool run_radix_based_benchmark = true;

    auto max_threads = std::thread::hardware_concurrency();

    std::string relation_path = "../../input_data/data/relation_int_large.bin";
    BinaryRelation<int> relation(relation_path);
    const auto relation_data = relation.get_data();
    const auto relation_size = relation.get_size();


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
    params.setParam("benchmark", "Partitioning Benchmark 7,5GB ints");
    params.setParam("num_partitions", num_partitions);

    if (run_sort_based_benchmark) {
        std::cout << "Sort based partitioning (in-place)\n";
        {
            params.setParam("impl", "UnsequencedUnstableSortBasedPartitioning");
            std::vector<int> data = std::vector(relation_data.get(), relation_data.get() + relation_size);

            PerfEventBlock e(1'000'000, params, true);
            auto partition_boundaries = sort_partition<int>(data, num_partitions, std::execution::seq);
            auto total_elements = std::accumulate(partition_boundaries.begin(), partition_boundaries.end(), 0,
                                                  [](int sum, const std::pair<int, int> &p) { return sum + p.second - p.first; });
            if (total_elements != relation_size) std::cerr << "Error: Total elements processed does not match relation size\n";
        }
        {
            params.setParam("impl", "ParallelUnstableSortBasedPartitioning   ");
            std::vector<int> data = std::vector(relation_data.get(), relation_data.get() + relation_size);

            PerfEventBlock e(1'000'000, params, false);
            auto partition_boundaries = sort_partition<int>(data, num_partitions, std::execution::par);
            auto total_elements = std::accumulate(partition_boundaries.begin(), partition_boundaries.end(), 0,
                                                  [](int sum, const std::pair<int, int> &p) { return sum + p.second - p.first; });
            if (total_elements != relation_size) std::cerr << "Error: Total elements processed does not match relation size\n";
        }
    }

    if (run_morsel_based_benchmark) {
        std::cout << "\nMorsel-driven partitioning\n";

        constexpr size_t morsel_size = 4096 * 100;
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
                run_morsel_driven_partitioning<int, StdVectorBasedPartitionManager<int>>(
                        relation_data, relation_size, morsel_size, num_partitions, num_threads);
            }
            {
                params.setParam("impl", "LockFreeListBasedPartitionManager");
                PerfEventBlock e(1'000'000, params, false);
                run_morsel_driven_partitioning<int, LockFreeListBasedPartitionManager<int>>(
                        relation_data, relation_size, morsel_size, num_partitions, num_threads);
            }
        }
    }

    if (run_radix_based_benchmark) {
        std::cout << "\nRadix partitioning\n";
        params.setParam("impl", "RadixPartitionManager");
        params.setParam("write_out_batch", radix_write_out_batch_size);

        for (auto num_threads = 1; num_threads <= max_threads; ++num_threads) {
            params.setParam("threads", num_threads);

            {
                params.setParam("method", "process_radix_chunk        ");
                PerfEventBlock e(1'000'000, params, num_threads == 1);
                run_radix_partitioning<int, num_partitions>(process_radix_chunk<int, num_partitions, radix_write_out_batch_size>, relation_data, relation_size, num_threads);
            }
            {
                params.setParam("method", "process_radix_chunk_futures");
                PerfEventBlock e(1'000'000, params, false);
                run_radix_partitioning<int, num_partitions>(process_radix_chunk_futures<int, num_partitions, radix_write_out_batch_size>, relation_data, relation_size, num_threads);
            }
        }
    }

    return 0;
}
