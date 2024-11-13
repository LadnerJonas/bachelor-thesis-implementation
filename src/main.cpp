
#include "on-demand/orchestration/OnDemandOrchestrator.hpp"
#include "radix/materialization/ContiniousMaterialization.hpp"
#include "radix/orchestration/RadixOrchestrator.hpp"
#include "tuple-types/tuple-types.hpp"

#include "hybrid/orchestration/HybridOrchestrator.hpp"
#include "on-demand/orchestration/OnDemandSingleThreadOrchestrator.hpp"
#include "radix/orchestration/RadixSelectiveOrchestrator.hpp"
#include "util/get_tuple_num_scaling_value.hpp"

constexpr size_t PAGE_SIZE = 5 * 1024 * 1024;
constexpr size_t PARTITIONS = 1024;
constexpr size_t THREADS = 16;

template<typename Tt>
void test_radix_orchestrator(size_t num_tuples) {
    std::cout << "Running radix orchestrator" << std::endl;
    auto time_start = std::chrono::high_resolution_clock::now();

    RadixOrchestrator<Tt, PARTITIONS, PAGE_SIZE> orchestrator(num_tuples, THREADS);
    orchestrator.run();
    auto written_tuples = orchestrator.get_written_tuples_per_partition();
    auto time_end = std::chrono::high_resolution_clock::now();

    auto actual_tuples = 0u;
    for (auto tuples: written_tuples) {
        actual_tuples += tuples;
    }

    if (num_tuples == actual_tuples) {
        std::cout << "Test passed" << std::endl;
    } else {
        std::cout << "Test failed: " << actual_tuples << "/" << num_tuples << std::endl;
        exit(1);
    }

    std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << "ms" << std::endl;
}

template<typename Tt>
void test_radix_selectiv_orchestrator(size_t num_tuples) {
    std::cout << "Running selectiv radix orchestrator" << std::endl;
    auto time_start = std::chrono::high_resolution_clock::now();

    RadixSelectiveOrchestrator<Tt, PARTITIONS, PAGE_SIZE> orchestrator(num_tuples, THREADS);
    orchestrator.run();
    auto written_tuples = orchestrator.get_written_tuples_per_partition();
    auto time_end = std::chrono::high_resolution_clock::now();

    auto actual_tuples = 0u;
    for (auto tuples: written_tuples) {
        actual_tuples += tuples;
    }

    if (num_tuples == actual_tuples) {
        std::cout << "Test passed" << std::endl;
    } else {
        std::cout << "Test failed: " << actual_tuples << "/" << num_tuples << std::endl;
        exit(1);
    }

    std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << "ms" << std::endl;
}

template<typename Tt>
void test_ondemand_orchestrator(size_t num_tuples) {
    std::cout << "Running on-demand orchestrator" << std::endl;
    auto time_start = std::chrono::high_resolution_clock::now();
    OnDemandOrchestrator<Tt, PARTITIONS, PAGE_SIZE> orchestrator(num_tuples, THREADS);
    orchestrator.run();

    auto written_tuples_per_partition = orchestrator.get_written_tuples_per_partition();
    auto actual_tuples = 0u;
    for (auto tuples: written_tuples_per_partition) {
        actual_tuples += tuples;
    }

    if (num_tuples == actual_tuples) {
        std::cout << "Test passed" << std::endl;
    } else {
        std::cout << "Test failed: " << actual_tuples << "/" << num_tuples << std::endl;
        exit(1);
    }
    std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - time_start).count() << "ms" << std::endl;
}

template<typename Tt>
void test_ondemand_single_thread_orchestrator(size_t num_tuples) {
    std::cout << "Running on-demand single thread orchestrator" << std::endl;
    auto time_start = std::chrono::high_resolution_clock::now();
    OnDemandSingleThreadOrchestrator<Tt, PARTITIONS, PAGE_SIZE> orchestrator(num_tuples);
    orchestrator.run();

    auto written_tuples_per_partition = orchestrator.get_written_tuples_per_partition();
    auto actual_tuples = 0u;
    for (auto tuples: written_tuples_per_partition) {
        actual_tuples += tuples;
    }

    if (num_tuples == actual_tuples) {
        std::cout << "Test passed" << std::endl;
    } else {
        std::cout << "Test failed: " << actual_tuples << "/" << num_tuples << std::endl;
        exit(1);
    }
    std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - time_start).count() << "ms" << std::endl;
}

template<typename Tt>
void test_hybrid_orchestrator(size_t num_tuples) {
    std::cout << "Running hybrid orchestrator" << std::endl;
    auto time_start = std::chrono::high_resolution_clock::now();
    HybridOrchestrator<Tt, PARTITIONS, PAGE_SIZE> orchestrator(num_tuples, THREADS);
    orchestrator.run();

    auto written_tuples_per_partition = orchestrator.get_written_tuples_per_partition();
    //auto time_start2 = std::chrono::high_resolution_clock::now();
    auto actual_tuples = 0u;
    for (auto tuples: written_tuples_per_partition) {
        actual_tuples += tuples;
    }

    if (num_tuples == actual_tuples) {
        std::cout << "Test passed" << std::endl;
    } else {
        std::cout << "Test failed: " << actual_tuples << "/" << num_tuples << std::endl;
        exit(1);
    }
    //std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - time_start2).count() << "ms" << std::endl;
    std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - time_start).count() << "ms" << std::endl;
}

auto main() -> int {
    using Tt = Tuple16;
    std::cout << "Running with tuple type: " << typeid(Tt).name() << std::endl;
    auto num_tuples_base = 40'000'000u;
    auto tuple_count_factor = get_tuple_num_scaling_value<Tt>();
    auto num_tuples = static_cast<unsigned>(num_tuples_base * tuple_count_factor);
    // auto num_tuples = 30000u;
    // auto num_tuples = 66'000'000u;
    auto net_needed_storage = static_cast<double>(num_tuples * sizeof(Tt));
    auto net_needed_storage_gb = net_needed_storage / 1024 / 1024 / 1024;
    std::cout << "Generating " << num_tuples << " tuples (" << net_needed_storage_gb << "GB of data)" << std::endl;
    auto average_tuples_per_partition = (num_tuples + PARTITIONS - 1) / PARTITIONS;
    auto max_tuples_per_page = RawSlottedPage<Tt>::get_max_tuples(PAGE_SIZE);

    auto num_pages = PARTITIONS * static_cast<unsigned>((average_tuples_per_partition + max_tuples_per_page - 1) / max_tuples_per_page);
    auto pages_size = static_cast<double>(num_pages) * PAGE_SIZE;
    auto pages_size_gb = pages_size / 1024 / 1024 / 1024;

    auto storage_overhead = static_cast<double>(num_tuples * (sizeof(SlotInfo<Tt>) - sizeof(Tt::KeyType)));
    auto storage_overhead_gb = storage_overhead / 1024 / 1024 / 1024;
    auto total_storage_gb = net_needed_storage_gb + storage_overhead_gb;
    auto load_factor = total_storage_gb / pages_size_gb;

    std::cout << "Needed Storage: " << total_storage_gb << "GB (" << net_needed_storage_gb << "GB (net) + " << storage_overhead_gb << "GB (overhead), " << 100 * net_needed_storage_gb / total_storage_gb << "% + " << 100 * storage_overhead_gb / total_storage_gb << "%)" << std::endl;
    std::cout << "Expecting " << num_pages << " pages (" << pages_size_gb << "GB of pages, " << num_pages / PARTITIONS << " pages per partition, " << 100 * load_factor << "% page load factor)" << std::endl;

    test_radix_orchestrator<Tt>(num_tuples);
    test_ondemand_single_thread_orchestrator<Tt>(num_tuples);
    test_ondemand_orchestrator<Tt>(num_tuples);
    test_hybrid_orchestrator<Tt>(num_tuples);
    test_radix_selectiv_orchestrator<Tt>(num_tuples);

    // auto time_start = std::chrono::high_resolution_clock::now();
    // ContinuousMaterialization<Tt> materialization(num_tuples);
    // materialization.materialize();
    // auto data = materialization.get_data();
    // if(data == nullptr) {
    //     std::cout << "Data is null" << std::endl;
    // }
    // auto time_end = std::chrono::high_resolution_clock::now();
    // std::cout << "Time taken: " << std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_start).count() << "ms" << std::endl;


    return 0;
}
