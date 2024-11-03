
#include "on-demand/orchestration/OnDemandOrchestrator.hpp"
#include "radix/materialization/ContiniousMaterialization.hpp"
#include "radix/orchestration/RadixOrchestrator.hpp"
#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"

#include "hybrid/orchestration/HybridOrchestrator.hpp"
#include "on-demand/orchestration/OnDemandSingleThreadOrchestrator.hpp"

template<typename Tt>
void test_radix_orchestrator(size_t num_tuples) {
    auto time_start = std::chrono::high_resolution_clock::now();

    RadixOrchestrator<Tt, 1024> orchestrator(num_tuples, 16);
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
    auto time_start = std::chrono::high_resolution_clock::now();
    OnDemandOrchestrator<Tt, 32> orchestrator(num_tuples, 1);
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
    auto time_start = std::chrono::high_resolution_clock::now();
    OnDemandSingleThreadOrchestrator<Tt, 32> orchestrator(num_tuples);
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
    auto time_start = std::chrono::high_resolution_clock::now();
    HybridOrchestrator<Tt, 1024> orchestrator(num_tuples, 16);
    orchestrator.run();

    auto written_tuples_per_partition = orchestrator.get_written_tuples_per_partition();
    auto time_start2 = std::chrono::high_resolution_clock::now();
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
    using Tt = Tuple4;
    auto num_tuples = 240'000'000u;
    // auto num_tuples = 30000u;
    // auto num_tuples = 10000000u;
    double gb_of_data = static_cast<double>(num_tuples) * sizeof(Tt) / 1024 / 1024 / 1024;
    std::cout << "Generating " << num_tuples << " tuples (" << gb_of_data << "GB of data)" << std::endl;

    // test_radix_orchestrator<Tt>(num_tuples);
    test_ondemand_single_thread_orchestrator<Tt>(num_tuples);
    test_ondemand_orchestrator<Tt>(num_tuples);
    // test_hybrid_orchestrator<Tt>(num_tuples);

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
