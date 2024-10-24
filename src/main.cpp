
#include "radix/materialization/ContiniousMaterialization.hpp"
#include "radix/orchestration/RadixOrchestrator.hpp"
#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "slotted-page/page-manager/RadixPageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"

auto main() -> int {
    // auto num_tuples = 230'000'000u;
    auto num_tuples = 10000000u;
    double gb_of_data = static_cast<double>(num_tuples) * 16 / 1024 / 1024 / 1024;
    std::cout << "Generating " << num_tuples << " tuples (" << gb_of_data << "GB of data)" << std::endl;


    auto time_start = std::chrono::high_resolution_clock::now();

    RadixOrchestrator<Tuple16, 2> orchestrator(num_tuples, 16);
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

    return 0;
}
