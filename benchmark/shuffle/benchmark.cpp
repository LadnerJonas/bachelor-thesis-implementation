#include "../external/perfevent/PerfEvent.hpp"
#include "radix/orchestration/RadixOrchestrator.hpp"
#include "radix/orchestration/RadixSelectiveOrchestrator.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"

template<typename T, unsigned... Partitions>
void benchmark_RadixOrchestrator() {
    unsigned tuple_count_factor = std::is_same_v<T, Tuple16> ? 5 : 1;
    unsigned tuples_base = 10'000'000;
    for (unsigned tuples_to_generate = tuples_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_base * tuple_count_factor; tuples_to_generate += tuples_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                params.setParam("A-Benchmark shuffle", "RadixOrchestrator");
                params.setParam("B-tuple_size", sizeof(T));
                params.setParam("C-Tuples", tuples_to_generate);
                double gb_size = static_cast<double>(tuples_to_generate) * sizeof(T) / 1024 / 1024 / 1024;
                std::ostringstream gb_str;
                gb_str << std::fixed << std::setprecision(1) << gb_size << " GB";
                params.setParam("D-GB", gb_str.str());
                params.setParam("E-Partitions", partition);
                params.setParam("F-Threads", threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_base * tuple_count_factor && threads == 1 && partition == 2);

                RadixOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                auto actual_tuples = 0u;
                for (auto tuples: written_tuples) {
                    actual_tuples += tuples;
                }

                if (tuples_to_generate != actual_tuples) {
                    std::cout << "Test failed: " << actual_tuples << "/" << tuples_to_generate << std::endl;
                    exit(1);
                }
            }
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_RadixSelectiveOrchestrator() {
    unsigned tuple_count_factor = std::is_same_v<T, Tuple16> ? 5 : 1;
    unsigned tuples_base = 10'000'000;
    for (unsigned tuples_to_generate = tuples_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_base * tuple_count_factor; tuples_to_generate += tuples_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                params.setParam("A-Benchmark shuffle", "RadixSelectiveOrchestrator");
                params.setParam("B-tuple_size", sizeof(T));
                params.setParam("C-Tuples", tuples_to_generate);
                double gb_size = static_cast<double>(tuples_to_generate) * sizeof(T) / 1024 / 1024 / 1024;
                std::ostringstream gb_str;
                gb_str << std::fixed << std::setprecision(1) << gb_size << " GB";
                params.setParam("D-GB", gb_str.str());
                params.setParam("E-Partitions", partition);
                params.setParam("F-Threads", threads);
                constexpr unsigned k = 2;
                params.setParam("G-k", k);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_base * tuple_count_factor && threads == 1 && partition == 2);

                RadixSelectiveOrchestrator<T, partition, k> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                auto actual_tuples = 0u;
                for (auto tuples: written_tuples) {
                    actual_tuples += tuples;
                }

                if (tuples_to_generate != actual_tuples) {
                    std::cout << "Test failed: " << actual_tuples << "/" << tuples_to_generate << std::endl;
                    exit(1);
                }
            }
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

int main() {
    //benchmark_RadixSelectiveOrchestrator<Tuple16, 2, 8, 32, 256, 1024>();
    benchmark_RadixOrchestrator<Tuple16, 2, 8, 32, 256, 1024>();
    //benchmark_RadixSelectiveOrchestrator<Tuple100, 2, 8, 32, 256, 1024>();
    benchmark_RadixOrchestrator<Tuple100, 2, 8, 32, 256, 1024>();

    return 0;
}
