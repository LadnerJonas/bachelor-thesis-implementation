#include "../external/perfevent/PerfEvent.hpp"
#include "on-demand/orchestration/OnDemandOrchestrator.hpp"
#include "radix/orchestration/RadixOrchestrator.hpp"
#include "radix/orchestration/RadixSelectiveOrchestrator.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"

void check_sum_of_written_tuples(size_t tuples_to_generate, std::vector<size_t> &written_tuples) {
    auto actual_tuples = 0u;
    for (auto tuples: written_tuples) {
        actual_tuples += tuples;
    }

    if (tuples_to_generate != actual_tuples) {
        std::cout << "Test failed: " << actual_tuples << "/" << tuples_to_generate << std::endl;
        exit(1);
    }
}

template<typename T>
void setup_benchmark_params(BenchmarkParameters &params, const std::string &impl, size_t tuples_to_generate, size_t partition, size_t threads) {
    params.setParam("A-Benchmark shuffle", impl);
    params.setParam("B-tuple_size", sizeof(T));
    params.setParam("C-Tuples", tuples_to_generate);
    double gb_size = static_cast<double>(tuples_to_generate) * sizeof(T) / 1024 / 1024 / 1024;
    std::ostringstream gb_str;
    gb_str << std::fixed << std::setprecision(1) << gb_size << " GB";
    params.setParam("D-GB", gb_str.str());
    params.setParam("E-Partitions", partition);
    params.setParam("F-Threads", threads);
}


template<typename T, unsigned... Partitions>
void benchmark_RadixOrchestrator(unsigned tuples_to_generate_base) {
    unsigned tuple_count_factor = std::is_same_v<T, Tuple16> ? (sizeof(Tuple100) / sizeof(Tuple16)) : 1;
    for (unsigned tuples_to_generate = tuples_to_generate_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_to_generate_base * tuple_count_factor; tuples_to_generate += tuples_to_generate_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "RadixOrchestrator" ,tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_to_generate_base * tuple_count_factor && threads == 1 && partition == 2);

                RadixOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
        };
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_RadixSelectiveOrchestrator(unsigned tuples_to_generate_base) {
    unsigned tuple_count_factor = std::is_same_v<T, Tuple16> ? (sizeof(Tuple100) / sizeof(Tuple16)) : 1;
    for (unsigned tuples_to_generate = tuples_to_generate_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_to_generate_base * tuple_count_factor; tuples_to_generate += tuples_to_generate_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "RadixSelectiveOrchestrator" , tuples_to_generate, partition, threads);
                constexpr unsigned k = 16;
                params.setParam("G-k", k);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_to_generate_base * tuple_count_factor && threads == 1 && partition == 2);

                RadixSelectiveOrchestrator<T, partition, k> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_OnDemandOrchestrator(unsigned tuples_to_generate_base) {
    unsigned tuple_count_factor = std::is_same_v<T, Tuple16> ? (sizeof(Tuple100) / sizeof(Tuple16)) : 1;
    for (unsigned tuples_to_generate = tuples_to_generate_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_to_generate_base * tuple_count_factor; tuples_to_generate += tuples_to_generate_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "OnDemandOrchestrator", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_to_generate_base * tuple_count_factor && threads == 1 && partition == 2);

                OnDemandOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

int main() {
    unsigned tuples_to_generate_base = 40'000'000u;
    benchmark_OnDemandOrchestrator<Tuple16, 2, 8, 32, 256, 1024>(tuples_to_generate_base);
    benchmark_RadixOrchestrator<Tuple16, 2, 8, 32, 256, 1024>(tuples_to_generate_base);
    benchmark_RadixSelectiveOrchestrator<Tuple16, 2, 8, 32, 256, 1024>(tuples_to_generate_base);

    benchmark_OnDemandOrchestrator<Tuple100, 2, 8, 32, 256, 1024>(tuples_to_generate_base);
    benchmark_RadixOrchestrator<Tuple100, 2, 8, 32, 256, 1024>(tuples_to_generate_base);
    benchmark_RadixSelectiveOrchestrator<Tuple100, 2, 8, 32, 256, 1024>(tuples_to_generate_base);

    return 0;
}
