#include "../external/perfevent/PerfEvent.hpp"
#include "hybrid/orchestration/HybridOrchestrator.hpp"
#include "on-demand/orchestration/OnDemandOrchestrator.hpp"
#include "on-demand/orchestration/OnDemandSingleThreadOrchestrator.hpp"
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

template<typename T>
size_t get_tuple_num_scaling_value() {
    if (std::is_same_v<T, Tuple100>) {
        return 1;
    }
    if (std::is_same_v<T, Tuple16>) {
        return sizeof(Tuple100) / sizeof(Tuple16);
    }
    if (std::is_same_v<T, Tuple4>) {
        return sizeof(Tuple100) / sizeof(Tuple4);
    }
    return 1;
}

template<typename T, unsigned... Partitions>
void benchmark_RadixOrchestrator(unsigned tuples_to_generate_base) {
    unsigned tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (unsigned tuples_to_generate = tuples_to_generate_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_to_generate_base * tuple_count_factor; tuples_to_generate += tuples_to_generate_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "RadixOrchestrator               ", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_to_generate_base * tuple_count_factor && threads == 1);

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
    unsigned tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (unsigned tuples_to_generate = tuples_to_generate_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_to_generate_base * tuple_count_factor; tuples_to_generate += tuples_to_generate_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "RadixSelectiveOrchestrator      ", tuples_to_generate, partition, threads);
                constexpr unsigned k = 16;
                params.setParam("G-k", k);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_to_generate_base * tuple_count_factor && threads == 1);

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
    unsigned tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (unsigned tuples_to_generate = tuples_to_generate_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_to_generate_base * tuple_count_factor; tuples_to_generate += tuples_to_generate_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "OnDemandOrchestrator            ", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_to_generate_base * tuple_count_factor && threads == 1);

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

template<typename T, unsigned... Partitions>
void benchmark_OnDemandSingleThreadOrchestrator(unsigned tuples_to_generate_base) {
    unsigned tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (unsigned tuples_to_generate = tuples_to_generate_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_to_generate_base * tuple_count_factor; tuples_to_generate += tuples_to_generate_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            unsigned threads = 1;
            BenchmarkParameters params;
            setup_benchmark_params<T>(params, "OnDemandSingleThreadOrchestrator", tuples_to_generate, partition, threads);
            PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_to_generate_base * tuple_count_factor && threads == 1);

            OnDemandSingleThreadOrchestrator<T, partition> orchestrator(tuples_to_generate);
            orchestrator.run();

            // Verify the result
            auto written_tuples = orchestrator.get_written_tuples_per_partition();
            check_sum_of_written_tuples(tuples_to_generate, written_tuples);
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_HybridOrchestrator(unsigned tuples_to_generate_base) {
    unsigned tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (unsigned tuples_to_generate = tuples_to_generate_base * tuple_count_factor; tuples_to_generate <= 1 * tuples_to_generate_base * tuple_count_factor; tuples_to_generate += tuples_to_generate_base * tuple_count_factor) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "HybridOrchestrator              ", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == tuples_to_generate_base * tuple_count_factor && threads == 1);

                HybridOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
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

void warmup_run(unsigned tuples_to_generate) {
    // std::cout << "Warming up..." << std::endl;
    OnDemandSingleThreadOrchestrator<Tuple100, 1024> orchestrator(tuples_to_generate);
    orchestrator.run();

    // Verify the result
    auto written_tuples = orchestrator.get_written_tuples_per_partition();
    check_sum_of_written_tuples(tuples_to_generate, written_tuples);

    // std::cout << "Warming up... (finished)" << std::endl;
}

int main() {
    unsigned tuples_to_generate_base = 40'000'000u;
    // let the program acquire the necessary resources
    warmup_run(tuples_to_generate_base);


    // Tuple16
    benchmark_OnDemandSingleThreadOrchestrator<Tuple16, 32>(tuples_to_generate_base);
    benchmark_OnDemandOrchestrator<Tuple16, 32>(tuples_to_generate_base);
    benchmark_RadixOrchestrator<Tuple16, 32>(tuples_to_generate_base);
    benchmark_HybridOrchestrator<Tuple16, 32>(tuples_to_generate_base);
    benchmark_RadixSelectiveOrchestrator<Tuple16, 32>(tuples_to_generate_base);

    benchmark_OnDemandSingleThreadOrchestrator<Tuple16, 1024>(tuples_to_generate_base);
    benchmark_OnDemandOrchestrator<Tuple16, 1024>(tuples_to_generate_base);
    benchmark_RadixOrchestrator<Tuple16, 1024>(tuples_to_generate_base);
    benchmark_HybridOrchestrator<Tuple16, 1024>(tuples_to_generate_base);
    benchmark_RadixSelectiveOrchestrator<Tuple16, 1024>(tuples_to_generate_base);

    // Tuple100
    benchmark_OnDemandSingleThreadOrchestrator<Tuple100, 32>(tuples_to_generate_base);
    benchmark_OnDemandOrchestrator<Tuple100, 32>(tuples_to_generate_base);
    benchmark_RadixOrchestrator<Tuple100, 32>(tuples_to_generate_base);
    benchmark_HybridOrchestrator<Tuple100, 32>(tuples_to_generate_base);
    benchmark_RadixSelectiveOrchestrator<Tuple100, 32>(tuples_to_generate_base);

    benchmark_OnDemandOrchestrator<Tuple100, 1024>(tuples_to_generate_base);
    benchmark_RadixOrchestrator<Tuple100, 1024>(tuples_to_generate_base);
    benchmark_HybridOrchestrator<Tuple100, 1024>(tuples_to_generate_base);
    benchmark_RadixSelectiveOrchestrator<Tuple100, 1024>(tuples_to_generate_base);

    // Tuple4
    benchmark_OnDemandSingleThreadOrchestrator<Tuple4, 32>(tuples_to_generate_base);
    benchmark_OnDemandOrchestrator<Tuple4, 32>(tuples_to_generate_base);
    benchmark_RadixOrchestrator<Tuple4, 32>(tuples_to_generate_base);
    benchmark_HybridOrchestrator<Tuple4, 32>(tuples_to_generate_base);
    benchmark_RadixSelectiveOrchestrator<Tuple4, 32>(tuples_to_generate_base);

    benchmark_OnDemandSingleThreadOrchestrator<Tuple4, 1024>(tuples_to_generate_base);
    benchmark_OnDemandOrchestrator<Tuple4, 1024>(tuples_to_generate_base);
    benchmark_RadixOrchestrator<Tuple4, 1024>(tuples_to_generate_base);
    benchmark_HybridOrchestrator<Tuple4, 1024>(tuples_to_generate_base);
    benchmark_RadixSelectiveOrchestrator<Tuple4, 1024>(tuples_to_generate_base);

    // benchmark_OnDemandOrchestrator<Tuple100, 32, 1024>(tuples_to_generate_base);
    // benchmark_RadixOrchestrator<Tuple100, 32, 1024>(tuples_to_generate_base);
    // benchmark_HybridOrchestrator<Tuple100, 32, 1024>(tuples_to_generate_base);
    // benchmark_RadixSelectiveOrchestrator<Tuple100, 32, 1024>(tuples_to_generate_base);

    return 0;
}
