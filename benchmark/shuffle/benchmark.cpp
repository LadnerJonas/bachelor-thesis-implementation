#include "../external/perfevent/PerfEvent.hpp"
#include "hybrid/orchestration/HybridOrchestrator.hpp"
#include "on-demand/orchestration/OnDemandOrchestrator.hpp"
#include "on-demand/orchestration/OnDemandSingleThreadOrchestrator.hpp"
#include "radix/orchestration/RadixOrchestrator.hpp"
#include "radix/orchestration/RadixSelectiveOrchestrator.hpp"
#include "smb/orchestration/SmbBatchedOrchestrator.hpp"
#include "smb/orchestration/SmbLockFreeOrchestrator.hpp"
#include "smb/orchestration/SmbOrchestrator.hpp"
#include "smb/orchestration/SmbSingleThreadOrchestrator.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"
#include "util/get_tuple_num_scaling_value.hpp"

constexpr unsigned SLEEP_TIME_MS = 500;

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
void benchmark_RadixOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "RadixOrchestrator               ", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                RadixOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_RadixSelectiveOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "RadixSelectiveOrchestrator      ", tuples_to_generate, partition, threads);
                constexpr unsigned k = 32;
                params.setParam("G-k", k);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                RadixSelectiveOrchestrator<T, partition, k> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_SmbOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "SmbOrchestrator                 ", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                SmbOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}
template<typename T, unsigned... Partitions>
void benchmark_SmbLockFreeOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "SmbLockFreeOrchestrator         ", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                SmbLockFreeOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}
template<typename T, unsigned... Partitions>
void benchmark_SmbSingleThreadOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            unsigned threads = 1;
            BenchmarkParameters params;
            setup_benchmark_params<T>(params, "SmbSingleThreadOrchestrator     ", tuples_to_generate, partition, threads);
            PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

            SmbSingleThreadOrchestrator<T, partition> orchestrator(tuples_to_generate);
            orchestrator.run();

            // Verify the result
            auto written_tuples = orchestrator.get_written_tuples_per_partition();
            check_sum_of_written_tuples(tuples_to_generate, written_tuples);

            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}
template<typename T, unsigned... Partitions>
void benchmark_SmbBatchedOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "SmbBatchedOrchestrator          ", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                SmbBatchedOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_OnDemandOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "OnDemandOrchestrator            ", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                OnDemandOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_OnDemandSingleThreadOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            unsigned threads = 1;
            BenchmarkParameters params;
            setup_benchmark_params<T>(params, "OnDemandSingleThreadOrchestrator", tuples_to_generate, partition, threads);
            PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

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
void benchmark_HybridOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads = 1; threads <= std::thread::hardware_concurrency(); threads *= 2) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "HybridOrchestrator              ", tuples_to_generate, partition, threads);
                PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                HybridOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                orchestrator.run();

                // Verify the result
                auto written_tuples = orchestrator.get_written_tuples_per_partition();
                check_sum_of_written_tuples(tuples_to_generate, written_tuples);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}
template<typename T>
void warmup_run(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor);
    OnDemandSingleThreadOrchestrator<T, 1024> orchestrator(tuples_to_generate);
    orchestrator.run();

    // Verify the result
    auto written_tuples = orchestrator.get_written_tuples_per_partition();
    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
}

template<typename T, unsigned... Partitions>
void run_benchmark_on_all_implementations(const unsigned tuples_to_generate_base) {
    warmup_run<T>(tuples_to_generate_base);

    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_OnDemandSingleThreadOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_OnDemandOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_SmbSingleThreadOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_SmbOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_SmbLockFreeOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_SmbBatchedOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_RadixOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_HybridOrchestrator<T, Partitions...>(tuples_to_generate_base);
    // std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    // benchmark_RadixSelectiveOrchestrator<T, Partitions...>(tuples_to_generate_base);
}

int main() {
    unsigned tuples_to_generate_base = 40'000'000u;

    run_benchmark_on_all_implementations<Tuple16, 32>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple16, 1024>(tuples_to_generate_base);

    run_benchmark_on_all_implementations<Tuple100, 32>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple100, 1024>(tuples_to_generate_base);

    run_benchmark_on_all_implementations<Tuple4, 32>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple4, 1024>(tuples_to_generate_base);
    return 0;
}
