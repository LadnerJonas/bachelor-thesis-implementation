#include "../external/perfevent/PerfEvent.hpp"
#include "cmp/orchestration/CollaborativeMorselProcessingOrchestrator.hpp"
#include "cmp/orchestration/CollaborativeMorselProcessingThreadPoolOrchestrator.hpp"
#include "cmp/orchestration/CollaborativeMorselProcessingThreadPoolWithProcessingUnitsOrchestrator.hpp"
#include "hybrid/orchestration/HybridOrchestrator.hpp"
#include "lpam/orchestrator/LocalPagesAndMergeOrchestrator.hpp"
#include "on-demand/orchestration/OnDemandOrchestrator.hpp"
#include "on-demand/orchestration/OnDemandSingleThreadOrchestrator.hpp"
#include "radix/orchestration/RadixOrchestrator.hpp"
#include "radix/orchestration/RadixSelectiveOrchestrator.hpp"
#include "smb/orchestration/SmbBatchedOrchestrator.hpp"
#include "smb/orchestration/SmbLockFreeBatchedOrchestrator.hpp"
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
            for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "RadixOrchestrator               ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                    RadixOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_SmbOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "SmbOrchestrator                 ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                    SmbOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

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
            for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "SmbLockFreeOrchestrator         ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                    SmbLockFreeOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}
template<typename T, unsigned... Partitions>
void benchmark_SmbLockFreeBatchedOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "SmbLockFreeBatchedOrchestrator  ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                    SmbLockFreeBatchedOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

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
            {
                PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                SmbSingleThreadOrchestrator<T, partition> orchestrator(tuples_to_generate);
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
void benchmark_SmbBatchedOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "SmbBatchedOrchestrator          ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                    SmbBatchedOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

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
            for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "OnDemandOrchestrator            ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                    OnDemandOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

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
            {
                PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                OnDemandSingleThreadOrchestrator<T, partition> orchestrator(tuples_to_generate);
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
void benchmark_HybridOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "HybridOrchestrator              ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                    HybridOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_LocalPagesAndMergeOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
                if (partition == 1024 && threads > 64) {
                    continue;
                }
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "LocalPagesAndMergeOrchestrator  ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                    LocalPagesAndMergeOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}
template<typename T, unsigned... Partitions>
void benchmark_CollaborativeMorselProcessingOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "CmpOrchestrator                 ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 1);

                    CollaborativeMorselProcessingOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}

template<typename T, unsigned... Partitions>
void benchmark_CollaborativeMorselProcessingThreadPoolOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads : {2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "CmpThreadPoolOrchestrator       ", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 2);

                    CollaborativeMorselProcessingThreadPoolOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

            }
            std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
        };

        // Use fold expression to call run_benchmark with each partition value
        (run_benchmark(std::integral_constant<unsigned, Partitions>{}), ...);
    }
}
template<typename T, unsigned... Partitions>
void benchmark_CollaborativeMorselProcessingThreadPoolWithProcessingUnitsOrchestrator(const unsigned tuples_to_generate_base) {
    auto tuple_count_factor = get_tuple_num_scaling_value<T>();
    for (auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate <= 1 * static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor); tuples_to_generate += static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor)) {
        auto run_benchmark = [&](auto partition) {
            for (unsigned threads : {2, 4, 8, 16, 32, 48, 64, 128}) {
                BenchmarkParameters params;
                setup_benchmark_params<T>(params, "CmpThreadPoolOrchestratorProUnit", tuples_to_generate, partition, threads);
                {
                    PerfEventBlock e(1'000'000, params, tuples_to_generate == static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * tuple_count_factor) && threads == 2);

                    CollaborativeMorselProcessingThreadPoolWithProcessingUnitsOrchestrator<T, partition> orchestrator(tuples_to_generate, threads);
                    orchestrator.run();

                    // Verify the result
                    auto written_tuples = orchestrator.get_written_tuples_per_partition();
                    check_sum_of_written_tuples(tuples_to_generate, written_tuples);
                }

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

    SmbSingleThreadOrchestrator<T, 1024> orchestrator2(tuples_to_generate);
    orchestrator2.run();

    auto written_tuples2 = orchestrator2.get_written_tuples_per_partition();
    check_sum_of_written_tuples(tuples_to_generate, written_tuples2);
}

template<typename T, unsigned... Partitions>
void run_benchmark_on_all_implementations(const unsigned tuples_to_generate_base) {
    warmup_run<T>(tuples_to_generate_base / 4);

    // std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    // benchmark_OnDemandSingleThreadOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_OnDemandOrchestrator<T, Partitions...>(tuples_to_generate_base);

    // std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    // benchmark_SmbSingleThreadOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_SmbOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_SmbBatchedOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_SmbLockFreeOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_SmbLockFreeBatchedOrchestrator<T, Partitions...>(tuples_to_generate_base);

    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_RadixOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_HybridOrchestrator<T, Partitions...>(tuples_to_generate_base);
    // std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    // benchmark_RadixSelectiveOrchestrator<T, Partitions...>(tuples_to_generate_base);

    // std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    // benchmark_LocalPagesAndMergeOrchestrator<T, Partitions...>(tuples_to_generate_base);

    // std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    // benchmark_CollaborativeMorselProcessingOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_CollaborativeMorselProcessingThreadPoolOrchestrator<T, Partitions...>(tuples_to_generate_base);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_CollaborativeMorselProcessingThreadPoolWithProcessingUnitsOrchestrator<T, Partitions...>(tuples_to_generate_base);
}

template<typename T, size_t partitions>
class OrchestratorWithSeparatePageManagers {
    std::atomic<bool> running = std::atomic(true);
    size_t written_tuples = 0;
    BatchedTupleGenerator<T> generator;
    OnDemandSingleThreadPageManager<T, partitions> page_manager{};
    unsigned num_threads;

public:
    explicit OrchestratorWithSeparatePageManagers(unsigned num_threads) : generator(SIZE_MAX), num_threads(num_threads) {
    }
    void run() {
        static constexpr unsigned buffer_base_value = 5 * 128;
        const static auto total_buffer_size = buffer_base_value * 1024 / (sizeof(T) * num_threads);
        auto buffer_index = 0u;
        std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);

        auto partition = 0ull;
        while (running.load()) {
            auto [ptr, size_of_batch] = generator.getBatchOfTuples();
            if (ptr == nullptr) {
                break;
            }

            for (size_t i = 0; i < size_of_batch; i++) {
                const auto &tuple = ptr[i];
                auto &index = buffer_index;
                if (index == total_buffer_size) {
                    page_manager.insert_buffer_of_tuples_batched(buffer.get(), total_buffer_size, partition);
                    index = 0;
                    written_tuples += total_buffer_size;
                    partition = (partition + 1) & (partitions - 1);
                }
                buffer[index] = tuple;
                ++buffer_index;
            }
        }
    }

    void stop() {
        running.store(false);
    }

    [[nodiscard]] size_t get_written_tuples() const {
        return written_tuples;
    }
};

template<typename TupleType, size_t partitions>
void print_benchmark_info(const std::chrono::milliseconds time_to_write_out, unsigned threads, size_t written_tuples, bool synchronised) {
    std::cout << "Benchmarking " << (synchronised ? "(synchronised)" : "(not-synchronised)") << " using " << partitions << " Partitions and " << threads << " Thread(s): "
              << "written " << sizeof(TupleType) << "B tuples: " << std::fixed << std::setprecision(2) << written_tuples / 1e6 << " Mio"
              << " (tuple-data: " << static_cast<double>(sizeof(TupleType)) * written_tuples / (1024.0 * 1024.0 * 1024.0) << " GiB"
              << ", slotted-page-data: " << static_cast<double>(sizeof(SlotInfo<TupleType>) + TupleType::get_size_of_variable_data()) * written_tuples / (1024.0 * 1024.0 * 1024.0) << " GiB"
              << ", avg: "
              << static_cast<double>(written_tuples) / (threads * (time_to_write_out.count() / 1e3) * 1e6) << " Mio/(thread+sec))"
              << " within " << time_to_write_out.count() << " ms" << std::endl;
}

template<typename TupleType, size_t partitions>
void benchmark_non_synchronised_write_out(const std::chrono::milliseconds time_to_write_out) {
    for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
        if (partitions == 1024 && threads > 64) {
            continue;
        }
        std::vector<std::jthread> threads_vector;
        std::deque<OrchestratorWithSeparatePageManagers<TupleType, partitions>> orchestrators;
        threads_vector.reserve(threads);
        for (unsigned j = 0; j < threads; ++j) {
            orchestrators.emplace_back(threads);
        }
        for (unsigned j = 0; j < threads; ++j) {
            threads_vector.emplace_back([&orchestrator = orchestrators[j]] {
                orchestrator.run();
            });
        }

        std::this_thread::sleep_for(time_to_write_out);
        for (auto &orchestrator: orchestrators) {
            orchestrator.stop();
        }

        for (auto &thread: threads_vector) {
            thread.join();
        }

        size_t written_tuples = 0;
        for (auto &orchestrator: orchestrators) {
            written_tuples += orchestrator.get_written_tuples();
        }
        print_benchmark_info<TupleType, partitions>(time_to_write_out, threads, written_tuples, false);

    }
    std::cout << std::endl;
}

template<typename T, size_t partitions>
class OrchestratorSinglePageManager {
    std::atomic<bool> running = std::atomic(true);
    size_t written_tuples = 0;
    BatchedTupleGenerator<T> generator;
    OnDemandPageManager<T, partitions> &page_manager{};
    unsigned num_threads;

public:
    explicit OrchestratorSinglePageManager(OnDemandPageManager<T, partitions> &page_manager, unsigned num_threads) : generator(SIZE_MAX), page_manager(page_manager), num_threads(num_threads) {
    }
    void run() {
        static constexpr unsigned buffer_base_value = 5 * 128;
        const static auto total_buffer_size = buffer_base_value * 1024 / (sizeof(T) * num_threads);
        auto buffer_index = 0u;
        std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);

        auto partition = 0ull;
        while (running.load()) {
            auto [ptr, size_of_batch] = generator.getBatchOfTuples();
            if (ptr == nullptr) {
                break;
            }

            for (size_t i = 0; i < size_of_batch; i++) {
                const auto &tuple = ptr[i];
                auto &index = buffer_index;
                if (index == total_buffer_size) {
                    page_manager.insert_buffer_of_tuples_batched(buffer.get(), total_buffer_size, partition);
                    index = 0;
                    written_tuples += total_buffer_size;
                    partition = (partition + 1) & (partitions - 1);
                }
                buffer[index] = tuple;
                ++buffer_index;
            }
        }
    }

    void stop() {
        running.store(false);
    }

    [[nodiscard]] size_t get_written_tuples() const {
        return written_tuples;
    }
};

template<typename TupleType, size_t partitions>
void benchmark_synchronised_write_out(const std::chrono::milliseconds time_to_write_out) {
    for (unsigned threads : {1, 2, 4, 8, 16, 32, 48, 64, 128}) {
        std::vector<std::jthread> threads_vector;
        OnDemandPageManager<TupleType, partitions> page_manager{};
        std::deque<OrchestratorSinglePageManager<TupleType, partitions>> orchestrators;
        threads_vector.reserve(threads);
        for (unsigned j = 0; j < threads; ++j) {
            orchestrators.emplace_back(page_manager, threads);
        }
        for (unsigned j = 0; j < threads; ++j) {
            threads_vector.emplace_back([&orchestrator = orchestrators[j]] {
                orchestrator.run();
            });
        }

        std::this_thread::sleep_for(time_to_write_out);
        for (auto &orchestrator: orchestrators) {
            orchestrator.stop();
        }

        for (auto &thread: threads_vector) {
            thread.join();
        }

        size_t written_tuples = 0;
        for (auto &orchestrator: orchestrators) {
            written_tuples += orchestrator.get_written_tuples();
        }
        print_benchmark_info<TupleType, partitions>(time_to_write_out, threads, written_tuples, true);

    }
    std::cout << std::endl;
}


int main() {
    constexpr unsigned tuples_to_generate_base = 5 * 40'000'000u;

    run_benchmark_on_all_implementations<Tuple4, 4>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple16, 4>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple100, 4>(tuples_to_generate_base);

    run_benchmark_on_all_implementations<Tuple4, 16>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple16, 16>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple100, 16>(tuples_to_generate_base);

    run_benchmark_on_all_implementations<Tuple4, 32>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple16, 32>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple100, 32>(tuples_to_generate_base);

    run_benchmark_on_all_implementations<Tuple4, 1024>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple16, 1024>(tuples_to_generate_base);
    run_benchmark_on_all_implementations<Tuple100, 1024>(tuples_to_generate_base);

    constexpr auto time_to_write_out = std::chrono::milliseconds(1000);
    benchmark_synchronised_write_out<Tuple4, 4>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple16, 4>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple100, 4>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple4, 16>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple16, 16>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple100, 16>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple4, 32>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple16, 32>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple100, 32>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple4, 1024>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple16, 1024>(time_to_write_out);
    benchmark_synchronised_write_out<Tuple100, 1024>(time_to_write_out);

    warmup_run<Tuple4>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple4, 4>(tuples_to_generate_base);
    warmup_run<Tuple16>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple16, 4>(tuples_to_generate_base);
    warmup_run<Tuple100>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple100, 4>(tuples_to_generate_base);

    warmup_run<Tuple4>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple4, 16>(tuples_to_generate_base);
    warmup_run<Tuple16>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple16, 16>(tuples_to_generate_base);
    warmup_run<Tuple100>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple100, 16>(tuples_to_generate_base);

    warmup_run<Tuple4>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple4, 32>(tuples_to_generate_base);
    warmup_run<Tuple16>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple16, 32>(tuples_to_generate_base);
    warmup_run<Tuple100>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple100, 32>(tuples_to_generate_base);

    benchmark_non_synchronised_write_out<Tuple4, 4>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple16, 4>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple100, 4>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple4, 16>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple16, 16>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple100, 16>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple4, 32>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple16, 32>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple100, 32>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple4, 1024>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple16, 1024>(time_to_write_out);
    benchmark_non_synchronised_write_out<Tuple100, 1024>(time_to_write_out);

    warmup_run<Tuple4>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple4, 1024>(tuples_to_generate_base);
    warmup_run<Tuple16>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple16, 1024>(tuples_to_generate_base);
    warmup_run<Tuple100>(tuples_to_generate_base / 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_TIME_MS));
    benchmark_LocalPagesAndMergeOrchestrator<Tuple100, 1024>(tuples_to_generate_base);

    return 0;
}

