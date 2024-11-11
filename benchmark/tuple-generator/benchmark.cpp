#include "../external/perfevent/PerfEvent.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"
#include "util/get_tuple_num_scaling_value.hpp"

constexpr size_t SEED = 42;

template<typename T, size_t batch_size>
void benchmark_batched(size_t tuples_to_generate) {
    BatchedTupleGenerator<T, batch_size> generator(tuples_to_generate, SEED);
    size_t seen_tuples = 0;
    while (true) {
        const auto [ptr, size_of_batch] = generator.getBatchOfTuples();
        if (ptr == nullptr) {
            break;
        }
        for (size_t i = 0; i < size_of_batch; i++) {
            seen_tuples++;
        }
    }
    if (seen_tuples != tuples_to_generate) {
        std::cerr << "Error: Seen tuples does not match tuples to generate\n";
    }
}
template<typename T, size_t batch_size>
void benchmark_non_batched(size_t tuples_to_generate) {
    BatchedTupleGenerator<T, batch_size> generator(tuples_to_generate, SEED);
    size_t seen_tuples = 0;
    while (true) {
        const auto tuple = generator.getTuple();
        if (tuple == nullptr) {
            break;
        }
        seen_tuples++;
    }
    if (seen_tuples != tuples_to_generate) {
        std::cerr << "Error: Seen tuples does not match tuples to generate\n";
    }
}

template<typename T, size_t batch_size>
void run_benchmarks(BenchmarkParameters &params, size_t tuples_to_generate, bool print_header = false) {
    // Set batch size parameter
    params.setParam("D-batch_size", batch_size);

    // Run single tuple benchmark
    params.setParam("A-Benchmark tuple-generator", "single tuple");
    {
        PerfEventBlock e(100'000, params, print_header);
        benchmark_non_batched<T, batch_size>(tuples_to_generate);
    }

    // Run tuple batch benchmark
    params.setParam("A-Benchmark tuple-generator", "tuple batch");
    {
        PerfEventBlock e(100'000, params, false);
        benchmark_batched<T, batch_size>(tuples_to_generate);
    }
}
constexpr size_t batch_sizes[] = {32, 1024, 2048, 4096};
int main() {
    BenchmarkParameters params;
    for (size_t tuples_to_generate_base = 40'000'000u; tuples_to_generate_base <= 40'000'000u; tuples_to_generate_base *= 10) {
        auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * get_tuple_num_scaling_value<Tuple16>());
        params.setParam("B-Tuples", tuples_to_generate);
        // Tuple16 benchmarks for all batch sizes
        params.setParam("C-tuple_size", sizeof(Tuple16));
        run_benchmarks<Tuple16, batch_sizes[0]>(params, tuples_to_generate, true);
        run_benchmarks<Tuple16, batch_sizes[1]>(params, tuples_to_generate);
        run_benchmarks<Tuple16, batch_sizes[2]>(params, tuples_to_generate);
        run_benchmarks<Tuple16, batch_sizes[3]>(params, tuples_to_generate);

        // Tuple100 benchmarks for all batch sizes
        tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * get_tuple_num_scaling_value<Tuple100>());
        params.setParam("B-Tuples", tuples_to_generate);
        params.setParam("C-tuple_size", sizeof(Tuple100));
        run_benchmarks<Tuple100, batch_sizes[0]>(params, tuples_to_generate, true);
        run_benchmarks<Tuple100, batch_sizes[1]>(params, tuples_to_generate);
        run_benchmarks<Tuple100, batch_sizes[2]>(params, tuples_to_generate);
        run_benchmarks<Tuple100, batch_sizes[3]>(params, tuples_to_generate);

        // Tuple4 benchmarks for all batch sizes
        tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * get_tuple_num_scaling_value<Tuple4>());
        params.setParam("B-Tuples", tuples_to_generate);
        params.setParam("C-tuple_size", sizeof(Tuple4));
        run_benchmarks<Tuple4, batch_sizes[0]>(params, tuples_to_generate, true);
        run_benchmarks<Tuple4, batch_sizes[1]>(params, tuples_to_generate);
        run_benchmarks<Tuple4, batch_sizes[2]>(params, tuples_to_generate);
        run_benchmarks<Tuple4, batch_sizes[3]>(params, tuples_to_generate);
    }
}
