#include "../external/perfevent/PerfEvent.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"

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

constexpr size_t batch_sizes[] = {8, 32, 512, 2048};
int main() {
    BenchmarkParameters params;
    for (size_t tuples_to_generate = 40'000'000u; tuples_to_generate <= 40'000'000u; tuples_to_generate *= 10) {
        tuples_to_generate = tuples_to_generate * (sizeof(Tuple100) / sizeof(Tuple16));
        params.setParam("B-Tuples", tuples_to_generate);
        // Tuple16 benchmarks for all batch sizes
        params.setParam("C-tuple_size", sizeof(Tuple16));
        params.setParam("D-batch_size", batch_sizes[0]);
        // Batch size 0
        {
            params.setParam("A-Benchmark tuple-generator", "single tuple");
            PerfEventBlock e(1'000'000, params, true);
            benchmark_non_batched<Tuple16, batch_sizes[0]>(tuples_to_generate);
        }

        {
            params.setParam("A-Benchmark tuple-generator", "tuple batch");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_batched<Tuple16, batch_sizes[0]>(tuples_to_generate);
        }

        // Batch size 1
        params.setParam("D-batch_size", batch_sizes[1]);
        {
            params.setParam("A-Benchmark tuple-generator", "single tuple");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_non_batched<Tuple16, batch_sizes[1]>(tuples_to_generate);
        }

        {
            params.setParam("A-Benchmark tuple-generator", "tuple batch");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_batched<Tuple16, batch_sizes[1]>(tuples_to_generate);
        }

        // Batch size 2
        params.setParam("D-batch_size", batch_sizes[2]);
        {
            params.setParam("A-Benchmark tuple-generator", "single tuple");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_non_batched<Tuple16, batch_sizes[2]>(tuples_to_generate);
        }

        {
            params.setParam("A-Benchmark tuple-generator", "tuple batch");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_batched<Tuple16, batch_sizes[2]>(tuples_to_generate);
        }

        // Batch size 3
        params.setParam("D-batch_size", batch_sizes[3]);
        {
            params.setParam("A-Benchmark tuple-generator", "single tuple");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_non_batched<Tuple16, batch_sizes[3]>(tuples_to_generate);
        }

        {
            params.setParam("A-Benchmark tuple-generator", "tuple batch");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_batched<Tuple16, batch_sizes[3]>(tuples_to_generate);
        }

        // Batch size 4
        // params.setParam("D-batch_size", batch_sizes[4]);
        // {
        //     params.setParam("A-Benchmark tuple-generator", "single tuple");
        //     PerfEventBlock e(1'000'000, params, false);
        //     benchmark_non_batched<Tuple16, batch_sizes[4]>(tuples_to_generate);
        // }
        //
        // {
        //     params.setParam("A-Benchmark tuple-generator", "tuple batch");
        //     PerfEventBlock e(1'000'000, params, false);
        //     benchmark_batched<Tuple16, batch_sizes[4]>(tuples_to_generate);
        // }
        //
        // // Batch size 5
        // params.setParam("D-batch_size", batch_sizes[5]);
        // {
        //     params.setParam("A-Benchmark tuple-generator", "single tuple");
        //     PerfEventBlock e(1'000'000, params, false);
        //     benchmark_non_batched<Tuple16, batch_sizes[5]>(tuples_to_generate);
        // }
        //
        // {
        //     params.setParam("A-Benchmark tuple-generator", "tuple batch");
        //     PerfEventBlock e(1'000'000, params, false);
        //     benchmark_batched<Tuple16, batch_sizes[5]>(tuples_to_generate);
        // }

        // Tuple100 benchmarks for all batch sizes
        params.setParam("C-tuple_size", sizeof(Tuple100));
        params.setParam("D-batch_size", batch_sizes[0]);
        tuples_to_generate = tuples_to_generate / (sizeof(Tuple100) / sizeof(Tuple16));
        params.setParam("B-Tuples", tuples_to_generate);
        // Batch size 0
        {
            params.setParam("A-Benchmark tuple-generator", "single tuple");
            PerfEventBlock e(1'000'000, params, true);
            benchmark_non_batched<Tuple100, batch_sizes[0]>(tuples_to_generate);
        }

        {
            params.setParam("A-Benchmark tuple-generator", "tuple batch");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_batched<Tuple100, batch_sizes[0]>(tuples_to_generate);
        }

        // Batch size 1
        params.setParam("D-batch_size", batch_sizes[1]);
        {
            params.setParam("A-Benchmark tuple-generator", "single tuple");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_non_batched<Tuple100, batch_sizes[1]>(tuples_to_generate);
        }

        {
            params.setParam("A-Benchmark tuple-generator", "tuple batch");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_batched<Tuple100, batch_sizes[1]>(tuples_to_generate);
        }

        // Batch size 2
        params.setParam("D-batch_size", batch_sizes[2]);
        {
            params.setParam("A-Benchmark tuple-generator", "single tuple");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_non_batched<Tuple100, batch_sizes[2]>(tuples_to_generate);
        }

        {
            params.setParam("A-Benchmark tuple-generator", "tuple batch");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_batched<Tuple100, batch_sizes[2]>(tuples_to_generate);
        }

        // Batch size 3
        params.setParam("D-batch_size", batch_sizes[3]);
        {
            params.setParam("A-Benchmark tuple-generator", "single tuple");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_non_batched<Tuple100, batch_sizes[3]>(tuples_to_generate);
        }

        {
            params.setParam("A-Benchmark tuple-generator", "tuple batch");
            PerfEventBlock e(1'000'000, params, false);
            benchmark_batched<Tuple100, batch_sizes[3]>(tuples_to_generate);
        }

        // Batch size 4
        // params.setParam("D-batch_size", batch_sizes[4]);
        // {
        //     params.setParam("A-Benchmark tuple-generator", "single tuple");
        //     PerfEventBlock e(1'000'000, params, false);
        //     benchmark_non_batched<Tuple100, batch_sizes[4]>(tuples_to_generate);
        // }
        //
        // {
        //     params.setParam("A-Benchmark tuple-generator", "tuple batch");
        //     PerfEventBlock e(1'000'000, params, false);
        //     benchmark_batched<Tuple100, batch_sizes[4]>(tuples_to_generate);
        // }
        //
        // // Batch size 5
        // params.setParam("D-batch_size", batch_sizes[5]);
        // {
        //     params.setParam("A-Benchmark tuple-generator", "single tuple");
        //     PerfEventBlock e(1'000'000, params, false);
        //     benchmark_non_batched<Tuple100, batch_sizes[5]>(tuples_to_generate);
        // }
        //
        // {
        //     params.setParam("A-Benchmark tuple-generator", "tuple batch");
        //     PerfEventBlock e(1'000'000, params, false);
        //     benchmark_batched<Tuple100, batch_sizes[5]>(tuples_to_generate);
        // }
    }
}
