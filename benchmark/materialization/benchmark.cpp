#include "../external/perfevent/PerfEvent.hpp"
#include "radix/materialization/ContiniousMaterialization.hpp"
#include "tuple-types/tuple-types.hpp"
#include "util/get_tuple_num_scaling_value.hpp"

template<typename T, size_t batch_size>
void benchmark_materialization(size_t tuples_to_generate) {
    ContinuousMaterialization<T, batch_size> materialization(tuples_to_generate);
    materialization.materialize();
    auto data = materialization.get_data();
    if (data == nullptr) {
        std::cerr << "Error: Data is null\n";
    }
}

constexpr size_t batch_sizes[] = {8, 32, 512, 2048};
int main() {
    BenchmarkParameters params;
    for (unsigned tuples_to_generate_base = 40'000'000u; tuples_to_generate_base <= 40'000'000u; tuples_to_generate_base *= 10) {
        params.setParam("A-Benchmark tuple-materialization", "ContinuousMaterialization");
        auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * get_tuple_num_scaling_value<Tuple16>());
        params.setParam("B-Tuples", tuples_to_generate);
        params.setParam("C-batch_size", sizeof(Tuple16));
        {
            params.setParam("D-batch_size", batch_sizes[0]);
            PerfEventBlock e(1'000'000, params, true);
            benchmark_materialization<Tuple16, batch_sizes[0]>(tuples_to_generate);
        }

        {
            params.setParam("D-batch_size", batch_sizes[1]);
            PerfEventBlock e(1'000'000, params, false);
            benchmark_materialization<Tuple16, batch_sizes[1]>(tuples_to_generate);
        }
        {
            params.setParam("D-batch_size", batch_sizes[2]);
            PerfEventBlock e(1'000'000, params, false);
            benchmark_materialization<Tuple16, batch_sizes[2]>(tuples_to_generate);
        }
        {
            params.setParam("D-batch_size", batch_sizes[3]);
            PerfEventBlock e(1'000'000, params, false);
            benchmark_materialization<Tuple16, batch_sizes[3]>(tuples_to_generate);
        }

        tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * get_tuple_num_scaling_value<Tuple100>());
        params.setParam("B-Tuples", tuples_to_generate);
        params.setParam("C-batch_size", sizeof(Tuple100));
        {
            params.setParam("D-batch_size", batch_sizes[0]);
            PerfEventBlock e(1'000'000, params, true);
            benchmark_materialization<Tuple100, batch_sizes[0]>(tuples_to_generate);
        }

        {
            params.setParam("D-batch_size", batch_sizes[1]);
            PerfEventBlock e(1'000'000, params, false);
            benchmark_materialization<Tuple100, batch_sizes[1]>(tuples_to_generate);
        }
        {
            params.setParam("D-batch_size", batch_sizes[2]);
            PerfEventBlock e(1'000'000, params, false);
            benchmark_materialization<Tuple100, batch_sizes[2]>(tuples_to_generate);
        }
        {
            params.setParam("D-batch_size", batch_sizes[3]);
            PerfEventBlock e(1'000'000, params, false);
            benchmark_materialization<Tuple100, batch_sizes[3]>(tuples_to_generate);
        }
        tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * get_tuple_num_scaling_value<Tuple4>());
        params.setParam("B-Tuples", tuples_to_generate);
        params.setParam("C-batch_size", sizeof(Tuple4));
        {
            params.setParam("D-batch_size", batch_sizes[0]);
            PerfEventBlock e(1'000'000, params, true);
            benchmark_materialization<Tuple4, batch_sizes[0]>(tuples_to_generate);
        }

        {
            params.setParam("D-batch_size", batch_sizes[1]);
            PerfEventBlock e(1'000'000, params, false);
            benchmark_materialization<Tuple4, batch_sizes[1]>(tuples_to_generate);
        }
        {
            params.setParam("D-batch_size", batch_sizes[2]);
            PerfEventBlock e(1'000'000, params, false);
            benchmark_materialization<Tuple4, batch_sizes[2]>(tuples_to_generate);
        }
        {
            params.setParam("D-batch_size", batch_sizes[3]);
            PerfEventBlock e(1'000'000, params, false);
            benchmark_materialization<Tuple4, batch_sizes[3]>(tuples_to_generate);
        }
    }
    return 0;
}
