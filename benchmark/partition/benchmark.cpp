#include "../external/perfevent/PerfEvent.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "tuple-types/tuple-types.hpp"
#include "util/get_tuple_num_scaling_value.hpp"
#include "util/partitioning_function.hpp"

struct BenchmarkParameters;
constexpr size_t SEED = 42;

template<typename T, size_t partitions>
void benchmark_simple(size_t tuples_to_generate) {
    BatchedTupleGenerator<T> generator(tuples_to_generate, SEED);
    std::array<unsigned, partitions> buffer_count = {};

    while (true) {
        const auto [ptr, size_of_batch] = generator.getBatchOfTuples();
        if (ptr == nullptr) {
            break;
        }
        for (size_t i = 0; i < size_of_batch; i++) {
            const auto &tuple = ptr[i];
            const auto partition = partition_function<T, partitions>(tuple);
            ++buffer_count[partition];
        }
    }
    size_t actual_tuples = 0;
    for (auto tuples: buffer_count) {
        actual_tuples += tuples;
    }
    if (actual_tuples != tuples_to_generate) {
        std::cerr << "Error: Seen tuples does not match tuples to generate\n";
    }
}

template<typename T, size_t partitions>
void benchmark_simple_vec(size_t tuples_to_generate) {
    BatchedTupleGenerator<T> generator(tuples_to_generate, SEED);
    std::array<std::vector<unsigned>, partitions> buffer_vecs = {};
    std::array<unsigned, partitions> buffer_count = {};
    for (size_t i = 0; i < partitions; i++) {
        buffer_vecs[i].reserve((2048+partitions)/partitions);
    }
    while (true) {
        const auto [ptr, size_of_batch] = generator.getBatchOfTuples();
        if (ptr == nullptr) {
            break;
        }
        for (size_t i = 0; i < size_of_batch; i++) {
            const auto &tuple = ptr[i];
            const auto partition = partition_function<T, partitions>(tuple);
            buffer_vecs[partition].emplace_back(i);
        }
        for (size_t i = 0; i < partitions; i++) {
            buffer_count[i] += buffer_vecs[i].size();
            buffer_vecs[i].clear();
        }
    }
    size_t actual_tuples = 0;
    for (auto tuples: buffer_count) {
        actual_tuples += tuples;
    }
    if (actual_tuples != tuples_to_generate) {
        std::cerr << "Error: Seen tuples does not match tuples to generate\n";
    }
}

template<typename T, size_t partitions>
void benchmark_simple_array(size_t tuples_to_generate) {
    BatchedTupleGenerator<T> generator(tuples_to_generate, SEED);
    std::array<std::vector<unsigned>, partitions> buffer_array = {};
    std::array<unsigned, partitions> buffer_count = {};
    std::array<unsigned, partitions> total_count = {};
    for (size_t i = 0; i < partitions; i++) {
        buffer_array[i].resize(2048);
    }
    while (true) {
        const auto [ptr, size_of_batch] = generator.getBatchOfTuples();
        if (ptr == nullptr) {
            break;
        }
        for (size_t i = 0; i < size_of_batch; i++) {
            const auto &tuple = ptr[i];
            const auto partition = partition_function<T, partitions>(tuple);
            if (partition >= partitions || buffer_count[partition] >= 2048) {
                std::cerr << "Error: Buffer overflow\n";
                std::cerr << partition << " " << buffer_count[partition] << " " << i << std::endl;
            }
            buffer_array[partition][buffer_count[partition]] = i;

            ++buffer_count[partition];
        }
        for (size_t i = 0; i < partitions; i++) {
            total_count[i] += buffer_count[i];
            buffer_count[i] = 0;
        }
    }
    size_t actual_tuples = 0;
    for (auto tuples: total_count) {
        actual_tuples += tuples;
    }
    if (actual_tuples != tuples_to_generate) {
        std::cerr << buffer_array[0][0] << std::endl;
        std::cerr << "Error: Seen tuples does not match tuples to generate\n";
    }
}

template<typename T, size_t partitions>
void benchmark_simd(size_t tuples_to_generate) {
    BatchedTupleGenerator<T> generator(tuples_to_generate, SEED);
    std::array<unsigned, partitions> buffer_count = {};

    while (true) {
        const auto [ptr, size_of_batch] = generator.getBatchOfTuples();
        if (ptr == nullptr) {
            break;
        }
        size_t i = 0;
        for (; i + 4 <= size_of_batch; i += 4) {
            __m128i partitions_result = partition_function_simd<T, partitions>(ptr.get() + i);
            for (size_t j = 0; j < 4; j++) {
                ++buffer_count[_mm_extract_epi32(partitions_result, j)];
            }
        }

        for (; i < size_of_batch; i++) {
            const auto &tuple = ptr[i];
            const auto partition = partition_function<T, partitions>(tuple);
            ++buffer_count[partition];
        }
    }
    size_t actual_tuples = 0;
    for (auto tuples: buffer_count) {
        actual_tuples += tuples;
    }
    if (actual_tuples != tuples_to_generate) {
        std::cerr << "Error: Seen tuples does not match tuples to generate\n";
    }
}



template<typename T, size_t partitions>
void run_benchmarks(BenchmarkParameters &params, size_t tuples_to_generate_base) {
    // Set batch size parameter
    const auto tuples_to_generate = static_cast<unsigned>(static_cast<double>(tuples_to_generate_base) * get_tuple_num_scaling_value<T>());
    params.setParam("B-Tuples", tuples_to_generate);

    params.setParam("C-tuple_size", sizeof(T));
    params.setParam("D-partitions", partitions);
    // Run tuple batch benchmark
    params.setParam("A-Benchmark partition", "simple");
    {
        PerfEventBlock e(100'000, params, true);
        benchmark_simple<T, partitions>(tuples_to_generate);
    }
    params.setParam("A-Benchmark partition", "simple_vec");
    {
        PerfEventBlock e(100'000, params, false);
        benchmark_simple_vec<T, partitions>(tuples_to_generate);
    }
    params.setParam("A-Benchmark partition", "simple_array");
    {
        PerfEventBlock e(100'000, params, false);
        benchmark_simple_array<T, partitions>(tuples_to_generate);
    }
    params.setParam("A-Benchmark partition", "simd");
    {
        PerfEventBlock e(100'000, params, false);
        benchmark_simd<T, partitions>(tuples_to_generate);
    }

}


int main() {
    BenchmarkParameters params;
    for (size_t tuples_to_generate_base = 40'000'000u; tuples_to_generate_base <= 40'000'000u; tuples_to_generate_base *= 2) {
        run_benchmarks<Tuple16, 32>(params, tuples_to_generate_base);
        run_benchmarks<Tuple16, 1024>(params, tuples_to_generate_base);

        run_benchmarks<Tuple100, 32>(params, tuples_to_generate_base);
        run_benchmarks<Tuple100, 1024>(params, tuples_to_generate_base);

        run_benchmarks<Tuple4, 32>(params, tuples_to_generate_base);
        run_benchmarks<Tuple4, 1024>(params, tuples_to_generate_base);
    }
    return 0;
}
