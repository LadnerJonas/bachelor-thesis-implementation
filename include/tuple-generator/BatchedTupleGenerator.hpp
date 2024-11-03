#pragma once

#include <cassert>
#include <immintrin.h>
#include <iostream>
#include <memory>
#include <random>

template<typename T, size_t batch_size = 2048>
class BatchedTupleGenerator {
    alignas(32) T batch[batch_size];
    size_t max_generated_tuples;
    size_t generated_tuples = 0;
    size_t current_batch_index = 0;

    std::mt19937_64 gen;

public:
    explicit BatchedTupleGenerator(const size_t max_generated_tuples, const uint64_t seed = 42) : max_generated_tuples(max_generated_tuples), gen(seed) {
        generateBatchOfTuples();
    }

    void generateBatchOfTuples() {
        constexpr size_t num_of_4_bytes = sizeof(T) * batch_size / sizeof(uint64_t);
        size_t i = 0;
        auto *p = reinterpret_cast<uint64_t *>(&batch);
        assert(reinterpret_cast<std::uintptr_t>(p) % 32 == 0 && "Pointer not 32-byte aligned!");

#ifdef __AVX2__
        for (; i + 3 < num_of_4_bytes; i += 4) {
            __m256i random_numbers = _mm256_set_epi64x(gen(), gen(), gen(), gen());
            _mm256_storeu_si256(reinterpret_cast<__m256i *>(&p[i]), random_numbers);
        }
#endif
        for (; i < num_of_4_bytes; ++i) {
            p[i] = gen();
        }

        current_batch_index = 0;
    }


    auto getTuple() -> std::unique_ptr<T> {
        if (generated_tuples >= max_generated_tuples) {
            return std::unique_ptr<T>(nullptr);
        }

        if (current_batch_index >= batch_size) {
            generateBatchOfTuples();
        }
        generated_tuples++;
        return std::make_unique<T>(batch[current_batch_index++]);
    }
    auto getBatchOfTuples() -> std::pair<std::unique_ptr<T[]>, size_t> {
        if (generated_tuples >= max_generated_tuples) {
            return {std::unique_ptr<T[]>(nullptr), 0};
        }
        generateBatchOfTuples();

        const auto length_of_batch = std::min(batch_size, max_generated_tuples - generated_tuples);

        std::unique_ptr<T[]> batch_ptr = std::make_unique<T[]>(length_of_batch);
        std::copy(batch, batch + length_of_batch, batch_ptr.get());
        generated_tuples += length_of_batch;

        return std::make_pair(std::move(batch_ptr), length_of_batch);
    }

    auto getBatchOfTuples(size_t max_tuples_generate) -> std::pair<std::unique_ptr<T[]>, size_t> {
        if (generated_tuples >= max_generated_tuples) {
            return {std::unique_ptr<T[]>(nullptr), 0};
        }
        generateBatchOfTuples();

        const auto length_of_batch = std::min(std::min(batch_size, max_generated_tuples - generated_tuples), max_tuples_generate);

        std::unique_ptr<T[]> batch_ptr = std::make_unique<T[]>(length_of_batch);
        std::copy(batch, batch + length_of_batch, batch_ptr.get());
        generated_tuples += length_of_batch;

        return std::make_pair(std::move(batch_ptr), length_of_batch);
    }
    auto static getBatchSize() {
        return batch_size;
    }
};