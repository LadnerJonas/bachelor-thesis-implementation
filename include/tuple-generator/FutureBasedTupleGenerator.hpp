#pragma once

#pragma once

#include <cassert>
#include <future>
#include <immintrin.h>
#include <memory>
#include <random>
#include <vector>

template<typename T, size_t batch_size = 2048>
class FutureBasedTupleGenerator {
    alignas(32) T batch[batch_size];// 32-byte aligned for AVX2
    size_t max_generated_tuples;
    size_t generated_tuples = 0;
    size_t current_batch_index = 0;

    std::mt19937_64 gen;
    std::future<void> batch_future;// Background task for batch generation

    // Helper function to generate the batch of tuples
#include <future>
#include <vector>

    void generateBatch() {
        constexpr size_t num_of_8_bytes = sizeof(T) * batch_size / sizeof(uint64_t);
        auto *p = reinterpret_cast<uint64_t *>(&batch);
        assert(reinterpret_cast<std::uintptr_t>(p) % 32 == 0 && "Pointer not 32-byte aligned!");

        size_t i = 0;

#ifdef __AVX2__
        // Divide the workload into chunks
        constexpr size_t num_threads = 4;// Adjust based on your hardware
        const size_t chunk_size = num_of_8_bytes / num_threads;

        std::vector<std::future<void>> futures;

        for (size_t t = 0; t < num_threads; ++t) {
            size_t start_index = t * chunk_size;
            size_t end_index = (t == num_threads - 1) ? num_of_8_bytes : (t + 1) * chunk_size;

            // Launch a task for each chunk using async (or use a custom thread pool)
            futures.push_back(std::async(std::launch::async, [this, p, start_index, end_index]() {
                std::mt19937_64 thread_gen(this->gen());// Each thread needs its own generator

                size_t i = start_index;
                for (; i + 3 < end_index; i += 4) {
                    __m256i random_numbers = _mm256_set_epi64x(thread_gen(), thread_gen(), thread_gen(), thread_gen());
                    _mm256_store_si256(reinterpret_cast<__m256i *>(&p[i]), random_numbers);// Aligned store
                }

                for (; i < end_index; ++i) {
                    p[i] = thread_gen();
                }
            }));
        }

        // Wait for all threads to finish
        for (auto &f: futures) {
            f.get();
        }
#else
        // Fallback for non-AVX2 systems (serial execution)
        for (; i < num_of_8_bytes; ++i) {
            p[i] = gen();
        }
#endif

        current_batch_index = 0;
    }


public:
    FutureBasedTupleGenerator(const size_t max_generated_tuples, const uint64_t seed)
        : max_generated_tuples(max_generated_tuples), gen(seed) {
        // Immediately launch the first batch generation in the background
        batch_future = std::async(std::launch::async, &FutureBasedTupleGenerator::generateBatch, this);
    }

    // Retrieve a single tuple. It waits for the future if a new batch is needed.
    auto getTuple() -> std::unique_ptr<T> {
        if (generated_tuples >= max_generated_tuples) {
            return std::unique_ptr<T>(nullptr);
        }

        // Check if the batch is depleted
        if (current_batch_index >= batch_size) {
            // Wait for the next batch to be ready
            if (batch_future.valid()) {
                batch_future.get();
            }
            // Launch next batch in the background
            batch_future = std::async(std::launch::async, &FutureBasedTupleGenerator::generateBatch, this);
        }

        generated_tuples++;
        return std::make_unique<T>(batch[current_batch_index++]);
    }

    // Retrieve a batch of tuples (similar to getTuple but for an entire batch).
    auto getBatchOfTuples() -> std::pair<std::unique_ptr<T[]>, size_t> {
        if (generated_tuples >= max_generated_tuples) {
            return {std::unique_ptr<T[]>(nullptr), 0};
        }

        // Wait for the current batch if needed
        if (current_batch_index != 0) {
            if (batch_future.valid()) {
                batch_future.get();
            }
        }

        // Calculate the size of the batch to return
        const auto length_of_batch = std::min(batch_size, max_generated_tuples - generated_tuples);
        current_batch_index = length_of_batch;

        // Return the current batch
        std::unique_ptr<T[]> batch_ptr = std::make_unique<T[]>(length_of_batch);
        std::copy(batch, batch + length_of_batch, batch_ptr.get());
        generated_tuples += length_of_batch;

        // Launch the next batch generation in the background
        batch_future = std::async(std::launch::async, &FutureBasedTupleGenerator::generateBatch, this);

        return std::make_pair(std::move(batch_ptr), length_of_batch);
    }

    auto static getBatchSize() {
        return batch_size;
    }
};
