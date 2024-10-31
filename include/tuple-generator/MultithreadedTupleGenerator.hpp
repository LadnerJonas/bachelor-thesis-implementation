#pragma once

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <immintrin.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

template<typename T, size_t batch_size = 2048>
class MultithreadedTupleGenerator {
    alignas(32) T batch[batch_size];// 32-byte aligned for AVX2
    size_t max_generated_tuples;
    std::atomic<size_t> generated_tuples{0};
    std::atomic<size_t> current_batch_index{0};

    std::vector<std::mt19937_64> generators;// One generator per thread
    std::mutex batch_mutex;

public:
    MultithreadedTupleGenerator(const size_t max_generated_tuples, const uint64_t seed, size_t num_threads = std::thread::hardware_concurrency())
        : max_generated_tuples(max_generated_tuples) {
        generators.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            generators.emplace_back(seed + i);// Different seed per thread
        }
        generateBatchOfTuples(num_threads);
    }

    // Generates a batch of tuples in parallel
    void generateBatchOfTuples(size_t num_threads) {
        constexpr size_t num_of_8_bytes = sizeof(T) * batch_size / sizeof(uint64_t);
        auto *p = reinterpret_cast<uint64_t *>(&batch);
        assert(reinterpret_cast<std::uintptr_t>(p) % 32 == 0 && "Pointer not 32-byte aligned!");

        size_t chunk_size = num_of_8_bytes / num_threads;

        std::vector<std::thread> threads;

        // Spawn threads to generate random numbers in parallel
        for (size_t t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                size_t start = t * chunk_size;
                size_t end = (t == num_threads - 1) ? num_of_8_bytes : start + chunk_size;

#ifdef __AVX2__
                for (size_t i = start; i + 3 < end; i += 4) {
                    __m256i random_numbers = _mm256_set_epi64x(generators[t](), generators[t](), generators[t](), generators[t]());
                    _mm256_store_si256(reinterpret_cast<__m256i *>(&p[i]), random_numbers);// Aligned store
                }
#endif
                for (size_t i = start; i < end; ++i) {
                    p[i] = generators[t]();
                }
            });
        }

        // Join threads
        for (auto &thread: threads) {
            thread.join();
        }

        current_batch_index.store(0, std::memory_order_release);
    }

    // Thread-safe tuple retrieval
    auto getTuple() -> std::unique_ptr<T> {
        if (generated_tuples.load(std::memory_order_acquire) >= max_generated_tuples) {
            return std::unique_ptr<T>(nullptr);
        }

        std::lock_guard<std::mutex> lock(batch_mutex);

        if (current_batch_index.load(std::memory_order_acquire) >= batch_size) {
            generateBatchOfTuples(std::thread::hardware_concurrency());
        }

        generated_tuples.fetch_add(1, std::memory_order_acq_rel);
        return std::make_unique<T>(batch[current_batch_index.fetch_add(1, std::memory_order_acq_rel)]);
    }

    // Thread-safe batch retrieval
    auto getBatchOfTuples() -> std::pair<std::unique_ptr<T[]>, size_t> {
        if (generated_tuples.load(std::memory_order_acquire) >= max_generated_tuples) {
            return {std::unique_ptr<T[]>(nullptr), 0};
        }

        std::lock_guard<std::mutex> lock(batch_mutex);

        if (current_batch_index.load(std::memory_order_acquire) != 0) {
            generateBatchOfTuples(std::thread::hardware_concurrency());
        }

        const auto length_of_batch = std::min(batch_size, max_generated_tuples - generated_tuples.load(std::memory_order_acquire));
        current_batch_index.store(length_of_batch, std::memory_order_release);

        std::unique_ptr<T[]> batch_ptr = std::make_unique<T[]>(length_of_batch);
        std::copy(batch, batch + length_of_batch, batch_ptr.get());
        generated_tuples.fetch_add(length_of_batch, std::memory_order_acq_rel);

        return std::make_pair(std::move(batch_ptr), length_of_batch);
    }

    auto static getBatchSize() {
        return batch_size;
    }
};
