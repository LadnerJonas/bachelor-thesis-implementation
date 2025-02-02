#pragma once
#include <tuple-generator/BatchedTupleGenerator.hpp>

template<typename T, size_t batch_size = 2048>
class ContinuousMaterialization {
    std::shared_ptr<T[]> data;
    size_t num_tuples;
    size_t num_threads;

public:
    explicit ContinuousMaterialization(const size_t num_tuples, const unsigned num_threads) : data(std::make_shared<T[]>(num_tuples)), num_tuples(num_tuples), num_threads(num_threads) {}
    void materialize() {
        size_t current_index = 0;
        std::vector<std::jthread> threads;
        threads.reserve(num_threads);
        for (unsigned i = 0; i < num_threads; i++) {
            const size_t current_thread_tuples_to_generate = num_tuples / num_threads + (i < num_tuples % num_threads);
            threads.emplace_back([this, current_thread_tuples_to_generate, current_index] {
                auto local_index = current_index;
                BatchedTupleGenerator<T, batch_size> generator(current_thread_tuples_to_generate);
                for (size_t j = 0; j < current_thread_tuples_to_generate;) {
                    auto [batch, local_batch_size] = generator.getBatchOfTuples();
                    std::memcpy(data.get() + local_index, batch.get(), local_batch_size * sizeof(T));
                    local_index += local_batch_size;
                    j += local_batch_size;
                }
            });
            current_index += current_thread_tuples_to_generate;
        }
    }
    auto get_data() -> std::shared_ptr<T[]> {
        return data;
    }
};