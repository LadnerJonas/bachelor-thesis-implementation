#pragma once

#include "tuple-generator/BatchedTupleGenerator.hpp"

#include <mutex>

template<typename T>
class ChunkCreator {
    BatchedTupleGenerator<T> generator;
    std::mutex mutex;
    size_t generated_tuples = 0;
    size_t tuples_to_generate;

public:
    explicit ChunkCreator(size_t num_tuples) : generator(num_tuples), tuples_to_generate(num_tuples) {
    }

    auto getChunkOfTuples(size_t max_tuple_count_in_chunk) -> std::pair<std::unique_ptr<T[]>, size_t> {
        std::lock_guard lock(mutex);
        const size_t tuples_left = tuples_to_generate - generated_tuples;
        const size_t tuples_to_generate_now = std::min(max_tuple_count_in_chunk, tuples_left);

        std::unique_ptr<T[]> chunk = std::make_unique<T[]>(tuples_to_generate_now);
        for (size_t i = 0; i < tuples_to_generate_now;) {
            const auto [batch, count] = generator.getBatchOfTuples(tuples_to_generate_now - i);
            assert(!(count == 0 || batch == nullptr));
            std::copy(batch.get(), batch.get() + count, chunk.get() + i);
            i += count;
        }
        tuples_to_generate -= tuples_to_generate_now;

        return std::make_pair(std::move(chunk), tuples_to_generate_now);
    }
};