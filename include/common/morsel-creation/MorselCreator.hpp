#pragma once
#include "tuple-generator/BatchedTupleGenerator.hpp"

#include <mutex>

template<typename T, size_t batch_size = 2048>
class MorselCreator {
    BatchedTupleGenerator<T, batch_size> generator;
    std::mutex mutex;

public:
    explicit MorselCreator(size_t num_tuples) : generator(num_tuples) {
    }

    auto getBatchOfTuples() -> std::pair<std::unique_ptr<T[]>, size_t> {
        std::lock_guard lock(mutex);
        return generator.getBatchOfTuples();
    }
};