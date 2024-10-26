#pragma once
#include "tuple-generator/BatchedTupleGenerator.hpp"

#include <mutex>

template<typename T>
class MorselCreator {
    BatchedTupleGenerator<T> generator;
    std::mutex mutex;

public:
    explicit MorselCreator(size_t num_tuples) : generator(num_tuples) {
    }

    auto getBatchOfTuples() -> std::pair<std::unique_ptr<T[]>, size_t> {
        std::unique_lock lock(mutex);
        return generator.getBatchOfTuples();
    }
};