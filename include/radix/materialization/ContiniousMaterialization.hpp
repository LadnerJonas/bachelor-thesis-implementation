#pragma once
#include <tuple-generator/BatchedTupleGenerator.hpp>
#include <tuple-generator/FutureBasedTupleGenerator.hpp>
#include <tuple-generator/MultithreadedTupleGenerator.hpp>

template<typename T>
class ContinuousMaterialization {
    std::shared_ptr<T[]> data;
    size_t current_index = 0;
    size_t size;

public:
    explicit ContinuousMaterialization(size_t size) : size(size), data(std::make_shared<T[]>(size)) {}
    void materialize() {
        BatchedTupleGenerator<T> generator(size, 42);
        while (true) {
            auto [batch, count] = generator.getBatchOfTuples();
            if (count == 0 || batch == nullptr) {
                break;
            }
            std::copy(batch.get(), batch.get() + count, data.get() + current_index);
            current_index += count;
        }
    }
    auto get_data() -> std::shared_ptr<T[]> {
        return data;
    }
};