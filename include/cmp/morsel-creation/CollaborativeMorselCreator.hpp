#pragma once
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <vector>

template<typename T>
class CollaborativeMorselCreator {
    BatchedTupleGenerator<T> generator;
    std::vector<std::pair<std::unique_ptr<T[]>, size_t>> batches;
    std::atomic<bool> done = false;// Flag to indicate no more batches to produce
    std::mutex mutex;              // Protects access to batches
    std::thread producer_thread;

public:
    explicit CollaborativeMorselCreator(size_t num_tuples)
        : generator(num_tuples) {
        // Spawn producer thread to generate batches
        producer_thread = std::thread([this]() {
            while (true) {
                auto [batch, size] = generator.getBatchOfTuples();
                if (size == 0) {// No more tuples to generate
                    break;
                }

                {
                    std::lock_guard lock(mutex);
                    batches.emplace_back(std::move(batch), size);
                }
            }
            done = true;// Signal end of production
        });
    }

    ~CollaborativeMorselCreator() {
        if (producer_thread.joinable()) {
            producer_thread.join();
        }
    }

    auto requestBatchCollaboratively(unsigned next_index_to_process) -> std::pair<T *, size_t> {
        while (true) {
            std::lock_guard lock(mutex);

            // If the batch exists, return it
            if (next_index_to_process < batches.size()) {
                auto &current_batch = batches[next_index_to_process];
                return {current_batch.first.get(), current_batch.second};
            }

            // If no more batches will be produced, signal end
            if (done) {
                return {nullptr, 0};
            }
        }
    }
};
