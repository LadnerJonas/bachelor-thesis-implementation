#pragma once
#include "common/morsel-creation/MorselCreator.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "util/padded/PaddedAtomic.hpp"
#include "util/padded/PaddedMutex.hpp"

#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

template<typename T>
class CollaborativeMorselCreator2 {
    MorselCreator<T, 10*2048> generator;
    std::vector<std::pair<std::unique_ptr<T[]>, size_t>> batches;
    std::vector<std::thread> producer_threads;
    PaddedAtomic<bool> done = PaddedAtomic(false);// Flag to indicate no more batches to produce
    PaddedMutex batch_mutex;                      // Protects access to batches
    std::condition_variable cv;                   // Condition variable for batch availability
    PaddedMutex generator_mutex;

public:
    explicit CollaborativeMorselCreator2(size_t num_tuples, size_t num_producer_threads = 1)
        : generator(num_tuples) {

        // Launch the specified number of producer threads
        for (size_t i = 0; i < num_producer_threads; ++i) {
            producer_threads.emplace_back(&CollaborativeMorselCreator2::produceBatches, this);
        }
    }

    ~CollaborativeMorselCreator2() {
        // Join all producer threads
        for (auto &t: producer_threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    // Method to produce batches in parallel
    void produceBatches() {
        auto time_start = std::chrono::high_resolution_clock::now();

        constexpr size_t reserve_size = 4096; // Reserve space for faster storage
        std::vector<std::pair<std::unique_ptr<T[]>, size_t>> local_batches;
        local_batches.reserve(reserve_size);

        while (true) {
            // Generate a batch
            auto [batch_ptr, batch_size] = generator.getBatchOfTuples();
            if (batch_size == 0) { // No more tuples to generate
                break;
            }
            // local_batches.emplace_back(std::move(batch_ptr), batch_size);

            // Early flush if local_batches gets large
            if (local_batches.size() >= reserve_size) {
                {
                    // std::lock_guard lock(batch_mutex.mutex);
                    // batches.insert(batches.end(),
                    //               std::make_move_iterator(local_batches.begin()),
                    //               std::make_move_iterator(local_batches.end()));
                }
                cv.notify_all();
                local_batches.clear(); // Reuse allocated memory
            }
        }

        // Move remaining batches
        if (!local_batches.empty()) {
            {
                // std::lock_guard lock(batch_mutex.mutex);
                // batches.insert(batches.end(),
                //               std::make_move_iterator(local_batches.begin()),
                //               std::make_move_iterator(local_batches.end()));
            }
        }

        done.store(true);
        cv.notify_all(); // Notify all waiting threads that production is done

        std::cout << "Produced in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::high_resolution_clock::now() - time_start)
                         .count() << "ms" << std::endl;
    }



    auto requestBatchCollaboratively(unsigned next_index_to_process) -> std::pair<T *, size_t> {
        unsigned attempts = 0;
        std::unique_lock lock(batch_mutex.mutex);

        // Wait for a batch to be available if there are no batches
        cv.wait(lock, [this, next_index_to_process, &attempts]() {
            attempts++;
            if (attempts > 2) {
                std::cout << "Waiting for batch " << next_index_to_process << std::endl;
            }

            return next_index_to_process < batches.size() || done.load();
        });

        if (next_index_to_process >= batches.size()) {
            std::cout << "Processed all batches:" << next_index_to_process << std::endl;
            return {nullptr, 0};// No more batches, producer is done
        }

        // Return the next batch from the vector
        auto &current_batch = batches[next_index_to_process];
        return {current_batch.first.get(), current_batch.second};
    }
};
