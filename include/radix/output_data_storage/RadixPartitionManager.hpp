#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <util/padded/PaddedAtomic.hpp>
#include <vector>

template<typename T, size_t num_partitions_>
class RadixPartitionManager {
    std::array<PaddedAtomic<size_t>, num_partitions_> histogram_ = {PaddedAtomic<size_t>(0)};

    std::unique_ptr<std::array<std::unique_ptr<T[]>, num_partitions_>> storage_ = std::make_unique<std::array<std::unique_ptr<T[]>, num_partitions_>>();
    std::array<PaddedAtomic<size_t>, num_partitions_> emitted_histogram_ = {PaddedAtomic<size_t>(0)};

    std::mutex thread_notify_mutex_;
    std::condition_variable thread_notify_cv_;
    size_t num_threads_;
    std::atomic<size_t> registered_threads_{0};

public:
    explicit RadixPartitionManager(const size_t num_threads) : num_threads_(num_threads) {
    }

    void register_thread_histogram(const std::array<size_t, num_partitions_> &histogram) {
        for (size_t i = 0; i < num_partitions_; ++i) {
            histogram_[i].fetch_add(histogram[i]);
        }

        if (registered_threads_.fetch_add(1) + 1 == num_threads_) {
            std::unique_lock lock(thread_notify_mutex_);

            std::for_each(std::execution::par_unseq,
                          std::begin(histogram_),
                          std::end(histogram_),
                          [this](PaddedAtomic<size_t> &value) {
                              size_t index = &value - histogram_.data();// Calculate index
                              (*storage_)[index] = std::make_unique<T[]>(value.load());
                          });

            thread_notify_cv_.notify_all();
        }
    }

    std::vector<std::pair<T *, size_t>> request_thread_storage_locations(
            const std::array<size_t, num_partitions_> &histogram) {
        {
            std::unique_lock lock(thread_notify_mutex_);
            thread_notify_cv_.wait(lock, [this] { return registered_threads_.load() == num_threads_; });
        }
        std::vector<std::pair<T *, size_t>> storage_locations(num_partitions_);
        std::for_each(std::execution::par_unseq,
                      std::begin(storage_locations),
                      std::end(storage_locations),
                      [this, &histogram, &storage_locations](std::pair<T *, size_t> &location) {
                          size_t index = &location - storage_locations.data();// Calculate index
                          location = {(*storage_)[index].get(), emitted_histogram_[index].fetch_add(histogram[index])};
                      });
        return storage_locations;
    }

    std::pair<T *, size_t> get_partition(size_t partition_id) {
        return {(*storage_)[partition_id].get(), emitted_histogram_[partition_id].get()};
    }

    size_t num_partitions() { return num_partitions_; }
};
