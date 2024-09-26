#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <util/padded/PaddedAtomic.hpp>
#include <vector>

template<typename T, size_t num_partitions_>
class RadixPartitionManager {
    std::mutex histogram_mutex_;
    std::vector<PaddedAtomic<size_t>> histogram_;

    std::unique_ptr<std::array<std::unique_ptr<T[]>, num_partitions_>> storage_;
    std::vector<PaddedAtomic<size_t>> emitted_histogram_;

    std::mutex thread_notify_mutex_;
    std::condition_variable thread_notify_cv_;
    size_t num_threads_;
    std::atomic<size_t> registered_threads_{0};

public:
    explicit RadixPartitionManager(const size_t num_threads) : histogram_(num_partitions_), num_threads_(num_threads) {
    }

    void register_thread_histogram(const std::vector<size_t> &histogram) {
        for (size_t i = 0; i < num_partitions_; ++i) {
            histogram_[i].fetch_add(histogram[i]);
        }

        if (registered_threads_.fetch_add(1) + 1 == num_threads_) {
            std::unique_lock lock(thread_notify_mutex_);
            storage_ = std::make_unique<std::array<std::unique_ptr<T[]>, num_partitions_>>();
            emitted_histogram_ = std::vector<PaddedAtomic<size_t>>(num_partitions_);
            for (size_t i = 0; i < num_partitions_; ++i) {
                (*storage_)[i] = std::make_unique<T[]>(histogram_[i].get());
                emitted_histogram_[i].store(0);
            }
            thread_notify_cv_.notify_all();
        }
    }

    std::vector<std::pair<T *, size_t>> request_thread_storage_locations(
            const std::vector<size_t> &histogram) {
        {
            std::unique_lock lock(thread_notify_mutex_);
            thread_notify_cv_.wait(lock, [this] { return registered_threads_.load() == num_threads_; });
        }
        std::vector<std::pair<T *, size_t>> storage_locations(num_partitions_);
        for (size_t i = 0; i < num_partitions_; ++i) {
            storage_locations[i] = {(*storage_)[i].get(), emitted_histogram_[i].fetch_add(histogram[i])};
        }
        return storage_locations;
    }

    std::pair<T *, size_t> get_partition(size_t partition_id) {
        return {(*storage_)[partition_id].get(), emitted_histogram_[partition_id].get()};
    }

    size_t num_partitions() { return num_partitions_; }
};
