#ifndef PARTITION_MANAGER_HPP
#define PARTITION_MANAGER_HPP

#include <vector>
#include <mutex>
#include <memory>
#include <thread>
#include <stdexcept>
#include <atomic>
#include "util/PaddedMutex.hpp"

template<typename T>
class PartitionManager {
public:
    explicit PartitionManager(size_t num_partitions);

    void store_partition(size_t partition_id, std::vector<T> partition);
    std::pair<T*, size_t> get_partition(size_t partition_id) const;
    size_t num_partitions() const;

private:
    std::vector<std::shared_ptr<std::vector<T>>> partitions_;
    mutable std::vector<PaddedMutex> mutexes_;
};

template<typename T>
PartitionManager<T>::PartitionManager(size_t num_partitions)
        : partitions_(num_partitions), mutexes_(num_partitions) {}

template<typename T>
void PartitionManager<T>::store_partition(size_t partition_id, std::vector<T> partition) {
    if (partition_id >= partitions_.size()) {
        throw std::out_of_range("Partition ID out of range");
    }

    std::lock_guard<std::mutex> lock(mutexes_[partition_id].mutex);
    // append to existing partition
    if (partitions_[partition_id]) {
        partitions_[partition_id]->insert(partitions_[partition_id]->end(), partition.begin(), partition.end());
    } else {
        partitions_[partition_id] = std::make_shared<std::vector<T>>(std::move(partition));
    }

}

template<typename T>
std::pair<T*, size_t> PartitionManager<T>::get_partition(size_t partition_id) const {
    if (partition_id >= partitions_.size()) {
        throw std::out_of_range("Partition ID out of range");
    }

    std::lock_guard<std::mutex> lock(mutexes_[partition_id].mutex);
    auto& partition = partitions_[partition_id];
    return std::make_pair(partition->data(), partition->size());
}

template<typename T>
size_t PartitionManager<T>::num_partitions() const {
    return partitions_.size();
}

#endif // PARTITION_MANAGER_HPP
