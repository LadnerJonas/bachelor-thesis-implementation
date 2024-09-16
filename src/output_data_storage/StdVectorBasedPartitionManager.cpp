#include "util/PaddedMutex.hpp"
#include <vector>
#include <memory>

#include "output_data_storage/PartitionManager.hpp"

template<typename T>
class StdVectorBasedPartitionManager : public PartitionManagerBase<StdVectorBasedPartitionManager<T>, T> {
private:
    std::vector<std::shared_ptr<std::vector<T>>> partitions_;
    mutable std::vector<PaddedMutex> mutexes_;

public:
    explicit StdVectorBasedPartitionManager(size_t num_partitions)
            : partitions_(num_partitions), mutexes_(num_partitions) {}

    void store_partition_impl(size_t partition_id, std::vector<T> &partition) {
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

    std::pair<T *, size_t> get_partition_impl(size_t partition_id) const {
        if (partition_id >= partitions_.size()) {
            throw std::out_of_range("Partition ID out of range");
        }

        std::lock_guard<std::mutex> lock(mutexes_[partition_id].mutex);
        auto &partition = partitions_[partition_id];
        return std::make_pair(partition->data(), partition->size());
    }

    size_t num_partitions_impl() const {
        return partitions_.size();
    }

};
