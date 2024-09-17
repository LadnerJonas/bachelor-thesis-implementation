
#include "util/PaddedMutex.hpp"
#include <vector>
#include <cassert>

#include "output_data_storage/PartitionManager.hpp"
#include "output_data_storage/LockFreeList.hpp"

template<typename T>
class LockFreeListBasedPartitionManager : public PartitionManagerBase<LockFreeListBasedPartitionManager<T>, T> {
private:
    std::vector<LockFreeList<T>> partitions_;
public:
    explicit LockFreeListBasedPartitionManager(size_t num_partitions)
            : partitions_(num_partitions) {}

    void store_partition_impl(size_t partition_id, std::vector<T> &partition) {
        assert(partition_id < partitions_.size());

        partitions_[partition_id].insert(partition.data(), partition.size());
    }

    std::pair<T *, size_t> get_partition_impl(size_t partition_id) const {
        assert(partition_id < partitions_.size());

        auto partition = partitions_[partition_id].get_bundled_vector();
        return std::make_pair(partition.data(), partition.size());
    }

    size_t num_partitions_impl() const {
        return partitions_.size();
    }

};