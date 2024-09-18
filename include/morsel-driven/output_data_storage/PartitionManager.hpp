#ifndef PARTITION_MANAGER_HPP
#define PARTITION_MANAGER_HPP

#include <utility>
#include <vector>

template<typename Derived, typename T>
class PartitionManagerBase {
public:
    void store_partition(size_t partition_id, std::vector<T> partition) {
        static_cast<Derived *>(this)->store_partition_impl(partition_id, partition);
    }

    std::pair<T *, size_t> get_partition(size_t partition_id) const {
        return static_cast<const Derived *>(this)->get_partition_impl(partition_id);
    }

    size_t num_partitions() const { return static_cast<const Derived *>(this)->num_partitions_impl(); }
};

#endif// PARTITION_MANAGER_HPP
