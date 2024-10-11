#ifndef SORT_PARTITION_HPP
#define SORT_PARTITION_HPP

#include <algorithm>
#include <execution>
#include <iostream>
#include <vector>

template<typename T>
int PartitionFunction(const T value, int num_partitions) {
    return value % num_partitions;
}

template<typename T>
int find_partition_start(std::vector<T> &relation, int partition, int num_partitions) {
    auto it = std::lower_bound(
            relation.begin(), relation.end(), partition,
            [&num_partitions](const int v, int partition) { return PartitionFunction(v, num_partitions) < partition; });
    return std::distance(relation.begin(), it);
}

template<typename T>
std::vector<std::pair<int, int>> sort_partition(std::vector<T> &relation, int num_partitions, auto execution_policy = std::execution::par_unseq) {
    std::sort(execution_policy, relation.begin(), relation.end(), [&num_partitions](const int &lhs, const int &rhs) {
        return PartitionFunction(lhs, num_partitions) < PartitionFunction(rhs, num_partitions);
    });

    std::vector<std::pair<int, int>> partition_boundaries(num_partitions);
    for (int partition = 0; partition < num_partitions; ++partition) {
        auto start = find_partition_start(relation, partition, num_partitions);
        auto end = (partition == num_partitions - 1) ? relation.size()
                                                     : find_partition_start(relation, partition + 1, num_partitions);
        partition_boundaries[partition] = {start, end};
    }

    return partition_boundaries;
}

#endif// SORT_PARTITION_HPP
