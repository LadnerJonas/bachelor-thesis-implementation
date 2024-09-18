#ifndef PARTITIONING_FUNCTION_HPP
#define PARTITIONING_FUNCTION_HPP

#include "iostream"
#include "memory"
#include "vector"

template<typename T>
void partition_function(T &&entry, std::vector<std::vector<T>> &thread_partitions, size_t num_partitions) {
    size_t partition_id = entry % num_partitions;// Example partitioning function based on entry value
    thread_partitions[partition_id].push_back(std::forward<T>(entry));
}

#endif// PARTITIONING_FUNCTION_HPP