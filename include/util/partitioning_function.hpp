#ifndef PARTITIONING_FUNCTION_HPP
#define PARTITIONING_FUNCTION_HPP

#include "iostream"
#include "memory"
#include "vector"

template<typename T>
size_t partition_function(T &entry, size_t num_partitions) {
    return entry % num_partitions;// Example partitioning function based on entry value
}

#endif// PARTITIONING_FUNCTION_HPP