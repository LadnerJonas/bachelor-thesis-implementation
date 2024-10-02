#ifndef PARTITIONING_FUNCTION_HPP
#define PARTITIONING_FUNCTION_HPP

template<typename T, size_t num_partitions>
size_t partition_function(T &entry) {
    constexpr size_t mask = num_partitions - 1;
    if ((num_partitions & mask) == 0) {
        return entry & mask;
    }
    return entry % num_partitions;
}
template<typename T>
size_t partition_function(T &entry, size_t num_partitions) {
    const size_t mask = num_partitions - 1;
    const bool is_power_of_2 = (num_partitions & mask) == 0;
    if (is_power_of_2) {
        return entry & mask;
    }
    // For non-power-of-2 partitions, use modulo operation
    return entry % num_partitions;
}

#endif// PARTITIONING_FUNCTION_HPP