#pragma once

template<typename T, size_t num_partitions>
size_t partition_function(T &entry) {
    constexpr size_t mask = num_partitions - 1;
    constexpr bool is_power_of_2 = (num_partitions & mask) == 0;
    if (is_power_of_2) {
        return entry.get_key() & mask;
    }
    return entry.get_key() % num_partitions;
}
template<typename T>
size_t partition_function(T &entry, size_t num_partitions) {
    const size_t mask = num_partitions - 1;
    const bool is_power_of_2 = (num_partitions & mask) == 0;
    if (is_power_of_2) {
        return entry & mask;
    }
    return entry % num_partitions;
}