#pragma once

#include <array>
#include <cstddef>
#include <cstdlib>
#include <immintrin.h>

template<typename T, size_t num_partitions>
size_t partition_function(const T &entry) {
    constexpr static size_t mask = num_partitions - 1;
    constexpr static bool is_power_of_2 = (num_partitions & mask) == 0;
    if constexpr (is_power_of_2) {
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


template<typename T, size_t num_partitions>
__m128i partition_function_simd(const T *entry) {
    constexpr static size_t mask = num_partitions - 1;
    constexpr static bool is_power_of_2 = (num_partitions & mask) == 0;
    if constexpr (is_power_of_2) {
        if constexpr (T::get_size_of_variable_data() == 0) {
            const __m128i keys = _mm_loadu_si128(reinterpret_cast<const __m128i *>(entry));
            const __m128i mask_vector = _mm_set1_epi32(mask);
            return _mm_and_si128(keys, mask_vector);
        } else {
            alignas(16) std::array<int, 4> keys{};
            for (int i = 0; i < 4; ++i) {
                keys[i] = entry[i].get_key();
            }
            const __m128i keys_vector = _mm_loadu_si128(reinterpret_cast<const __m128i *>(keys.data()));
            const __m128i mask_vector = _mm_set1_epi32(static_cast<int>(mask));
            return _mm_and_si128(keys_vector, mask_vector);
        }
    }

    exit(2);
}