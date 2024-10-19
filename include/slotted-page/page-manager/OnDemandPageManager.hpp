#pragma once

#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "util/padded/PaddedMutex.hpp"
#include <array>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class OnDemandPageManager {
    std::array<PaddedMutex, partitions> partition_locks;
    std::array<ManagedSlottedPage<T>, partitions> pages = {ManagedSlottedPage<T>(page_size)};
    std::array<size_t, partitions> send_pages{0};

    void insert_tuple(const T &tuple, size_t partition) {
        std::lock_guard lock(partition_locks[partition]);
        if (!pages[partition].add_tuple(tuple)) {
            ++send_pages[partition];
            pages[partition] = ManagedSlottedPage<T>(page_size);
            pages[partition].add_tuple(tuple);
        }
    }

    std::array<size_t, partitions> get_send_page_info() {
        std::array<size_t, partitions> result = send_pages;
        for (int i = 0; i < partitions; ++i) {
            result[i] += pages[i].get_num_slots() > 0;
        }
        return result;
    }
};
