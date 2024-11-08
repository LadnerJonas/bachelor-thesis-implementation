#pragma once

#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "util/padded/PaddedMutex.hpp"
#include <array>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class OnDemandPageManager {
    std::array<PaddedMutex, partitions> partition_locks;
    std::array<std::vector<ManagedSlottedPage<T>>, partitions> pages;

public:
    OnDemandPageManager() {
        for (size_t i = 0; i < partitions; ++i) {
            pages[i].emplace_back(page_size);
        }
    }

    void insert_tuple(const T &tuple, size_t partition) {
        std::unique_lock lock(partition_locks[partition].mutex);
        if (!pages[partition].back().add_tuple(tuple)) {
            pages[partition].emplace_back(page_size);
            pages[partition].back().add_tuple(tuple);
        }
    }

    std::vector<size_t> get_written_tuples_per_partition() {
        std::vector<size_t> result(partitions, 0);
        for (size_t i = 0; i < partitions; ++i) {
            for (const auto &page: pages[i]) {
                result[i] += page.get_tuple_count();
            }
        }
        return result;
    }
};
