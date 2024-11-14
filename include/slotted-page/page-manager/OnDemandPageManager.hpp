#pragma once

#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "slotted-page/page-pool/SlottedPagePool.hpp"
#include "util/padded/PaddedMutex.hpp"
#include <array>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class OnDemandPageManager {
    std::array<PaddedMutex, partitions> partition_locks;
    std::array<std::vector<ManagedSlottedPage<T>>, partitions> pages;
    SlottedPagePool<T, partitions, page_size> pool;

public:
    OnDemandPageManager() {
        for (size_t i = 0; i < partitions; ++i) {
            pages[i].emplace_back(page_size);
        }
    }
    explicit OnDemandPageManager(size_t tuples) : pool(tuples) {
        const auto max_tuples_per_page = ManagedSlottedPage<T>::get_max_tuples(page_size);
        const auto pages_to_reserve_per_partition = ((tuples + partitions - 1) / partitions + max_tuples_per_page - 1) / max_tuples_per_page;
        for (size_t i = 0; i < partitions; ++i) {
            pages[i].reserve(pages_to_reserve_per_partition);
            pages[i].emplace_back(page_size, pool.get_single_page());
        }
    }


    void insert_tuple(const T &tuple, size_t partition) {
        std::lock_guard lock(partition_locks[partition].mutex);
        if (!pages[partition].back().add_tuple(tuple)) {
            pages[partition].emplace_back(page_size);
            pages[partition].back().add_tuple(tuple);
        }
    }

    void insert_buffer_of_tuples(const T *buffer, const size_t num_tuples, const size_t partition) {
        std::lock_guard lock(partition_locks[partition].mutex);
        for (unsigned i = 0; i < num_tuples; i++) {
            const auto &tuple = buffer[i];
            if (!pages[partition].back().add_tuple(tuple)) {
                pages[partition].emplace_back(page_size);
                pages[partition].back().add_tuple(tuple);
            }
        }
    }

    void insert_buffer_of_tuples_batched(const T *buffer, const size_t num_tuples, const size_t partition) {
        std::unique_lock lock(partition_locks[partition].mutex);
        auto &current_page = pages[partition].back();
        const auto index = current_page.get_tuple_count();
        auto tuples_left_on_page = ManagedSlottedPage<T>::get_max_tuples(page_size) - index;
        if (tuples_left_on_page == 0) {
            pages[partition].emplace_back(page_size, pool.get_single_page());
            lock.unlock();
            insert_buffer_of_tuples_batched(buffer, num_tuples, partition);
            return;
        }
        auto const tuples_left = num_tuples - std::min(tuples_left_on_page, num_tuples);
        auto const tuples_to_write = num_tuples - tuples_left;
        current_page.increase_tuple_count(tuples_to_write);
        lock.unlock();
        current_page.add_tuple_batch_with_index(buffer, index, tuples_to_write);

        if (tuples_left > 0) {
            insert_buffer_of_tuples_batched(buffer + num_tuples - tuples_left, tuples_left, partition);
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
