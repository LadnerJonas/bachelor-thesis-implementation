#pragma once

#include "slotted-page/page-implementation/LockFreeManagedSlottedPage.hpp"
#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "slotted-page/page-pool/SlottedPagePool.hpp"
#include "util/padded/PaddedAtomic.hpp"
#include "util/padded/PaddedMutex.hpp"
#include <array>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class LockFreePageManager {
    std::array<std::vector<LockFreeManagedSlottedPage<T>>, partitions> pages;
    std::array<PaddedAtomic<unsigned>, partitions> partition_index = {};

public:
    LockFreePageManager() {
        for (size_t i = 0; i < partitions; ++i) {
            pages[i].emplace_back(page_size);

        }
    }
    explicit LockFreePageManager(const size_t tuples){
        const auto max_tuples_per_page = ManagedSlottedPage<T>::get_max_tuples(page_size);
        const auto pages_to_reserve_per_partition = ((tuples + partitions - 1) / partitions + max_tuples_per_page - 1) / max_tuples_per_page;
        for (size_t i = 0; i < partitions; ++i) {
            pages[i].reserve(pages_to_reserve_per_partition);
            pages[i].emplace_back(page_size);
        }
    }

    void insert_tuple(const T &tuple, unsigned partition) {
        auto wi = pages[partition][partition_index[partition].load()].increment_and_fetch_opt_write_info();
        while(wi.page_data == nullptr) {
            if(wi.tuple_index == LockFreeManagedSlottedPage<T>::get_max_tuples(page_size)) {
                pages[partition].emplace_back(page_size);
                ++partition_index[partition];
            }
            wi = pages[partition][partition_index[partition].load()].increment_and_fetch_opt_write_info();
        }
        LockFreeManagedSlottedPage<T>::add_tuple_using_index(wi, tuple);
    }

    void insert_buffer_of_tuples(const T *buffer, const size_t num_tuples, const size_t partition) {
        for (unsigned i = 0; i < num_tuples; i++) {
            const auto &tuple = buffer[i];
            insert_tuple(tuple, partition);
        }
    }

    // void insert_buffer_of_tuples_batched(const T *buffer, const size_t num_tuples, const size_t partition) {
    //     std::unique_lock lock(partition_locks[partition].mutex);
    //     auto &current_page = pages[partition].back();
    //     const auto index = current_page.get_tuple_count();
    //     auto tuples_left_on_page = ManagedSlottedPage<T>::get_max_tuples(page_size) - index;
    //     if (tuples_left_on_page == 0) {
    //         pages[partition].emplace_back(page_size, pool.get_single_page());
    //         lock.unlock();
    //         insert_buffer_of_tuples_batched(buffer, num_tuples, partition);
    //         return;
    //     }
    //     auto const tuples_left = num_tuples - std::min(tuples_left_on_page, num_tuples);
    //     auto const tuples_to_write = num_tuples - tuples_left;
    //     current_page.increase_tuple_count(tuples_to_write);
    //     lock.unlock();
    //     current_page.add_tuple_batch_with_index(buffer, index, tuples_to_write);
    //
    //     if (tuples_left > 0) {
    //         insert_buffer_of_tuples_batched(buffer + num_tuples - tuples_left, tuples_left, partition);
    //     }
    // }

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
