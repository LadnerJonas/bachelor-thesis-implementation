#pragma once

#include "slotted-page/page-implementation/LockFreeManagedSlottedPage.hpp"
#include "util/padded/PaddedAtomic.hpp"
#include <array>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class LockFreePageManager {
    std::array<std::vector<LockFreeManagedSlottedPage<T>>, partitions> pages;
    std::array<PaddedAtomic<unsigned>, partitions> partition_index = {};
    // SlottedPagePool<page_size> page_pool{};
    constexpr static auto pre_allocated_pages = partitions > 40 ? 2u : 50u;

public:
    // LockFreePageManager() : page_pool(pre_allocated_pages * partitions) {
    //     std::for_each(std::execution::par, pages.begin(), pages.end(), [&](std::vector<LockFreeManagedSlottedPage<T>> &partition_pages) {
    //         partition_pages.reserve(pre_allocated_pages);
    //         auto page_data = page_pool.get_multiple_pages(pre_allocated_pages);
    //         for (int j = 0; j < pre_allocated_pages; ++j) {
    //             partition_pages.emplace_back(page_size, page_data + j * (page_size + 4096));
    //         }
    //     });
    // }
    LockFreePageManager() {
        for (unsigned i = 0; i < partitions; ++i) {
            pages[i].reserve(pre_allocated_pages);
            for (unsigned j = 0; j < pre_allocated_pages; ++j) {
                pages[i].emplace_back(page_size);
            }
        }
    }

    void insert_tuple(const T &tuple, unsigned partition) {
        auto wi = pages[partition][partition_index[partition].load()].increment_and_fetch_opt_write_info();
        while (wi.page_data == nullptr) {
            wi = pages[partition][partition_index[partition].load()].increment_and_fetch_opt_write_info();
        }
        if (wi.tuple_index == LockFreeManagedSlottedPage<T>::get_max_tuples(page_size) - 1) {
            // pages[partition].emplace_back(page_size);
            partition_index[partition].fetch_add(1);
        }
        LockFreeManagedSlottedPage<T>::add_tuple_using_index(wi, tuple);
    }

    void insert_buffer_of_tuples(const T *buffer, const size_t num_tuples, const size_t partition) {
        for (unsigned i = 0; i < num_tuples; i++) {
            const auto &tuple = buffer[i];
            insert_tuple(tuple, partition);
        }
    }

    void insert_buffer_of_tuples_batched(const T *buffer, const size_t num_tuples, const size_t partition) {
        auto tuples_left = num_tuples;
        while (tuples_left > 0) {
            auto wi = pages[partition][partition_index[partition].load()].increment_and_fetch_opt_write_info(tuples_left);
            while (wi.page_data == nullptr) {
                wi = pages[partition][partition_index[partition].load()].increment_and_fetch_opt_write_info(tuples_left);
            }
            if (wi.tuples_to_write < tuples_left) {
                // pages[partition].emplace_back(page_size);
                partition_index[partition].fetch_add(1);
            }

            LockFreeManagedSlottedPage<T>::add_batch_using_index(buffer + num_tuples - tuples_left, wi);
            tuples_left -= wi.tuples_to_write;
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
