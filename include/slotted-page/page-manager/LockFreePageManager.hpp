#pragma once

#include "slotted-page/page-implementation/LockFreeManagedSlottedPage.hpp"
#include "util/padded/PaddedAtomic.hpp"
#include <array>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class LockFreePageManager {
    std::array<std::vector<std::shared_ptr<LockFreeManagedSlottedPage<T>>>, partitions> pages{};
    std::array<PaddedAtomic<std::shared_ptr<LockFreeManagedSlottedPage<T>>>, partitions> current_pages{};

    void add_page(unsigned partition) {
        pages[partition].emplace_back(std::make_shared<LockFreeManagedSlottedPage<T>>(page_size));
        current_pages[partition].store(pages[partition].back());
    }


public:
    LockFreePageManager() {
        for (unsigned i = 0; i < partitions; ++i) {
            add_page(i);
        }
    }

    void insert_tuple(const T &tuple, unsigned partition) {
        auto wi = current_pages[partition].load()->increment_and_fetch_opt_write_info();
        while (wi.page_data == nullptr) {
            wi = current_pages[partition].load()->increment_and_fetch_opt_write_info();
        }
        if (wi.tuple_index == LockFreeManagedSlottedPage<T>::get_max_tuples(page_size) - 1) {
            add_page(partition);
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
            auto wi = current_pages[partition].load()->increment_and_fetch_opt_write_info(tuples_left);
            while (wi.page_data == nullptr) {
                wi = current_pages[partition].load()->increment_and_fetch_opt_write_info(tuples_left);
            }
            if (wi.tuples_to_write < tuples_left || wi.tuple_index + wi.tuples_to_write >= LockFreeManagedSlottedPage<T>::get_max_tuples(page_size) - 1) {
                add_page(partition);
            }
            LockFreeManagedSlottedPage<T>::add_batch_using_index(buffer + num_tuples - tuples_left, wi);
            tuples_left -= wi.tuples_to_write;
        }
    }

    std::vector<size_t> get_written_tuples_per_partition() {
        std::vector<size_t> result(partitions, 0);
        for (size_t i = 0; i < partitions; ++i) {
            for (const auto &page: pages[i]) {
                result[i] += page->get_tuple_count();
            }
        }
        return result;
    }
};
