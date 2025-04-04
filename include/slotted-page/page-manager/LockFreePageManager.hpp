#pragma once

#include "slotted-page/page-implementation/LockFreeManagedSlottedPage.hpp"
#include "util/padded/PaddedAtomic.hpp"
#include <array>
#include <memory>
#include <deque>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class LockFreePageManager {
    std::array<std::deque<std::unique_ptr<LockFreeManagedSlottedPage<T>>>, partitions> pages{};
    std::array<PaddedAtomic<LockFreeManagedSlottedPage<T>*>, partitions> current_pages{};

    void add_page(unsigned partition) {
        pages[partition].emplace_back(std::make_unique<LockFreeManagedSlottedPage<T>>(page_size));
        current_pages[partition].store(pages[partition].back().get());
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
            insert_tuple(buffer[i], partition);
        }
    }

    void insert_buffer_of_tuples_batched(const T *buffer, const size_t num_tuples, const size_t partition) {
        auto tuples_left = num_tuples;
        while (tuples_left > 0) {
            auto wi = current_pages[partition].load()->increment_and_fetch_opt_write_info(tuples_left);
            while (wi.page_data == nullptr) {
                wi = current_pages[partition].load()->increment_and_fetch_opt_write_info(tuples_left);
            }
            if (wi.tuples_to_write < tuples_left || wi.tuple_index + wi.tuples_to_write >= LockFreeManagedSlottedPage<T>::get_max_tuples(page_size)) {
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

    std::vector<std::vector<T>> get_all_tuples_per_partition() {
        std::vector<std::vector<T>> result(partitions);
        for (size_t i = 0; i < partitions; ++i) {
            for (const auto &page: pages[i]) {
                auto tuples = page->get_all_tuples();
                result[i].insert(result[i].end(), tuples.begin(), tuples.end());
            }
        }
        return result;
    }
};
