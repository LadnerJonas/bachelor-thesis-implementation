#pragma once
#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "util/padded/PaddedMutex.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <mutex>
#include <ranges>
#include <vector>


template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class LocalPagesAndMergePageManager {
    std::array<std::vector<ManagedSlottedPage<T>>, partitions> pages;
    std::array<std::vector<ManagedSlottedPage<T>>, partitions> pages_to_merge;
    unsigned num_threads;
    std::barrier<> thread_barrier;
    std::once_flag distribute_flag;
    PaddedMutex page_mutex;
    std::atomic<unsigned> thread_chunk_released = 0;
    const unsigned total_partitions_per_thread = partitions / num_threads;
    const unsigned total_partitions_per_thread_remainder = partitions % num_threads;

public:
    explicit LocalPagesAndMergePageManager(const unsigned num_threads) : num_threads(num_threads), thread_barrier(num_threads) {}


    std::vector<std::vector<ManagedSlottedPage<T>>> hand_in_thread_local_pages(std::vector<std::vector<ManagedSlottedPage<T>>> &thread_local_pages) {
        {
            std::lock_guard lock(page_mutex.mutex);
            for (size_t partition = 0; partition < partitions; ++partition) {
                pages[partition].insert(pages[partition].end(),
                                        std::make_move_iterator(thread_local_pages[partition].begin()),
                                        std::make_move_iterator(thread_local_pages[partition].end()));
            }
        }
        if (num_threads == 1) {
            return {};
        }
        thread_barrier.arrive_and_wait();
        std::call_once(distribute_flag, [&]() {
            sort_and_prepare_pages();
        });

        const auto thread_chunk = thread_chunk_released.fetch_add(1);
        const auto start_partition = thread_chunk * total_partitions_per_thread + std::min(thread_chunk, total_partitions_per_thread_remainder);
        const auto end_partition = start_partition + total_partitions_per_thread + (thread_chunk < total_partitions_per_thread_remainder ? 1 : 0);
        std::vector<std::vector<ManagedSlottedPage<T>>> thread_pages_to_merge(partitions);

        thread_barrier.arrive_and_wait();
        for (size_t partition = start_partition; partition < end_partition; ++partition) {
            auto &pages_to_merge_partition = pages_to_merge[partition];
            if (pages_to_merge_partition.empty()) {
                continue;
            }
            thread_pages_to_merge[partition].swap(pages_to_merge_partition);
        }
        return thread_pages_to_merge;
    }

    void sort_and_prepare_pages() {
        for (size_t partition = 0; partition < partitions; ++partition) {
            std::ranges::sort(pages[partition], [](const ManagedSlottedPage<T> &a, const ManagedSlottedPage<T> &b) {
                return a.get_tuple_count() < b.get_tuple_count();
            });
            auto first_non_full_page = std::find_if(pages[partition].begin(), pages[partition].end(), [](const ManagedSlottedPage<T> &page) {
                return page.get_tuple_count() < ManagedSlottedPage<T>::get_max_tuples(page_size);
            });
            if (first_non_full_page != pages[partition].end()) {
                pages_to_merge[partition].insert(pages_to_merge[partition].end(),
                                                 std::make_move_iterator(first_non_full_page),
                                                 std::make_move_iterator(pages[partition].end()));
                pages[partition].erase(first_non_full_page, pages[partition].end());
            }
        }
    }

    void hand_in_merged_pages(std::vector<std::vector<ManagedSlottedPage<T>>> &thread_pages_to_merge) {
        for (size_t partition = 0; partition < partitions; ++partition) {
            if (thread_pages_to_merge[partition].empty()) {
                continue;
            }
            pages[partition].insert(pages[partition].end(),
                                    std::make_move_iterator(thread_pages_to_merge[partition].begin()),
                                    std::make_move_iterator(thread_pages_to_merge[partition].end()));
        }
    }

    std::vector<size_t> get_written_tuples_per_partition() {
        std::vector<size_t> written_tuples(partitions, 0);
        for (size_t partition = 0; partition < partitions; ++partition) {
            for (const auto &page: pages[partition]) {
                written_tuples[partition] += page.get_tuple_count();
            }
        }
        return written_tuples;
    }
};