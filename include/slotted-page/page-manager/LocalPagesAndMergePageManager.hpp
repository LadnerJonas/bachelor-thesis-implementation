#pragma once

#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "util/padded/PaddedAtomic.hpp"
#include "util/padded/PaddedMutex.hpp"

#include <algorithm>
#include <array>
#include <barrier>
#include <mutex>
#include <vector>


template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class LocalPagesAndMergePageManager {
    std::array<std::vector<ManagedSlottedPage<T>>, partitions> pages;
    std::barrier<> thread_barrier;
    PaddedMutex page_mutex;
    PaddedAtomic<unsigned> thread_chunk_released = PaddedAtomic<unsigned>(0);
    const unsigned num_threads;
    const unsigned total_partitions_per_thread = partitions / num_threads;
    const unsigned total_partitions_per_thread_remainder = partitions % num_threads;

public:
    explicit LocalPagesAndMergePageManager(const unsigned num_threads) : thread_barrier(num_threads), num_threads(num_threads) {}


    std::vector<std::vector<ManagedSlottedPage<T>>> hand_in_thread_local_pages(std::array<std::vector<ManagedSlottedPage<T>>, partitions> &thread_local_pages) {
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

        const unsigned thread_chunk = thread_chunk_released.fetch_add(1);
        const auto start_partition = thread_chunk * total_partitions_per_thread + std::min(thread_chunk, total_partitions_per_thread_remainder);
        const auto end_partition = start_partition + total_partitions_per_thread + (thread_chunk < total_partitions_per_thread_remainder ? 1 : 0);
        std::vector<std::vector<ManagedSlottedPage<T>>> thread_pages_to_merge(partitions);

        thread_barrier.arrive_and_wait();

        for (size_t partition = start_partition; partition < end_partition; ++partition) {
            thread_pages_to_merge[partition] = std::move(pages[partition]);
        }
        return thread_pages_to_merge;
    }

    void hand_in_merged_pages(std::vector<std::vector<ManagedSlottedPage<T>>> &thread_pages_to_merge) {
        for (size_t partition = 0; partition < partitions; ++partition) {
            if (thread_pages_to_merge[partition].empty()) {
                continue;
            }
            pages[partition] = std::move(thread_pages_to_merge[partition]);
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