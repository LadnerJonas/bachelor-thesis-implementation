#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include "slotted-page/page-manager/PageWriteInfo.hpp"
#include "util/padded/PaddedMutex.hpp"
#include <array>
#include <barrier>
#include <mutex>
#include <thread>
#include <vector>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class RadixPageManager {
    struct PartitionData {
        std::vector<std::unique_ptr<RawSlottedPage<T>>> pages;
        size_t current_page = 0;
        size_t current_tuple_offset = 0;
    };

    size_t num_threads;
    size_t tuples_per_page = page_size / sizeof(T);
    std::array<size_t, partitions> global_histogram{};
    std::array<PartitionData, partitions> partitions_data;
    std::array<PaddedMutex, partitions> partition_locks;
    std::barrier<> thread_barrier;
    std::mutex global_histogram_mutex;

    void allocate_new_page(size_t partition) {
        partitions_data[partition].pages.emplace_back(std::make_unique<RawSlottedPage<T>>(page_size));
        partitions_data[partition].current_tuple_offset = 0;
    }

public:
    explicit RadixPageManager(size_t num_threads)
        : num_threads(num_threads), thread_barrier(num_threads, [this]() { distribute_pages(); }) {
        global_histogram.fill(0);
    }

    void add_histogram_chunk(const std::vector<size_t> &histogram_chunk) {
        std::lock_guard lock(global_histogram_mutex);
        for (size_t i = 0; i < partitions; ++i) {
            global_histogram[i] += histogram_chunk[i];
        }
    }

    void distribute_pages() {
        // Calculate page allocation per partition based on the global histogram
        for (size_t partition = 0; partition < partitions; ++partition) {
            size_t total_tuples = global_histogram[partition];
            size_t num_full_pages = total_tuples / tuples_per_page;
            size_t remaining_tuples = total_tuples % tuples_per_page;

            // Allocate full pages
            for (size_t i = 0; i < num_full_pages; ++i) {
                allocate_new_page(partition);
            }

            // Allocate page for remaining tuples
            if (remaining_tuples > 0) {
                allocate_new_page(partition);
            }
        }
    }

    std::vector<std::vector<PageWriteInfo<T>>> get_write_info(const std::vector<size_t> &local_histogram) {
        std::lock_guard lock(global_histogram_mutex);
        std::vector<std::pair<std::shared_ptr<RawSlottedPage<T>>, size_t>> thread_write_info(partitions);

        // For each partition, calculate which pages and how many tuples the thread should write
        for (size_t partition = 0; partition < partitions; ++partition) {
            size_t tuples_to_write = local_histogram[partition];
            size_t current_tuple_offset = partitions_data[partition].current_tuple_offset;

            std::vector<PageWriteInfo<T>> write_page_infos;

            while (tuples_to_write > 0) {
                auto &current_page = partitions_data[partition].pages[partitions_data[partition].current_page];
                size_t free_space = tuples_per_page - current_tuple_offset;
                size_t tuples_for_page = std::min(free_space, tuples_to_write);

                PageWriteInfo<T> write_info(current_page, current_tuple_offset, tuples_for_page);

                write_page_infos.emplace_back(write_info);
                tuples_to_write -= tuples_for_page;

                partitions_data[partition].current_tuple_offset += tuples_for_page;

                if (tuples_to_write > 0) {
                    ++partitions_data[partition].current_page;
                    partitions_data[partition].current_tuple_offset = 0;
                }
            }

            thread_write_info[partition] = write_page_infos;
        }

        return thread_write_info;
    }
};
