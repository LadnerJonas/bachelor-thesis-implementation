#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include "slotted-page/page-manager/PageWriteInfo.hpp"
#include "util/padded/PaddedMutex.hpp"
#include <array>
#include <barrier>
#include <mutex>
#include <vector>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class RadixPageManager {
    struct PartitionData {
        std::vector<std::shared_ptr<RawSlottedPage<T>>> pages;
        size_t current_page = 0;
        size_t current_tuple_offset = 0;
    };

    size_t num_threads;
    size_t tuples_per_page = (page_size - sizeof(HeaderInfo)) / (sizeof(T) + sizeof(SlotInfo<T>));
    std::array<size_t, partitions> global_histogram;
    std::array<PartitionData, partitions> partitions_data;
    std::array<PaddedMutex, partitions> partition_locks;
    std::barrier<> thread_barrier;
    std::mutex global_histogram_mutex;
    std::once_flag distribute_flag;

    void allocate_new_page(size_t partition) {
        partitions_data[partition].pages.emplace_back(std::make_shared<RawSlottedPage<T>>(page_size));
        partitions_data[partition].current_tuple_offset = 0;
    }

public:
    explicit RadixPageManager(size_t num_threads)
        : num_threads(num_threads),
          thread_barrier(static_cast<long>(num_threads)) {
        global_histogram.fill(0);
    }

    void add_histogram_chunk(const std::vector<unsigned> &histogram_chunk) {
        {
            std::unique_lock lock(global_histogram_mutex);
            for (size_t i = 0; i < partitions; ++i) {
                global_histogram[i] += histogram_chunk[i];
            }
        }
        thread_barrier.arrive_and_wait();

        std::call_once(distribute_flag, [&]() {
            // auto distribute_time_start = std::chrono::high_resolution_clock::now();
            distribute_pages();
            // auto distribute_time_end = std::chrono::high_resolution_clock::now();
            // std::cout << "Distributed pages in " << std::chrono::duration_cast<std::chrono::milliseconds>(distribute_time_end - distribute_time_start).count() << "ms" << std::endl;
        });
    }

    void distribute_pages() {
        // Allocate pages concurrently for each partition
        std::vector<std::future<void>> futures;
        for (size_t partition = 0; partition < partitions; ++partition) {
            futures.push_back(std::async(std::launch::async, [this, partition]() {
                size_t total_tuples = global_histogram[partition];
                size_t num_full_pages = total_tuples / tuples_per_page;
                size_t remaining_tuples = total_tuples % tuples_per_page;

                size_t total_pages = num_full_pages + (remaining_tuples > 0 ? 1 : 0);
                for (size_t i = 0; i < total_pages; ++i) {
                    allocate_new_page(partition);
                }

                // Verify page allocation correctness
                assert(partitions_data[partition].pages.size() == total_pages);
            }));
        }

        // Wait for all threads to finish
        for (auto &fut: futures) {
            fut.get();
        }
    }

    std::vector<std::vector<PageWriteInfo<T>>> get_write_info(const std::vector<unsigned> &local_histogram) {
        std::unique_lock lock(global_histogram_mutex);
        std::vector<std::vector<PageWriteInfo<T>>> thread_write_info(partitions);

        for (size_t partition = 0; partition < partitions; ++partition) {
            size_t tuples_to_write = local_histogram[partition];

            while (tuples_to_write > 0) {
                const size_t current_tuple_offset = partitions_data[partition].current_tuple_offset;
                auto current_page = partitions_data[partition].pages[partitions_data[partition].current_page];
                assert(partitions_data[partition].pages.size() > partitions_data[partition].current_page);
                const size_t free_space = tuples_per_page - current_tuple_offset;
                const size_t tuples_for_page = std::min(free_space, tuples_to_write);

                PageWriteInfo<T> write_info(current_page, current_tuple_offset, tuples_for_page);

                thread_write_info[partition].emplace_back(write_info);
                tuples_to_write -= tuples_for_page;
                partitions_data[partition].current_tuple_offset += tuples_for_page;

                if (tuples_to_write > 0) {
                    ++partitions_data[partition].current_page;
                    partitions_data[partition].current_tuple_offset = 0;
                }
            }
        }

        return thread_write_info;
    }

    std::vector<size_t> get_written_tuples_per_partition() {
        std::vector<size_t> written_tuples(partitions, 0);
        for (size_t partition = 0; partition < partitions; ++partition) {
            for (const auto &page: partitions_data[partition].pages) {
                written_tuples[partition] += page->get_tuple_count();
            }
        }
        return written_tuples;
    }
};
