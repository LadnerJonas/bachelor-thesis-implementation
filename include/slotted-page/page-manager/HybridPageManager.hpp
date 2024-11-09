#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include "slotted-page/page-manager/PageWriteInfo.hpp"
#include "slotted-page/page-manager/PartitionData.hpp"
#include "slotted-page/page-pool/SlottedPagePool.hpp"
#include "util/padded/PaddedMutex.hpp"
#include <algorithm>
#include <array>
#include <barrier>
#include <execution>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class HybridPageManager {
    size_t tuples_per_page = RawSlottedPage<T>::get_max_tuples(page_size);
    size_t num_threads;
    std::array<PartitionData<T>, partitions> partitions_data;
    std::array<PaddedMutex, partitions> partition_locks;
    std::barrier<> thread_barrier;
    std::mutex global_histogram_mutex;
    std::once_flag distribute_flag;

    void allocate_new_page(size_t partition) {
        assert(page_pool.has_free_page());
        partitions_data[partition].pages.emplace_back(page_size);
        partitions_data[partition].current_tuple_offset = 0;
    }

public:
    explicit HybridPageManager(size_t num_threads)
        : num_threads(num_threads),
          thread_barrier(static_cast<long>(num_threads)) {
        for (size_t i = 0; i < partitions; i++) {
            allocate_new_page(i);
        }
    }

    std::vector<std::vector<PageWriteInfo<T>>> get_write_info(const std::array<unsigned, partitions> &local_histogram) {
        std::unique_lock lock(global_histogram_mutex);
        std::vector<std::vector<PageWriteInfo<T>>> thread_write_info(partitions);

        for (size_t partition = 0; partition < partitions; ++partition) {
            size_t tuples_to_write = local_histogram[partition];

            while (tuples_to_write > 0) {
                const size_t current_tuple_offset = partitions_data[partition].current_tuple_offset;
                assert(partitions_data[partition].pages.size() > partitions_data[partition].current_page);
                const size_t free_space = tuples_per_page - current_tuple_offset;
                const size_t tuples_for_page = std::min(free_space, tuples_to_write);

                if (free_space == 0) {
                    allocate_new_page(partition);
                    ++partitions_data[partition].current_page;
                    partitions_data[partition].current_tuple_offset = 0;
                    continue;
                }

                const auto &current_page = partitions_data[partition].pages[partitions_data[partition].current_page];
                PageWriteInfo<T> write_info(current_page, current_tuple_offset, tuples_for_page);

                thread_write_info[partition].emplace_back(write_info);
                tuples_to_write -= tuples_for_page;
                partitions_data[partition].current_tuple_offset += tuples_for_page;
            }
        }

        return thread_write_info;
    }

    std::vector<std::vector<PageWriteInfo<T>>> get_write_info_futures(const std::vector<unsigned> &local_histogram) {
        std::vector<std::vector<PageWriteInfo<T>>> thread_write_info(partitions);
        std::vector<std::mutex> partition_mutexes(partitions);// Mutex per partition

        auto process_partition = [&](size_t partition) {
            size_t tuples_to_write = local_histogram[partition];
            while (tuples_to_write > 0) {
                std::unique_lock lock(partition_mutexes[partition]);
                const size_t current_tuple_offset = partitions_data[partition].current_tuple_offset;
                assert(partitions_data[partition].pages.size() > partitions_data[partition].current_page);
                const size_t free_space = tuples_per_page - current_tuple_offset;
                const size_t tuples_for_page = std::min(free_space, tuples_to_write);

                if (free_space == 0) {
                    allocate_new_page(partition);
                    ++partitions_data[partition].current_page;
                    partitions_data[partition].current_tuple_offset = 0;
                    continue;
                }

                auto current_page = partitions_data[partition].pages[partitions_data[partition].current_page];
                PageWriteInfo<T> write_info(current_page, current_tuple_offset, tuples_for_page);

                thread_write_info[partition].emplace_back(write_info);
                tuples_to_write -= tuples_for_page;
                partitions_data[partition].current_tuple_offset += tuples_for_page;
            }
        };

        // Create and launch threads for each partition
        std::vector<std::future<void>> futures;
        for (size_t partition = 0; partition < partitions; ++partition) {
            futures.emplace_back(std::async(std::launch::async, process_partition, partition));
        }

        // Wait for all threads to finish
        for (auto &fut: futures) {
            fut.get();
        }

        return thread_write_info;
    }

    std::vector<size_t> get_written_tuples_per_partition() {
        std::vector<size_t> written_tuples(partitions, 0);
        for (size_t partition = 0; partition < partitions; ++partition) {
            for (const auto &page: partitions_data[partition].pages) {
                written_tuples[partition] += page.get_tuple_count();
            }
        }
        return written_tuples;
    }
};
