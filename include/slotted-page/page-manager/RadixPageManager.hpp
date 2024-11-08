#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include "slotted-page/page-manager/PageWriteInfo.hpp"
#include "slotted-page/page-manager/PartitionData.hpp"
#include "slotted-page/page-pool/SlottedPagePool.hpp"
#include "util/padded/PaddedMutex.hpp"
#include <array>
#include <barrier>
#include <mutex>
#include <vector>

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class RadixPageManager {
    size_t num_threads;
    size_t tuples_per_page = RawSlottedPage<T>::get_max_tuples(page_size);
    std::array<size_t, partitions> global_histogram;
    std::array<PartitionData<T>, partitions> partitions_data;
    std::array<PaddedMutex, partitions> partition_locks;
    std::barrier<> thread_barrier;
    std::mutex global_histogram_mutex;
    std::once_flag distribute_flag;
    SlottedPagePool<T, partitions, page_size> page_pool;

    void allocate_new_page(size_t partition) {
        assert(page_pool.has_free_page());
        partitions_data[partition].pages.emplace_back(page_size, page_pool.get_single_page());
        partitions_data[partition].current_tuple_offset = 0;
    }

public:
    explicit RadixPageManager(size_t num_threads)
        : num_threads(num_threads),
          thread_barrier(static_cast<long>(num_threads)) {
        global_histogram.fill(0);
    }
    explicit RadixPageManager(size_t num_threads, size_t num_tuples)
        : num_threads(num_threads),
          thread_barrier(static_cast<long>(num_threads)), page_pool(num_tuples) {
        global_histogram.fill(0);
    }

    void add_histogram_chunk(const std::array<unsigned, partitions> &histogram_chunk) {
        {
            std::unique_lock lock(global_histogram_mutex);
            for (size_t i = 0; i < partitions; ++i) {
                global_histogram[i] += histogram_chunk[i];
            }
        }
        thread_barrier.arrive_and_wait();

        std::call_once(distribute_flag, [&]() {
            distribute_pages();
        });
    }

    void distribute_pages() {
        for (size_t partition = 0; partition < partitions; ++partition) {
            const size_t total_tuples = global_histogram[partition];
            const size_t total_pages = (total_tuples + tuples_per_page - 1) / tuples_per_page;
            for (size_t i = 0; i < total_pages; ++i) {
                allocate_new_page(partition);
            }
            assert(partitions_data[partition].pages.size() == total_pages);
        }
    }

    std::vector<std::vector<PageWriteInfo<T>>> get_write_info(const std::array<unsigned, partitions> &local_histogram) {
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
                written_tuples[partition] += page.get_tuple_count();
            }
        }
        return written_tuples;
    }
};
