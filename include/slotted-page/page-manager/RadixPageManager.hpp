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

    void allocate_new_page(size_t partition) {
        // assert(page_pool.has_free_page());
        partitions_data[partition].pages.emplace_back(page_size);
        partitions_data[partition].current_tuple_offset = 0;
    }

public:
    explicit RadixPageManager(const size_t num_threads) : num_threads(num_threads) {
        global_histogram.fill(0);
    }

    void allocate_pages_for_new_histogram_state(const size_t partition, const size_t tuples_to_write, const size_t old_histogram_state) {
        const size_t total_pages_old = (old_histogram_state + tuples_per_page - 1) / tuples_per_page;
        const size_t total_pages_new = (old_histogram_state + tuples_to_write + tuples_per_page - 1) / tuples_per_page;
        auto page_diff = total_pages_new - total_pages_old;
        while (page_diff--) {
            allocate_new_page(partition);
        }
    }

    void assign_pages(const size_t partition, std::array<std::vector<PageWriteInfo<T>>, partitions> &thread_write_info, size_t tuples_to_write) {
        do {
            const size_t current_tuple_offset = partitions_data[partition].current_tuple_offset;
            auto &current_page = partitions_data[partition].pages[partitions_data[partition].current_page];
            assert(partitions_data[partition].pages.size() > partitions_data[partition].current_page);
            const size_t free_space = tuples_per_page - current_tuple_offset;
            const size_t tuples_for_page = std::min(free_space, tuples_to_write);

            thread_write_info[partition].emplace_back(current_page, current_tuple_offset, tuples_for_page);
            tuples_to_write -= tuples_for_page;
            partitions_data[partition].current_tuple_offset += tuples_for_page;

            if (tuples_to_write > 0) {
                ++partitions_data[partition].current_page;
                partitions_data[partition].current_tuple_offset = 0;
            }
        } while (tuples_to_write > 0);
    }
    std::array<std::vector<PageWriteInfo<T>>, partitions> add_histogram_chunk(const std::array<unsigned, partitions> &local_histogram) {
        const unsigned random_start_partition = rand() % partitions;

        std::array<std::vector<PageWriteInfo<T>>, partitions> thread_write_info;
        for (size_t i = 0; i < partitions; ++i) {
            const auto partition = (random_start_partition + i) % partitions;
            size_t tuples_to_write = local_histogram[partition];
            if (tuples_to_write > 0) {
                std::lock_guard lock(partition_locks[partition]);
                const size_t old_histogram_state = global_histogram[partition];
                global_histogram[partition] += tuples_to_write;
                allocate_pages_for_new_histogram_state(partition, tuples_to_write, old_histogram_state);
                assign_pages(partition, thread_write_info, tuples_to_write);
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
