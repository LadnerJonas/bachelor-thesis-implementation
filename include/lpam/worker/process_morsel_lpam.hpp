#pragma once

#include "slotted-page/page-manager/LocalPagesAndMergePageManager.hpp"
#include "slotted-page/page-manager/OnDemandSingleThreadPageManager.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"
#include "util/partitioning_function.hpp"


template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_morsel_lpam(BatchedTupleGenerator<T> &tuple_generator, LocalPagesAndMergePageManager<T, partitions, page_size> &page_manager) {
    OnDemandSingleThreadPageManager<T, partitions, page_size> thread_local_page_manager;

    static constexpr unsigned buffer_base_value = partitions <= 32 ? 512 : 2 * 1024;
    const static auto total_buffer_size = buffer_base_value * 1024 / sizeof(T);
    const static auto buffer_size_per_partition = total_buffer_size / partitions;
    std::array<unsigned, partitions> buffer_index = {};
    std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);

    for (auto [batch, batch_size] = tuple_generator.getBatchOfTuples(); batch; std::tie(batch, batch_size) = tuple_generator.getBatchOfTuples()) {
        for (size_t i = 0; i < batch_size; ++i) {
            const auto &tuple = batch[i];
            const auto partition = partition_function<T, partitions>(tuple);
            auto &index = buffer_index[partition];
            const auto partition_offset = partition * buffer_size_per_partition;

            if (index == buffer_size_per_partition) {
                thread_local_page_manager.insert_buffer_of_tuples_batched(buffer.get() + partition_offset, buffer_size_per_partition, partition);
                index = 0;
            }

            buffer[partition_offset + index] = tuple;
            ++index;
        }
    }

    for (unsigned i = 0; i < partitions; ++i) {
        if (buffer_index[i] > 0) {
            const auto partition_offset = i * buffer_size_per_partition;
            thread_local_page_manager.insert_buffer_of_tuples_batched(buffer.get() + partition_offset, buffer_index[i], i);
        }
    }

    auto thread_pages_to_merge = page_manager.hand_in_thread_local_pages(thread_local_page_manager.get_all_pages());
    if (thread_pages_to_merge.empty()) {
        return;
    }
    for (unsigned partition = 0; partition < partitions; ++partition) {
        auto &pages_to_merge_partition = thread_pages_to_merge[partition];
        if (pages_to_merge_partition.empty()) {
            continue;
        }

        const auto max_tuples_per_page = ManagedSlottedPage<T>::get_max_tuples(page_size);
        unsigned front = 0, back = pages_to_merge_partition.size() - 1;
        while (front < back) {
            auto &back_page = pages_to_merge_partition[back];
            const auto back_tuples = back_page.get_all_tuples();
            auto back_page_index = 0u;

            while (front != back && back_page_index < back_tuples.size()) {
                auto &front_page = pages_to_merge_partition[front];
                auto front_index = front_page.get_tuple_count();

                auto possible_tuples = max_tuples_per_page - front_index;
                auto tuples_to_copy = std::min(possible_tuples, back_tuples.size() - back_page_index);

                front_page.add_tuple_batch_with_index(back_tuples.data() + back_page_index, front_index, tuples_to_copy);
                front_page.increase_tuple_count(tuples_to_copy);

                back_page_index += tuples_to_copy;
                if (back_page_index < back_tuples.size()) {
                    ++front;
                }
            }

            if (front == back && back_page_index < back_tuples.size()) {
                back_page.clear();
                back_page.add_tuple_batch_with_index(back_tuples.data() + back_page_index, 0, back_tuples.size() - back_page_index);
                back_page.increase_tuple_count(back_tuples.size() - back_page_index);
                break;
            }
            --back;
            thread_pages_to_merge[partition].pop_back();
        }
    }
    page_manager.hand_in_merged_pages(thread_pages_to_merge);
}
