#pragma once

#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "slotted-page/page-manager/LocalPagesAndMergePageManager.hpp"
#include "util/partitioning_function.hpp"
#include <vector>


template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_morsel_lpam(MorselCreator<T> &morsel_creator, LocalPagesAndMergePageManager<T, partitions, page_size> &page_manager) {
    std::vector<std::vector<ManagedSlottedPage<T>>> thread_local_pages(partitions);
    for (unsigned i = 0; i < partitions; ++i) {
        thread_local_pages[i].emplace_back(page_size);
    }

    for (auto [batch, batch_size] = morsel_creator.getBatchOfTuples(); batch; std::tie(batch, batch_size) = morsel_creator.getBatchOfTuples()) {
        for (size_t i = 0; i < batch_size; ++i) {
            const auto &tuple = batch[i];
            const auto partition = partition_function<T, partitions>(tuple);
            if (!thread_local_pages[partition].back().add_tuple(tuple)) {
                thread_local_pages[partition].emplace_back(page_size);
                thread_local_pages[partition].back().add_tuple(tuple);
            }
        }
    }

    auto thread_pages_to_merge = page_manager.hand_in_thread_local_pages(thread_local_pages);
    if (thread_pages_to_merge.empty()) {
        return;
    }
    for (unsigned partition = 0; partition < partitions; ++partition) {
        auto &pages_to_merge_partition = thread_pages_to_merge[partition];
        if (pages_to_merge_partition.empty()) {
            continue;
        }
        unsigned front = 0, back = pages_to_merge_partition.size() - 1;
        while (front < back) {
            auto &back_page = pages_to_merge_partition[back];

            for (auto back_tuples = back_page.get_all_tuples(); const auto &tuple: back_tuples) {
                while (!pages_to_merge_partition[front].add_tuple(tuple)) {
                    ++front;
                    if (front == back) {
                        pages_to_merge_partition[front].clear();
                    }
                }
            }
            --back;
        }
        thread_pages_to_merge[partition].erase(thread_pages_to_merge[partition].begin() + front + 1, thread_pages_to_merge[partition].end());
    }
    page_manager.hand_in_merged_pages(thread_pages_to_merge);
}