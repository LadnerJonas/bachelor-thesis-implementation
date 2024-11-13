#pragma once

#include <thread>

#include "common/morsel-creation/MorselCreator.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "smb/worker/process_morsel_smb_batched.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class SmbBatchedOrchestrator {
    MorselCreator<T> morsel_creator;
    OnDemandPageManager<T, partitions, page_size> page_manager;
    std::vector<std::thread> threads;
    size_t num_threads;
    size_t num_tuples;

public:
    explicit SmbBatchedOrchestrator(size_t num_tuples, size_t num_threads) : morsel_creator(num_tuples), page_manager(num_tuples), num_threads(num_threads), num_tuples(num_tuples) {
    }

    void run() {
        for (unsigned i = 0; i < num_threads; i++) {
            threads.emplace_back([this]() {
                process_morsel_smb_batched<T, partitions, page_size>(morsel_creator, page_manager);
            });
        }

        for (auto &thread: threads) {
            thread.join();
        }
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};