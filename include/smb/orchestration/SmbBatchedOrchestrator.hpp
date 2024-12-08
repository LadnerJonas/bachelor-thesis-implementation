#pragma once

#include <thread>

#include "common/morsel-creation/MorselCreator.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "smb/worker/process_morsel_smb_batched.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class SmbBatchedOrchestrator {
    MorselCreator<T> morsel_creator;
    OnDemandPageManager<T, partitions, page_size> page_manager;
    size_t num_threads;

public:
    explicit SmbBatchedOrchestrator(size_t num_tuples, size_t num_threads) : morsel_creator(num_tuples), page_manager(), num_threads(num_threads) {
    }

    void run() {
        std::vector<std::jthread> threads;
        threads.reserve(num_threads);
        for (unsigned i = 0; i < num_threads; i++) {
            threads.emplace_back([this] {
                process_morsel_smb_batched<T, partitions, page_size>(morsel_creator, page_manager, num_threads);
            });
        }
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};