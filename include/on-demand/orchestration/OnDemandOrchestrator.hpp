#pragma once

#include <thread>

#include "common/morsel-creation/MorselCreator.hpp"
#include "on-demand/worker/process_morsel.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class OnDemandOrchestrator {
    MorselCreator<T> morsel_creator;
    OnDemandPageManager<T, partitions, page_size> page_manager;
    size_t num_threads;

public:
    explicit OnDemandOrchestrator(size_t num_tuples, size_t num_threads) : morsel_creator(num_tuples), num_threads(num_threads) {
    }

    void run() {
        std::vector<std::jthread> threads;
        threads.reserve(num_threads);
        for (unsigned i = 0; i < num_threads; i++) {
            threads.emplace_back([this]() {
                process_morsel<T, partitions, page_size>(morsel_creator, page_manager);
            });
        }
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};