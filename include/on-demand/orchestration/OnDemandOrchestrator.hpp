#pragma once

#include <thread>

#include "on-demand/morsel-creation/MorselCreator.hpp"
#include "on-demand/worker/process_morsel.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"

template<typename T, size_t partitions>
class OnDemandOrchestrator {
    MorselCreator<T> morsel_creator;
    OnDemandPageManager<T, partitions> page_manager;
    std::vector<std::thread> threads;
    size_t num_threads;
    size_t num_tuples;

public:
    explicit OnDemandOrchestrator(size_t num_tuples, size_t num_threads) : morsel_creator(num_tuples), num_threads(num_threads), num_tuples(num_tuples) {
    }

    void run() {
        for (unsigned i = 0; i < num_threads; i++) {
            threads.emplace_back([this]() {
                process_morsel<T, partitions>(morsel_creator, page_manager);
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