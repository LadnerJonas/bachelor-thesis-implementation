#pragma once

#include "cmp/worker/CmpProcessorOfUnit.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"

#include <thread>
#include <vector>
template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CmpThreadPoolWithProcessingUnits {
    OnDemandPageManager<T, partitions, page_size> &page_manager;
    const unsigned processingUnits;
    const unsigned worker_threads;

    std::vector<std::vector<std::jthread>> workers;
    std::vector<std::pair<std::unique_ptr<T[]>, size_t>> current_tasks;
    std::vector<bool> running;
    std::vector<std::mutex> dispatch_mutex;
    std::vector<unsigned> all_workers_done_mask;
    std::vector<unsigned> thread_finished;

public:
    explicit CmpThreadPoolWithProcessingUnits(const unsigned processingUnits, const unsigned worker_threads, OnDemandPageManager<T, partitions, page_size> &page_manager)
        : page_manager(page_manager), processingUnits(processingUnits), worker_threads(worker_threads) {
        current_tasks.reserve(processingUnits);
        running.reserve(processingUnits);
        all_workers_done_mask.reserve(processingUnits);
        thread_finished.reserve(processingUnits);
        {
            std::vector<std::mutex> temp_dispatch_mutex(processingUnits);
            dispatch_mutex.swap(temp_dispatch_mutex);
        }

        for (size_t pu = 0; pu < processingUnits; ++pu) {
            current_tasks.emplace_back(std::make_pair(nullptr, 0));
            running.emplace_back(true);
            workers.emplace_back();

            const unsigned num_worker = worker_threads / processingUnits + (pu < worker_threads % processingUnits ? 1 : 0);
            all_workers_done_mask.emplace_back((1u << num_worker) - 1);

            thread_finished.emplace_back(all_workers_done_mask[pu]);
            for (size_t w = 0; w < num_worker; ++w) {
                workers[pu].emplace_back([this, pu, w, num_worker] {
                    CmpProcessorOfUnit<T, partitions, page_size> processor(w, num_worker, this->page_manager);
                    T *last_ptr = nullptr;
                    while (running[pu]) {
                        while ((thread_finished[pu] & 1u << w) != 0) {
                            std::this_thread::yield();
                        }
                        auto new_ptr = current_tasks[pu].first.get();
                        if (running[pu] && new_ptr != last_ptr) {
                            last_ptr = new_ptr;
                            processor.process(current_tasks[pu].first.get(), current_tasks[pu].second);
                        }
                        thread_finished[pu] |= 1u << w;
                    }

                    processor.process(nullptr, 0);
                });
            }
        }
    }

    void dispatchTask(const unsigned processingUnitId, std::unique_ptr<T[]> data, size_t size) {
        std::lock_guard lock(dispatch_mutex[processingUnitId]);
        while (thread_finished[processingUnitId] != all_workers_done_mask[processingUnitId]) {
            std::this_thread::yield();
        }
        current_tasks[processingUnitId].first = std::move(data);
        current_tasks[processingUnitId].second = size;
        thread_finished[processingUnitId] = 0;
    }

    void stop(const unsigned processingUnitId) {
        std::lock_guard lock(dispatch_mutex[processingUnitId]);
        while (thread_finished[processingUnitId] != all_workers_done_mask[processingUnitId]) {
            std::this_thread::yield();
        }
        running[processingUnitId] = false;
        thread_finished[processingUnitId] = 0;
        for (auto &worker: workers[processingUnitId]) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }
};
