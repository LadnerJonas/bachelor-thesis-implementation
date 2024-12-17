#pragma once

#include "cmp/worker/CmpProcessorOfUnit.hpp"
#include "slotted-page/page-manager/LockFreePageManager.hpp"

#include <thread>
#include <vector>
template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CmpThreadPoolWithProcessingUnits {
    LockFreePageManager<T, partitions, page_size> &page_manager;
    const unsigned processingUnits;
    const unsigned worker_threads;

    std::vector<std::vector<std::jthread>> workers;
    std::vector<std::pair<std::unique_ptr<T[]>, size_t>> current_tasks;
    std::vector<PaddedMutex> dispatch_mutex;
    std::vector<unsigned> all_workers_done_mask;
    std::vector<PaddedAtomic<bool>> running;
    std::vector<PaddedAtomic<unsigned>> thread_finished;

public:
    explicit CmpThreadPoolWithProcessingUnits(const unsigned processingUnits, const unsigned worker_threads, LockFreePageManager<T, partitions, page_size> &page_manager)
        : page_manager(page_manager), processingUnits(processingUnits), worker_threads(worker_threads) {
        current_tasks.reserve(processingUnits);
        all_workers_done_mask.reserve(processingUnits);
        {
            std::vector<PaddedMutex> temp_dispatch_mutex(processingUnits);
            dispatch_mutex.swap(temp_dispatch_mutex);

            std::vector<PaddedAtomic<unsigned>> temp_thread_finished(processingUnits);
            thread_finished.swap(temp_thread_finished);

            std::vector<PaddedAtomic<bool>> temp_running(processingUnits);
            running.swap(temp_running);
        }

        for (size_t pu = 0; pu < processingUnits; ++pu) {
            current_tasks.emplace_back(std::make_pair(nullptr, 0));
            running[pu].store(true);
            workers.emplace_back();

            const unsigned num_worker = worker_threads / processingUnits + (pu < worker_threads % processingUnits ? 1 : 0);
            all_workers_done_mask.emplace_back((1u << num_worker) - 1);

            thread_finished[pu].store(all_workers_done_mask[pu]);
            for (size_t w = 0; w < num_worker; ++w) {
                workers[pu].emplace_back([this, pu, w, num_worker, worker_threads] {
                    CmpProcessorOfUnit<T, partitions, page_size> processor(w, num_worker, worker_threads, this->page_manager);
                    T *last_ptr = nullptr;
                    while (running[pu].load()) {
                        while ((thread_finished[pu].load() & 1u << w) != 0) {
                            std::this_thread::yield();
                        }
                        auto new_ptr = current_tasks[pu].first.get();
                        if (running[pu].load() && new_ptr != last_ptr) {
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
        while (thread_finished[processingUnitId].load() != all_workers_done_mask[processingUnitId]) {
            std::this_thread::yield();
        }
        current_tasks[processingUnitId].first = std::move(data);
        current_tasks[processingUnitId].second = size;
        thread_finished[processingUnitId].store(0);
    }

    void stop(const unsigned processingUnitId) {
        std::lock_guard lock(dispatch_mutex[processingUnitId]);
        while (thread_finished[processingUnitId].load() != all_workers_done_mask[processingUnitId]) {
            std::this_thread::yield();
        }
        running[processingUnitId].store(false);
        thread_finished[processingUnitId].store(0);
        for (auto &worker: workers[processingUnitId]) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }
};
