#pragma once

#include "cmp/worker/CmpProcessor.hpp"
#include "util/padded/PaddedAtomic.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CmpThreadPool {
    OnDemandSingleThreadPageManager<T, partitions, page_size> &page_manager;
    std::vector<std::jthread> workers;
    std::pair<std::unique_ptr<T[]>, size_t> current_task = {nullptr, 0};
    const unsigned all_workers_done_mask;
    PaddedAtomic<unsigned> thread_finished;
    PaddedAtomic<bool> running;
    std::mutex dispatch_mutex{};

public:
    explicit CmpThreadPool(size_t numThreads, OnDemandSingleThreadPageManager<T, partitions, page_size> &page_manager)
        : page_manager(page_manager), all_workers_done_mask((1u << numThreads) - 1), thread_finished(all_workers_done_mask), running(true) {
        workers.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this, i, numThreads] {
                CmpProcessor<T, partitions, page_size> processor(i, numThreads, this->page_manager);
                T *last_ptr = nullptr;
                while (running.load()) {
                    while ((thread_finished.load() & 1u << i) != 0) {
                    }
                    auto new_ptr = current_task.first.get();
                    if (running.load() && new_ptr != last_ptr) {
                        last_ptr = new_ptr;
                        processor.process(current_task.first.get(), current_task.second);
                        thread_finished |= 1u << i;
                    }
                }

                processor.process(nullptr, 0);
            });
        }
    }

    void dispatchTask(std::unique_ptr<T[]> data, size_t size) {
        std::lock_guard lock(dispatch_mutex);
        while (thread_finished.load() != all_workers_done_mask) {
        }
        current_task.first = std::move(data);
        current_task.second = size;
        thread_finished.store(0);
    }

    void stop() {
        while (thread_finished.load() != all_workers_done_mask) {
        }
        running.store(false);
        current_task = {nullptr, 0};
        thread_finished.store(0);
        for (auto &worker: workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }
};
