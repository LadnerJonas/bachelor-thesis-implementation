#pragma once

#include "cmp/worker/CmpProcessor.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CmpThreadPool {
    std::vector<std::jthread> workers;
    std::pair<std::unique_ptr<T[]>, size_t> current_task = {nullptr, 0};
    bool running;
    OnDemandSingleThreadPageManager<T, partitions, page_size> &page_manager;
    const unsigned all_workers_done_mask;
    std::atomic<unsigned> thread_finished;

public:
    explicit CmpThreadPool(size_t numThreads, OnDemandSingleThreadPageManager<T, partitions, page_size> &page_manager)
        : running(true), page_manager(page_manager), all_workers_done_mask((1u << numThreads) - 1), thread_finished(all_workers_done_mask) {
        workers.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this, i, numThreads] {
                CmpProcessor<T, partitions, page_size> processor(i, numThreads, this->page_manager);

                while (running) {
                    while ((thread_finished.load() & 1u << i) != 0) {
                        std::this_thread::yield();
                    }
                    if (running && current_task.first != nullptr) {
                        processor.process(current_task.first.get(), current_task.second);
                        thread_finished.fetch_or(1u << i);
                    }
                }

                processor.process(nullptr, 0);
            });
        }
    }

    void dispatchTask(std::unique_ptr<T[]> data, size_t size) {
        while (thread_finished.load() != all_workers_done_mask) {
            std::this_thread::yield();
        }
        current_task.first = std::move(data);
        current_task.second = size;
        thread_finished.store(0);
    }

    void stop() {
        while (thread_finished.load() != all_workers_done_mask) {
            std::this_thread::yield();
        }
        running = false;
        thread_finished.store(0);
        for (auto &worker: workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }
};
