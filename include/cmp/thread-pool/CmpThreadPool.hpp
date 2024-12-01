#pragma once

#include "cmp/worker/CmpProcessor.hpp"


#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>
template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class CmpThreadPool {
    std::vector<std::jthread> workers;
    std::pair<std::unique_ptr<T[]>, size_t> current_task = {nullptr, 0};
    bool running;
    OnDemandSingleThreadPageManager<T, partitions, page_size> &page_manager;
    std::vector<PaddedAtomic<bool>> thread_finished;

public:
    explicit CmpThreadPool(size_t numThreads, OnDemandSingleThreadPageManager<T, partitions, page_size> &page_manager) : workers(numThreads), running(true), page_manager(page_manager), thread_finished(numThreads) {
        for (size_t i = 0; i < numThreads; ++i) {
            thread_finished[i].store(true);
            workers[i] = std::jthread([this, i, numThreads] {
                // Each worker thread will initialize its own processor instance
                CmpProcessor<T, partitions, page_size> processor(i, numThreads, this->page_manager);

                while (running) {

                    // Wait for the task to be available
                    while (thread_finished[i].get()) {
                        std::this_thread::yield();
                    }

                    // Only process the task if it's available
                    if (running && current_task.first != nullptr) {
                        // std::cout << "Processing task " << current_task.second << std::endl;
                        processor.process(current_task.first.get(), current_task.second);
                    }
                    if (!running) {
                        processor.process(nullptr, 0);
                    }
                    thread_finished[i].store(true);
                }
            });
        }
    }

    // Dispatch tasks immediately to all workers (write lock)
    void dispatchTask(std::unique_ptr<T[]> data, size_t size) {
        bool worker_finished;
        do {
            worker_finished = true;
            unsigned thread_num = 0;
            for (auto &&i: thread_finished) {
                worker_finished &= i.load();
                if (i.load() == false) {
                    // std::cout << "Worker " << thread_num<< " not finished" << std::endl;
                    break;
                }
                thread_num++;
            }
            if (!worker_finished) {
                std::this_thread::yield();
            }
        } while (!worker_finished);

        // Set the task
        current_task.first = std::move(data);
        current_task.second = size;
        for (auto &&i: thread_finished) {
            i.store(false);
        }
        // std::cout << "Dispatched task" << std::endl;
    }

    void stop() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        bool worker_finished;
        do {
            worker_finished = true;
            unsigned thread_num = 0;
            for (auto &&i: thread_finished) {
                worker_finished &= i.load();
                if (i.load() == false) {
                    // std::cout << "Worker " << thread_num<< " not finished" << std::endl;
                    break;
                }
                thread_num++;
            }
            if (!worker_finished) {
                std::this_thread::yield();
            }
        } while (!worker_finished);
        running = false;
        for (auto &&i: thread_finished) {
            i.store(false);
        }
        for (auto &worker: workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }
};