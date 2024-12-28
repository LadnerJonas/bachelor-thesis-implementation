#pragma once

#include <thread>

#include "slotted-page/page-manager/LockFreePageManager.hpp"
#include "smb/worker/process_morsel_smb_lock_free.hpp"
#include "tuple-generator/BatchedTupleGenerator.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class SmbLockFreeOrchestrator {
    LockFreePageManager<T, partitions, page_size> page_manager;
    size_t num_tuples;
    size_t num_threads;

public:
    explicit SmbLockFreeOrchestrator(const size_t num_tuples, const size_t num_threads) : page_manager(), num_tuples(num_tuples), num_threads(num_threads) {
    }

    void run() {
        std::vector<std::jthread> threads;
        threads.reserve(num_threads);
        std::vector<BatchedTupleGenerator<T>> generators;
        generators.reserve(num_threads);
        for (unsigned i = 0; i < num_threads; i++) {
            const auto tuple_to_process = num_tuples / num_threads + (i < num_tuples % num_threads);
            generators.emplace_back(tuple_to_process);
            threads.emplace_back([&, i] {
                process_morsel_smb_lock_free<T, partitions, page_size>(generators[i], page_manager, num_threads);
            });
        }

        for (auto &thread: threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};