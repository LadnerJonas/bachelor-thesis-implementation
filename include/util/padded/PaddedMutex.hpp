#pragma once

#include <mutex>
#include <new>

struct alignas(std::hardware_destructive_interference_size) PaddedMutex {
    std::mutex mutex;
    void lock() {
        mutex.lock();
    }
    void unlock() {
        mutex.unlock();
    }
};
