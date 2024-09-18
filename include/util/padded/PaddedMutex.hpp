#ifndef PADDEDMUTEX_HPP
#define PADDEDMUTEX_HPP

#include <mutex>
#include <new>

struct alignas(std::hardware_destructive_interference_size) PaddedMutex {
    std::mutex mutex;
};
#endif// PADDEDMUTEX_HPP
