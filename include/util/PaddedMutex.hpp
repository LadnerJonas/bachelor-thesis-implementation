#ifndef PADDEDMUTEX_HPP
#define PADDEDMUTEX_HPP

#include <new>
#include <mutex>

struct alignas(std::hardware_destructive_interference_size) PaddedMutex {
    std::mutex mutex;
};
#endif //PADDEDMUTEX_HPP
