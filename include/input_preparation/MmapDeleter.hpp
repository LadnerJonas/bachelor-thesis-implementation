#ifndef MMAP_DELETER_HPP
#define MMAP_DELETER_HPP

#include <sys/mman.h>
#include <cstddef>

// A deleter for the mmap'd memory to be used with smart pointers
struct MmapDeleter {
    size_t size_;
    explicit MmapDeleter(size_t size) : size_(size) {}
    void operator()(void* ptr) const {
        if (ptr && ptr != MAP_FAILED) {
            munmap(ptr, size_);
        }
    }
};

#endif // MMAP_DELETER_HPP
