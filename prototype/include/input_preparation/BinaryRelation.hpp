#ifndef BINARY_RELATION_HPP
#define BINARY_RELATION_HPP

#include "../morsel-driven/input_data_storage/MorselManager.hpp"
#include "MmapDeleter.hpp"
#include <fcntl.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

template<typename T>
class BinaryRelation {
public:
    BinaryRelation(const std::string &filepath) : size_(0), fd_(-1) {
        fd_ = open(filepath.c_str(), O_RDONLY);
        if (fd_ == -1) {
            throw std::runtime_error("Failed to open file: " + filepath);
        }

        struct stat sb {};
        if (fstat(fd_, &sb) == -1) {
            close(fd_);
            throw std::runtime_error("Failed to get file size: " + filepath);
        }

        size_ = sb.st_size;
        void *mapped_data = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (mapped_data == MAP_FAILED) {
            close(fd_);
            throw std::runtime_error("Failed to map file: " + filepath);
        }

        data_ = std::shared_ptr<T[]>(static_cast<T *>(mapped_data), MmapDeleter(size_));
    }

    ~BinaryRelation() {
        if (fd_ != -1) {
            close(fd_);
        }
    }

    std::shared_ptr<T[]> get_data() { return data_; }

    size_t get_size() { return size_ / sizeof(T); }

private:
    std::shared_ptr<T[]> data_;
    size_t size_;
    int fd_;
};

#endif// BINARY_RELATION_HPP
