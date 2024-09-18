#ifndef MORSEL_MANAGER_HPP
#define MORSEL_MANAGER_HPP

#include <memory>
#include <atomic>
#include <optional>
#include <utility>
#include <iostream>

template<typename T>
class MorselManager {
public:
    MorselManager(std::shared_ptr<T[]> data, size_t total_elements, size_t morsel_size)
            : data_(std::move(data)), total_elements_(total_elements), morsel_size_(morsel_size), current_index_(0) {}

    std::optional<std::pair<std::shared_ptr<T[]>, size_t>> get_next_morsel() {
        size_t index = current_index_.fetch_add(morsel_size_);
        if (index >= total_elements_) {
            return std::nullopt;
        }
        //std::cout << "Morsel index: " << index << " and total number of elements: " << total_elements_ <<std::endl;

        std::shared_ptr<T[]> morsel_start(data_, data_.get() + index);
        size_t actual_morsel_size = std::min(morsel_size_, total_elements_ - index);
        return std::make_pair(morsel_start, actual_morsel_size);
    }

private:
    std::shared_ptr<T[]> data_;
    size_t total_elements_;
    size_t morsel_size_;
    std::atomic<size_t> current_index_;
};

#endif // MORSEL_MANAGER_HPP
