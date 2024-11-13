#pragma once
#include "slotted-page/page-implementation/RawSlottedPage.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class SlottedPagePool {
    std::shared_ptr<uint8_t[]> page_data;
    size_t current_page = 0;
    size_t max_pages = 0;
    std::mutex mutex;

public:
    SlottedPagePool() = default;
    explicit SlottedPagePool(const size_t tuples_to_store) {
        const auto tuples_per_page = RawSlottedPage<T>::get_max_tuples(page_size);
        const auto tuples_per_partition = (tuples_to_store + partitions - 1) / partitions;
        const auto pages = partitions * ((tuples_per_partition + tuples_per_page - 1) / tuples_per_page);
        max_pages = pages;
        page_data = std::make_shared<uint8_t[]>(pages * page_size);
    }

    auto get_single_page() {
        std::lock_guard lock(mutex);
        assert(current_page < max_pages);
        return page_data.get() + current_page++ * page_size;
    }
    auto has_free_page() const {
        return current_page < max_pages;
    }
};
