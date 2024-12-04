#pragma once

constexpr auto PADDING = 4096u;

template<size_t page_size = 5 * 1024 * 1024>
class SlottedPagePool {
    std::shared_ptr<uint8_t[]> page_data;
    std::atomic<unsigned> current_page = 0;
    unsigned max_pages = 0;

public:
    SlottedPagePool() = default;
    explicit SlottedPagePool(const size_t pages) {
        max_pages = pages;
        page_data = std::make_shared<uint8_t[]>(pages * (page_size + PADDING));
    }

    uint8_t *get_single_page() {
        assert(current_page < max_pages);
        return page_data.get() + current_page++ * (page_size + PADDING);
    }

    uint8_t *get_multiple_pages(const size_t num_pages) {
        assert(current_page + num_pages <= max_pages);
        auto start = current_page.fetch_add(num_pages);
        return page_data.get() + start * (page_size + PADDING);
    }
    auto has_free_page() const {
        return current_page < max_pages;
    }
};
