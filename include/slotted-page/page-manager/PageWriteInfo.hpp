#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include <memory>

template<typename T>
class PageWriteInfo {
public:
    uint8_t *page_data;
    size_t start_num;
    size_t tuples_to_write;
    explicit PageWriteInfo(const RawSlottedPage<T> &page, const size_t offset, const size_t tuples_to_write)
        : page_data(page.get_page_data()), start_num(offset), tuples_to_write(tuples_to_write) {}
};
