#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include <memory>

template<typename T>
class PageWriteInfo {
public:
    uint8_t *page_data;
    unsigned start_num;
    unsigned tuples_to_write;
    unsigned written_tuples = 0;
    explicit PageWriteInfo(const RawSlottedPage<T> &page, const size_t offset, const size_t tuples_to_write)
        : page_data(page.get_page_data()), start_num(offset), tuples_to_write(tuples_to_write) {}
};
