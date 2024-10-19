#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include <memory>

template<typename T>
class PageWriteInfo {
public:
    std::shared_ptr<RawSlottedPage<T>> page;
    size_t offset;
    size_t tuples_to_write;
    explicit PageWriteInfo(std::shared_ptr<RawSlottedPage<T>> page, size_t offset, size_t tuples_to_write)
        : page(std::move(page)), offset(offset), tuples_to_write(tuples_to_write) {}
};
