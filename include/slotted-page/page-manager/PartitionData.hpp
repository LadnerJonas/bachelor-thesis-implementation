#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"

template<typename T>
struct PartitionData {
    std::vector<std::shared_ptr<RawSlottedPage<T>>> pages;
    size_t current_page = 0;
    size_t current_tuple_offset = 0;
};