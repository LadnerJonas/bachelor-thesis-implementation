#pragma once

#include "slotted-page/page-implementation/RawSlottedPage.hpp"

template<typename T>
struct PartitionData {
    std::vector<RawSlottedPage<T>> pages;
    unsigned current_page = 0;
    unsigned current_tuple_offset = 0;
};