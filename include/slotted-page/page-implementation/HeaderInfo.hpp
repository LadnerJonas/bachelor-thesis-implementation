#pragma once
#include <atomic>

struct HeaderInfo {
    std::atomic<std::size_t> tuple_count;
};