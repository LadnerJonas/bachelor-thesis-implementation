#pragma once
#include <atomic>

struct HeaderInfo {
    std::atomic<unsigned> tuple_count;
};