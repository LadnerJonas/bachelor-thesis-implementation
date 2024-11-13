#pragma once
#include <atomic>

struct HeaderInfoAtomic {
    std::atomic<unsigned> tuple_count;
};

struct HeaderInfoNonAtomic {
    unsigned tuple_count;
};