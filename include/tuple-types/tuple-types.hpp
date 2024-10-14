#pragma once
#include <array>

class BenchmarkTuple {
    uint32_t key;

public:
    BenchmarkTuple() : key(0) {
    }

    auto getKey() const {
        return key;
    }
};

class Tuple16 : public BenchmarkTuple {
    std::array<uint32_t, 3> data;

public:
    Tuple16() : data{0, 0, 0} {
    }

    Tuple16(const Tuple16 &other) = default;
    Tuple16 &operator=(const Tuple16 &other) = default;
};

class Tuple100 : public BenchmarkTuple {
    std::array<uint32_t, 24> data;

public:
    Tuple100() : data{} {
    }

    Tuple100(const Tuple100 &other) = default;
    Tuple100 &operator=(const Tuple100 &other) = default;
};
