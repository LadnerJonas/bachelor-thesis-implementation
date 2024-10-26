#pragma once
#include <array>
#include <cstdint>

class BenchmarkTuple {
public:
    using KeyType = uint32_t;

private:
    KeyType key;

public:
    BenchmarkTuple() : key(0) {}

    BenchmarkTuple(KeyType key) : key(key) {}

    KeyType get_key() const {
        return key;
    }

    void setKey(KeyType new_key) {
        key = new_key;
    }
};

class Tuple16 : public BenchmarkTuple {
    std::array<uint32_t, 3> data;

public:
    Tuple16() : data{} {}

    Tuple16(KeyType key, const std::array<uint32_t, 3> &data)
        : BenchmarkTuple(key), data(data) {}

    Tuple16(const Tuple16 &other) = default;
    Tuple16 &operator=(const Tuple16 &other) = default;
};

class Tuple100 : public BenchmarkTuple {
    std::array<uint32_t, 24> data;

public:
    Tuple100() : data{} {}

    Tuple100(KeyType key, const std::array<uint32_t, 24> &data)
        : BenchmarkTuple(key), data(data) {}

    Tuple100(const Tuple100 &other) = default;
    Tuple100 &operator=(const Tuple100 &other) = default;
};
