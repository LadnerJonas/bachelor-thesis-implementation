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
};

class Tuple4 : public BenchmarkTuple {
public:
    Tuple4() = default;
    Tuple4(const Tuple4 &other) = default;
    Tuple4 &operator=(const Tuple4 &other) = default;
    static auto get_variable_data() -> int {
        throw std::runtime_error("Tuple4 does not have variable data");
    }
    static auto get_size_of_variable_data() {
        return 0;
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

    auto get_variable_data() -> std::array<uint32_t, 3> & {
        return data;
    }
    static auto get_size_of_variable_data() {
        return sizeof(std::array<uint32_t, 3>);
    }
};

class Tuple100 : public BenchmarkTuple {
    std::array<uint32_t, 24> data;

public:
    Tuple100() : data{} {}

    Tuple100(KeyType key, const std::array<uint32_t, 24> &data)
        : BenchmarkTuple(key), data(data) {}

    Tuple100(const Tuple100 &other) = default;
    Tuple100 &operator=(const Tuple100 &other) = default;

    auto get_variable_data() -> std::array<uint32_t, 24> & {
        return data;
    }
    static auto get_size_of_variable_data() {
        return sizeof(std::array<uint32_t, 3>);
    }
};
