#pragma once
#include <array>
#include <cassert>
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
    explicit Tuple4(const KeyType key) : BenchmarkTuple(key) {}
    Tuple4(const KeyType key, const std::array<uint32_t, 0> &) : BenchmarkTuple(key) {
        throw std::runtime_error("Tuple4 does not have variable data");
    }
    Tuple4(const Tuple4 &other) = default;
    Tuple4 &operator=(const Tuple4 &other) = default;
    [[nodiscard]] static const auto get_variable_data() -> int & {
        throw std::runtime_error("Tuple4 does not have variable data");
    }
    constexpr static auto get_size_of_variable_data() {
        return 0;
    }
};

class Tuple16 : public BenchmarkTuple {
    std::array<uint32_t, 3> data;

public:
    Tuple16() : data{} {}
    explicit Tuple16(const KeyType key) : BenchmarkTuple(key), data{} {}
    Tuple16(const KeyType key, const std::array<uint32_t, 3> &data)
        : BenchmarkTuple(key), data(data) {}

    Tuple16(const Tuple16 &other) = default;
    Tuple16 &operator=(const Tuple16 &other) = default;

    [[nodiscard]] const auto get_variable_data() const -> const std::array<uint32_t, 3> & {
        return data;
    }
    constexpr static auto get_size_of_variable_data() {
        assert(sizeof(data) == 12);
        return sizeof(data);
    }
};

class Tuple100 : public BenchmarkTuple {
    std::array<uint32_t, 24> data;

public:
    Tuple100() : data{} {}
    explicit Tuple100(const KeyType key) : BenchmarkTuple(key), data{} {}
    Tuple100(const KeyType key, const std::array<uint32_t, 24> &data)
        : BenchmarkTuple(key), data(data) {}

    Tuple100(const Tuple100 &other) = default;
    Tuple100 &operator=(const Tuple100 &other) = default;

    [[nodiscard]] const auto get_variable_data() const -> const std::array<uint32_t, 24> & {
        return data;
    }
    constexpr static auto get_size_of_variable_data() {
        assert(sizeof(std::array<uint32_t, 24>) == 96);
        return sizeof(std::array<uint32_t, 24>);
    }
};
