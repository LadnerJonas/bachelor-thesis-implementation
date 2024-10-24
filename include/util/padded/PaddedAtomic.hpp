#ifndef PADDEDATOMIC_HPP
#define PADDEDATOMIC_HPP

#include <atomic>
#include <new>

template<typename T>
struct alignas(std::hardware_destructive_interference_size) PaddedAtomic {
    std::atomic<T> value;

    PaddedAtomic() : value() {}

    explicit PaddedAtomic(const T &init) : value(init) {}

    T load(std::memory_order order = std::memory_order_seq_cst) const { return value.load(order); }

    void store(T desired, std::memory_order order = std::memory_order_seq_cst) { value.store(desired, order); }

    bool compare_exchange_strong(T &expected, T desired, std::memory_order order = std::memory_order_seq_cst) {
        return value.compare_exchange_strong(expected, desired, order);
    }

    template<typename U = T>
    typename std::enable_if<std::is_integral<U>::value || std::is_floating_point<U>::value || std::is_pointer<U>::value, U>::type
    fetch_add(U arg) {
        return value.fetch_add(arg);
    }

    T get() {
        return value.load();
    }
};
#endif// PADDEDATOMIC_HPP
