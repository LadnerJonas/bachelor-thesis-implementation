#ifndef LOCKFREELIST_HPP
#define LOCKFREELIST_HPP

#include <atomic>
#include <vector>
#include <memory>
#include <stdexcept>
#include <cassert>
#include <algorithm>
#include <cstdlib> // For std::rand

template<typename T>
struct alignas(std::hardware_destructive_interference_size) PaddedAtomic {
    std::atomic<T> value;

    PaddedAtomic() : value() {}

    explicit PaddedAtomic(const T &init) : value(init) {}

    T load(std::memory_order order = std::memory_order_seq_cst) const {
        return value.load(order);
    }

    void store(T desired, std::memory_order order = std::memory_order_seq_cst) {
        value.store(desired, order);
    }

    bool compare_exchange_strong(T &expected, T desired, std::memory_order order = std::memory_order_seq_cst) {
        return value.compare_exchange_strong(expected, desired, order);
    }
};

template<typename T>
class LockFreeList {
public:
    explicit LockFreeList(size_t num_buckets = 4);

    void insert(const T *array, size_t size);

    std::vector<T> get_bundled_vector() const;

private:
    struct Node {
        std::unique_ptr<T[]> array;
        size_t size;
        std::atomic<Node *> next;

        Node(const T *arr, size_t sz)
                : array(new T[sz]), size(sz), next(nullptr) {
            std::copy(arr, arr + sz, array.get());
        }
    };

    std::vector<PaddedAtomic<Node *>> heads_;
    std::vector<PaddedAtomic<Node *>> tails_;
    size_t num_buckets_;

    size_t get_random_bucket() const;
};

template<typename T>
LockFreeList<T>::LockFreeList(size_t num_buckets)
        : heads_(num_buckets), tails_(num_buckets), num_buckets_(num_buckets) {
    for (size_t i = 0; i < num_buckets; ++i) {
        Node *dummy_node = new Node(nullptr, 0); // Dummy node
        heads_[i].store(dummy_node);
        tails_[i].store(dummy_node);
    }
}

template<typename T>
size_t LockFreeList<T>::get_random_bucket() const {
    return std::rand() % num_buckets_;
}

template<typename T>
void LockFreeList<T>::insert(const T *array, size_t size) {
    Node *new_node = new Node(array, size);

    size_t bucket = get_random_bucket();
    Node *dummy_node = new Node(nullptr, 0); // Dummy node for comparison
    Node *old_tail = tails_[bucket].load();

    while (true) {
        if (old_tail->next.compare_exchange_weak(dummy_node, new_node)) {
            delete dummy_node;
            tails_[bucket].compare_exchange_strong(old_tail, new_node);
            break;
        }
        old_tail = tails_[bucket].load();
    }
}

template<typename T>
std::vector<T> LockFreeList<T>::get_bundled_vector() const {
    std::vector<T> result;

    for (size_t i = 0; i < num_buckets_; ++i) {
        Node *current = heads_[i].load();
        while (current) {
            result.insert(result.end(), current->array.get(), current->array.get() + current->size);
            auto next = current->next.load();
            delete current;
            current = next;
        }
    }

    return result;
}

#endif // LOCKFREELIST_HPP
