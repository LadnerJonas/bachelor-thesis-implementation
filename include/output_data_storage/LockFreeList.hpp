#ifndef LOCKFREELIST_HPP
#define LOCKFREELIST_HPP

#include <atomic>
#include <vector>
#include <memory>
#include <algorithm>
#include <cstdlib>

template<typename T>
struct alignas(std::hardware_destructive_interference_size) PaddedAtomic {
    std::atomic<T> value;

    PaddedAtomic() : value(nullptr) {}

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
    explicit LockFreeList(size_t num_buckets = 16);

    void insert(const T *array, size_t size);

    std::vector<T> get_bundled_vector() const;

private:
    struct Node {
        std::unique_ptr<T[]> array;
        size_t size;
        std::atomic<Node *> next;

        Node(const T *arr, size_t sz)
                : array(arr ? new T[sz] : nullptr), size(sz), next(nullptr) {
            if (arr) {
                std::copy(arr, arr + sz, array.get());
            }
        }
    };

    std::vector<PaddedAtomic<Node *>> heads_;
    size_t num_buckets_;

    size_t get_random_bucket() const;
};

template<typename T>
LockFreeList<T>::LockFreeList(size_t num_buckets)
        : heads_(num_buckets), num_buckets_(num_buckets) {
    for (size_t i = 0; i < num_buckets; ++i) {
        heads_[i].store(nullptr); // Initially, all heads point to nullptr
    }
}

template<typename T>
size_t LockFreeList<T>::get_random_bucket() const {
    return std::rand() % num_buckets_;
}

template<typename T>
void LockFreeList<T>::insert(const T *array, size_t size) {
    Node *new_node = new Node(array, size); // Allocate new node

    size_t bucket = get_random_bucket();
    Node *old_head = heads_[bucket].load();

    while (!heads_[bucket].compare_exchange_strong(old_head, new_node)) {
        new_node->next.store(old_head);
    }
    new_node->next.store(old_head); // Set next pointer after successful insertion
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
