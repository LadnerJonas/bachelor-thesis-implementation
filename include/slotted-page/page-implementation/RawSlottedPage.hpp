#pragma once

#include <atomic>
#include <cassert>
#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include "slotted-page/page-implementation/SlotInfo.hpp"

template<typename T>
class RawSlottedPage {
    size_t page_size;
    std::atomic<size_t> tuple_count;
    size_t free_space_offset;
    std::shared_ptr<T[]> page_data;
    std::vector<SlotInfo<T>> slots;

public:
    explicit RawSlottedPage(size_t page_size)
        : page_size(page_size), tuple_count(0), free_space_offset(page_size) {
        size_t max_tuples = page_size / (sizeof(T) + sizeof(SlotInfo<T>));
        page_data = std::make_shared<T[]>(max_tuples);
        slots.reserve(max_tuples);
    }

    std::shared_ptr<T[]> get_page_data() const {
        return page_data;
    }

    bool add_tuple_at_offset(const T &tuple, size_t offset, size_t slot_index) {
        assert(offset + sizeof(T) <= page_size);
        std::memcpy(page_data.get() + offset, &tuple, sizeof(T));

        slots[slot_index] = SlotInfo<T>(offset, tuple.getKey());
        return true;
    }
    void update_tuple_count(size_t count) {
        tuple_count.fetch_add(count, std::memory_order_relaxed);
    }

    std::optional<T> get_tuple(const typename T::KeyType &key) const {
        for (const auto &slot: slots) {
            if (slot.key == key) {
                T tuple;
                std::memcpy(&tuple, page_data.get() + slot.offset, sizeof(T));
                return tuple;
            }
        }
        return std::nullopt;
    }

    std::vector<T> get_all_tuples() const {
        std::vector<T> all_tuples;
        for (const auto &slot: slots) {
            T tuple;
            std::memcpy(&tuple, page_data.get() + slot.offset, sizeof(T));
            all_tuples.push_back(tuple);
        }
        return all_tuples;
    }

    size_t get_tuple_count() const {
        return tuple_count.load(std::memory_order_relaxed);
    }
};
