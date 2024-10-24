#pragma once

#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include "slotted-page/page-implementation/SlotInfo.hpp"

template<typename T>
class ManagedSlottedPage {
    size_t page_size;
    size_t free_space_offset;
    size_t num_slots;
    std::unique_ptr<char[]> page_data;
    std::vector<SlotInfo<T>> slots;

public:
    explicit ManagedSlottedPage(size_t page_size)
        : page_size(page_size), free_space_offset(page_size), num_slots(0) {
        page_data = std::make_unique<char[]>(page_size);
    }

    bool add_tuple(const T &tuple) {
        size_t tuple_size = sizeof(T);
        if (free_space_offset - (num_slots + 1) * sizeof(SlotInfo<T>) < tuple_size) {
            return false;
        }
        free_space_offset -= tuple_size;
        std::memcpy(page_data.get() + free_space_offset, &tuple, tuple_size);
        slots.emplace_back(free_space_offset, tuple.get_key());
        num_slots++;
        return true;
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
    size_t get_num_slots() const {
        return num_slots;
    }
};
