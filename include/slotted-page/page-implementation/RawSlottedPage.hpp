#pragma once

#include <atomic>
#include <cstring>
#include <memory>
#include <optional>
#include <vector>

template<typename T>
class RawSlottedPage {
public:
    explicit RawSlottedPage(size_t page_size)
        : page_size(page_size), tuple_count(0), free_space_offset(page_size) {
        page_data = std::shared_ptr<char[]>(new char[page_size], std::default_delete<char[]>());
        size_t max_tuples = page_size / (sizeof(T) + sizeof(SlotInfo));
        slots.reserve(max_tuples);
    }

    std::shared_ptr<char[]> get_page_data() const {
        return page_data;
    }

    bool add_tuple_at_offset(const T &tuple, size_t offset, size_t slot_index) {
        if (offset + sizeof(T) > page_size) {
            return false;
        }

        std::memcpy(page_data.get() + offset, &tuple, sizeof(T));

        slots[slot_index] = SlotInfo(offset, tuple.getKey());
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

private:
    struct SlotInfo {
        size_t offset;
        typename T::KeyType key;

        SlotInfo(size_t off, typename T::KeyType k) : offset(off), key(k) {}
    };

    size_t page_size;
    std::atomic<size_t> tuple_count;
    size_t free_space_offset;
    std::shared_ptr<char[]> page_data;
    std::vector<SlotInfo> slots;
};
