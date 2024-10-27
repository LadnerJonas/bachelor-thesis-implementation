#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include "slotted-page/page-implementation/HeaderInfo.hpp"
#include "slotted-page/page-implementation/SlotInfo.hpp"

template<typename T>
class RawSlottedPage {
    size_t page_size;
    std::shared_ptr<uint8_t[]> page_data;// Page data is now a byte array
    HeaderInfo *header;
    SlotInfo<T> *slots;
    T *data_section;
    size_t max_tuples;

public:
    explicit RawSlottedPage(size_t page_size)
        : page_size(page_size) {
        // Compute max possible tuples based on page_data size and tuple/slot size
        max_tuples = (page_size - sizeof(HeaderInfo)) / (sizeof(T) + sizeof(SlotInfo<T>));
        page_data = std::make_shared<uint8_t[]>(page_size);

        // Set up header, slots, and data pointers within the page_data
        header = reinterpret_cast<HeaderInfo *>(page_data.get());
        slots = reinterpret_cast<SlotInfo<T> *>(page_data.get() + sizeof(HeaderInfo));
        data_section = reinterpret_cast<T *>(page_data.get() + page_size - max_tuples * sizeof(T));

        // Initialize header values
        header->tuple_count = 0;
    }

    std::shared_ptr<uint8_t[]> get_page_data() const {
        return page_data;
    }

    static void write_tuple(const std::shared_ptr<uint8_t[]> &page_data, const size_t page_size, const T &tuple, unsigned entry_num) {
        //store tuple starting from the end of the page_data
        size_t tuple_offset_from_end = page_size - (entry_num + 1) * sizeof(T);
        auto tuple_start = page_data.get() + tuple_offset_from_end;
        std::memcpy(tuple_start, &tuple, sizeof(T));

        //store slot
        SlotInfo<T> slot(tuple_offset_from_end, sizeof(T), tuple.get_key());
        auto slot_start = page_data.get() + sizeof(HeaderInfo) + entry_num * sizeof(SlotInfo<T>);
        std::memcpy(slot_start, &slot, sizeof(SlotInfo<T>));
    }

    static void increase_tuple_count(const std::shared_ptr<uint8_t[]> &page_data, size_t tuple_count) {
        auto header = reinterpret_cast<HeaderInfo *>(page_data.get());
        header->tuple_count += tuple_count;
    }

    static size_t get_max_tuples(size_t page_size) {
        return (page_size - sizeof(HeaderInfo)) / (sizeof(T) + sizeof(SlotInfo<T>));
    }

    std::optional<T> get_tuple(const typename T::KeyType &key) const {
        for (size_t i = 0; i < header->tuple_count; ++i) {
            if (slots[i].key == key) {
                T tuple;
                std::memcpy(&tuple, data_section + i, sizeof(T));
                return tuple;
            }
        }
        return std::nullopt;
    }

    std::vector<T> get_all_tuples() const {
        std::vector<T> all_tuples;
        for (size_t i = 0; i < header->tuple_count; ++i) {
            T tuple;
            std::memcpy(&tuple, data_section + i, sizeof(T));
            all_tuples.push_back(tuple);
        }
        return all_tuples;
    }

    size_t get_tuple_count() const {
        return header->tuple_count;
    }
};
