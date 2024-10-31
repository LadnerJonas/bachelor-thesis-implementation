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
    uint8_t *page_data;
    HeaderInfo *header;
    SlotInfo<T> *slots;
    T *data_section;
    size_t max_tuples;
    void init_page(size_t page_size) {
        max_tuples = get_max_tuples(page_size);
        header = reinterpret_cast<HeaderInfo *>(page_data);
        header->tuple_count = 0;
        slots = reinterpret_cast<SlotInfo<T> *>(page_data + sizeof(HeaderInfo));
        data_section = reinterpret_cast<T *>(page_data + page_size - max_tuples * sizeof(T));
    }

public:
    explicit RawSlottedPage(size_t page_size, uint8_t *page_data2)
        : page_size(page_size), page_data(page_data2) {
        init_page(page_size);
    }

    uint8_t *get_page_data() const {
        return page_data;
    }

    static void write_tuple(uint8_t *page_data, const size_t page_size, const T &tuple, unsigned entry_num) {
        //store tuple starting from the end of the page_data
        size_t tuple_offset_from_end = page_size - (entry_num + 1) * sizeof(T);
        const auto tuple_start = page_data + tuple_offset_from_end;
        std::memcpy(tuple_start, &tuple, sizeof(T));

        //store slot
        SlotInfo<T> slot(tuple_offset_from_end, sizeof(T), tuple.get_key());
        const auto slot_start = page_data + sizeof(HeaderInfo) + entry_num * sizeof(SlotInfo<T>);
        std::memcpy(slot_start, &slot, sizeof(SlotInfo<T>));
    }

    static void increase_tuple_count(const uint8_t *page_data, size_t tuple_count) {
        const auto header = (HeaderInfo *) page_data;
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
            all_tuples.emplace_back(tuple);
        }
        return all_tuples;
    }

    size_t get_tuple_count() const {
        return header->tuple_count;
    }
};
