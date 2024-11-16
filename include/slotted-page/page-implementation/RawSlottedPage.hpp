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
    std::shared_ptr<uint8_t[]> page_data;
    HeaderInfoAtomic *header;
    SlotInfo<T> *slots;
    T *data_section;
    size_t max_tuples;

public:
    explicit RawSlottedPage(size_t page_size)
        : page_size(page_size) {
        page_data = std::make_shared<uint8_t[]>(page_size);

        max_tuples = get_max_tuples(page_size);
        header = reinterpret_cast<HeaderInfoAtomic *>(page_data.get());
        header->tuple_count = 0;
        slots = reinterpret_cast<SlotInfo<T> *>(page_data.get() + sizeof(HeaderInfoAtomic));
        data_section = reinterpret_cast<T *>(page_data.get() + page_size - max_tuples * T::get_size_of_variable_data());
    }

    uint8_t *get_page_data() const {
        return page_data.get();
    }

    static void write_tuple(uint8_t *page_data, const size_t page_size, T &tuple, const unsigned entry_num) {
        size_t tuple_offset_from_end = 0;
        if (T::get_size_of_variable_data() > 0) {
            //store tuple starting from the end of the page_data
            tuple_offset_from_end = page_size - (entry_num + 1) * T::get_size_of_variable_data();
            const auto tuple_start = page_data + tuple_offset_from_end;
            const auto data_to_store = tuple.get_variable_data();
            std::memcpy(tuple_start, &data_to_store, T::get_size_of_variable_data());
        }

        //store slot
        const auto slot_start = reinterpret_cast<SlotInfo<T> *>(page_data + sizeof(HeaderInfoAtomic) + entry_num * sizeof(SlotInfo<T>));
        new (slot_start) SlotInfo<T>(tuple_offset_from_end, T::get_size_of_variable_data(), tuple.get_key());
    }

    static void increase_tuple_count(uint8_t *page_data, const size_t tuple_count) {
        const auto header = reinterpret_cast<HeaderInfoAtomic *>(page_data);
        header->tuple_count += tuple_count;
    }

    static size_t get_max_tuples(size_t page_size) {
        return (page_size - sizeof(HeaderInfoAtomic)) / (T::get_size_of_variable_data() + sizeof(SlotInfo<T>));
    }

    std::optional<T> get_tuple(const typename T::KeyType &key) const {
        for (size_t i = 0; i < header->tuple_count; ++i) {
            if (slots[i].key == key) {
                if (T::get_size_of_variable_data() > 0) {
                    auto offset_from_page_start = slots[i].offset;
                    std::array<uint32_t, T::get_size_of_variable_data() / sizeof(uint32_t)> tuple_data;
                    std::memcpy(tuple_data.data(), page_data.get() + offset_from_page_start, T::get_size_of_variable_data());
                    T tuple(key, tuple_data);
                    return tuple;
                }
                T tuple(slots[i].key);
                return tuple;
            }
        }
        return std::nullopt;
    }

    std::vector<T> get_all_tuples() const {
        std::vector<T> all_tuples;
        for (size_t i = 0; i < header->tuple_count; ++i) {
            if (T::get_size_of_variable_data() > 0) {
                auto offset_from_page_start = slots[i].offset;
                std::array<uint32_t, T::get_size_of_variable_data() / sizeof(uint32_t)> tuple_data;
                std::memcpy(tuple_data.data(), page_data.get() + offset_from_page_start, T::get_size_of_variable_data());
                T tuple(slots[i].key, tuple_data);
                all_tuples.emplace_back(tuple);
            } else {
                T tuple(slots[i].key);
                all_tuples.emplace_back(tuple);
            }
        }
        return all_tuples;
    }

    size_t get_tuple_count() const {
        return header->tuple_count;
    }
};
