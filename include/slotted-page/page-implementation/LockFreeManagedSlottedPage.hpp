#pragma once

#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include "slotted-page/page-implementation/HeaderInfo.hpp"
#include "slotted-page/page-implementation/SlotInfo.hpp"

template<typename T>
class LockFreeManagedSlottedPage {
    size_t page_size;
    uint8_t *page_data;
    HeaderInfoAtomic *header;
    SlotInfo<T> *slots;
    T *data_section;
    size_t max_tuples;
    bool has_to_be_freed = true;
    struct WriteInfo {
        uint8_t *page_data;
        unsigned tuple_index;
        unsigned page_size;
    };
public:
    explicit LockFreeManagedSlottedPage(size_t page_size)
        : page_size(page_size) {
        // Compute max possible tuples based on page_data size and tuple/slot size
        max_tuples = get_max_tuples(page_size);
        page_data = new uint8_t[page_size];

        // Set up header, slots, and data pointers within the page_data
        header = reinterpret_cast<HeaderInfoAtomic *>(page_data);
        slots = reinterpret_cast<SlotInfo<T> *>(page_data + sizeof(HeaderInfoAtomic));
        data_section = reinterpret_cast<T *>(page_data + page_size - max_tuples * sizeof(T));

        // Initialize header values
        header->tuple_count = 0;
    }

    LockFreeManagedSlottedPage(size_t page_size, uint8_t *page_data)
        : page_size(page_size), page_data(page_data), has_to_be_freed(false) {
        // Compute max possible tuples based on page_data size and tuple/slot size
        max_tuples = get_max_tuples(page_size);

        // Set up header, slots, and data pointers within the page_data
        header = reinterpret_cast<HeaderInfoAtomic *>(page_data);
        slots = reinterpret_cast<SlotInfo<T> *>(page_data + sizeof(HeaderInfoAtomic));
        data_section = reinterpret_cast<T *>(page_data + page_size - max_tuples * sizeof(T));
        // Initialize header values
        header->tuple_count = 0;
    }

    ~LockFreeManagedSlottedPage() {
        if (has_to_be_freed) {
            delete[] page_data;
        }
    }

    // move constructor
    LockFreeManagedSlottedPage(LockFreeManagedSlottedPage &&other) noexcept
        : page_size(other.page_size), page_data(other.page_data), header(other.header), slots(other.slots), data_section(other.data_section), max_tuples(other.max_tuples), has_to_be_freed(other.has_to_be_freed) {
        other.slots = nullptr;
        other.data_section = nullptr;
        other.has_to_be_freed = false;
    }
    //copy constructor delete
    LockFreeManagedSlottedPage(const LockFreeManagedSlottedPage &other) = delete;

    [[nodiscard]] WriteInfo increment_and_fetch_opt_write_info() {
        const auto tuple_count = header->tuple_count.fetch_add(1);
        if(tuple_count >= max_tuples) {
            if (tuple_count == max_tuples) {
                return {nullptr, max_tuples, 0};
            }
            return {nullptr, 0, 0};
        }
        return {page_data, tuple_count, page_size};
    }

    static void add_tuple_using_index(WriteInfo wi, const T &tuple) {
        size_t tuple_offset_from_end = 0;
        if (T::get_size_of_variable_data() > 0) {
            //store tuple starting from the end of the page_data
            tuple_offset_from_end = wi.page_size - (wi.tuple_index + 1) * T::get_size_of_variable_data();
            auto tuple_start = wi.page_data + tuple_offset_from_end;
            std::memcpy(tuple_start, &tuple, T::get_size_of_variable_data());
        }
        //store slot
        auto slot_start = reinterpret_cast<SlotInfo<T> *>(wi.page_data + sizeof(HeaderInfoAtomic) + wi.tuple_index * sizeof(SlotInfo<T>));
        new (slot_start) SlotInfo<T>(tuple_offset_from_end, T::get_size_of_variable_data(), tuple.get_key());
    }



    static size_t get_max_tuples(const size_t page_size) {
        return (page_size - sizeof(HeaderInfoAtomic)) / (T::get_size_of_variable_data() + sizeof(SlotInfo<T>));
    }

    std::optional<T> get_tuple(const typename T::KeyType &key) const {
        for (size_t i = 0; i < header->tuple_count; ++i) {
            if (slots[i].key == key) {
                T tuple(key, data_section[i].get_variable_data());
                return tuple;
            }
        }
        return std::nullopt;
    }

    // void add_tuple_batch_with_index(const T *buffer, const unsigned index, const unsigned tuples_to_write) {
    //     size_t first_tuple_offset_from_end = 0;
    //     if (T::get_size_of_variable_data() > 0) {
    //         first_tuple_offset_from_end = page_size - (index + 1) * T::get_size_of_variable_data();
    //         for (unsigned i = 0; i < tuples_to_write; i++) {
    //             auto tuple_start = page_data + first_tuple_offset_from_end - i * T::get_size_of_variable_data();
    //             std::memcpy(tuple_start, &buffer[i], T::get_size_of_variable_data());
    //         }
    //     }
    //     for (unsigned i = 0; i < tuples_to_write; i++) {
    //         auto slot_start = reinterpret_cast<SlotInfo<T> *>(page_data + sizeof(HeaderInfoAtomic) + (index + i) * sizeof(SlotInfo<T>));
    //         new (slot_start) SlotInfo<T>(first_tuple_offset_from_end - i * T::get_size_of_variable_data(), T::get_size_of_variable_data(), buffer[i].get_key());
    //     }
    // }

    std::vector<T> get_all_tuples() const {
        std::vector<T> all_tuples;
        for (size_t i = 0; i < header->tuple_count; ++i) {
            if (T::get_size_of_variable_data() > 0) {
                T tuple(slots[i].key, data_section[i].get_variable_data());
                all_tuples.emplace_back(tuple);
            } else {
                T tuple(slots[i].key);
                all_tuples.emplace_back(tuple);
            }
        }
        return all_tuples;
    }

    [[nodiscard]] size_t get_tuple_count() const {
        return std::min(static_cast<size_t>(header->tuple_count.load()), max_tuples);
    }
};
