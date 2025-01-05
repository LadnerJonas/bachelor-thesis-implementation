#pragma once

#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include "slotted-page/page-implementation/HeaderInfo.hpp"
#include "slotted-page/page-implementation/SlotInfo.hpp"

template<typename T>
class ManagedSlottedPage {
    std::unique_ptr<uint8_t[]> page_data;
    size_t page_size;
    size_t max_tuples;
    HeaderInfoNonAtomic *header;
    SlotInfo<T> *slots;
    T *data_section;

public:
    explicit ManagedSlottedPage(const size_t page_size)
        : page_data(std::make_unique<uint8_t[]>(page_size)), page_size(page_size), max_tuples(get_max_tuples(page_size)) {

        header = reinterpret_cast<HeaderInfoNonAtomic *>(page_data.get());
        slots = reinterpret_cast<SlotInfo<T> *>(page_data.get() + sizeof(HeaderInfoAtomic));
        data_section = reinterpret_cast<T *>(page_data.get() + page_size - get_max_tuples(page_size) * sizeof(T));

        header->tuple_count = 0;
    }

    //copy constructor delete
    ManagedSlottedPage(const ManagedSlottedPage &other) = delete;
    ManagedSlottedPage &operator=(const ManagedSlottedPage &) = delete;

    ManagedSlottedPage(ManagedSlottedPage &&other) = default;
    ManagedSlottedPage &operator=(ManagedSlottedPage &&) = default;

    bool add_tuple(const T &tuple) {
        if (header->tuple_count == max_tuples) {
            return false;
        }
        unsigned tuple_offset_from_end = 0;
        if constexpr (T::get_size_of_variable_data() > 0) {
            //store tuple starting from the end of the page_data
            tuple_offset_from_end = page_size - (header->tuple_count + 1) * T::get_size_of_variable_data();
            auto tuple_start = page_data.get() + tuple_offset_from_end;
            std::memcpy(tuple_start, &tuple.get_variable_data(), T::get_size_of_variable_data());
        }
        //store slot
        auto slot_start = reinterpret_cast<SlotInfo<T> *>(page_data.get() + sizeof(HeaderInfoNonAtomic) + header->tuple_count * sizeof(SlotInfo<T>));
        new (slot_start) SlotInfo<T>{tuple_offset_from_end, T::get_size_of_variable_data(), tuple.get_key()};

        // Increase tuple count
        header->tuple_count += 1;
        return true;
    }

    void add_tuple_batch_with_index(const T *buffer, const unsigned index, const unsigned tuples_to_write) {
        unsigned first_tuple_offset_from_end = 0;
        if constexpr (T::get_size_of_variable_data() > 0) {
            first_tuple_offset_from_end = page_size - (index + 1) * T::get_size_of_variable_data();
            for (unsigned i = 0; i < tuples_to_write; i++) {
                auto tuple_start = page_data.get() + first_tuple_offset_from_end - i * T::get_size_of_variable_data();
                std::memcpy(tuple_start, &buffer[i].get_variable_data(), T::get_size_of_variable_data());
            }
        }
        const auto slot_start = reinterpret_cast<SlotInfo<T> *>(page_data.get() + sizeof(HeaderInfoNonAtomic) + index * sizeof(SlotInfo<T>));
        for (unsigned i = 0; i < tuples_to_write; i++) {
            new (slot_start + i) SlotInfo<T>{first_tuple_offset_from_end - i * T::get_size_of_variable_data(), T::get_size_of_variable_data(), buffer[i].get_key()};
        }
    }

    void increase_tuple_count(const unsigned count) const {
        header->tuple_count += count;
    }

    constexpr static size_t get_max_tuples(const size_t page_size) {
        return (page_size - sizeof(HeaderInfoNonAtomic)) / (T::get_size_of_variable_data() + sizeof(SlotInfo<T>));
    }

    std::optional<T> get_tuple(const typename T::KeyType &key) const {
        for (size_t i = 0; i < header->tuple_count; ++i) {
            if (slots[i].key == key) {
                if constexpr (T::get_size_of_variable_data() > 0) {
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
            if constexpr (T::get_size_of_variable_data() > 0) {
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

    [[nodiscard]] size_t get_tuple_count() const {
        return header->tuple_count;
    }

    void clear() const {
        header->tuple_count = 0;
    }
};
