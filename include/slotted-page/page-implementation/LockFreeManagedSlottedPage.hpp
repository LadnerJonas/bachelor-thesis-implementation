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
    const size_t max_tuples;
    bool has_to_be_freed = true;
    struct WriteInfo {
        uint8_t *page_data;
        unsigned page_size;
        unsigned tuple_index;
    };
    struct BatchedWriteInfo {
        uint8_t *page_data;
        unsigned page_size;
        unsigned tuple_index;
        unsigned tuples_to_write;
    };

public:
    explicit LockFreeManagedSlottedPage(size_t page_size)
        : page_size(page_size), max_tuples(get_max_tuples(page_size)) {
        page_data = new uint8_t[page_size];

        // Set up header, slots, and data pointers within the page_data
        header = reinterpret_cast<HeaderInfoAtomic *>(page_data);
        slots = reinterpret_cast<SlotInfo<T> *>(page_data + sizeof(HeaderInfoAtomic));
        data_section = reinterpret_cast<T *>(page_data + page_size - max_tuples * sizeof(T));

        // Initialize header values
        header->tuple_count = 0;
    }

    LockFreeManagedSlottedPage(size_t page_size, uint8_t *page_data)
        : page_size(page_size), page_data(page_data), max_tuples(get_max_tuples(page_size)), has_to_be_freed(false) {
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
    //copy assignment delete
    LockFreeManagedSlottedPage &operator=(const LockFreeManagedSlottedPage &other) = delete;

    [[nodiscard]] WriteInfo increment_and_fetch_opt_write_info() {
        unsigned current_tuple_count = header->tuple_count.load();
        do {
            if (current_tuple_count >= this->max_tuples) {
                return {nullptr, 0, 0};
            }
        } while (!header->tuple_count.compare_exchange_strong(current_tuple_count, current_tuple_count + 1));

        return {page_data, static_cast<unsigned>(page_size), current_tuple_count};
    }

    static void add_tuple_using_index(WriteInfo wi, const T &tuple) {
        unsigned tuple_offset_from_end = 0;
        if constexpr (T::get_size_of_variable_data() > 0) {
            //store tuple starting from the end of the page_data
            tuple_offset_from_end = wi.page_size - (wi.tuple_index + 1) * T::get_size_of_variable_data();
            auto tuple_start = wi.page_data + tuple_offset_from_end;
            std::memcpy(tuple_start, &tuple.get_variable_data(), T::get_size_of_variable_data());
        }
        //store slot
        auto slot_start = reinterpret_cast<SlotInfo<T> *>(wi.page_data + sizeof(HeaderInfoAtomic) + wi.tuple_index * sizeof(SlotInfo<T>));
        new (slot_start) SlotInfo<T>{tuple_offset_from_end, T::get_size_of_variable_data(), tuple.get_key()};
    }

    [[nodiscard]] BatchedWriteInfo increment_and_fetch_opt_write_info(const unsigned max_tuples_to_write) {
        unsigned current_tuple_count = header->tuple_count.load();
        do {
            if (current_tuple_count >= this->max_tuples) {
                return {nullptr, 0, 0, 0};
            }
        } while (!header->tuple_count.compare_exchange_strong(current_tuple_count, current_tuple_count + max_tuples_to_write));

        const auto tuples_to_write = std::min(max_tuples_to_write, static_cast<unsigned>(this->max_tuples - current_tuple_count));
        return {page_data, static_cast<unsigned>(page_size), current_tuple_count, tuples_to_write};
    }

    static void add_batch_using_index(const T *buffer, BatchedWriteInfo &wi) {
        SlotInfo<T> slot_buffer[wi.tuples_to_write];
        unsigned tuple_offset = wi.page_size - (wi.tuple_index + 1) * T::get_size_of_variable_data();
        for (unsigned i = 0; i < wi.tuples_to_write; ++i) {
            if constexpr (T::get_size_of_variable_data() > 0) {
                std::memcpy(wi.page_data + tuple_offset, &buffer[i].get_variable_data(), T::get_size_of_variable_data());
            }

            slot_buffer[i] = {tuple_offset, T::get_size_of_variable_data(), buffer[i].get_key()};
            tuple_offset -= T::get_size_of_variable_data();
        }
        auto slot_start = reinterpret_cast<SlotInfo<T> *>(wi.page_data + sizeof(HeaderInfoAtomic));
        std::memcpy(slot_start + wi.tuple_index, slot_buffer, wi.tuples_to_write * sizeof(SlotInfo<T>));
    }

    static size_t get_max_tuples(const size_t page_size) {
        return (page_size - sizeof(HeaderInfoAtomic)) / (T::get_size_of_variable_data() + sizeof(SlotInfo<T>));
    }

    std::optional<T> get_tuple(const typename T::KeyType &key) const {
        for (size_t i = 0; i < get_tuple_count(); ++i) {
            if (slots[i].key == key) {
                if constexpr (T::get_size_of_variable_data() > 0) {
                    auto offset_from_page_start = slots[i].offset;
                    std::array<uint32_t, T::get_size_of_variable_data() / sizeof(uint32_t)> tuple_data;
                    std::memcpy(tuple_data.data(), page_data + offset_from_page_start, T::get_size_of_variable_data());
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
        for (size_t i = 0; i < get_tuple_count(); ++i) {
            if constexpr (T::get_size_of_variable_data() > 0) {
                auto offset_from_page_start = slots[i].offset;
                std::array<uint32_t, T::get_size_of_variable_data() / sizeof(uint32_t)> tuple_data;
                std::memcpy(tuple_data.data(), page_data + offset_from_page_start, T::get_size_of_variable_data());
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
        return std::min(static_cast<size_t>(header->tuple_count.load()), max_tuples);
    }
};
