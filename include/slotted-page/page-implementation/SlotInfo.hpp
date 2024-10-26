#pragma once

template<typename T>
struct SlotInfo {
    unsigned offset;
    unsigned length;
    typename T::KeyType key;
    SlotInfo() = default;
    SlotInfo(unsigned offset, unsigned length, typename T::KeyType key) : offset(offset), length(length), key(key) {}
};