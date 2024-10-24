#pragma once

template<typename T>
struct SlotInfo {
    size_t offset;
    typename T::KeyType key;

    SlotInfo(size_t offset, typename T::KeyType key) : offset(offset), key(key) {}
};