#pragma once
#include <type_traits>


class Tuple4;
class Tuple16;
class Tuple100;
template<typename T>
double get_tuple_num_scaling_value() {
    if (std::is_same_v<T, Tuple100>) {
        return 1.87;
    }
    if (std::is_same_v<T, Tuple16>) {
        return 8.4;
    }
    if (std::is_same_v<T, Tuple4>) {
        return 16.8;
    }
    return 1;
}