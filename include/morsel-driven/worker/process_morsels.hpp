#ifndef PROCESS_MORSELS_HPP
#define PROCESS_MORSELS_HPP

#include "morsel-driven/input_data_storage/MorselManager.hpp"
#include "morsel-driven/output_data_storage/PartitionManager.hpp"
#include "util/partitioning_function.hpp"

template<typename P, typename T>
void process_morsels(MorselManager<T> &morsel_manager, PartitionManagerBase<P, T> &partition_manager);

#endif// PROCESS_MORSELS_HPP
