#pragma once
#include "on-demand/morsel-creation/MorselCreator.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "util/partitioning_function.hpp"

template<typename T, size_t partitions>
void process_morsel(MorselCreator<T> &morsel_creator, OnDemandPageManager<T, partitions> &page_manager) {
    for (auto [batch, batch_size] = morsel_creator.getBatchOfTuples(); batch;
         std::tie(batch, batch_size) = morsel_creator.getBatchOfTuples()) {
        for (size_t i = 0; i < batch_size; ++i) {
            page_manager.insert_tuple(batch[i], partition_function<T, partitions>(batch[i]));
        }
    }
}