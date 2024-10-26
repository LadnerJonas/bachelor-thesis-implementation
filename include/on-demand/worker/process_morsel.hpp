#pragma once
#include "on-demand/morsel-creation/MorselCreator.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"
#include "util/partitioning_function.hpp"

template<typename T, size_t partitions>
void process_morsel(MorselCreator<T> &morsel_creator, OnDemandPageManager<T, partitions> &page_manager) {
    while (true) {
        auto [batch, batch_size] = morsel_creator.getBatchOfTuples();
        if (batch == nullptr) {
            break;
        }
        for (size_t i = 0; i < batch_size; ++i) {
            auto partition = partition_function<T, partitions>(batch[i]);
            page_manager.insert_tuple(batch[i], partition);
        }
    }
}