#pragma once
#include "common/morsel-creation/MorselCreator.hpp"
#include "slotted-page/page-manager/OnDemandPageManager.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
void process_morsel_smb(MorselCreator<T> &morsel_creator, OnDemandPageManager<T, partitions, page_size> &page_manager) {
    thread_local std::shared_ptr<T[]> buffer;
    thread_local std::array<unsigned, partitions> buffer_index = {0};
    const auto buffer_size = page_size / sizeof(T) >> 1;
    // std::cout << "Buffer size in KB: " << buffer_size * sizeof(T) / 1024 << " and " << buffer_size / sizeof(T) << " Tuples per partition"  <<  std::endl;
    const auto total_buffer_size = buffer_size * partitions;

    if (!buffer) {
        buffer = std::make_shared<T[]>(total_buffer_size);
    }

    for (auto [batch, batch_size] = morsel_creator.getBatchOfTuples(); batch; std::tie(batch, batch_size) = morsel_creator.getBatchOfTuples()) {
        for (size_t i = 0; i < batch_size; ++i) {
            auto tuple = batch[i];
            auto partition = partition_function<T, partitions>(tuple);
            auto &index = buffer_index[partition];
            auto partition_offset = partition * buffer_size;

            if (index == buffer_size) {
                page_manager.insert_buffer_of_tuples(buffer.get() + partition_offset, buffer_size, partition);
                index = 0;
            }

            buffer[partition_offset + index] = tuple;
            ++index;
        }
    }

    for (unsigned i = 0; i < partitions; ++i) {
        auto partition_offset = i * buffer_size;
        if (buffer_index[i] > 0) {
            page_manager.insert_buffer_of_tuples(buffer.get() + partition_offset, buffer_index[i], i);
        }
    }
}