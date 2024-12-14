#pragma once

#include "common/morsel-creation/MorselCreator.hpp"
#include "slotted-page/page-manager/OnDemandSingleThreadPageManager.hpp"
#include "smb/worker/process_morsel_smb.hpp"

template<typename T, size_t partitions, size_t page_size = 5 * 1024 * 1024>
class SmbSingleThreadOrchestrator {
    BatchedTupleGenerator<T> generator;
    OnDemandSingleThreadPageManager<T, partitions, page_size> page_manager;

public:
    explicit SmbSingleThreadOrchestrator(size_t num_tuples) : generator(num_tuples) {
    }

    void run() {
        constexpr static auto total_buffer_size = 16 * 1024 * 1024 / sizeof(T);
        constexpr static auto buffer_size_per_partition = total_buffer_size / partitions;
        std::array<unsigned, partitions> buffer_index = {};
        std::unique_ptr<T[]> buffer = std::make_unique<T[]>(total_buffer_size);

        for (auto [batch, batch_size] = generator.getBatchOfTuples(); batch; std::tie(batch, batch_size) = generator.getBatchOfTuples()) {
            for (size_t i = 0; i < batch_size; ++i) {
                const auto &tuple = batch[i];
                const auto partition = partition_function<T, partitions>(tuple);
                auto &index = buffer_index[partition];
                const auto partition_offset = partition * buffer_size_per_partition;

                if (index == buffer_size_per_partition) {
                    page_manager.insert_buffer_of_tuples_batched(buffer.get() + partition_offset, buffer_size_per_partition, partition);
                    index = 0;
                }

                buffer[partition_offset + index] = tuple;
                ++index;
            }
        }

        for (unsigned i = 0; i < partitions; ++i) {
            auto partition_offset = i * buffer_size_per_partition;
            if (buffer_index[i] > 0) {
                page_manager.insert_buffer_of_tuples_batched(buffer.get() + partition_offset, buffer_index[i], i);
            }
        }
    }
    std::vector<size_t> get_written_tuples_per_partition() {
        return page_manager.get_written_tuples_per_partition();
    }
};