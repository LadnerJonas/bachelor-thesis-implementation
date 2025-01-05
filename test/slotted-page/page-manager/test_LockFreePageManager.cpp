#include "slotted-page/page-manager/LockFreePageManager.hpp"
#include "tuple-types/tuple-types.hpp"

#include <gtest/gtest.h>

TEST(LockFreePageManagerTest, BasicInsertionsTuple4) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple4, partitions, page_size> page_manager;

    for (unsigned i = 0; i < 1024; ++i) {
        Tuple4 tuple(i);
        page_manager.insert_tuple(tuple, i % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 32);
    }


    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 32);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < 32; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i + j * partitions);
        }
    }
}

TEST(LockFreePageManagerTest, BasicInsertionsTuple16) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple16, partitions, page_size> page_manager;

    for (unsigned i = 0; i < 1024; ++i) {
        Tuple16 tuple(i, {i + 1, i + 2, i + 3});
        page_manager.insert_tuple(tuple, i % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 32);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 32);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < 32; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i + j * partitions);
            ASSERT_EQ(all_tuples[i][j].get_variable_data(), (std::array{i + j * partitions + 1, i + j * partitions + 2, i + j * partitions + 3}));
        }
    }
}

TEST(LockFreePageManagerTest, BasicInsertionsTuple100) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple100, partitions, page_size> page_manager;

    for (unsigned i = 0; i < 1024; ++i) {
        Tuple100 tuple(i, {i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7, i + 8, i + 9, i + 10, i + 11, i + 12, i + 13, i + 14, i + 15, i + 16, i + 17, i + 18, i + 19, i + 20, i + 21, i + 22, i + 23, i + 24});
        page_manager.insert_tuple(tuple, i % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 32);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 32);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < 32; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i + j * partitions);
            ASSERT_EQ(all_tuples[i][j].get_variable_data(), (std::array{i + j * partitions + 1, i + j * partitions + 2, i + j * partitions + 3, i + j * partitions + 4, i + j * partitions + 5, i + j * partitions + 6, i + j * partitions + 7, i + j * partitions + 8, i + j * partitions + 9, i + j * partitions + 10, i + j * partitions + 11, i + j * partitions + 12, i + j * partitions + 13, i + j * partitions + 14, i + j * partitions + 15, i + j * partitions + 16, i + j * partitions + 17, i + j * partitions + 18, i + j * partitions + 19, i + j * partitions + 20, i + j * partitions + 21, i + j * partitions + 22, i + j * partitions + 23, i + j * partitions + 24}));
        }
    }
}

TEST(LockFreePageManagerTest, BasicInsertionsTuple4WithMultiplePages) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple4, partitions, page_size> page_manager;

    constexpr auto max_tuples_per_page = LockFreeManagedSlottedPage<Tuple4>::get_max_tuples(page_size);
    constexpr auto tuples_to_write_per_page = max_tuples_per_page + 32 - max_tuples_per_page % 32;

    for (unsigned i = 0; i < tuples_to_write_per_page * partitions; ++i) {
        Tuple4 tuple(i);
        page_manager.insert_tuple(tuple, i % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], tuples_to_write_per_page);
    }


    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), tuples_to_write_per_page);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < tuples_to_write_per_page; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i + j * partitions);
        }
    }
}

TEST(LockFreePageManagerTest, BasicInsertionsTuple16WithMultiplePages) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple16, partitions, page_size> page_manager;

    constexpr auto max_tuples_per_page = LockFreeManagedSlottedPage<Tuple4>::get_max_tuples(page_size);
    constexpr auto tuples_to_write_per_page = max_tuples_per_page + 32 - max_tuples_per_page % 32;

    for (unsigned i = 0; i < tuples_to_write_per_page * partitions; ++i) {
        Tuple16 tuple(i, {i + 1, i + 2, i + 3});
        page_manager.insert_tuple(tuple, i % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], tuples_to_write_per_page);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), tuples_to_write_per_page);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < tuples_to_write_per_page; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i + j * partitions);
            ASSERT_EQ(all_tuples[i][j].get_variable_data(), (std::array{i + j * partitions + 1, i + j * partitions + 2, i + j * partitions + 3}));
        }
    }
}

TEST(LockFreePageManagerTest, BasicInsertionsTuple100WithMultiplePages) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple100, partitions, page_size> page_manager;

    constexpr auto max_tuples_per_page = LockFreeManagedSlottedPage<Tuple4>::get_max_tuples(page_size);
    constexpr auto tuples_to_write_per_page = max_tuples_per_page + 32 - max_tuples_per_page % 32;

    for (unsigned i = 0; i < tuples_to_write_per_page * partitions; ++i) {
        Tuple100 tuple(i, {i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7, i + 8, i + 9, i + 10, i + 11, i + 12, i + 13, i + 14, i + 15, i + 16, i + 17, i + 18, i + 19, i + 20, i + 21, i + 22, i + 23, i + 24});
        page_manager.insert_tuple(tuple, i % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], tuples_to_write_per_page);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), tuples_to_write_per_page);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < tuples_to_write_per_page; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i + j * partitions);
            ASSERT_EQ(all_tuples[i][j].get_variable_data(), (std::array{i + j * partitions + 1, i + j * partitions + 2, i + j * partitions + 3, i + j * partitions + 4, i + j * partitions + 5, i + j * partitions + 6, i + j * partitions + 7, i + j * partitions + 8, i + j * partitions + 9, i + j * partitions + 10, i + j * partitions + 11, i + j * partitions + 12, i + j * partitions + 13, i + j * partitions + 14, i + j * partitions + 15, i + j * partitions + 16, i + j * partitions + 17, i + j * partitions + 18, i + j * partitions + 19, i + j * partitions + 20, i + j * partitions + 21, i + j * partitions + 22, i + j * partitions + 23, i + j * partitions + 24}));
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionsTuple4) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple4, partitions, page_size> page_manager;

    std::unique_ptr<Tuple4[]> buffer(new Tuple4[32]);
    for (unsigned i = 0; i < 1024; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple4(i + j);
        }
        page_manager.insert_buffer_of_tuples(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 32);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 32);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < 32; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i * partitions + j);
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionsTuple16) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple16, partitions, page_size> page_manager;

    std::unique_ptr<Tuple16[]> buffer(new Tuple16[32]);
    for (unsigned i = 0; i < 1024; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple16(i + j, {i + j + 1, i + j + 2, i + j + 3});
        }
        page_manager.insert_buffer_of_tuples(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 32);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 32);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < 32; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i * partitions + j);
            ASSERT_EQ(all_tuples[i][j].get_variable_data(), (std::array{i * partitions + j + 1, i * partitions + j + 2, i * partitions + j + 3}));
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionsTuple100) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple100, partitions, page_size> page_manager;

    std::unique_ptr<Tuple100[]> buffer(new Tuple100[32]);
    for (unsigned i = 0; i < 1024; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple100(i + j, {i + j + 1, i + j + 2, i + j + 3, i + j + 4, i + j + 5, i + j + 6, i + j + 7, i + j + 8, i + j + 9, i + j + 10, i + j + 11, i + j + 12, i + j + 13, i + j + 14, i + j + 15, i + j + 16, i + j + 17, i + j + 18, i + j + 19, i + j + 20, i + j + 21, i + j + 22, i + j + 23, i + j + 24});
        }
        page_manager.insert_buffer_of_tuples(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 32);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 32);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < 32; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i * partitions + j);
            ASSERT_EQ(all_tuples[i][j].get_variable_data(), (std::array{i * partitions + j + 1, i * partitions + j + 2, i * partitions + j + 3, i * partitions + j + 4, i * partitions + j + 5, i * partitions + j + 6, i * partitions + j + 7, i * partitions + j + 8, i * partitions + j + 9, i * partitions + j + 10, i * partitions + j + 11, i * partitions + j + 12, i * partitions + j + 13, i * partitions + j + 14, i * partitions + j + 15, i * partitions + j + 16, i * partitions + j + 17, i * partitions + j + 18, i * partitions + j + 19, i * partitions + j + 20, i * partitions + j + 21, i * partitions + j + 22, i * partitions + j + 23, i * partitions + j + 24}));
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionTuple4WithMultiplePages) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple4, partitions, page_size> page_manager;

    constexpr auto max_tuples_per_page = LockFreeManagedSlottedPage<Tuple4>::get_max_tuples(page_size);
    constexpr auto tuples_to_write_per_page = max_tuples_per_page + 32 - (max_tuples_per_page % 32);

    std::unique_ptr<Tuple4[]> buffer(new Tuple4[32]);
    for (unsigned i = 0; i < tuples_to_write_per_page * partitions; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple4(i + j);
        }
        page_manager.insert_buffer_of_tuples(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 448);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 448);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned offset = 0; offset < tuples_to_write_per_page; offset += 32) {
            for (unsigned j = 0; j < 32; ++j) {
                ASSERT_EQ(all_tuples[i][offset + j].get_key(), i * partitions + offset * partitions + j);
            }
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionTuple16WithMultiplePages) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple16, partitions, page_size> page_manager;

    constexpr auto max_tuples_per_page = LockFreeManagedSlottedPage<Tuple16>::get_max_tuples(page_size);
    constexpr auto tuples_to_write_per_page = max_tuples_per_page + 32 - (max_tuples_per_page % 32);

    std::unique_ptr<Tuple16[]> buffer(new Tuple16[32]);
    for (unsigned i = 0; i < tuples_to_write_per_page * partitions; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple16(i + j, {i + j + 1, i + j + 2, i + j + 3});
        }
        page_manager.insert_buffer_of_tuples(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], tuples_to_write_per_page);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), tuples_to_write_per_page);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned offset = 0; offset < tuples_to_write_per_page; offset += 32) {
            for (unsigned j = 0; j < 32; ++j) {
                ASSERT_EQ(all_tuples[i][offset + j].get_key(), i * partitions + offset * partitions + j);
                ASSERT_EQ(all_tuples[i][offset + j].get_variable_data(), (std::array{i * partitions + offset * partitions + j + 1, i * partitions + offset * partitions + j + 2, i * partitions + offset * partitions + j + 3}));
            }
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionTuple100WithMultiplePages) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple100, partitions, page_size> page_manager;

    constexpr auto max_tuples_per_page = LockFreeManagedSlottedPage<Tuple100>::get_max_tuples(page_size);
    constexpr auto tuples_to_write_per_page = max_tuples_per_page + 32 - (max_tuples_per_page % 32);

    std::unique_ptr<Tuple100[]> buffer(new Tuple100[32]);
    for (unsigned i = 0; i < tuples_to_write_per_page * partitions; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple100(i + j, {i + j + 1, i + j + 2, i + j + 3, i + j + 4, i + j + 5, i + j + 6, i + j + 7, i + j + 8, i + j + 9, i + j + 10, i + j + 11, i + j + 12, i + j + 13, i + j + 14, i + j + 15, i + j + 16, i + j + 17, i + j + 18, i + j + 19, i + j + 20, i + j + 21, i + j + 22, i + j + 23, i + j + 24});
        }
        page_manager.insert_buffer_of_tuples(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], tuples_to_write_per_page);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), tuples_to_write_per_page);
    }

    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned offset = 0; offset < tuples_to_write_per_page; offset += 32) {
            for (unsigned j = 0; j < 32; ++j) {
                ASSERT_EQ(all_tuples[i][offset + j].get_key(), i * partitions + offset * partitions + j);
                ASSERT_EQ(all_tuples[i][offset + j].get_variable_data(), (std::array{i * partitions + offset * partitions + j + 1, i * partitions + offset * partitions + j + 2, i * partitions + offset * partitions + j + 3, i * partitions + offset * partitions + j + 4, i * partitions + offset * partitions + j + 5, i * partitions + offset * partitions + j + 6, i * partitions + offset * partitions + j + 7, i * partitions + offset * partitions + j + 8, i * partitions + offset * partitions + j + 9, i * partitions + offset * partitions + j + 10, i * partitions + offset * partitions + j + 11, i * partitions + offset * partitions + j + 12, i * partitions + offset * partitions + j + 13, i * partitions + offset * partitions + j + 14, i * partitions + offset * partitions + j + 15, i * partitions + offset * partitions + j + 16, i * partitions + offset * partitions + j + 17, i * partitions + offset * partitions + j + 18, i * partitions + offset * partitions + j + 19, i * partitions + offset * partitions + j + 20, i * partitions + offset * partitions + j + 21, i * partitions + offset * partitions + j + 22, i * partitions + offset * partitions + j + 23, i * partitions + offset * partitions + j + 24}));
            }
        }
    }
}


TEST(LockFreePageManagerTest, BatchedInsertionWithBatchedWriteoutTuple4) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple4, partitions, page_size> page_manager;

    std::unique_ptr<Tuple4[]> buffer(new Tuple4[32]);
    for (unsigned i = 0; i < 1024; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple4(i + j);
        }
        page_manager.insert_buffer_of_tuples_batched(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 32);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 32);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < 32; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i * partitions + j);
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionWithBatchedWriteoutTuple16) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple16, partitions, page_size> page_manager;

    std::unique_ptr<Tuple16[]> buffer(new Tuple16[32]);
    for (unsigned i = 0; i < 1024; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple16(i + j, {i + j + 1, i + j + 2, i + j + 3});
        }
        page_manager.insert_buffer_of_tuples_batched(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 32);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 32);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < 32; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i * partitions + j);
            ASSERT_EQ(all_tuples[i][j].get_variable_data(), (std::array{i * partitions + j + 1, i * partitions + j + 2, i * partitions + j + 3}));
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionWithBatchedWriteoutTuple100) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple100, partitions, page_size> page_manager;

    std::unique_ptr<Tuple100[]> buffer(new Tuple100[32]);
    for (unsigned i = 0; i < 1024; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple100(i + j, {i + j + 1, i + j + 2, i + j + 3, i + j + 4, i + j + 5, i + j + 6, i + j + 7, i + j + 8, i + j + 9, i + j + 10, i + j + 11, i + j + 12, i + j + 13, i + j + 14, i + j + 15, i + j + 16, i + j + 17, i + j + 18, i + j + 19, i + j + 20, i + j + 21, i + j + 22, i + j + 23, i + j + 24});
        }
        page_manager.insert_buffer_of_tuples_batched(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 32);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 32);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned j = 0; j < 32; ++j) {
            ASSERT_EQ(all_tuples[i][j].get_key(), i * partitions + j);
            ASSERT_EQ(all_tuples[i][j].get_variable_data(), (std::array{i * partitions + j + 1, i * partitions + j + 2, i * partitions + j + 3, i * partitions + j + 4, i * partitions + j + 5, i * partitions + j + 6, i * partitions + j + 7, i * partitions + j + 8, i * partitions + j + 9, i * partitions + j + 10, i * partitions + j + 11, i * partitions + j + 12, i * partitions + j + 13, i * partitions + j + 14, i * partitions + j + 15, i * partitions + j + 16, i * partitions + j + 17, i * partitions + j + 18, i * partitions + j + 19, i * partitions + j + 20, i * partitions + j + 21, i * partitions + j + 22, i * partitions + j + 23, i * partitions + j + 24}));
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionWithBatchedWriteoutTuple4WithMultiplePages) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple4, partitions, page_size> page_manager;

    constexpr auto max_tuples_per_page = LockFreeManagedSlottedPage<Tuple4>::get_max_tuples(page_size);
    const auto tuples_to_write_per_page = max_tuples_per_page + 32 - (max_tuples_per_page % 32);

    std::unique_ptr<Tuple4[]> buffer(new Tuple4[32]);
    for (unsigned i = 0; i < tuples_to_write_per_page * partitions; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple4(i + j);
        }
        page_manager.insert_buffer_of_tuples_batched(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], 448);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), 448);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned offset = 0; offset < tuples_to_write_per_page; offset += 32) {
            for (unsigned j = 0; j < 32; ++j) {
                ASSERT_EQ(all_tuples[i][offset + j].get_key(), i * partitions + offset * partitions + j);
            }
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionWithBatchedWriteoutTuple16WithMultiplePages) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple16, partitions, page_size> page_manager;

    constexpr auto max_tuples_per_page = LockFreeManagedSlottedPage<Tuple16>::get_max_tuples(page_size);
    const auto tuples_to_write_per_page = max_tuples_per_page + 32 - (max_tuples_per_page % 32);

    std::unique_ptr<Tuple16[]> buffer(new Tuple16[32]);
    for (unsigned i = 0; i < tuples_to_write_per_page * partitions; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple16(i + j, {i + j + 1, i + j + 2, i + j + 3});
        }
        page_manager.insert_buffer_of_tuples_batched(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], tuples_to_write_per_page);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), tuples_to_write_per_page);
    }
    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned offset = 0; offset < tuples_to_write_per_page; offset += 32) {
            for (unsigned j = 0; j < 32; ++j) {
                ASSERT_EQ(all_tuples[i][offset + j].get_key(), i * partitions + offset * partitions + j);
                ASSERT_EQ(all_tuples[i][offset + j].get_variable_data(), (std::array{i * partitions + offset * partitions + j + 1, i * partitions + offset * partitions + j + 2, i * partitions + offset * partitions + j + 3}));
            }
        }
    }
}

TEST(LockFreePageManagerTest, BatchedInsertionWithBatchedWriteoutTuple100WithMultiplePages) {
    constexpr unsigned page_size = 5 * 1024;
    constexpr unsigned partitions = 32;
    LockFreePageManager<Tuple100, partitions, page_size> page_manager;

    constexpr auto max_tuples_per_page = LockFreeManagedSlottedPage<Tuple100>::get_max_tuples(page_size);
    constexpr auto tuples_to_write_per_page = max_tuples_per_page + 32 - (max_tuples_per_page % 32);

    std::unique_ptr<Tuple100[]> buffer(new Tuple100[32]);
    for (unsigned i = 0; i < tuples_to_write_per_page * partitions; i += 32) {
        for (unsigned j = 0; j < 32; ++j) {
            buffer[j] = Tuple100(i + j, {i + j + 1, i + j + 2, i + j + 3, i + j + 4, i + j + 5, i + j + 6, i + j + 7, i + j + 8, i + j + 9, i + j + 10, i + j + 11, i + j + 12, i + j + 13, i + j + 14, i + j + 15, i + j + 16, i + j + 17, i + j + 18, i + j + 19, i + j + 20, i + j + 21, i + j + 22, i + j + 23, i + j + 24});
        }
        page_manager.insert_buffer_of_tuples_batched(buffer.get(), 32, i / 32 % partitions);
    }

    const auto written_tuples = page_manager.get_written_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(written_tuples[i], tuples_to_write_per_page);
    }

    const auto all_tuples = page_manager.get_all_tuples_per_partition();
    for (unsigned i = 0; i < partitions; ++i) {
        ASSERT_EQ(all_tuples[i].size(), tuples_to_write_per_page);
    }

    for (unsigned i = 0; i < partitions; ++i) {
        for (unsigned offset = 0; offset < tuples_to_write_per_page; offset += 32) {
            for (unsigned j = 0; j < 32; ++j) {
                ASSERT_EQ(all_tuples[i][offset + j].get_key(), i * partitions + offset * partitions + j);
                ASSERT_EQ(all_tuples[i][offset + j].get_variable_data(), (std::array{i * partitions + offset * partitions + j + 1, i * partitions + offset * partitions + j + 2, i * partitions + offset * partitions + j + 3, i * partitions + offset * partitions + j + 4, i * partitions + offset * partitions + j + 5, i * partitions + offset * partitions + j + 6, i * partitions + offset * partitions + j + 7, i * partitions + offset * partitions + j + 8, i * partitions + offset * partitions + j + 9, i * partitions + offset * partitions + j + 10, i * partitions + offset * partitions + j + 11, i * partitions + offset * partitions + j + 12, i * partitions + offset * partitions + j + 13, i * partitions + offset * partitions + j + 14, i * partitions + offset * partitions + j + 15, i * partitions + offset * partitions + j + 16, i * partitions + offset * partitions + j + 17, i * partitions + offset * partitions + j + 18, i * partitions + offset * partitions + j + 19, i * partitions + offset * partitions + j + 20, i * partitions + offset * partitions + j + 21, i * partitions + offset * partitions + j + 22, i * partitions + offset * partitions + j + 23, i * partitions + offset * partitions + j + 24}));
            }
        }
    }
}
