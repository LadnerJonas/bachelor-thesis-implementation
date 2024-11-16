#include "slotted-page/page-implementation/ManagedSlottedPage.hpp"
#include "tuple-types/tuple-types.hpp"
#include <gtest/gtest.h>

TEST(ManagedSlottedPageTest, BasicInsertionsTuple4) {
    unsigned page_size = 5 * 1024;
    ManagedSlottedPage<Tuple4> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);

    Tuple4 tuple(0);
    ASSERT_TRUE(page.add_tuple(tuple));

    ASSERT_EQ(page.get_tuple_count(), 1);
    auto read_tuple = page.get_tuple(0);
    ASSERT_TRUE(read_tuple.has_value());
    ASSERT_EQ(read_tuple.value().get_key(), 0);
    ASSERT_EQ(page.get_all_tuples().size(), 1);
    ASSERT_EQ(page.get_all_tuples()[0].get_key(), 0);

    Tuple4 tuple2(1);
    ASSERT_TRUE(page.add_tuple(tuple2));
    ASSERT_EQ(page.get_tuple_count(), 2);
    auto read_tuple2 = page.get_tuple(1);
    ASSERT_TRUE(read_tuple2.has_value());
    ASSERT_EQ(read_tuple2.value().get_key(), 1);

    ASSERT_GT(ManagedSlottedPage<Tuple4>::get_max_tuples(page_size), ManagedSlottedPage<Tuple16>::get_max_tuples(page_size));
}

TEST(ManagedSlottedPageTest, BasicInsertionsTuple16) {
    unsigned page_size = 5 * 1024;
    ManagedSlottedPage<Tuple16> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);

    Tuple16 tuple(0, {1, 2, 3});
    ASSERT_TRUE(page.add_tuple(tuple));

    ASSERT_EQ(page.get_tuple_count(), 1);
    auto read_tuple = page.get_tuple(0);
    ASSERT_TRUE(read_tuple.has_value());
    ASSERT_EQ(read_tuple.value().get_key(), 0);
    ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, 3>{1, 2, 3}));
    ASSERT_EQ(page.get_all_tuples().size(), 1);
    ASSERT_EQ(page.get_all_tuples()[0].get_key(), 0);
    ASSERT_EQ(page.get_all_tuples()[0].get_variable_data(), (std::array<uint32_t, 3>{1, 2, 3}));

    Tuple16 tuple2(1, {4, 5, 6});
    ASSERT_TRUE(page.add_tuple(tuple2));
    ASSERT_EQ(page.get_tuple_count(), 2);
    auto read_tuple2 = page.get_tuple(1);
    ASSERT_TRUE(read_tuple2.has_value());
    ASSERT_EQ(read_tuple2.value().get_key(), 1);
    ASSERT_EQ(read_tuple2.value().get_variable_data(), (std::array<uint32_t, 3>{4, 5, 6}));
    ASSERT_EQ(page.get_all_tuples().size(), 2);
    ASSERT_EQ(page.get_all_tuples()[1].get_key(), 1);
    ASSERT_EQ(page.get_all_tuples()[1].get_variable_data(), (std::array<uint32_t, 3>{4, 5, 6}));

    ASSERT_GT(ManagedSlottedPage<Tuple16>::get_max_tuples(page_size), ManagedSlottedPage<Tuple100>::get_max_tuples(page_size));
}

TEST(ManagedSlottedPageTest, BasicInsertionsTuple100) {
    unsigned page_size = 5 * 1024;
    ManagedSlottedPage<Tuple100> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);

    Tuple100 tuple(0, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24});
    ASSERT_TRUE(page.add_tuple(tuple));

    ASSERT_EQ(page.get_tuple_count(), 1);
    auto read_tuple = page.get_tuple(0);
    ASSERT_TRUE(read_tuple.has_value());
    ASSERT_EQ(read_tuple.value().get_key(), 0);
    ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, 24>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}));
    ASSERT_EQ(page.get_all_tuples().size(), 1);
    ASSERT_EQ(page.get_all_tuples()[0].get_key(), 0);
    ASSERT_EQ(page.get_all_tuples()[0].get_variable_data(), (std::array<uint32_t, 24>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}));

    Tuple100 tuple2(1, {25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48});
    ASSERT_TRUE(page.add_tuple(tuple2));
    ASSERT_EQ(page.get_tuple_count(), 2);
    auto read_tuple2 = page.get_tuple(1);
    ASSERT_TRUE(read_tuple2.has_value());
    ASSERT_EQ(read_tuple2.value().get_key(), 1);
    ASSERT_EQ(read_tuple2.value().get_variable_data(), (std::array<uint32_t, 24>{25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48}));
    ASSERT_EQ(page.get_all_tuples().size(), 2);
    ASSERT_EQ(page.get_all_tuples()[1].get_key(), 1);
    ASSERT_EQ(page.get_all_tuples()[1].get_variable_data(), (std::array<uint32_t, 24>{25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48}));

    ASSERT_LT(ManagedSlottedPage<Tuple100>::get_max_tuples(page_size), ManagedSlottedPage<Tuple16>::get_max_tuples(page_size));
}

TEST(ManagedSlottedPageTest, BatchedInsertionTuple4) {
    using tuple = Tuple4;
    auto page_size = 5 * 1024u;
    size_t tuples_to_write = ManagedSlottedPage<tuple>::get_max_tuples(page_size);
    std::unique_ptr<tuple[]> buffer(new tuple[tuples_to_write]);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        buffer[i] = tuple(i);
    }

    ManagedSlottedPage<tuple> page(page_size);
    page.add_tuple_batch_with_index(buffer.get(), 0, tuples_to_write);
    page.increase_tuple_count(tuples_to_write);
    ASSERT_EQ(page.get_tuple_count(), tuples_to_write);
    ASSERT_EQ(page.get_all_tuples().size(), tuples_to_write);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        auto read_tuple = page.get_tuple(i);
        ASSERT_TRUE(read_tuple.has_value());
        ASSERT_EQ(read_tuple.value().get_key(), i);
    }
}
TEST(ManagedSlottedPageTest, BatchedInsertionTuple16) {
    using tuple = Tuple16;
    auto page_size = 5 * 1024u;
    size_t tuples_to_write = ManagedSlottedPage<tuple>::get_max_tuples(page_size);
    std::unique_ptr<tuple[]> buffer(new tuple[tuples_to_write]);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        buffer[i] = tuple(i, {i + 1});
    }

    ManagedSlottedPage<tuple> page(page_size);
    page.add_tuple_batch_with_index(buffer.get(), 0, tuples_to_write);
    page.increase_tuple_count(tuples_to_write);
    ASSERT_EQ(page.get_tuple_count(), tuples_to_write);
    ASSERT_EQ(page.get_all_tuples().size(), tuples_to_write);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        auto read_tuple = page.get_tuple(i);
        ASSERT_TRUE(read_tuple.has_value());
        ASSERT_EQ(read_tuple.value().get_key(), i);
        ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, tuple::get_size_of_variable_data() / sizeof(uint32_t)>{i + 1}));
    }
}
TEST(ManagedSlottedPageTest, BatchedInsertionTuple100) {
    using tuple = Tuple100;
    auto page_size = 5 * 1024u;
    size_t tuples_to_write = ManagedSlottedPage<tuple>::get_max_tuples(page_size);
    std::unique_ptr<tuple[]> buffer(new tuple[tuples_to_write]);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        buffer[i] = tuple(i, {i + 1});
    }

    ManagedSlottedPage<tuple> page(page_size);
    page.add_tuple_batch_with_index(buffer.get(), 0, tuples_to_write);
    page.increase_tuple_count(tuples_to_write);
    ASSERT_EQ(page.get_tuple_count(), tuples_to_write);
    ASSERT_EQ(page.get_all_tuples().size(), tuples_to_write);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        auto read_tuple = page.get_tuple(i);
        ASSERT_TRUE(read_tuple.has_value());
        ASSERT_EQ(read_tuple.value().get_key(), i);
        ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, tuple::get_size_of_variable_data() / sizeof(uint32_t)>{i + 1}));
    }
}

TEST(ManagedSlottedPageTest, FullPageInsertion) {
    const auto page_size = 5 * 1024;
    ManagedSlottedPage<Tuple16> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);
    auto max_amount_of_tuples = ManagedSlottedPage<Tuple16>::get_max_tuples(page_size);
    ASSERT_EQ(max_amount_of_tuples, 213);

    std::vector<Tuple16> tuples;
    for (unsigned i = 0; i < max_amount_of_tuples; ++i) {
        tuples.emplace_back(i, std::array{i + 1, i + 2, i + 3});
    }

    for (const auto &tuple: tuples) {
        ASSERT_TRUE(page.add_tuple(tuple));
    }

    ASSERT_FALSE(page.add_tuple(Tuple16()));
    ASSERT_EQ(page.get_tuple_count(), max_amount_of_tuples);
    ASSERT_EQ(page.get_all_tuples().size(), max_amount_of_tuples);
    for (unsigned i = 0; i < max_amount_of_tuples; ++i) {
        auto read_tuple = page.get_tuple(i);
        ASSERT_TRUE(read_tuple.has_value());
        ASSERT_EQ(read_tuple.value().get_key(), i);
        ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array{i + 1, i + 2, i + 3}));
    }
}
