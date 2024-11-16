#include "slotted-page/page-implementation/RawSlottedPage.hpp"
#include "tuple-types/tuple-types.hpp"
#include <gtest/gtest.h>

TEST(RawSlottedPageTest, BasicInsertionsTuple4) {
    unsigned page_size = 5 * 1024;
    RawSlottedPage<Tuple4> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);
    const auto page_data = page.get_page_data();

    Tuple4 tuple(0);
    RawSlottedPage<Tuple4>::write_tuple(page_data, page_size, tuple, 0);
    RawSlottedPage<Tuple4>::increase_tuple_count(page_data,1);

    ASSERT_EQ(page.get_tuple_count(), 1);
    auto read_tuple = page.get_tuple(0);
    ASSERT_TRUE(read_tuple.has_value());
    ASSERT_EQ(read_tuple.value().get_key(), 0);
    ASSERT_EQ(page.get_all_tuples().size(), 1);
    ASSERT_EQ(page.get_all_tuples()[0].get_key(), 0);

    Tuple4 tuple2(1);
    RawSlottedPage<Tuple4>::write_tuple(page_data, page_size, tuple2, 1);
    RawSlottedPage<Tuple4>::increase_tuple_count(page_data,1);
    ASSERT_EQ(page.get_tuple_count(), 2);
    auto read_tuple2 = page.get_tuple(1);
    ASSERT_TRUE(read_tuple2.has_value());
    ASSERT_EQ(read_tuple2.value().get_key(), 1);

    ASSERT_GT(RawSlottedPage<Tuple4>::get_max_tuples(page_size), RawSlottedPage<Tuple16>::get_max_tuples(page_size));
}

TEST(RawSlottedPageTest, BasicInsertionsTuple16) {
    unsigned page_size = 5 * 1024;
    RawSlottedPage<Tuple16> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);

    const auto page_data = page.get_page_data();
    Tuple16 tuple(0, {1, 2, 3});
    RawSlottedPage<Tuple16>::write_tuple(page_data, page_size, tuple, 0);
    RawSlottedPage<Tuple16>::increase_tuple_count(page_data,1);

    ASSERT_EQ(page.get_tuple_count(), 1);
    auto read_tuple = page.get_tuple(0);
    ASSERT_TRUE(read_tuple.has_value());
    ASSERT_EQ(read_tuple.value().get_key(), 0);
    ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, 3>{1, 2, 3}));
    ASSERT_EQ(page.get_all_tuples().size(), 1);
    ASSERT_EQ(page.get_all_tuples()[0].get_key(), 0);
    ASSERT_EQ(page.get_all_tuples()[0].get_variable_data(), (std::array<uint32_t, 3>{1, 2, 3}));

    Tuple16 tuple2(1, {4, 5, 6});
    RawSlottedPage<Tuple16>::write_tuple(page_data, page_size, tuple2, 1);
    RawSlottedPage<Tuple16>::increase_tuple_count(page_data,1);
    ASSERT_EQ(page.get_tuple_count(), 2);
    auto read_tuple2 = page.get_tuple(1);
    ASSERT_TRUE(read_tuple2.has_value());
    ASSERT_EQ(read_tuple2.value().get_key(), 1);
    ASSERT_EQ(read_tuple2.value().get_variable_data(), (std::array<uint32_t, 3>{4, 5, 6}));
    ASSERT_EQ(page.get_all_tuples().size(), 2);
    ASSERT_EQ(page.get_all_tuples()[1].get_key(), 1);
    ASSERT_EQ(page.get_all_tuples()[1].get_variable_data(), (std::array<uint32_t, 3>{4, 5, 6}));

    ASSERT_GT(RawSlottedPage<Tuple16>::get_max_tuples(page_size), RawSlottedPage<Tuple100>::get_max_tuples(page_size));
}

TEST(RawSlottedPageTest, BasicInsertionsTuple100) {
    unsigned page_size = 5 * 1024;
    RawSlottedPage<Tuple100> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);

    const auto page_data = page.get_page_data();
    Tuple100 tuple(0, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24});
    RawSlottedPage<Tuple100>::write_tuple(page_data, page_size, tuple, 0);
    RawSlottedPage<Tuple100>::increase_tuple_count(page_data,1);

    ASSERT_EQ(page.get_tuple_count(), 1);
    auto read_tuple = page.get_tuple(0);
    ASSERT_TRUE(read_tuple.has_value());
    ASSERT_EQ(read_tuple.value().get_key(), 0);
    ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, 24>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}));
    ASSERT_EQ(page.get_all_tuples().size(), 1);
    ASSERT_EQ(page.get_all_tuples()[0].get_key(), 0);
    ASSERT_EQ(page.get_all_tuples()[0].get_variable_data(), (std::array<uint32_t, 24>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}));

    Tuple100 tuple2(1, {25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48});
    RawSlottedPage<Tuple100>::write_tuple(page_data, page_size, tuple2, 1);
    RawSlottedPage<Tuple100>::increase_tuple_count(page_data,1);

    ASSERT_EQ(page.get_tuple_count(), 2);
    auto read_tuple2 = page.get_tuple(1);
    ASSERT_TRUE(read_tuple2.has_value());
    ASSERT_EQ(read_tuple2.value().get_key(), 1);
    ASSERT_EQ(read_tuple2.value().get_variable_data(), (std::array<uint32_t, 24>{25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48}));
    ASSERT_EQ(page.get_all_tuples().size(), 2);
    ASSERT_EQ(page.get_all_tuples()[1].get_key(), 1);
    ASSERT_EQ(page.get_all_tuples()[1].get_variable_data(), (std::array<uint32_t, 24>{25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48}));

    ASSERT_LT(RawSlottedPage<Tuple100>::get_max_tuples(page_size), RawSlottedPage<Tuple16>::get_max_tuples(page_size));
}

TEST(RawSlottedPageTest, FullPageInsertion) {
    const auto page_size = 5 * 1024;
    RawSlottedPage<Tuple16> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);
    auto max_amount_of_tuples = RawSlottedPage<Tuple16>::get_max_tuples(page_size);
    ASSERT_EQ(max_amount_of_tuples, 213);

    const auto page_data = page.get_page_data();

    std::vector<Tuple16> tuples;
    for (unsigned i = 0; i < max_amount_of_tuples; ++i) {
        tuples.emplace_back(i, std::array{i + 1, i + 2, i + 3});
    }

    unsigned entry_num = 0;
    for (auto &tuple: tuples) {
        RawSlottedPage<Tuple16>::write_tuple(page_data, page_size, tuple, entry_num++);
    }

    RawSlottedPage<Tuple16>::increase_tuple_count(page_data,entry_num);

    ASSERT_EQ(page.get_tuple_count(), max_amount_of_tuples);
    ASSERT_EQ(page.get_all_tuples().size(), max_amount_of_tuples);
    for (unsigned i = 0; i < max_amount_of_tuples; ++i) {
        auto read_tuple = page.get_tuple(i);
        ASSERT_TRUE(read_tuple.has_value());
        ASSERT_EQ(read_tuple.value().get_key(), i);
        ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array{i + 1, i + 2, i + 3}));
    }
}
