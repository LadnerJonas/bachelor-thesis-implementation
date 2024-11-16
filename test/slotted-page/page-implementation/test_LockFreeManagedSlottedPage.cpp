#include "slotted-page/page-implementation/LockFreeManagedSlottedPage.hpp"
#include "tuple-types/tuple-types.hpp"
#include <gtest/gtest.h>

template<typename T>
void write_tuple(LockFreeManagedSlottedPage<T> &page, const T &tuple) {
    auto wi = page.increment_and_fetch_opt_write_info();
    LockFreeManagedSlottedPage<T>::add_tuple_using_index(wi, tuple);
}

template<typename T>
void write_tuple_batched(LockFreeManagedSlottedPage<T> &page, const T *buffer, unsigned tuples_to_write) {
    auto bwi = page.increment_and_fetch_opt_write_info(tuples_to_write);
    LockFreeManagedSlottedPage<T>::add_batch_using_index(buffer, bwi);
}

TEST(LockFreeManagedSlottedPageTest, BasicInsertionsTuple4) {
    unsigned page_size = 5 * 1024;
    LockFreeManagedSlottedPage<Tuple4> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);

    Tuple4 tuple(0);
    write_tuple<Tuple4>(page, tuple);

    ASSERT_EQ(page.get_tuple_count(), 1);
    auto read_tuple = page.get_tuple(0);
    ASSERT_TRUE(read_tuple.has_value());
    ASSERT_EQ(read_tuple.value().get_key(), 0);
    ASSERT_EQ(page.get_all_tuples().size(), 1);
    ASSERT_EQ(page.get_all_tuples()[0].get_key(), 0);

    Tuple4 tuple2(1);
    write_tuple<Tuple4>(page, tuple2);

    ASSERT_EQ(page.get_tuple_count(), 2);
    auto read_tuple2 = page.get_tuple(1);
    ASSERT_TRUE(read_tuple2.has_value());
    ASSERT_EQ(read_tuple2.value().get_key(), 1);

    ASSERT_GT(LockFreeManagedSlottedPage<Tuple4>::get_max_tuples(page_size), LockFreeManagedSlottedPage<Tuple16>::get_max_tuples(page_size));
}

TEST(LockFreeManagedSlottedPageTest, BasicInsertionsTuple16) {
    unsigned page_size = 5 * 1024;
    LockFreeManagedSlottedPage<Tuple16> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);

    Tuple16 tuple(0, {1, 2, 3});
    write_tuple<Tuple16>(page, tuple);

    ASSERT_EQ(page.get_tuple_count(), 1);
    auto read_tuple = page.get_tuple(0);
    ASSERT_TRUE(read_tuple.has_value());
    ASSERT_EQ(read_tuple.value().get_key(), 0);
    ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, 3>{1, 2, 3}));
    ASSERT_EQ(page.get_all_tuples().size(), 1);
    ASSERT_EQ(page.get_all_tuples()[0].get_key(), 0);
    ASSERT_EQ(page.get_all_tuples()[0].get_variable_data(), (std::array<uint32_t, 3>{1, 2, 3}));

    Tuple16 tuple2(1, {4, 5, 6});
    write_tuple<Tuple16>(page, tuple2);

    ASSERT_EQ(page.get_tuple_count(), 2);
    auto read_tuple2 = page.get_tuple(1);
    ASSERT_TRUE(read_tuple2.has_value());
    ASSERT_EQ(read_tuple2.value().get_key(), 1);
    ASSERT_EQ(read_tuple2.value().get_variable_data(), (std::array<uint32_t, 3>{4, 5, 6}));
    ASSERT_EQ(page.get_all_tuples().size(), 2);
    ASSERT_EQ(page.get_all_tuples()[1].get_key(), 1);
    ASSERT_EQ(page.get_all_tuples()[1].get_variable_data(), (std::array<uint32_t, 3>{4, 5, 6}));

    ASSERT_GT(LockFreeManagedSlottedPage<Tuple16>::get_max_tuples(page_size), LockFreeManagedSlottedPage<Tuple100>::get_max_tuples(page_size));
}

TEST(LockFreeManagedSlottedPageTest, BasicInsertionsTuple100) {
    unsigned page_size = 5 * 1024;
    LockFreeManagedSlottedPage<Tuple100> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);

    Tuple100 tuple(0, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24});
    write_tuple<Tuple100>(page, tuple);

    ASSERT_EQ(page.get_tuple_count(), 1);
    auto read_tuple = page.get_tuple(0);
    ASSERT_TRUE(read_tuple.has_value());
    ASSERT_EQ(read_tuple.value().get_key(), 0);
    ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, 24>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}));
    ASSERT_EQ(page.get_all_tuples().size(), 1);
    ASSERT_EQ(page.get_all_tuples()[0].get_key(), 0);
    ASSERT_EQ(page.get_all_tuples()[0].get_variable_data(), (std::array<uint32_t, 24>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}));

    Tuple100 tuple2(1, {25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48});
    write_tuple<Tuple100>(page, tuple2);
    ASSERT_EQ(page.get_tuple_count(), 2);
    auto read_tuple2 = page.get_tuple(1);
    ASSERT_TRUE(read_tuple2.has_value());
    ASSERT_EQ(read_tuple2.value().get_key(), 1);
    ASSERT_EQ(read_tuple2.value().get_variable_data(), (std::array<uint32_t, 24>{25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48}));
    ASSERT_EQ(page.get_all_tuples().size(), 2);
    ASSERT_EQ(page.get_all_tuples()[1].get_key(), 1);
    ASSERT_EQ(page.get_all_tuples()[1].get_variable_data(), (std::array<uint32_t, 24>{25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48}));

    ASSERT_LT(LockFreeManagedSlottedPage<Tuple100>::get_max_tuples(page_size), LockFreeManagedSlottedPage<Tuple16>::get_max_tuples(page_size));
}

TEST(LockFreeManagedSlottedPageTest, BatchedInsertionTuple4) {
    using tuple = Tuple4;
    auto page_size = 5 * 1024u;
    size_t tuples_to_write = LockFreeManagedSlottedPage<tuple>::get_max_tuples(page_size);
    std::unique_ptr<tuple[]> buffer(new tuple[tuples_to_write]);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        buffer[i] = tuple(i);
    }

    LockFreeManagedSlottedPage<tuple> page(page_size);
    write_tuple_batched<Tuple4>(page, buffer.get(), tuples_to_write);
    ASSERT_EQ(page.get_tuple_count(), tuples_to_write);
    ASSERT_EQ(page.get_all_tuples().size(), tuples_to_write);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        auto read_tuple = page.get_tuple(i);
        ASSERT_TRUE(read_tuple.has_value());
        ASSERT_EQ(read_tuple.value().get_key(), i);
    }
}
TEST(LockFreeManagedSlottedPageTest, BatchedInsertionTuple16) {
    using tuple = Tuple16;
    auto page_size = 5 * 1024u;
    size_t tuples_to_write = LockFreeManagedSlottedPage<tuple>::get_max_tuples(page_size);
    std::unique_ptr<tuple[]> buffer(new tuple[tuples_to_write]);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        buffer[i] = tuple(i, {i + 1});
    }

    LockFreeManagedSlottedPage<tuple> page(page_size);
    write_tuple_batched<Tuple16>(page, buffer.get(), tuples_to_write);
    ASSERT_EQ(page.get_tuple_count(), tuples_to_write);
    ASSERT_EQ(page.get_all_tuples().size(), tuples_to_write);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        auto read_tuple = page.get_tuple(i);
        ASSERT_TRUE(read_tuple.has_value());
        ASSERT_EQ(read_tuple.value().get_key(), i);
        ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, tuple::get_size_of_variable_data() / sizeof(uint32_t)>{i + 1}));
    }
}
TEST(LockFreeManagedSlottedPageTest, BatchedInsertionTuple100) {
    using tuple = Tuple100;
    auto page_size = 5 * 1024u;
    size_t tuples_to_write = LockFreeManagedSlottedPage<tuple>::get_max_tuples(page_size);
    std::unique_ptr<tuple[]> buffer(new tuple[tuples_to_write]);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        buffer[i] = tuple(i, {i + 1});
    }

    LockFreeManagedSlottedPage<tuple> page(page_size);
    write_tuple_batched<Tuple100>(page, buffer.get(), tuples_to_write);
    ASSERT_EQ(page.get_tuple_count(), tuples_to_write);
    ASSERT_EQ(page.get_all_tuples().size(), tuples_to_write);
    for (unsigned i = 0; i < tuples_to_write; ++i) {
        auto read_tuple = page.get_tuple(i);
        ASSERT_TRUE(read_tuple.has_value());
        ASSERT_EQ(read_tuple.value().get_key(), i);
        ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array<uint32_t, tuple::get_size_of_variable_data() / sizeof(uint32_t)>{i + 1}));
    }
}

TEST(LockFreeManagedSlottedPageTest, FullPageInsertion) {
    const auto page_size = 5 * 1024;
    LockFreeManagedSlottedPage<Tuple16> page(page_size);
    ASSERT_EQ(page.get_tuple_count(), 0);
    auto max_amount_of_tuples = LockFreeManagedSlottedPage<Tuple16>::get_max_tuples(page_size);
    ASSERT_EQ(max_amount_of_tuples, 213);

    std::vector<Tuple16> tuples;
    for (unsigned i = 0; i < max_amount_of_tuples; ++i) {
        tuples.emplace_back(i, std::array{i + 1, i + 2, i + 3});
    }

    for (const auto &tuple: tuples) {
        write_tuple<Tuple16>(page, tuple);
    }
    ASSERT_EQ(page.get_all_tuples().size(), max_amount_of_tuples);

    auto wi_new_page = page.increment_and_fetch_opt_write_info();
    ASSERT_EQ(wi_new_page.page_data, nullptr);
    ASSERT_EQ(wi_new_page.page_size, 0);
    ASSERT_EQ(wi_new_page.tuple_index, max_amount_of_tuples);
    ASSERT_EQ(page.get_tuple_count(), max_amount_of_tuples);
    ASSERT_EQ(page.get_all_tuples().size(), max_amount_of_tuples);
    for (unsigned i = 0; i < max_amount_of_tuples; ++i) {
        auto read_tuple = page.get_tuple(i);
        ASSERT_TRUE(read_tuple.has_value());
        ASSERT_EQ(read_tuple.value().get_key(), i);
        ASSERT_EQ(read_tuple.value().get_variable_data(), (std::array{i + 1, i + 2, i + 3}));
    }
}
