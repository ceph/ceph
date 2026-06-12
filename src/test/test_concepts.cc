/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include "include/concepts.h"

#include <algorithm>
#include <array>
#include <deque>
#include <forward_list>
#include <initializer_list>
#include <list>
#include <map>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using ceph::concepts::associative_container;
using ceph::concepts::can_append;
using ceph::concepts::can_append_range;
using ceph::concepts::can_emplace_append;
using ceph::concepts::can_erase_if;
using ceph::concepts::can_insert_after;
using ceph::concepts::can_prepend;
using ceph::concepts::can_remove_front;
using ceph::concepts::container_appendable;
using ceph::concepts::container_compatible_range;
using ceph::concepts::has_size;
using ceph::concepts::reservable_container;
using ceph::concepts::unordered_container;

// Helper gadgets for the unit tests:
namespace {

struct vector_backed_range {
  using value_type = int;
  using iterator = std::vector<int>::iterator;
  using const_iterator = std::vector<int>::const_iterator;

  std::vector<int> values;

  vector_backed_range() = default;
  vector_backed_range(std::initializer_list<int> init) : values(init) {}

  iterator begin() { return values.begin(); }
  iterator end() { return values.end(); }
  const_iterator begin() const { return values.begin(); }
  const_iterator end() const { return values.end(); }
};

// Expose iterator interface to prove can_append() and push_back() will
// fall back to insert() when push_back()/emplace_back() don't exist:
struct append_only final : vector_backed_range {
  using vector_backed_range::vector_backed_range;

  void insert(iterator pos, int value) {
    values.insert(pos, value);
  }
};

struct append_range_only final : vector_backed_range {
  using vector_backed_range::vector_backed_range;

  void append_range(const std::array<int, 3>& range) {
    values.insert(values.end(), range.begin(), range.end());
  }
};

struct insert_range_only final : vector_backed_range {
  using vector_backed_range::vector_backed_range;

  iterator insert_range(iterator pos, const std::array<int, 3>& range) {
    return values.insert(pos, range.begin(), range.end());
  }
};

// Only exposes erase(I), to help verify can_remove_front() and pop_front()
// can work without pop_front() directly being available:
struct front_erasable final : vector_backed_range {
  using vector_backed_range::vector_backed_range;

  iterator erase(iterator pos) {
    return values.erase(pos);
  }
};

// May be constructed from int, but does not convert TO one:
struct explicit_from_int final {
  explicit explicit_from_int(int) {}
};

template <typename RangeT, typename ValueT>
struct range_compat_case final {
  using range_type = RangeT;
  using value_type = ValueT;
};

using int_predicate = bool (*)(int);
using int_deque = std::deque<int>;
using int_forward_list = std::forward_list<int>;
using int_list = std::list<int>;
using int_set = std::set<int>;
using int_vector = std::vector<int>;
using small_array = std::array<int, 3>;

bool is_even(int value)
{
  return value % 2 == 0;
}

template <typename ContainerT>
requires can_append_range<ContainerT, const std::array<int, 4>&> &&
         can_prepend<ContainerT, int> &&
         can_erase_if<ContainerT, int_predicate>
ContainerT collect_odd_values()
{
  const std::array input{1, 2, 3, 4};
  ContainerT values;

  ceph::util::maybe_reserve(values, input.size() + 1);
  ceph::util::append_range(values, input);
  std::erase_if(values, is_even);
  ceph::util::push_front(values, 0);

  return values;
}

} // namespace

/*** Tests for library general Concepts: */

TEMPLATE_PRODUCT_TEST_CASE("standard sequence containers model input_range",
                           "[concepts]",
                           (std::vector, std::list, std::forward_list),
                           (int))
{
  STATIC_REQUIRE(std::ranges::input_range<TestType>);
}

TEMPLATE_TEST_CASE("contiguous sequence containers model contiguous_range",
                   "[concepts]", std::vector<int>, (std::array<int, 3>))
{
  STATIC_REQUIRE(std::ranges::contiguous_range<TestType>);
}

TEMPLATE_TEST_CASE("compatible ranges model container_compatible_range",
                   "[concepts]",
                   (range_compat_case<std::vector<int>, int>),
                   (range_compat_case<const std::vector<int>, int>),
                   (range_compat_case<std::array<short, 3>, int>),
                   (range_compat_case<std::string, char>),
                   (range_compat_case<std::vector<int>, const int&>))
{
  STATIC_REQUIRE(container_compatible_range<typename TestType::range_type,
                                            typename TestType::value_type>);
}

TEMPLATE_TEST_CASE("reservable containers model reservable_container",
                   "[concepts]", std::vector<int>, std::string)
{
  STATIC_REQUIRE(reservable_container<TestType>);
}

TEMPLATE_TEST_CASE("appendable containers model container_appendable",
                   "[concepts]", std::vector<int>, std::deque<int>,
                   std::list<int>, std::set<int>, append_only)
{
  STATIC_REQUIRE(container_appendable<TestType, int>);
}

TEST_CASE("append capability avoids invalid hinted emplace paths",
          "[concepts]")
{
  STATIC_REQUIRE(can_append<std::set<int>, int>);
  STATIC_REQUIRE(container_appendable<std::set<int>, int>);
  STATIC_REQUIRE_FALSE(ceph::concepts::has_emplace_append<std::set<int>, int>);
  STATIC_REQUIRE(ceph::concepts::has_insert_append<std::set<int>, int>);
}

TEMPLATE_TEST_CASE("ordered associative containers model associative_container",
                   "[concepts]", std::set<int>, std::multiset<int>,
                   (std::map<int, std::string>),
                   (std::multimap<int, std::string>))
{
  STATIC_REQUIRE(associative_container<TestType>);
}

TEMPLATE_TEST_CASE("unordered associative containers model unordered_container",
                   "[concepts]", std::unordered_set<int>,
                   std::unordered_multiset<int>,
                   (std::unordered_map<int, std::string>),
                   (std::unordered_multimap<int, std::string>))
{
  STATIC_REQUIRE(unordered_container<TestType>);
}

/*** Tests for aggregate capability checks: */

TEMPLATE_TEST_CASE("appendable containers model can_append", "[concepts]",
                   std::vector<int>, std::deque<int>, std::list<int>,
                   std::set<int>, append_only)
{
  STATIC_REQUIRE(can_append<TestType, int>);
}

TEMPLATE_TEST_CASE("appendable containers model can_emplace_append",
                   "[concepts]",
                   std::vector<std::string>,
                   std::deque<std::string>,
                   std::list<std::string>,
                   std::set<std::string>)
{
  STATIC_REQUIRE(can_emplace_append<TestType, std::string_view>);
}

TEMPLATE_PRODUCT_TEST_CASE("prependable containers model can_prepend",
                           "[concepts]",
                           (std::deque, std::list, std::forward_list,
                            std::vector),
                           (int))
{
  STATIC_REQUIRE(can_prepend<TestType, int>);
}

TEMPLATE_PRODUCT_TEST_CASE("standard range appendable containers model "
                           "can_append_range",
                           "[concepts]", (std::vector, std::set), (int))
{
  STATIC_REQUIRE(can_append_range<TestType, std::array<int, 3>>);
}

TEST_CASE("forward-only containers model can_insert_after",
          "[concepts]")
{
  STATIC_REQUIRE(can_insert_after<int_forward_list,
                                  int_forward_list::iterator,
                                  int>);
  STATIC_REQUIRE_FALSE(can_insert_after<int_vector,
                                        int_vector::iterator,
                                        int>);
}

TEMPLATE_PRODUCT_TEST_CASE("predicate erasable containers model can_erase_if",
                           "[concepts]",
                           (std::vector, std::list, std::forward_list),
                           (int))
{
  STATIC_REQUIRE(can_erase_if<TestType, int_predicate>);
}

TEMPLATE_PRODUCT_TEST_CASE("standard front removable containers model "
                           "can_remove_front",
                           "[concepts]",
                           (std::deque, std::list, std::forward_list,
                            std::vector),
                           (int))
{
  STATIC_REQUIRE(can_remove_front<TestType>);
}

TEST_CASE("concept edge cases",
          "[concepts]")
{
  STATIC_REQUIRE_FALSE(std::ranges::contiguous_range<std::deque<int>>);
  STATIC_REQUIRE_FALSE(container_compatible_range<std::vector<int>,
                                                  std::string>);
  STATIC_REQUIRE_FALSE(container_compatible_range<std::vector<int>,
                                                  explicit_from_int>);
  STATIC_REQUIRE_FALSE(container_compatible_range<int, int>);
  STATIC_REQUIRE_FALSE(associative_container<std::vector<int>>);
  STATIC_REQUIRE_FALSE(unordered_container<std::set<int>>);
  STATIC_REQUIRE_FALSE(can_append<std::forward_list<int>, int>);
  STATIC_REQUIRE_FALSE(can_emplace_append<std::forward_list<int>, int>);
  STATIC_REQUIRE_FALSE(container_appendable<std::forward_list<int>, int>);
  STATIC_REQUIRE(can_append_range<append_only, std::array<int, 3>>);
  STATIC_REQUIRE_FALSE(can_append_range<std::forward_list<int>,
                                        std::array<int, 3>>);
  STATIC_REQUIRE_FALSE(has_size<std::forward_list<int>>);
  STATIC_REQUIRE_FALSE(reservable_container<std::list<int>>);
  STATIC_REQUIRE(can_remove_front<front_erasable>);
}

/*** Tests for library helpers: */

TEMPLATE_PRODUCT_TEST_CASE("helpers compose into container-generic algorithms",
                           "[concepts][util]",
                           (std::deque, std::list, std::vector),
                           (int))
{
  const TestType expected{0, 1, 3};

  CHECK(collect_odd_values<TestType>() == expected);
}

TEST_CASE("push_back appends through the best available container operation",
          "[concepts][util]")
{
  SECTION("vector uses back insertion") {
    std::vector<int> values;

    ceph::util::push_back(values, 1);
    ceph::util::push_back(values, 2);

    CHECK(values == std::vector{1, 2});
  }

  SECTION("set falls back to hinted insertion at end") {
    std::set<int> values;

    ceph::util::push_back(values, 2);
    ceph::util::push_back(values, 1);

    CHECK(values == std::set{1, 2});
  }

  SECTION("custom append-only type uses insert(end, value)") {
    append_only values;

    ceph::util::push_back(values, 1);
    ceph::util::push_back(values, 2);

    CHECK(values.values == std::vector{1, 2});
  }
}

TEST_CASE("emplace_append constructs at the natural append position",
          "[concepts][util]")
{
  SECTION("vector constructs from string_view") {
    std::vector<std::string> values;

    ceph::util::emplace_append(values, std::string_view { "alpha" });
    ceph::util::emplace_append(values, std::string_view { "beta" });

    CHECK(values == std::vector<std::string> { "alpha", "beta" });
  }

  SECTION("list constructs from string_view") {
    std::list<std::string> values;

    ceph::util::emplace_append(values, std::string_view { "alpha" });
    ceph::util::emplace_append(values, std::string_view { "beta" });

    CHECK(values == std::list<std::string> { "alpha", "beta" });
  }

  SECTION("set falls back to value insertion") {
    std::set<std::string> values;

    ceph::util::emplace_append(values, std::string_view { "beta" });
    ceph::util::emplace_append(values, std::string_view { "alpha" });

    CHECK(values == std::set<std::string> { "alpha", "beta" });
  }
}

TEST_CASE("append_range appends ranges with container-specific fallbacks",
          "[concepts][util]")
{
  SECTION("vector appends iterator ranges") {
    std::vector<int> values{1};
    const std::array more{2, 3};

    ceph::util::append_range(values, more);

    CHECK(values == std::vector{1, 2, 3});
  }

  SECTION("set inserts an input range") {
    std::set<int> values{1};
    const std::array more{3, 2};

    ceph::util::append_range(values, more);

    CHECK(values == std::set{1, 2, 3});
  }

  SECTION("custom append-only type falls back to element appends") {
    append_only values;
    const std::array more{1, 2, 3};

    ceph::util::append_range(values, more);

    CHECK(values.values == std::vector{1, 2, 3});
  }
}

TEST_CASE("collect_as materializes ranges with append fallbacks",
          "[concepts][util]")
{
  SECTION("sequence from view") {
    auto values = ceph::util::collect_as<std::vector<int>>(
      std::views::iota(0, 4));

    CHECK(values == std::vector{0, 1, 2, 3});
  }

  SECTION("set from transformed view") {
    auto values = ceph::util::collect_as<std::set<int>>(
      std::views::iota(0, 4) |
      std::views::transform([](int i) { return 3 - i; }));

    CHECK(values == std::set{0, 1, 2, 3});
  }

  SECTION("custom append-only type") {
    auto values = ceph::util::collect_as<append_only>(
      std::array{1, 2, 3});

    CHECK(values.values == std::vector{1, 2, 3});
  }
}

TEST_CASE("insert_range inserts at the requested position", "[concepts][util]")
{
  std::vector<int> values{1, 4};
  const std::array more{2, 3};

  ceph::util::insert_range(values, values.begin() + 1, more);

  CHECK(values == std::vector{1, 2, 3, 4});
}

TEST_CASE("insert_after inserts after the requested position",
          "[concepts][util]")
{
  std::forward_list<int> values;
  auto pos = values.before_begin();

  pos = ceph::util::insert_after(values, pos, 1);
  pos = ceph::util::emplace_after(values, pos, 2);
  ceph::util::insert_after(values, pos, 3);

  CHECK(values == std::forward_list{1, 2, 3});
}

TEST_CASE("make_appender appends through container-specific operations",
          "[concepts][util]")
{
  SECTION("vector appender constructs at the back") {
    std::vector<std::string> values;
    auto append = ceph::util::make_appender(values);

    append.emplace(std::string_view { "alpha" });
    append.emplace(std::string_view { "beta" });

    CHECK(values == std::vector<std::string> { "alpha", "beta" });
  }

  SECTION("forward_list appender tracks the insertion position") {
    std::forward_list<std::string> values;
    auto append = ceph::util::make_appender(values);

    append.emplace(std::string_view { "alpha" });
    append.emplace(std::string_view { "beta" });

    CHECK(values == std::forward_list<std::string> { "alpha", "beta" });
  }
}

TEST_CASE("front helpers use front-specific operations or begin erasure",
          "[concepts][util]")
{
  SECTION("push_front prepends values") {
    std::vector<int> values{2, 3};

    ceph::util::push_front(values, 1);

    CHECK(values == std::vector{1, 2, 3});
  }

  SECTION("pop_front removes the first value") {
    front_erasable values{{1, 2, 3}};

    ceph::util::pop_front(values);

    CHECK(values.values == std::vector{2, 3});
  }
}

TEST_CASE("optional sizing helpers call supported operations only",
          "[concepts][util]")
{
  std::vector<int> values;

  ceph::util::maybe_reserve(values, 4);
  ceph::util::maybe_resize(values, 3);

  CHECK(ceph::util::capacity(values) >= 4);
  CHECK(std::ranges::size(values) == 3);
  CHECK(!std::ranges::empty(values));

  ceph::util::clear(values);

  CHECK(std::ranges::empty(values));
}

/* The use of X-macros here is a bit unfortunate, however as we're not yet using C++26 I
 * don't think there's another way to express this without having... well, a *LOT* of essentially
 * redundant tests: */
#define CEPH_HAS_MEMBER_CAPABILITY_CASES(X)                                     \
  X(emplace_back, (ceph::concepts::has_emplace_back<int_vector, int>),          \
    (ceph::concepts::has_emplace_back<int_set, int>))                           \
  X(push_back, (ceph::concepts::has_push_back<int_vector, int>),                \
    (ceph::concepts::has_push_back<int_set, int>))                              \
  X(emplace_append, (ceph::concepts::has_emplace_append<int_vector, int>),      \
    (ceph::concepts::has_emplace_append<int_set, int>))                         \
  X(insert_append, (ceph::concepts::has_insert_append<int_vector, int>),        \
    (ceph::concepts::has_insert_append<int_forward_list, int>))                 \
  X(emplace_after, (ceph::concepts::has_emplace_after<int_forward_list, int_forward_list::iterator, int>), \
    (ceph::concepts::has_emplace_after<int_vector, int_vector::iterator, int>)) \
  X(insert_after, (ceph::concepts::has_insert_after<int_forward_list, int_forward_list::iterator, int>), \
    (ceph::concepts::has_insert_after<int_vector, int_vector::iterator, int>))  \
  X(append_range, (ceph::concepts::has_append_range<append_range_only, small_array>), \
    (ceph::concepts::has_append_range<int_vector, small_array>))                \
  X(insert_range_append, (ceph::concepts::has_insert_range_append<insert_range_only, small_array>), \
    (ceph::concepts::has_insert_range_append<int_vector, small_array>))         \
  X(insert_iterator_range_append, (ceph::concepts::has_insert_iterator_range_append<int_vector, small_array>), \
    (ceph::concepts::has_insert_iterator_range_append<int_forward_list, small_array>)) \
  X(insert_range, (ceph::concepts::has_insert_range<insert_range_only, insert_range_only::iterator, small_array>), \
    (ceph::concepts::has_insert_range<int_vector, int_vector::iterator, small_array>)) \
  X(insert_iterator_range, (ceph::concepts::has_insert_iterator_range<int_vector, int_vector::iterator, small_array>), \
    (ceph::concepts::has_insert_iterator_range<int_forward_list, int_forward_list::iterator, small_array>)) \
  X(emplace_front, (ceph::concepts::has_emplace_front<int_deque, int>),         \
    (ceph::concepts::has_emplace_front<int_vector, int>))                       \
  X(push_front, (ceph::concepts::has_push_front<int_deque, int>),               \
    (ceph::concepts::has_push_front<int_vector, int>))                          \
  X(emplace_at_begin, (ceph::concepts::has_emplace_at_begin<int_vector, int>),  \
    (ceph::concepts::has_emplace_at_begin<int_forward_list, int>))              \
  X(insert_at_begin, (ceph::concepts::has_insert_at_begin<int_vector, int>),    \
    (ceph::concepts::has_insert_at_begin<int_forward_list, int>))               \
  X(pop_front, (ceph::concepts::has_pop_front<int_deque>),                      \
    (ceph::concepts::has_pop_front<int_vector>))                                \
  X(erase_begin, (ceph::concepts::has_erase_begin<int_vector>),                 \
    (ceph::concepts::has_erase_begin<int_forward_list>))                        \
  X(erase, (ceph::concepts::has_erase<int_set, int>),                           \
    (ceph::concepts::has_erase<int_vector, int>))                               \
  X(erase_range, (ceph::concepts::has_erase_range<int_vector, int_vector::iterator, int_vector::iterator>), \
    (ceph::concepts::has_erase_range<int_forward_list, int_forward_list::iterator, int_forward_list::iterator>)) \
  X(remove_if, (ceph::concepts::has_remove_if<int_list, int_predicate>),        \
    (ceph::concepts::has_remove_if<int_vector, int_predicate>))                 \
  X(clear, (ceph::concepts::has_clear<int_vector>),                             \
    (ceph::concepts::has_clear<int>))                                           \
  X(reserve, (ceph::concepts::has_reserve<int_vector>),                         \
    (ceph::concepts::has_reserve<int_list>))                                    \
  X(capacity, (ceph::concepts::has_capacity<int_vector>),                       \
    (ceph::concepts::has_capacity<int_list>))                                   \
  X(max_size, (ceph::concepts::has_max_size<int_vector>),                       \
    (ceph::concepts::has_max_size<int>))                                        \
  X(size, (ceph::concepts::has_size<int_vector>),                               \
    (ceph::concepts::has_size<int_forward_list>))                               \
  X(empty, (ceph::concepts::has_empty<int_vector>),                             \
    (ceph::concepts::has_empty<int>))                                           \
  X(resize, (ceph::concepts::has_resize<int_vector>),                           \
    (ceph::concepts::has_resize<int_set>))

#define CEPH_CHECK_HAS_MEMBER_CAPABILITY(member, positive, negative) \
  STATIC_REQUIRE positive;                                           \
  STATIC_REQUIRE_FALSE negative;

TEST_CASE("has_* member capability predicates match member availability",
          "[concepts]")
{
  CEPH_HAS_MEMBER_CAPABILITY_CASES(CEPH_CHECK_HAS_MEMBER_CAPABILITY)
}

// Put the X-macro machinery back into the original packaging:
#undef CEPH_CHECK_HAS_MEMBER_CAPABILITY
#undef CEPH_HAS_MEMBER_CAPABILITY_CASES
