// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers_all.hpp>

#include "rgw/fdb/fdb.h"

#include <algorithm>
#include <array>
#include <compare>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

namespace lfdb = ceph::libfdb;
namespace li = ceph::libfdb::interval;
namespace lq = ceph::libfdb::query;

using namespace std::literals;

struct int_domain final
{
 using value_type = int;

 static constexpr std::strong_ordering compare(const int lhs, const int rhs) noexcept
 {
  return lhs <=> rhs;
 }
};

struct described_interval final
{
 std::string begin_key;
 std::string end_key;
 bool begin_inclusive;
 bool end_inclusive;

 bool operator==(const described_interval&) const noexcept = default;
};

auto describe(const lfdb::select& interval)
{
 return described_interval {
  .begin_key = interval.begin_key,
  .end_key = interval.end_key,
  .begin_inclusive = interval.begin_inclusive,
  .end_inclusive = interval.end_inclusive
 };
}

auto emitted_intervals(const lq::expression auto& expr)
{
 std::vector<described_interval> out;

 lq::for_each_interval(expr, [&out](const lfdb::select& interval) {
  out.push_back(describe(interval));
 });

 return out;
}

struct described_int_interval final
{
 int begin;
 int end;
 bool begin_inclusive;
 bool end_inclusive;

 bool operator==(const described_int_interval&) const noexcept = default;
};

auto describe(const li::query<int_domain>& interval)
{
 return described_int_interval {
  .begin = interval.lower().finite_key(),
  .end = interval.upper().finite_key(),
  .begin_inclusive = interval.lower().inclusive(),
  .end_inclusive = interval.upper().inclusive()
 };
}

auto emitted_int_intervals(const li::expression auto& expr)
{
 std::vector<described_int_interval> out;

 li::for_each_interval(expr, [&out](const li::query<int_domain>& interval) {
  out.push_back(describe(interval));
 });

 return out;
}

auto execution_intervals(lq::expression auto expr)
{
 std::vector<described_interval> out;

 std::ranges::transform(lfdb::detail::intervals(std::move(expr)),
                        std::back_inserter(out),
                        [](const auto& interval) {
                         return describe(interval);
                        });

 return out;
}

bool contains_key(const described_interval& interval, const std::string_view key)
{
 const auto begin_key = std::string_view(interval.begin_key);
 const auto end_key = std::string_view(interval.end_key);

 const bool after_begin = begin_key < key or
                          (begin_key == key and interval.begin_inclusive);
 const bool before_end = key < end_key or
                         (key == end_key and interval.end_inclusive);

 return after_begin and before_end;
}

bool contains_key(const std::vector<described_interval>& intervals,
                  const std::string_view key)
{
 return std::ranges::any_of(intervals, [key](const auto& interval) {
  return contains_key(interval, key);
 });
}

auto sampled_keys_in(const lq::expression auto& expr)
{
 constexpr auto sample_keys = std::array {
  ""sv, "a"sv, "aa"sv, "b"sv, "c"sv, "d"sv,
  "m"sv, "n"sv, "p"sv, "q"sv, "z"sv
 };

 const auto intervals = emitted_intervals(expr);
 std::vector<std::string_view> out;

 std::ranges::copy_if(sample_keys,
                      std::back_inserter(out),
                      [&intervals](const auto key) {
                       return contains_key(intervals, key);
                      });

 return out;
}

void check_equivalent(std::string_view law,
                      const lq::expression auto& lhs,
                      const lq::expression auto& rhs)
{
 INFO(law);
 CHECK(sampled_keys_in(lhs) == sampled_keys_in(rhs));
}

void check_disjoint(std::string_view law,
                    const lq::expression auto& lhs,
                    const lq::expression auto& rhs)
{
 INFO(law);
 const auto lhs_keys = sampled_keys_in(lhs);
 const auto rhs_keys = sampled_keys_in(rhs);

 CHECK(std::ranges::none_of(lhs_keys, [&rhs_keys](const auto& key) {
  return std::ranges::contains(rhs_keys, key);
 }));
}

void check_partitions_sampled_universe(std::string_view law,
                                       const lq::expression auto& lhs,
                                       const lq::expression auto& rhs)
{
 INFO(law);
 const auto lhs_keys = sampled_keys_in(lhs);
 const auto rhs_keys = sampled_keys_in(rhs);
 const auto universal_keys = sampled_keys_in(lq::universal());

 std::vector<std::string_view> merged;
 merged.reserve(std::size(lhs_keys) + std::size(rhs_keys));
 std::ranges::set_union(lhs_keys, rhs_keys, std::back_inserter(merged));

 CHECK(universal_keys == merged);
}

std::string record_subspace(const std::string_view collection_id)
{
 return std::string(collection_id) + "/records/";
}

lfdb::select record_query(const std::string_view collection_id,
                          const std::string_view prefix = {},
                          const std::string_view cursor = {},
                          const bool cursor_inclusive = true)
{
 const auto base = record_subspace(collection_id);
 const auto prefix_key = base + std::string(prefix);
 auto query = lq::prefix(prefix_key);

 if (cursor.empty()) {
  return query;
 }

 const auto cursor_key = base + std::string(cursor);
 const auto lower = cursor_inclusive ? lq::closed(cursor_key) : lq::open(cursor_key);

 return lq::intersection(query,
                         lq::between(lower, lq::open(lq::successor(prefix_key))));
}

std::string revision_subspace(const std::string_view collection_id,
                              const std::string_view record_id)
{
 return std::string(collection_id) + "#" + std::string(record_id) + "/revisions/";
}

std::string revision_index(const std::string_view collection_id,
                           const std::string_view record_id,
                           const std::string_view encoded_rank,
                           const std::string_view revision)
{
 return revision_subspace(collection_id, record_id) +
        std::string(encoded_rank) + "/" + std::string(revision);
}

lfdb::select revisions_before_cursor_query(const std::string_view collection_id,
                                           const std::string_view record_id,
                                           const std::string_view cursor_rank,
                                           const std::string_view cursor_revision)
{
 auto query = lq::between(
  revision_subspace(collection_id, record_id),
  revision_index(collection_id, record_id, cursor_rank, cursor_revision));
 query.options.reverse_order = true;

 return query;
}

lfdb::select revision_rank_query(const std::string_view collection_id,
                                 const std::string_view record_id,
                                 const std::string_view min_rank,
                                 const std::string_view max_rank)
{
 const auto base = revision_subspace(collection_id, record_id);
 const auto begin_prefix = base + std::string(min_rank) + "/";
 const auto end_prefix = base + std::string(max_rank) + "/";

 return lq::between(begin_prefix, lq::successor(end_prefix));
}

struct malformed_interval_expression final : li::detail::expression_tag
{};

struct incomplete_configured_expression final
{
 using interval_expression_type = lq::interval;
 using domain_type = lq::byte_string_domain;
};

using configured_difference =
 decltype(lq::with_options(lq::difference(lq::empty(), lq::universal()),
                           lq::query_options {}));

static_assert(lq::expression<lq::interval>);
static_assert(lq::expression<decltype(lq::difference(lq::empty(), lq::universal()))>);
static_assert(lq::expression<decltype(lq::set_union(lq::empty(), lq::universal()))>);
static_assert(lq::expression<decltype(lq::after("m"))>);
static_assert(lq::expression<decltype(lq::before("m"))>);
static_assert(lq::expression<decltype(lq::without(lq::universal(), "m"))>);
static_assert(lq::non_interval_expression<decltype(lq::difference(lq::empty(), lq::universal()))>);
static_assert(not lq::expression<lq::interval_bound>);
static_assert(not lq::non_interval_expression<lq::interval>);
static_assert(lq::configured_expression<configured_difference>);
static_assert(not lq::configured_expression<incomplete_configured_expression>);
static_assert(li::expression<li::query<int_domain>>);
static_assert(li::expression<decltype(li::difference(li::query<int_domain>::empty(),
                                                     li::query<int_domain>::universal()))>);
static_assert(not li::expression<malformed_interval_expression>);

using composite_intersection_query =
 decltype(lq::intersection(lq::difference(lq::empty(), lq::universal()),
                           lq::universal()));

static_assert(lq::expression<composite_intersection_query>);

TEST_CASE("generic interval algebra emits canonical intervals", "[fdb][query]")
{
 using int_query = li::query<int_domain>;

 const auto split = li::difference(int_query::closed(0, 10),
                                   int_query::closed(2, 4));
 CHECK(emitted_int_intervals(split) ==
       std::vector {
        described_int_interval { 0, 2, true, false },
        described_int_interval { 4, 10, false, true }
       });

 const auto adjacent = li::set_union(int_query::closed_open(0, 2),
                                     int_query::closed_open(2, 4));
 CHECK(emitted_int_intervals(adjacent) ==
       std::vector { described_int_interval { 0, 4, true, false } });

 const auto touching = li::intersection(int_query::closed_open(0, 2),
                                        int_query::closed_open(2, 4));
 CHECK(li::is_empty(touching));
 CHECK(li::is_empty_expression(touching));
}

TEST_CASE("query interval preserves select compatibility", "[fdb][query]")
{
 const lfdb::select legacy_half_open { "a", "b" };
 CHECK(describe(legacy_half_open) == described_interval { "a", "b", true, false });

 const lfdb::select legacy_open_closed { lfdb::exclusive("a"), lfdb::inclusive("b") };
 CHECK(describe(legacy_open_closed) == described_interval { "a", "b", false, true });

 auto configured = lfdb::select { "a", "b" };
 configured.options.result_limit = 7;
 configured.options.target_bytes = 4096;
 configured.options.reverse_order = true;

 CHECK(7 == configured.options.result_limit);
 CHECK(4096 == configured.options.target_bytes);
 CHECK(configured.options.reverse_order);
}

TEST_CASE("query primitives describe local intervals", "[fdb][query]")
{
 SECTION("default interval is closed-open")
 {
  CHECK(describe(lq::interval("a", "b")) == described_interval { "a", "b", true, false });
 }

 SECTION("friendly between helper defaults to closed-open")
 {
  CHECK(describe(lq::between("a", "b")) == described_interval { "a", "b", true, false });
 }

 SECTION("friendly between helper preserves explicit interval bounds")
 {
  CHECK(describe(lq::between(lq::open("a"), lq::closed("b"))) ==
        described_interval { "a", "b", false, true });
 }

 SECTION("explicit interval notation controls both boundaries")
 {
  CHECK(describe(lq::interval(lq::open("a"), lq::closed("b"))) ==
        described_interval { "a", "b", false, true });
  CHECK(describe(lq::interval(lq::closed("a"), lq::closed("b"))) ==
        described_interval { "a", "b", true, true });
  CHECK(describe(lq::interval(lq::open("a"), lq::open("b"))) ==
        described_interval { "a", "b", false, false });
 }

 SECTION("prefix successor computes finite upper bounds")
 {
  CHECK(lq::successor("") == std::string("\xFF", 1));
  CHECK(lq::successor("abc") == "abd");
  CHECK(lq::successor("abc\xFF"sv) == "abd");
  CHECK_THROWS_AS(lq::successor("\xFF"sv), lfdb::libfdb_exception);
 }

 SECTION("prefix computes the finite successor bound")
 {
  CHECK(describe(lq::prefix("abc")) == described_interval { "abc", "abd", true, false });
  CHECK(describe(lq::prefix("abc\xFF"sv)) == described_interval { "abc\xFF", "abd", true, false });
 }

 SECTION("universal covers the ordinary FDB keyspace")
 {
  CHECK(describe(lq::universal()) == described_interval { "", "\xFF", true, false });
 }

 SECTION("singleton is closed on both ends")
 {
  CHECK(describe(lq::singleton("key")) == described_interval { "key", "key", true, true });
 }
}

TEST_CASE("query primitives reject or normalize locally invalid intervals", "[fdb][query]")
{
 CHECK(lq::is_empty(lq::empty()));
 CHECK(lq::is_empty(lq::interval("z", "a")));
 CHECK(lq::is_empty(lq::interval("a", "a")));
 CHECK(lq::is_empty(lq::interval(lq::open("a"), lq::closed("a"))));
 CHECK_FALSE(lq::is_empty(lq::singleton("a")));

 const auto system_prefix = std::string("\xFF", 1) + "abc";

 CHECK(lq::is_empty(lq::prefix("\xFF"sv)));
 CHECK(lq::is_empty(lq::prefix(system_prefix)));
}

TEST_CASE("query helpers clamp algebraic infinity to ordinary FDB keyspace", "[fdb][query]")
{
 const auto keyspace_limit = std::string("\xFF", 1);

 CHECK(lq::is_universal(lq::universal()));
 CHECK(lq::contains(lq::universal(), "z"));
 CHECK_FALSE(lq::contains(lq::universal(), keyspace_limit));

 CHECK(lq::is_empty(lq::at(keyspace_limit)));
 CHECK(lq::is_empty_expression(lq::from(lq::lower_at_or_after(keyspace_limit))));
 CHECK(emitted_intervals(lq::until(lq::upper_at_or_before(keyspace_limit))) ==
       std::vector { described_interval { "", "\xFF", true, false } });
 CHECK(emitted_intervals(lq::closed_between("z", keyspace_limit)) ==
       std::vector { described_interval { "z", "\xFF", true, false } });

 const lfdb::select raw_closed_limit { lfdb::inclusive("z"),
                                       lfdb::inclusive(keyspace_limit) };
 CHECK(execution_intervals(raw_closed_limit) ==
       std::vector { described_interval { "z", "\xFF", true, false } });

 const auto composed_raw_limit = lq::set_union(lq::prefix("a"), raw_closed_limit);
 CHECK(emitted_intervals(composed_raw_limit) ==
       std::vector {
        described_interval { "a", "b", true, false },
        described_interval { "z", "\xFF", true, false }
       });
}

TEST_CASE("query execution intervals suppress empty selections", "[fdb][query]")
{
 CHECK(execution_intervals(lq::empty()).empty());
 CHECK(execution_intervals(lq::intersection(lq::interval("a", "b"),
                                            lq::interval("b", "c"))).empty());

 CHECK(execution_intervals(lq::interval("a", "b")) ==
       std::vector { described_interval { "a", "b", true, false } });
}

TEST_CASE("query intersection trims intervals and folds empty results", "[fdb][query]")
{
 const auto overlap = lq::intersection(lq::interval("a", "m"),
                                       lq::interval("d", "z"));
 CHECK(emitted_intervals(overlap) ==
       std::vector { described_interval { "d", "m", true, false } });

 const auto touching_open = lq::intersection(lq::interval("a", "m"),
                                             lq::interval("m", "z"));
 CHECK(lq::is_empty_expression(touching_open));

 const auto touching_closed = lq::intersection(lq::interval(lq::closed("a"), lq::closed("m")),
                                               lq::interval(lq::closed("m"), lq::open("z")));
 CHECK(emitted_intervals(touching_closed) ==
       std::vector { described_interval { "m", "m", true, true } });

 const auto identity = lq::intersection(lq::universal(), lq::interval("a", "b"));
 CHECK(emitted_intervals(identity) ==
       std::vector { described_interval { "a", "b", true, false } });

 auto configured = lq::interval("a", "z");
 configured.options.result_limit = 17;

 const auto trimmed = lq::intersection(configured, lq::interval("m", "z"));
 CHECK(trimmed.options.result_limit == configured.options.result_limit);
}

TEST_CASE("query difference emits only necessary intervals", "[fdb][query]")
{
 const auto split = lq::difference(lq::interval("a", "z"),
                                   lq::interval("m", "p"));
 CHECK(emitted_intervals(split) ==
       std::vector {
        described_interval { "a", "m", true, false },
        described_interval { "p", "z", true, false }
       });

 const auto remove_singleton = lq::difference(lq::interval(lq::closed("a"), lq::closed("c")),
                                              lq::singleton("b"));
 CHECK(emitted_intervals(remove_singleton) ==
       std::vector {
        described_interval { "a", "b", true, false },
        described_interval { "b", "c", false, true }
       });

 const auto identity = lq::difference(lq::interval("a", "b"), lq::empty());
 CHECK(emitted_intervals(identity) ==
       std::vector { described_interval { "a", "b", true, false } });

 const auto annihilated = lq::difference(lq::interval("a", "b"), lq::universal());
 CHECK(lq::is_empty_expression(annihilated));

 const auto multi_cut = lq::difference(lq::interval("a", "z"),
                                       lq::set_union(lq::interval("m", "p"),
                                                     lq::interval("q", "t")));
 CHECK(emitted_intervals(multi_cut) ==
       std::vector {
        described_interval { "a", "m", true, false },
        described_interval { "p", "q", true, false },
        described_interval { "t", "z", true, false }
       });

 auto configured = lq::interval("a", "z");
 configured.options.target_bytes = 4096;

 lq::for_each_interval(lq::difference(configured, lq::interval("m", "p")),
                       [expected_options = configured.options](const lfdb::select& interval) {
                         CHECK(interval.options == expected_options);
                       });
}

TEST_CASE("query complement subtracts from universal", "[fdb][query]")
{
 const auto without_prefix = lq::complement(lq::prefix("m"));

 CHECK(emitted_intervals(without_prefix) ==
       std::vector {
        described_interval { "", "m", true, false },
        described_interval { "n", "\xFF", true, false }
       });
}

TEST_CASE("query set union coalesces adjacent and overlapping intervals", "[fdb][query]")
{
 const auto adjacent = lq::set_union(lq::interval("a", "b"),
                                     lq::interval("b", "c"));
 CHECK(emitted_intervals(adjacent) ==
       std::vector { described_interval { "a", "c", true, false } });

 const auto overlap = lq::set_union(lq::interval("a", "m"),
                                    lq::interval("d", "z"));
 CHECK(emitted_intervals(overlap) ==
       std::vector { described_interval { "a", "z", true, false } });

 const auto disjoint = lq::set_union(lq::interval("a", "b"),
                                     lq::interval("m", "z"));
 CHECK(emitted_intervals(disjoint) ==
       std::vector {
        described_interval { "a", "b", true, false },
        described_interval { "m", "z", true, false }
       });
}

TEST_CASE("query endpoint helpers compose range expressions", "[fdb][query]")
{
 SECTION("point-relative helpers describe half-bounded selections")
 {
  CHECK(emitted_intervals(lq::after("b")) ==
        std::vector { described_interval { "b", "\xFF", false, false } });
  CHECK(emitted_intervals(lq::before("m")) ==
        std::vector { described_interval { "", "m", true, false } });
 }

 SECTION("between helpers control both endpoints")
 {
  CHECK(emitted_intervals(lq::closed_between("b", "m")) ==
        std::vector { described_interval { "b", "m", true, true } });
  CHECK(emitted_intervals(lq::open_between("b", "m")) ==
        std::vector { described_interval { "b", "m", false, false } });
 }

 SECTION("cursor helpers trim an existing selection")
 {
  const auto records = lq::prefix("record/");

  CHECK(emitted_intervals(lq::starting_at(records, "record/0002")) ==
        std::vector { described_interval { "record/0002", "record0", true, false } });
  CHECK(emitted_intervals(lq::starting_after(records, "record/0002")) ==
        std::vector { described_interval { "record/0002", "record0", false, false } });
  CHECK(emitted_intervals(lq::ending_at(records, "record/0002")) ==
        std::vector { described_interval { "record/", "record/0002", true, true } });
  CHECK(emitted_intervals(lq::ending_before(records, "record/0002")) ==
        std::vector { described_interval { "record/", "record/0002", true, false } });
 }

 SECTION("exclusion helpers remove exact keys and prefixes")
 {
  CHECK(emitted_intervals(lq::without(lq::prefix("record/"), "record/0002")) ==
        std::vector {
         described_interval { "record/", "record/0002", true, false },
         described_interval { "record/0002", "record0", false, false }
        });

  CHECK(emitted_intervals(lq::without_prefix(lq::prefix("record/"), "record/private/")) ==
        std::vector {
         described_interval { "record/", "record/private/", true, false },
         described_interval { "record/private0", "record0", true, false }
        });
 }

 SECTION("prefix cursor helpers combine prefix and cursor bounds")
 {
  CHECK(emitted_intervals(lq::prefix_starting_at("record/", "record/0002")) ==
        std::vector { described_interval { "record/0002", "record0", true, false } });
  CHECK(emitted_intervals(lq::prefix_starting_after("record/", "record/0002")) ==
        std::vector { described_interval { "record/0002", "record0", false, false } });
 }

 SECTION("predicates classify interval relationships")
 {
  const auto records = lq::prefix("record/");
  const auto private_records = lq::prefix("record/private/");

  CHECK(lq::contains(lq::open_between("b", "m"), "c"));
  CHECK_FALSE(lq::contains(lq::open_between("b", "m"), "b"));
  CHECK_FALSE(lq::contains(lq::open_between("b", "m"), "m"));

  CHECK(lq::intersects(records, private_records));
  CHECK(lq::encloses(records, private_records));
  CHECK_FALSE(lq::intersects(records, lq::prefix("tenant/")));
 }

 SECTION("compiled intervals use the same canonical stream")
 {
  const auto visible_records =
   lq::without_prefix(lq::prefix("record/"), "record/private/");

  CHECK(lq::compile_intervals(visible_records).size() ==
        emitted_intervals(visible_records).size());
  CHECK(lq::compile_intervals<2>(visible_records).size() == 2);
 }
}

TEST_CASE("query expressions satisfy set algebra laws over sampled keys", "[fdb][query]")
{
 const auto empty = lq::empty();
 const auto universal = lq::universal();
 const auto a = lq::interval("a", "q");
 const auto b = lq::interval("d", "z");
 const auto c = lq::interval("m", "z");
 const auto prefix = lq::prefix("a");

 SECTION("intersection identity and annihilator laws")
 {
  check_equivalent("A intersection U == A",
                   lq::intersection(a, universal), a);
  check_equivalent("U intersection A == A",
                   lq::intersection(universal, a), a);
  check_equivalent("A intersection empty == empty",
                   lq::intersection(a, empty), empty);
 }

 SECTION("intersection idempotence, commutativity, and associativity")
 {
  check_equivalent("A intersection A == A",
                   lq::intersection(a, a), a);
  check_equivalent("A intersection B == B intersection A",
                   lq::intersection(a, b),
                   lq::intersection(b, a));
  check_equivalent("(A intersection B) intersection C == A intersection (B intersection C)",
                   lq::intersection(lq::intersection(a, b), c),
                   lq::intersection(a, lq::intersection(b, c)));
 }

 SECTION("union identity, idempotence, commutativity, and associativity")
 {
  check_equivalent("A union empty == A",
                   lq::set_union(a, empty), a);
  check_equivalent("empty union A == A",
                   lq::set_union(empty, a), a);
  check_equivalent("A union A == A",
                   lq::set_union(a, a), a);
  check_equivalent("A union B == B union A",
                   lq::set_union(a, b),
                   lq::set_union(b, a));
  check_equivalent("(A union B) union C == A union (B union C)",
                   lq::set_union(lq::set_union(a, b), c),
                   lq::set_union(a, lq::set_union(b, c)));
 }

 SECTION("difference identity and annihilator laws")
 {
  check_equivalent("A difference empty == A",
                   lq::difference(a, empty), a);
  check_equivalent("A difference A == empty",
                   lq::difference(a, a), empty);
  check_equivalent("A difference U == empty",
                   lq::difference(a, universal), empty);
 }

 SECTION("complement partitions the sampled universe")
 {
  check_disjoint("A and complement(A) are disjoint",
                 a, lq::complement(a));
  check_partitions_sampled_universe("A and complement(A) cover U",
                                    a, lq::complement(a));
  check_equivalent("A union complement(A) == U",
                   lq::set_union(a, lq::complement(a)), universal);
  check_equivalent("U difference complement(A) == A",
                   lq::difference(universal, lq::complement(a)), a);
 }

 SECTION("De Morgan laws")
 {
  check_equivalent("complement(A union B) == complement(A) intersection complement(B)",
                   lq::complement(lq::set_union(a, b)),
                   lq::intersection(lq::complement(a), lq::complement(b)));
  check_equivalent("complement(A intersection B) == complement(A) union complement(B)",
                   lq::complement(lq::intersection(a, b)),
                   lq::set_union(lq::complement(a), lq::complement(b)));
 }

 SECTION("prefix queries compose with the same laws")
 {
  check_equivalent("prefix difference empty == prefix",
                   lq::difference(prefix, empty), prefix);
  check_equivalent("prefix intersection U == prefix",
                   lq::intersection(prefix, universal), prefix);
  check_disjoint("prefix difference A is disjoint from A",
                 lq::difference(prefix, a), a);
 }
}

TEST_CASE("query examples compose record and revision selectors", "[fdb][query][example]")
{
 SECTION("record prefix scans replace hand-built prefix bounds")
 {
  CHECK(emitted_intervals(record_query("collection-a", "active/")) ==
        std::vector {
         described_interval { "collection-a/records/active/",
                              "collection-a/records/active0",
                              true,
                              false }
        });
 }

 SECTION("record prefix scans clamp cursors into the selected prefix")
 {
  CHECK(emitted_intervals(record_query("collection-a",
                                       "active/",
                                       "abandoned/9999",
                                       true)) ==
        std::vector {
         described_interval { "collection-a/records/active/",
                              "collection-a/records/active0",
                              true,
                              false }
        });

  CHECK(emitted_intervals(record_query("collection-a",
                                       "active/",
                                       "active/0007",
                                       false)) ==
        std::vector {
         described_interval { "collection-a/records/active/0007",
                              "collection-a/records/active0",
                              false,
                              false }
        });

  CHECK(lq::is_empty_expression(record_query("collection-a",
                                             "active/",
                                             "expired/0001",
                                             true)));
 }

 SECTION("plain record scans are prefix queries on the record subspace")
 {
  CHECK(emitted_intervals(record_query("collection-a")) ==
        std::vector {
         described_interval { "collection-a/records/",
                              "collection-a/records0",
                              true,
                              false }
        });
 }

 SECTION("revision scans can use an upper cursor and reverse ordering")
 {
  const auto query = revisions_before_cursor_query("collection-a",
                                                   "active/0001",
                                                   "0000000000003.000000",
                                                   "v3");

  CHECK(emitted_intervals(query) ==
        std::vector {
         described_interval { "collection-a#active/0001/revisions/",
                              "collection-a#active/0001/revisions/0000000000003.000000/v3",
                              true,
                              false }
        });
  CHECK(query.options.reverse_order);
 }

 SECTION("rank scans can avoid filtering a whole revision prefix client-side")
 {
  CHECK(emitted_intervals(revision_rank_query("collection-a",
                                              "active/0001",
                                              "0000000000002.000000",
                                              "0000000000004.000000")) ==
        std::vector {
         described_interval { "collection-a#active/0001/revisions/0000000000002.000000/",
                              "collection-a#active/0001/revisions/0000000000004.0000000",
                              true,
                              false }
        });
 }
}

TEST_CASE("query examples compose set operations", "[fdb][query][example]")
{
 SECTION("union combines related prefixes into one canonical interval stream")
 {
  const auto active_cache =
   lq::set_union(lq::prefix("cache/hot/"),
                 lq::prefix("cache/warm/"));

  CHECK(emitted_intervals(active_cache) ==
        std::vector {
         described_interval { "cache/hot/", "cache/hot0", true, false },
         described_interval { "cache/warm/", "cache/warm0", true, false }
        });
 }

 SECTION("difference removes a subrange from a larger prefix query")
 {
  const auto visible_objects =
   lq::difference(lq::prefix("bucket/objects/"),
                  lq::prefix("bucket/objects/.hidden/"));

  CHECK(emitted_intervals(visible_objects) ==
        std::vector {
         described_interval { "bucket/objects/",
                              "bucket/objects/.hidden/",
                              true,
                              false },
         described_interval { "bucket/objects/.hidden0",
                              "bucket/objects0",
                              true,
                              false }
        });
 }

 SECTION("intersection narrows composite expressions")
 {
  const auto visible_objects =
   lq::difference(lq::prefix("bucket/objects/"),
                  lq::prefix("bucket/objects/m/"));
  const auto page_window =
   lq::between("bucket/objects/a", "bucket/objects/z");

  CHECK(emitted_intervals(lq::intersection(visible_objects, page_window)) ==
        std::vector {
         described_interval { "bucket/objects/a",
                              "bucket/objects/m/",
                              true,
                              false },
         described_interval { "bucket/objects/m0",
                              "bucket/objects/z",
                              true,
                              false }
        });
 }

 SECTION("complement expresses everything outside a reserved keyspace")
 {
  const auto public_keys = lq::complement(lq::prefix("tenant/private/"));

  CHECK(emitted_intervals(public_keys) ==
        std::vector {
         described_interval { "", "tenant/private/", true, false },
         described_interval { "tenant/private0", "\xFF", true, false }
        });
 }

 SECTION("singleton and interval notation can exclude one exact key")
 {
  const auto without_tombstone =
   lq::difference(lq::prefix("object/"),
                  lq::singleton("object/tombstone"));

  CHECK(emitted_intervals(without_tombstone) ==
        std::vector {
         described_interval { "object/", "object/tombstone", true, false },
         described_interval { "object/tombstone", "object0", false, false }
        });
 }
}

TEST_CASE("query expression benchmarks", "[.benchmark][benchmark][fdb][query]")
{
 const auto active_records =
  lq::difference(lq::prefix("tenant/records/"),
                 lq::set_union(lq::prefix("tenant/records/.delete-marker/"),
                               lq::prefix("tenant/records/.multipart/")));
 const auto paged_records =
  lq::intersection(active_records,
                   lq::between("tenant/records/a", "tenant/records/z"));
 const auto public_records =
  lq::complement(lq::set_union(lq::prefix("tenant/private/"),
                               lq::prefix("tenant/internal/")));

 auto measure = [](const auto& expr) {
  std::size_t total = 0;

  lq::for_each_interval(expr, [&total](const lfdb::select& interval) {
   total += std::size(interval.begin_key) + std::size(interval.end_key);
  });

  return total;
 };

 BENCHMARK("emit difference over excluded prefixes")
 {
  return measure(active_records);
 };

 BENCHMARK("emit composite intersection")
 {
  return measure(paged_records);
 };

 BENCHMARK("emit complement of union")
 {
  return measure(public_records);
 };
}
