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

#include <catch2/matchers/catch_matchers_all.hpp>

#include "rgw/fdb/fdb.h"

#include <algorithm>
#include <array>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

namespace lfdb = ceph::libfdb;
namespace lq = ceph::libfdb::query;

using namespace std::literals;

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

 for (const auto key : sample_keys) {
  if (contains_key(intervals, key)) {
   out.emplace_back(key);
  }
 }

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

static_assert(lq::expression<lq::interval>);
static_assert(lq::expression<decltype(lq::difference(lq::empty(), lq::universal()))>);
static_assert(lq::expression<decltype(lq::set_union(lq::empty(), lq::universal()))>);
static_assert(lq::non_interval_expression<decltype(lq::difference(lq::empty(), lq::universal()))>);
static_assert(not lq::expression<lq::interval_bound>);
static_assert(not lq::non_interval_expression<lq::interval>);

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
  CHECK(lq::successor("") == std::string("\xff", 1));
  CHECK(lq::successor("abc") == "abd");
  CHECK(lq::successor("abc\xff"sv) == "abd");
  CHECK_THROWS_AS(lq::successor("\xff"sv), lfdb::libfdb_exception);
 }

 SECTION("prefix computes the finite successor bound")
 {
  CHECK(describe(lq::prefix("abc")) == described_interval { "abc", "abd", true, false });
  CHECK(describe(lq::prefix("abc\xff"sv)) == described_interval { "abc\xff", "abd", true, false });
 }

 SECTION("universal covers the ordinary exposed keyspace")
 {
  CHECK(describe(lq::universal()) == described_interval { "", "\xff", true, false });
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

 CHECK_THROWS_AS(lq::prefix("\xff"sv), lfdb::libfdb_exception);
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
        described_interval { "n", "\xff", true, false }
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

 SECTION("complement expresses everything outside a reserved keyspace")
 {
  const auto public_keys = lq::complement(lq::prefix("tenant/private/"));

  CHECK(emitted_intervals(public_keys) ==
        std::vector {
         described_interval { "", "tenant/private/", true, false },
         described_interval { "tenant/private0", "\xff", true, false }
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
