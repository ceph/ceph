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

#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_all.hpp>

#include "test/rgw/test_fdb_common.h"

#include <algorithm>
#include <compare>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

namespace content = ceph::libfdb::layer::content;

using namespace std::literals;

namespace {

void check_comparison_laws(const auto& lesser, const auto& equal_lesser, const auto& greater)
{
 CHECK(lesser == equal_lesser);
 CHECK_FALSE(lesser != equal_lesser);

 CHECK(lesser < greater);
 CHECK(lesser <= greater);
 CHECK(greater > lesser);
 CHECK(greater >= lesser);

 CHECK((lesser <=> equal_lesser) == std::strong_ordering::equal);
 CHECK((lesser <=> greater) == std::strong_ordering::less);
 CHECK((greater <=> lesser) == std::strong_ordering::greater);
}

void append_manual_string_segment(std::string& out, const std::string_view segment)
{
 out.append(segment);
 out.push_back('\0');
}

std::string manually_encoded_key(const std::vector<std::string_view>& segments,
                                 const std::string_view dynamic_segment)
{
 std::string out;

 for (const auto segment : segments) {
  append_manual_string_segment(out, segment);
 }

 append_manual_string_segment(out, dynamic_segment);

 return out;
}

std::string materialized_key(const content::compiled_key& key)
{
 return lfdb::select(key, key).begin_key;
}

} // anonymous namespace

TEST_CASE("content keys compile string segments", "[fdb][content]")
{
 const auto key = content::keyspace("tenant") / "bucket" / "object";
 const auto expected = std::string("tenant\0bucket\0object\0", 21);

 CHECK(expected == materialized_key(key));
 CHECK(expected.size() == key.size());
}

TEST_CASE("content key function composition matches operator composition", "[fdb][content]")
{
 const auto via_operator = content::keyspace("tenant") / "bucket" / "object";
 const auto via_function = content::key("tenant", "bucket", "object");

 CHECK(via_operator == via_function);
}

TEST_CASE("content key assembly rejects invalid inputs", "[fdb][content]")
{
 STATIC_REQUIRE(content::key_segments<std::string_view>);
 STATIC_REQUIRE_FALSE(content::key_segments<>);
 STATIC_REQUIRE_FALSE(content::key_segments<int>);
 STATIC_REQUIRE_FALSE(content::key_segments<content::compiled_key>);
 STATIC_REQUIRE(lfdb::concepts::libfdb_key<content::compiled_key>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::stringview_convertible<content::compiled_key>);

 REQUIRE_THROWS_AS(content::keyspace(std::string_view()),
                   ceph::libfdb::libfdb_exception);

 REQUIRE_THROWS_AS(content::keyspace(std::string_view("\xFF"sv)),
                   ceph::libfdb::libfdb_exception);
}

TEST_CASE("content key assembly constrains only the root segment",
          "[fdb][content]")
{
 constexpr char high_segment[] = { static_cast<char>(0xFF), 'x' };
 const auto empty_segment_key = content::keyspace("tenant") / "" / "object";
 const auto high_segment_key =
  content::keyspace("tenant") /
  std::string_view(high_segment, sizeof(high_segment));
 const auto expected_empty = std::string("tenant\0\0object\0", 15);
 auto expected_high = std::string("tenant\0", 7);
 expected_high.append(high_segment, sizeof(high_segment));
 expected_high.push_back('\0');

 CHECK_THAT(materialized_key(empty_segment_key),
            Catch::Matchers::RangeEquals(expected_empty));
 CHECK_THAT(materialized_key(high_segment_key),
            Catch::Matchers::RangeEquals(expected_high));
}

TEST_CASE("content key string segments escape embedded nulls", "[fdb][content]")
{
 constexpr char segment_bytes[] = { 'a', '\0', 'b' };
 const auto key = content::keyspace(std::string_view(segment_bytes, sizeof(segment_bytes)));
 const auto expected = std::string("a\0\xFF""b\0", 5);

 CHECK_THAT(materialized_key(key), Catch::Matchers::RangeEquals(expected));
}

TEST_CASE("content key string segments may grow past the lower-bound reserve",
          "[fdb][content]")
{
 constexpr char segment_bytes[] = { 'a', '\0', 'b', '\0', 'c' };
 const auto segment = std::string_view(segment_bytes, sizeof(segment_bytes));
 const auto key = content::keyspace("tenant") / segment;
 auto expected = std::string("tenant\0", 7);

 expected.append("a\0\xFF""b\0\xFF""c\0", 8);

 CHECK_THAT(materialized_key(key), Catch::Matchers::RangeEquals(expected));
}

TEST_CASE("content key string literals preserve embedded nulls", "[fdb][content]")
{
 const auto key = content::keyspace("a\0b");
 const auto expected = std::string("a\0\xFF""b\0", 5);

 CHECK_THAT(materialized_key(key), Catch::Matchers::RangeEquals(expected));
}

TEST_CASE("content key segment boundaries are unambiguous", "[fdb][content]")
{
 constexpr char embedded_null_bytes[] = { 'a', '\0', 'b' };

 const auto one_segment = content::keyspace(std::string_view(embedded_null_bytes,
                                                            sizeof(embedded_null_bytes)));
 const auto two_segments = content::keyspace("a") / "b";

 CHECK_FALSE(one_segment == two_segments);
 CHECK(two_segments < one_segment);
}

TEST_CASE("content key comparison algebra follows compiled byte order", "[fdb][content]")
{
 const auto alice = content::keyspace("tenant") / "alice";
 const auto alice_again = content::key("tenant", "alice");
 const auto bob = content::keyspace("tenant") / "bob";

 check_comparison_laws(alice, alice_again, bob);

 std::vector keys = {
  bob,
  content::keyspace("tenant") / "carol",
  alice
 };

 std::ranges::sort(keys);

 CHECK_THAT(keys, Catch::Matchers::RangeEquals(std::vector {
  alice,
  bob,
  content::key("tenant", "carol")
 }));
}

TEST_CASE("content prefix creates a libfdb selector", "[fdb][content]")
{
 const auto tenant = content::keyspace("tenant");
 const auto selector = content::prefix(tenant);
 const auto tenant_key = materialized_key(tenant);

 CHECK(selector.begin_key == tenant_key);
 CHECK(selector.end_key == lfdb::select(tenant).end_key);

 auto contains = [&selector](const content::compiled_key& key) {
  const auto candidate_key = materialized_key(key);
  return selector.begin_key <= candidate_key && candidate_key < selector.end_key;
 };

 CHECK(contains(content::keyspace("tenant") / "bucket"));
 CHECK_FALSE(contains(content::keyspace("tenant-other")));
}

TEST_CASE("content key composition benchmarks", "[.benchmark][benchmark][fdb][content]")
{
 constexpr auto segment_count = 4;
 [[maybe_unused]] constexpr auto keys_per_sample = 1000;
 const std::vector<std::string_view> segments = {
  "tenant"sv,
  "bucket"sv,
  "object"sv,
  "attribute"sv
 };

 BENCHMARK("baseline manual string composition") {
  std::size_t total = 0;

  for (auto i : std::views::iota(0, keys_per_sample)) {
   const auto suffix = std::to_string(i % segment_count);
   const auto key = manually_encoded_key(segments, suffix);
   total += key.size();
  }

  return total;
 };

 BENCHMARK("compiled content key composition") {
  std::size_t total = 0;

  for (auto i : std::views::iota(0, keys_per_sample)) {
   const auto key = content::keyspace(segments[0]) / segments[1] / segments[2] /
                    segments[3] / std::to_string(i % segment_count);
   total += key.size();
  }

  return total;
 };

 BENCHMARK("compiled content key function composition") {
  std::size_t total = 0;

  for (auto i : std::views::iota(0, keys_per_sample)) {
   const auto suffix = std::to_string(i % segment_count);
   const auto key = content::key(segments[0], segments[1], segments[2],
                                 segments[3], suffix);
   total += key.size();
  }

  return total;
 };
}
