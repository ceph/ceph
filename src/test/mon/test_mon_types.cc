// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <list>
#include <vector>
#include <sstream>
#include <iostream>

#include "mon/health_check.h"
#include "mon/mon_types.h"

#include "common/JSONFormatter.h"
#include "gtest/gtest.h"

namespace {

template <typename DETAIL_T>
ceph::buffer::list encode_health_check(
  const health_status_t severity,
  const std::string& summary,
  const DETAIL_T& detail,
  const int64_t count)
{
  ceph::buffer::list encoded;
  ENCODE_START(2, 1, encoded);
  encode(severity, encoded);
  encode(summary, encoded);
  encode(detail, encoded);
  encode(count, encoded);
  ENCODE_FINISH(encoded);

  return encoded;
}

std::vector<std::string> copy_detail(const auto& detail)
{
  return {std::cbegin(detail), std::cend(detail)};
}

void expect_health_detail_wire_compatibility(
  const std::vector<std::string>& detail)
{
  constexpr auto severity = HEALTH_WARN;
  const std::string summary = "summary";
  constexpr int64_t count = 37;

  health_check_t check;
  check.severity = severity;
  check.summary = summary;
  check.detail.assign(std::cbegin(detail), std::cend(detail));
  check.count = count;

  ceph::buffer::list current_bytes;
  encode(check, current_bytes);
  const std::list<std::string> legacy_detail(std::cbegin(detail), std::cend(detail));
  const auto legacy_bytes = encode_health_check(
    severity, summary, legacy_detail, count);
  const auto contiguous_bytes = encode_health_check(
    severity, summary, detail, count);
  ASSERT_TRUE(current_bytes.contents_equal(legacy_bytes));
  ASSERT_TRUE(current_bytes.contents_equal(contiguous_bytes));
  ASSERT_TRUE(legacy_bytes.contents_equal(contiguous_bytes));

  health_check_t decoded;
  auto cursor = legacy_bytes.cbegin();
  decode(decoded, cursor);
  EXPECT_EQ(severity, decoded.severity);
  EXPECT_EQ(summary, decoded.summary);
  EXPECT_EQ(detail, copy_detail(decoded.detail));
  EXPECT_EQ(count, decoded.count);
}

} // namespace

TEST(HealthChecks, DetailContainerRemainsWireCompatible)
{
  expect_health_detail_wire_compatibility({});
  expect_health_detail_wire_compatibility({"one"});
  expect_health_detail_wire_compatibility(
    {"first", "duplicate", "duplicate", "last"});
}

TEST(HealthChecks, MergePreservesSequenceAndOriginalDescription)
{
  health_check_map_t checks;
  auto& original = checks.add("SHARED", HEALTH_WARN, "original", 17);
  original.detail = {"first", "duplicate"};

  health_check_map_t incoming;
  auto& duplicate = incoming.add("SHARED", HEALTH_ERR, "replacement", 5);
  duplicate.detail = {"duplicate", "last"};
  auto& added = incoming.add("NEW", HEALTH_ERR, "new", 9);
  added.detail = {"new detail"};

  checks.merge(incoming);

  const auto& merged = checks.checks.at("SHARED");
  EXPECT_EQ(HEALTH_WARN, merged.severity);
  EXPECT_EQ("original", merged.summary);
  EXPECT_EQ(22, merged.count);
  EXPECT_EQ(
    (std::vector<std::string> {"first", "duplicate", "duplicate", "last"}),
    copy_detail(merged.detail));
  EXPECT_EQ(added, checks.checks.at("NEW"));
}

TEST(HealthChecks, AddAndGetOrAddKeepCountIndependentOfDetail)
{
  health_check_map_t checks;
  auto& check = checks.add("CODE", HEALTH_WARN, "first", 41);
  check.detail = {"one", "two"};

  EXPECT_EQ(41, check.count);
  EXPECT_EQ(2, std::size(check.detail));

  auto& updated = checks.get_or_add("CODE", HEALTH_ERR, "updated", 1);
  EXPECT_EQ(&check, &updated);
  EXPECT_EQ(HEALTH_ERR, updated.severity);
  EXPECT_EQ("updated", updated.summary);
  EXPECT_EQ(42, updated.count);
  EXPECT_EQ(
    (std::vector<std::string> {"one", "two"}),
    copy_detail(updated.detail));
}

TEST(HealthChecks, FormatterPreservesDetailOrder)
{
  health_check_t check;
  check.severity = HEALTH_WARN;
  check.summary = "summary";
  check.detail = {"first detail", "middle detail", "last detail"};
  check.count = 3;

  ceph::JSONFormatter formatter(false);
  formatter.open_object_section("check");
  check.dump(&formatter);
  formatter.close_section();

  std::ostringstream output;
  formatter.flush(output);
  const auto json = output.str();
  const auto first = json.find("first detail");
  const auto middle = json.find("middle detail");
  const auto last = json.find("last detail");

  ASSERT_NE(std::string::npos, first);
  ASSERT_NE(std::string::npos, middle);
  ASSERT_NE(std::string::npos, last);
  EXPECT_LT(first, middle);
  EXPECT_LT(middle, last);
}

TEST(mon_features, supported_v_persistent) {

  mon_feature_t supported = ceph::features::mon::get_supported();
  mon_feature_t persistent = ceph::features::mon::get_persistent();

  ASSERT_EQ(supported.intersection(persistent), persistent);
  ASSERT_TRUE(supported.contains_all(persistent));

  mon_feature_t diff = supported.diff(persistent);
  ASSERT_TRUE((persistent | diff) == supported);
  ASSERT_TRUE((supported & persistent) == persistent);
}

TEST(mon_features, binary_ops) {

  mon_feature_t FEATURE_NONE(0ULL);
  mon_feature_t FEATURE_A((1ULL << 1));
  mon_feature_t FEATURE_B((1ULL << 2));
  mon_feature_t FEATURE_C((1ULL << 3));
  mon_feature_t FEATURE_D((1ULL << 4));

  mon_feature_t FEATURE_ALL(
      FEATURE_A | FEATURE_B |
      FEATURE_C | FEATURE_D
  );

  mon_feature_t foo(FEATURE_A|FEATURE_B);
  mon_feature_t bar(FEATURE_C|FEATURE_D);

  ASSERT_EQ(FEATURE_A|FEATURE_B, foo);
  ASSERT_EQ(FEATURE_C|FEATURE_D, bar);

  ASSERT_NE(FEATURE_C, foo);
  ASSERT_NE(FEATURE_B, bar);
  ASSERT_NE(FEATURE_NONE, foo);
  ASSERT_NE(FEATURE_NONE, bar);

  ASSERT_FALSE(foo.empty());
  ASSERT_FALSE(bar.empty());
  ASSERT_TRUE(FEATURE_NONE.empty());

  ASSERT_EQ(FEATURE_ALL, (foo ^ bar));
  ASSERT_EQ(FEATURE_NONE, (foo & bar));

  mon_feature_t baz = foo;
  ASSERT_EQ(baz, foo);

  baz |= bar;
  ASSERT_EQ(FEATURE_ALL, baz);
  baz ^= foo;
  ASSERT_EQ(baz, bar);

  baz |= FEATURE_A;
  ASSERT_EQ(FEATURE_C, baz & FEATURE_C);
  ASSERT_EQ((FEATURE_A|FEATURE_D), baz & (FEATURE_A|FEATURE_D));
  ASSERT_EQ(FEATURE_B|FEATURE_C|FEATURE_D, (baz ^ foo));
}

TEST(mon_features, set_funcs) {

  mon_feature_t FEATURE_A((1ULL << 1));
  mon_feature_t FEATURE_B((1ULL << 2));
  mon_feature_t FEATURE_C((1ULL << 3));
  mon_feature_t FEATURE_D((1ULL << 4));

  mon_feature_t FEATURE_ALL(
      FEATURE_A | FEATURE_B |
      FEATURE_C | FEATURE_D
  );

  mon_feature_t foo(FEATURE_A|FEATURE_B);
  mon_feature_t bar(FEATURE_C|FEATURE_D);

  ASSERT_TRUE(FEATURE_ALL.contains_all(foo));
  ASSERT_TRUE(FEATURE_ALL.contains_all(bar));
  ASSERT_TRUE(FEATURE_ALL.contains_all(foo|bar));

  ASSERT_EQ(foo.diff(bar), foo);
  ASSERT_EQ(bar.diff(foo), bar);
  ASSERT_EQ(FEATURE_ALL.diff(foo), bar);
  ASSERT_EQ(FEATURE_ALL.diff(bar), foo);

  ASSERT_TRUE(foo.contains_any(FEATURE_A|bar));
  ASSERT_TRUE(bar.contains_any(FEATURE_ALL));
  ASSERT_TRUE(FEATURE_ALL.contains_any(foo));

  mon_feature_t FEATURE_X((1ULL << 10));

  ASSERT_FALSE(FEATURE_ALL.contains_any(FEATURE_X));
  ASSERT_FALSE(FEATURE_ALL.contains_all(FEATURE_X));
  ASSERT_EQ(FEATURE_ALL.diff(FEATURE_X), FEATURE_ALL);

  ASSERT_EQ(foo.intersection(FEATURE_ALL), foo);
  ASSERT_EQ(bar.intersection(FEATURE_ALL), bar);
}

TEST(mon_features, set_unset) {

  mon_feature_t FEATURE_A((1ULL << 1));
  mon_feature_t FEATURE_B((1ULL << 2));
  mon_feature_t FEATURE_C((1ULL << 3));

  mon_feature_t foo;
  ASSERT_EQ(ceph::features::mon::FEATURE_NONE, foo);

  foo.set_feature(FEATURE_A);
  ASSERT_EQ(FEATURE_A, foo);
  ASSERT_TRUE(foo.contains_all(FEATURE_A));

  foo.set_feature(FEATURE_B|FEATURE_C);
  ASSERT_EQ((FEATURE_A|FEATURE_B|FEATURE_C), foo);
  ASSERT_TRUE(foo.contains_all((FEATURE_A|FEATURE_B|FEATURE_C)));

  foo.unset_feature(FEATURE_A);
  ASSERT_EQ((FEATURE_B|FEATURE_C), foo);
  ASSERT_FALSE(foo.contains_any(FEATURE_A));
  ASSERT_TRUE(foo.contains_all((FEATURE_B|FEATURE_C)));

  foo.unset_feature(FEATURE_B|FEATURE_C);
  ASSERT_EQ(ceph::features::mon::FEATURE_NONE, foo);
  ASSERT_FALSE(foo.contains_any(FEATURE_A|FEATURE_B|FEATURE_C));
}
