// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#include <gtest/gtest.h>
#include "rgw_range_parse.h"

using namespace std;

static rgw_byte_range int_range(uint64_t first, uint64_t last) {
  rgw_byte_range r;
  r.first = first;
  r.last = last;
  r.has_last = true;
  return r;
}

static rgw_byte_range open_range(uint64_t first) {
  rgw_byte_range r;
  r.first = first;
  return r;
}

static rgw_byte_range suffix_range(uint64_t len) {
  rgw_byte_range r;
  r.is_suffix = true;
  r.suffix_length = len;
  return r;
}

namespace {

struct parse_case {
  const char* spec;
  rgw_range_parse_result result;
  vector<rgw_byte_range> ranges;
};

void run(const parse_case& c) {
  vector<rgw_byte_range> ranges;
  const auto r = rgw_parse_byte_ranges(c.spec, 0, &ranges);
  EXPECT_EQ(c.result, r) << "spec: " << c.spec;
  if (r == rgw_range_parse_result::ok) {
    EXPECT_EQ(c.ranges, ranges) << "spec: " << c.spec;
  } else {
    EXPECT_TRUE(ranges.empty()) << "spec: " << c.spec;
  }
}

} // namespace

TEST(RGWRangeParse, SingleRange)
{
  const parse_case cases[] = {
    {"bytes=0-100",  rgw_range_parse_result::ok, {int_range(0, 100)}},
    {"bytes=0-0",    rgw_range_parse_result::ok, {int_range(0, 0)}},
    {"bytes=100-100",rgw_range_parse_result::ok, {int_range(100, 100)}},
    {"bytes=0-",     rgw_range_parse_result::ok, {open_range(0)}},
    {"bytes=42-",    rgw_range_parse_result::ok, {open_range(42)}},
    {"bytes=-100",   rgw_range_parse_result::ok, {suffix_range(100)}},
    /* suffix-length is 1*DIGIT, so "-0" is syntax-valid; whether the resulting
     * empty range is satisfiable is not the parser's call. */
    {"bytes=-0",     rgw_range_parse_result::ok, {suffix_range(0)}},
  };
  for (const auto& c : cases)
    run(c);
}

TEST(RGWRangeParse, MultipleRanges)
{
  const parse_case cases[] = {
    {"bytes=0-100,300-400", rgw_range_parse_result::ok,
     {int_range(0, 100), int_range(300, 400)}},
    /* RFC 9110 5.6.1 permits optional whitespace around the list separator */
    {"bytes=0-100, 300-400", rgw_range_parse_result::ok,
     {int_range(0, 100), int_range(300, 400)}},
    {"bytes=0-100 , 300-400", rgw_range_parse_result::ok,
     {int_range(0, 100), int_range(300, 400)}},
    /* and requires a recipient to skip empty list elements */
    {"bytes=0-100,,300-400", rgw_range_parse_result::ok,
     {int_range(0, 100), int_range(300, 400)}},
    {"bytes=0-100,", rgw_range_parse_result::ok, {int_range(0, 100)}},
    /* ascending order is a SHOULD for clients, not a MUST for servers */
    {"bytes=300-400,0-100", rgw_range_parse_result::ok,
     {int_range(300, 400), int_range(0, 100)}},
    /* overlapping ranges are well formed */
    {"bytes=0-100,50-150", rgw_range_parse_result::ok,
     {int_range(0, 100), int_range(50, 150)}},
    {"bytes=0-10,-5", rgw_range_parse_result::ok,
     {int_range(0, 10), suffix_range(5)}},
    {"bytes=0-1,2-3,4-5,6-7", rgw_range_parse_result::ok,
     {int_range(0, 1), int_range(2, 3), int_range(4, 5), int_range(6, 7)}},
  };
  for (const auto& c : cases)
    run(c);
}

TEST(RGWRangeParse, RangeUnit)
{
  const parse_case cases[] = {
    /* range units are case-insensitive */
    {"BYTES=0-100", rgw_range_parse_result::ok, {int_range(0, 100)}},
    {"Bytes=0-100", rgw_range_parse_result::ok, {int_range(0, 100)}},
    {"bytes = 0-100", rgw_range_parse_result::ok, {int_range(0, 100)}},
    /* an unknown range unit must be ignored, not rejected: RFC 9110 14.2 */
    {"items=0-100", rgw_range_parse_result::not_bytes, {}},
    {"seconds=1-2", rgw_range_parse_result::not_bytes, {}},
    /* the whole unit has to match. The previous parser searched for "bytes="
     * anywhere in the value, so a unit merely ending in it was taken for a
     * byte range; range-unit is a token and "xbytes" is not "bytes". */
    {"xbytes=0-100", rgw_range_parse_result::not_bytes, {}},
    {"kbytes=0-100", rgw_range_parse_result::not_bytes, {}},
    {"bytesx=0-100", rgw_range_parse_result::not_bytes, {}},
    /* no range-unit at all is likewise not a bytes range */
    {"0-100", rgw_range_parse_result::not_bytes, {}},
    {"", rgw_range_parse_result::not_bytes, {}},
  };
  for (const auto& c : cases)
    run(c);
}

TEST(RGWRangeParse, Invalid)
{
  const parse_case cases[] = {
    /* first-pos and last-pos are 1*DIGIT: no signs, no trailing junk.
     * The previous parser ran these through atoll(), which stops at the first
     * non-digit, so "abc-def" was accepted as byte 0 and "0-100xyz" as 0-100. */
    {"bytes=abc-def",  rgw_range_parse_result::invalid, {}},
    {"bytes=0-100xyz", rgw_range_parse_result::invalid, {}},
    {"bytes=0x10-",    rgw_range_parse_result::invalid, {}},
    {"bytes=+0-100",   rgw_range_parse_result::invalid, {}},
    {"bytes=-1-2",     rgw_range_parse_result::invalid, {}},
    {"bytes=1-2-3",    rgw_range_parse_result::invalid, {}},
    {"bytes= 0 - 100", rgw_range_parse_result::invalid, {}},
    /* "An int-range is invalid if the last-pos value is present and less than
     * the first-pos." RFC 9110 14.1.1 */
    {"bytes=5-2",      rgw_range_parse_result::invalid, {}},
    /* range-set = 1#range-spec, so it may not be empty */
    {"bytes=",         rgw_range_parse_result::invalid, {}},
    {"bytes=,",        rgw_range_parse_result::invalid, {}},
    {"bytes=,,",       rgw_range_parse_result::invalid, {}},
    {"bytes=100",      rgw_range_parse_result::invalid, {}},
    {"bytes=-",        rgw_range_parse_result::invalid, {}},
    /* one bad range-spec invalidates the whole range-set */
    {"bytes=0-100,bad", rgw_range_parse_result::invalid, {}},
    {"bytes=bad,0-100", rgw_range_parse_result::invalid, {}},
  };
  for (const auto& c : cases)
    run(c);
}

TEST(RGWRangeParse, Saturation)
{
  /* first-pos, last-pos and suffix-length are 1*DIGIT with no upper bound, so
   * a value too large for uint64_t is well formed and may not be rejected.
   * RFC 9110 14.1.2 gives it the same meaning as the largest representable
   * one: an out-of-length last-pos is the remainder of the representation, an
   * over-long suffix-length is the whole representation, and a first-pos past
   * the end is unsatisfiable either way. */
  const parse_case cases[] = {
    {"bytes=99999999999999999999-", rgw_range_parse_result::ok,
     {open_range(UINT64_MAX)}},
    {"bytes=0-99999999999999999999", rgw_range_parse_result::ok,
     {int_range(0, UINT64_MAX)}},
    {"bytes=-99999999999999999999", rgw_range_parse_result::ok,
     {suffix_range(UINT64_MAX)}},
    /* the largest value that still fits is not saturated into something else */
    {"bytes=18446744073709551615-", rgw_range_parse_result::ok,
     {open_range(UINT64_MAX)}},
    {"bytes=0-18446744073709551615", rgw_range_parse_result::ok,
     {int_range(0, UINT64_MAX)}},
  };
  for (const auto& c : cases)
    run(c);

  /* saturating last-pos must not collide with an absent one: "0-<huge>" is a
   * closed range, "0-" is open, and they are not the same range-spec */
  vector<rgw_byte_range> closed, open;
  ASSERT_EQ(rgw_range_parse_result::ok,
            rgw_parse_byte_ranges("bytes=0-99999999999999999999", 0, &closed));
  ASSERT_EQ(rgw_range_parse_result::ok,
            rgw_parse_byte_ranges("bytes=0-", 0, &open));
  ASSERT_EQ(1u, closed.size());
  ASSERT_EQ(1u, open.size());
  EXPECT_TRUE(closed[0].has_last);
  EXPECT_FALSE(open[0].has_last);
  EXPECT_FALSE(closed[0] == open[0]);

  /* a saturated last-pos is still checked against first-pos, including when
   * both saturate and the stored values no longer order them */
  const parse_case ordering[] = {
    {"bytes=99999999999999999999-5", rgw_range_parse_result::invalid, {}},
    {"bytes=99999999999999999999-88888888888888888888",
     rgw_range_parse_result::invalid, {}},
    {"bytes=18446744073709551615-18446744073709551614",
     rgw_range_parse_result::invalid, {}},
    {"bytes=88888888888888888888-99999999999999999999",
     rgw_range_parse_result::ok, {int_range(UINT64_MAX, UINT64_MAX)}},
    {"bytes=99999999999999999999-99999999999999999999",
     rgw_range_parse_result::ok, {int_range(UINT64_MAX, UINT64_MAX)}},
    /* leading zeros must not make a value look smaller than it is */
    {"bytes=0000000000000000000000005-2", rgw_range_parse_result::invalid, {}},
    {"bytes=00005-00010", rgw_range_parse_result::ok, {int_range(5, 10)}},
  };
  for (const auto& c : ordering)
    run(c);
}

TEST(RGWRangeParse, MaxRanges)
{
  vector<rgw_byte_range> ranges;

  /* a range-set at the limit is accepted */
  EXPECT_EQ(rgw_range_parse_result::ok,
            rgw_parse_byte_ranges("bytes=0-1,2-3", 2, &ranges));
  EXPECT_EQ(2u, ranges.size());

  /* one past it is reported separately from a syntax error, because the
   * request is well formed and a long range-set is the shape a
   * denial-of-service attempt takes: RFC 9110 17.15 */
  EXPECT_EQ(rgw_range_parse_result::too_many,
            rgw_parse_byte_ranges("bytes=0-1,2-3,4-5", 2, &ranges));

  /* max_ranges == 0 means unbounded */
  EXPECT_EQ(rgw_range_parse_result::ok,
            rgw_parse_byte_ranges("bytes=0-1,2-3,4-5", 0, &ranges));
  EXPECT_EQ(3u, ranges.size());

  /* the limit does not affect a single range */
  EXPECT_EQ(rgw_range_parse_result::ok,
            rgw_parse_byte_ranges("bytes=0-100", 1, &ranges));
  EXPECT_EQ(1u, ranges.size());
}

TEST(RGWRangeParse, OutputIsClearedOnFailure)
{
  vector<rgw_byte_range> ranges{int_range(7, 9)};

  EXPECT_EQ(rgw_range_parse_result::invalid,
            rgw_parse_byte_ranges("bytes=5-2", 0, &ranges));
  EXPECT_TRUE(ranges.empty());

  ranges.push_back(int_range(7, 9));
  EXPECT_EQ(rgw_range_parse_result::not_bytes,
            rgw_parse_byte_ranges("items=0-1", 0, &ranges));
  EXPECT_TRUE(ranges.empty());
}
