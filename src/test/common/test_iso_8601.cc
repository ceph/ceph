// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat <contact@redhat.com>
 *
 * LGPL-2.1 (see COPYING-LGPL2.1) or later
 */

#include <chrono>
#include <locale>
#include <ostream>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "common/ceph_time.h"
#include "common/iso_8601.h"

using namespace std::literals;

namespace chrono = std::chrono;

using chrono::hours;
using chrono::minutes;
using chrono::seconds;
using chrono::milliseconds;
using chrono::microseconds;
using chrono::nanoseconds;
using chrono::time_point_cast;

using ceph::from_iso_8601;
using ceph::iso_8601_format;
using ceph::iso_8601_mode;
using ceph::real_clock;
using ceph::real_time;
using ceph::to_iso_8601;

// gtest prints time_points as byte dumps. Give it something a human
// can read when an assertion fails. Printing the raw count rather
// than calling to_iso_8601 keeps the formatter under test out of the
// diagnostic path.
namespace ceph {
void PrintTo(const real_time& t, std::ostream* os)
{
  *os << "real_time{" << t.time_since_epoch().count() << "ns}";
}
} // namespace ceph

namespace {

// Build a time with every field distinct, no zeros, no palindromes,
// and a year past 2262. Why 2262: a signed 64-bit count of
// nanoseconds since the epoch overflows at 2262-04-11T23:47:16Z, so
// any rewrite that casts real_time (unsigned nanoseconds) to
// std::chrono::sys_time<std::chrono::nanoseconds> before formatting
// will produce garbage here while still passing every test set in
// the 20th or 21st century.
//
// 2345-06-07T08:09:10.123456789Z
//   = 11,847,456,550 s since the epoch, per `date -u -d 2345-06-07T08:09:10Z +%s`.
const real_time distinct{11'847'456'550s + 123'456'789ns};
constexpr auto distinct_str = "2345-06-07T08:09:10.123456789Z"sv;

// Something in living memory with all fields distinct and single-digit
// month/day/hour/second, so padding is exercised.
// 2001-03-09T19:36:07.800000000Z = 984166567 s
const real_time y2001{984'166'567s + 800'000'000ns};

} // namespace

// ---------------------------------------------------------------------
// to_iso_8601
// ---------------------------------------------------------------------

TEST(to_iso_8601, epoch) {
  const auto epoch = real_clock::from_time_t(0);

  EXPECT_EQ("1970", to_iso_8601(epoch, iso_8601_format::Y));
  EXPECT_EQ("1970-01", to_iso_8601(epoch, iso_8601_format::YM));
  EXPECT_EQ("1970-01-01", to_iso_8601(epoch, iso_8601_format::YMD));
  EXPECT_EQ("1970-01-01T00Z", to_iso_8601(epoch, iso_8601_format::YMDh));
  EXPECT_EQ("1970-01-01T00:00Z", to_iso_8601(epoch, iso_8601_format::YMDhm));
  EXPECT_EQ("1970-01-01T00:00:00Z",
            to_iso_8601(epoch, iso_8601_format::YMDhms));
  EXPECT_EQ("1970-01-01T00:00:00.000Z",
            to_iso_8601(epoch, iso_8601_format::YMDhms_ms));
  EXPECT_EQ("1970-01-01T00:00:00.000000Z",
            to_iso_8601(epoch, iso_8601_format::YMDhms_us));
  EXPECT_EQ("1970-01-01T00:00:00.000000000Z",
            to_iso_8601(epoch, iso_8601_format::YMDhms_ns));

  EXPECT_EQ("1970", to_iso_8601(epoch, iso_8601_format::BasicY));
  EXPECT_EQ("19700101", to_iso_8601(epoch, iso_8601_format::BasicYMD));
  EXPECT_EQ("19700101T00Z", to_iso_8601(epoch, iso_8601_format::BasicYMDh));
  EXPECT_EQ("19700101T0000Z", to_iso_8601(epoch, iso_8601_format::BasicYMDhm));
  EXPECT_EQ("19700101T000000Z",
            to_iso_8601(epoch, iso_8601_format::BasicYMDhms));
  EXPECT_EQ("19700101T000000.000Z",
            to_iso_8601(epoch, iso_8601_format::BasicYMDhms_ms));
  EXPECT_EQ("19700101T000000.000000Z",
            to_iso_8601(epoch, iso_8601_format::BasicYMDhms_us));
  EXPECT_EQ("19700101T000000.000000000Z",
            to_iso_8601(epoch, iso_8601_format::BasicYMDhms_ns));
}

// The epoch is all zeros, so it cannot tell a transposed month and day
// apart, nor detect a missing +1 on tm_mon, nor a dropped field. This
// can.
TEST(to_iso_8601, distinct_fields) {
  EXPECT_EQ("2345", to_iso_8601(distinct, iso_8601_format::Y));
  EXPECT_EQ("2345-06", to_iso_8601(distinct, iso_8601_format::YM));
  EXPECT_EQ("2345-06-07", to_iso_8601(distinct, iso_8601_format::YMD));
  EXPECT_EQ("2345-06-07T08Z", to_iso_8601(distinct, iso_8601_format::YMDh));
  EXPECT_EQ("2345-06-07T08:09Z",
            to_iso_8601(distinct, iso_8601_format::YMDhm));
  EXPECT_EQ("2345-06-07T08:09:10Z",
            to_iso_8601(distinct, iso_8601_format::YMDhms));
  EXPECT_EQ("2345-06-07T08:09:10.123Z",
            to_iso_8601(distinct, iso_8601_format::YMDhms_ms));
  EXPECT_EQ("2345-06-07T08:09:10.123456Z",
            to_iso_8601(distinct, iso_8601_format::YMDhms_us));
  EXPECT_EQ(distinct_str, to_iso_8601(distinct, iso_8601_format::YMDhms_ns));

  // Default format is full precision, extended.
  EXPECT_EQ(distinct_str, to_iso_8601(distinct));

  EXPECT_EQ("2001-03-09T19:36:07.800000000Z", to_iso_8601(y2001));
  EXPECT_EQ("2001-03-09T19:36:07.800000Z",
            to_iso_8601(y2001, iso_8601_format::YMDhms_us));
  EXPECT_EQ("2001-03-09T19:36:07.800Z",
            to_iso_8601(y2001, iso_8601_format::YMDhms_ms));
  EXPECT_EQ("2001-03-09T19:36:07Z",
            to_iso_8601(y2001, iso_8601_format::YMDhms));
}

// The basic (undelimited) format. The SigV4 fixture is from the AWS
// documentation:
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
// 2013-05-24T00:00:00Z = 1369353600
TEST(to_iso_8601, basic_distinct_fields) {
  const auto sigv4 = real_clock::from_time_t(1'369'353'600);
  EXPECT_EQ("20130524T000000Z",
            to_iso_8601(sigv4, iso_8601_format::BasicYMDhms));

  EXPECT_EQ("2345", to_iso_8601(distinct, iso_8601_format::BasicY));
  EXPECT_EQ("23450607", to_iso_8601(distinct, iso_8601_format::BasicYMD));
  EXPECT_EQ("23450607T08Z", to_iso_8601(distinct, iso_8601_format::BasicYMDh));
  EXPECT_EQ("23450607T0809Z",
            to_iso_8601(distinct, iso_8601_format::BasicYMDhm));
  EXPECT_EQ("23450607T080910Z",
            to_iso_8601(distinct, iso_8601_format::BasicYMDhms));
  EXPECT_EQ("23450607T080910.123Z",
            to_iso_8601(distinct, iso_8601_format::BasicYMDhms_ms));
  EXPECT_EQ("23450607T080910.123456Z",
            to_iso_8601(distinct, iso_8601_format::BasicYMDhms_us));
  EXPECT_EQ("23450607T080910.123456789Z",
            to_iso_8601(distinct, iso_8601_format::BasicYMDhms_ns));

  EXPECT_EQ("20010309T193607Z",
            to_iso_8601(y2001, iso_8601_format::BasicYMDhms));
  EXPECT_EQ("20010309T193607.800Z",
            to_iso_8601(y2001, iso_8601_format::BasicYMDhms_ms));
  EXPECT_EQ("20010309T193607.800000Z",
            to_iso_8601(y2001, iso_8601_format::BasicYMDhms_us));
  EXPECT_EQ("20010309T193607.800000000Z",
            to_iso_8601(y2001, iso_8601_format::BasicYMDhms_ns));
}

// Two-digit values in every field, so a formatter that pads correctly
// for single digits but mangles double digits (or vice versa) is caught.
TEST(to_iso_8601, two_digit_fields) {
  // 1999-12-31T23:59:58.999999999Z = 946684798 s
  const real_time t{946'684'798s + 999'999'999ns};
  EXPECT_EQ("1999", to_iso_8601(t, iso_8601_format::Y));
  EXPECT_EQ("1999-12", to_iso_8601(t, iso_8601_format::YM));
  EXPECT_EQ("1999-12-31", to_iso_8601(t, iso_8601_format::YMD));
  EXPECT_EQ("1999-12-31T23Z", to_iso_8601(t, iso_8601_format::YMDh));
  EXPECT_EQ("1999-12-31T23:59Z", to_iso_8601(t, iso_8601_format::YMDhm));
  EXPECT_EQ("1999-12-31T23:59:58Z", to_iso_8601(t, iso_8601_format::YMDhms));
  EXPECT_EQ("1999-12-31T23:59:58.999Z",
            to_iso_8601(t, iso_8601_format::YMDhms_ms));
  EXPECT_EQ("1999-12-31T23:59:58.999999Z",
            to_iso_8601(t, iso_8601_format::YMDhms_us));
  EXPECT_EQ("1999-12-31T23:59:58.999999999Z",
            to_iso_8601(t, iso_8601_format::YMDhms_ns));

  EXPECT_EQ("19991231", to_iso_8601(t, iso_8601_format::BasicYMD));
  EXPECT_EQ("19991231T23Z", to_iso_8601(t, iso_8601_format::BasicYMDh));
  EXPECT_EQ("19991231T2359Z", to_iso_8601(t, iso_8601_format::BasicYMDhm));
  EXPECT_EQ("19991231T235958Z", to_iso_8601(t, iso_8601_format::BasicYMDhms));
  EXPECT_EQ("19991231T235958.999Z",
            to_iso_8601(t, iso_8601_format::BasicYMDhms_ms));
  EXPECT_EQ("19991231T235958.999999Z",
            to_iso_8601(t, iso_8601_format::BasicYMDhms_us));
  EXPECT_EQ("19991231T235958.999999999Z",
            to_iso_8601(t, iso_8601_format::BasicYMDhms_ns));
}

// Reduced precision must truncate toward the epoch, never round.
// 23:59:59.999999999 printed as seconds is 23:59:59, not 00:00:00 of
// the next day, and printed as milliseconds is .999, not 1.000. A
// rewrite using chrono::round, or fmt's %S on a time_point that has
// been rounded rather than floored, fails here.
TEST(to_iso_8601, truncates_never_rounds) {
  // 1999-12-31T23:59:59.999999999Z
  const real_time t{946'684'799s + 999'999'999ns};
  EXPECT_EQ("1999", to_iso_8601(t, iso_8601_format::Y));
  EXPECT_EQ("1999-12", to_iso_8601(t, iso_8601_format::YM));
  EXPECT_EQ("1999-12-31", to_iso_8601(t, iso_8601_format::YMD));
  EXPECT_EQ("1999-12-31T23Z", to_iso_8601(t, iso_8601_format::YMDh));
  EXPECT_EQ("1999-12-31T23:59Z", to_iso_8601(t, iso_8601_format::YMDhm));
  EXPECT_EQ("1999-12-31T23:59:59Z", to_iso_8601(t, iso_8601_format::YMDhms));
  EXPECT_EQ("1999-12-31T23:59:59.999Z",
            to_iso_8601(t, iso_8601_format::YMDhms_ms));
  EXPECT_EQ("1999-12-31T23:59:59.999999Z",
            to_iso_8601(t, iso_8601_format::YMDhms_us));
  EXPECT_EQ("1999-12-31T23:59:59.999999999Z",
            to_iso_8601(t, iso_8601_format::YMDhms_ns));
  EXPECT_EQ("19991231T235959.999Z",
            to_iso_8601(t, iso_8601_format::BasicYMDhms_ms));
  EXPECT_EQ("19991231T235959.999999Z",
            to_iso_8601(t, iso_8601_format::BasicYMDhms_us));

  // Half a unit up at each precision, still truncates.
  const real_time half_s{946'684'799s + 500'000'000ns};
  EXPECT_EQ("1999-12-31T23:59:59Z",
            to_iso_8601(half_s, iso_8601_format::YMDhms));
  EXPECT_EQ("1999-12-31T23:59:59.500Z",
            to_iso_8601(half_s, iso_8601_format::YMDhms_ms));
  EXPECT_EQ("1999-12-31T23:59:59.500000Z",
            to_iso_8601(half_s, iso_8601_format::YMDhms_us));
  EXPECT_EQ("1999-12-31T23:59:59.500000000Z",
            to_iso_8601(half_s, iso_8601_format::YMDhms_ns));

  const real_time half_ms{946'684'799s + 500'000ns};
  EXPECT_EQ("1999-12-31T23:59:59.000Z",
            to_iso_8601(half_ms, iso_8601_format::YMDhms_ms));
  EXPECT_EQ("1999-12-31T23:59:59.000500Z",
            to_iso_8601(half_ms, iso_8601_format::YMDhms_us));

  const real_time half_us{946'684'799s + 500ns};
  EXPECT_EQ("1999-12-31T23:59:59.000Z",
            to_iso_8601(half_us, iso_8601_format::YMDhms_ms));
  EXPECT_EQ("1999-12-31T23:59:59.000000Z",
            to_iso_8601(half_us, iso_8601_format::YMDhms_us));
  EXPECT_EQ("1999-12-31T23:59:59.000000500Z",
            to_iso_8601(half_us, iso_8601_format::YMDhms_ns));

  // Thirty seconds past the minute still prints the minute it is in.
  const real_time half_min{946'684'770s};
  EXPECT_EQ("1999-12-31T23:59Z",
            to_iso_8601(half_min, iso_8601_format::YMDhm));
}

// Sub-second field is zero-padded on the left to its full width at
// every precision and never shortened.
TEST(to_iso_8601, subsecond_padding) {
  using enum iso_8601_format;
  const auto base = real_clock::from_time_t(984'166'567);
  const auto at = [&](auto delta, iso_8601_format f) {
    return to_iso_8601(base + delta, f);
  };

  EXPECT_EQ("2001-03-09T19:36:07.000000001Z", at(1ns, YMDhms_ns));
  EXPECT_EQ("2001-03-09T19:36:07.000000Z", at(1ns, YMDhms_us));
  EXPECT_EQ("2001-03-09T19:36:07.000Z", at(1ns, YMDhms_ms));

  EXPECT_EQ("2001-03-09T19:36:07.000000010Z", at(10ns, YMDhms_ns));

  EXPECT_EQ("2001-03-09T19:36:07.000001000Z", at(1us, YMDhms_ns));
  EXPECT_EQ("2001-03-09T19:36:07.000001Z", at(1us, YMDhms_us));
  EXPECT_EQ("2001-03-09T19:36:07.000Z", at(1us, YMDhms_ms));

  EXPECT_EQ("2001-03-09T19:36:07.001000000Z", at(1ms, YMDhms_ns));
  EXPECT_EQ("2001-03-09T19:36:07.001000Z", at(1ms, YMDhms_us));
  EXPECT_EQ("2001-03-09T19:36:07.001Z", at(1ms, YMDhms_ms));

  EXPECT_EQ("2001-03-09T19:36:07.100000000Z", at(100ms, YMDhms_ns));
  EXPECT_EQ("2001-03-09T19:36:07.100000Z", at(100ms, YMDhms_us));
  EXPECT_EQ("2001-03-09T19:36:07.100Z", at(100ms, YMDhms_ms));

  EXPECT_EQ("2001-03-09T19:36:07.999999999Z", at(999'999'999ns, YMDhms_ns));
  EXPECT_EQ("2001-03-09T19:36:07.999999Z", at(999'999'999ns, YMDhms_us));
  EXPECT_EQ("2001-03-09T19:36:07.999Z", at(999'999'999ns, YMDhms_ms));

  EXPECT_EQ("20010309T193607.000000001Z", at(1ns, BasicYMDhms_ns));
  EXPECT_EQ("20010309T193607.000001Z", at(1us, BasicYMDhms_us));
  EXPECT_EQ("20010309T193607.001Z", at(1ms, BasicYMDhms_ms));
}

TEST(to_iso_8601, calendar_edges) {
  // Leap day in a leap century year. 2000-02-29T12:00:00Z = 951825600
  EXPECT_EQ("2000-02-29", to_iso_8601(real_clock::from_time_t(951'825'600),
                                      iso_8601_format::YMD));
  // Ordinary leap year. 2024-02-29T00:00:00Z = 1709164800
  EXPECT_EQ("2024-02-29", to_iso_8601(real_clock::from_time_t(1'709'164'800),
                                      iso_8601_format::YMD));
  // 2100 is not a leap year: the day after 2100-02-28 is 03-01.
  // 2100-02-28T00:00:00Z = 4107456000
  EXPECT_EQ("2100-02-28", to_iso_8601(real_clock::from_time_t(4'107'456'000),
                                      iso_8601_format::YMD));
  EXPECT_EQ("2100-03-01",
            to_iso_8601(real_clock::from_time_t(4'107'456'000) + 24h,
                        iso_8601_format::YMD));
  // Last nanosecond of a year, first nanosecond of the next.
  EXPECT_EQ("1999-12-31T23:59:59.999999999Z",
            to_iso_8601(real_time{946'684'799s + 999'999'999ns}));
  EXPECT_EQ("2000-01-01T00:00:00.000000000Z",
            to_iso_8601(real_time{946'684'800s}));
  // 2038-01-19T03:14:08Z = 2^31: just past the 32-bit time_t cliff.
  EXPECT_EQ("2038-01-19T03:14:08Z",
            to_iso_8601(real_clock::from_time_t(2'147'483'648),
                        iso_8601_format::YMDhms));
}

// The largest value real_time can hold, and the one just past the
// signed-nanosecond cliff. Both must format exactly at every precision.
TEST(to_iso_8601, range_limits) {
  using enum iso_8601_format;
  const auto max = real_time::max();
  EXPECT_EQ("2554-07-21T23:34:33.709551615Z", to_iso_8601(max));
  EXPECT_EQ("2554-07-21T23:34:33.709551Z", to_iso_8601(max, YMDhms_us));
  EXPECT_EQ("2554-07-21T23:34:33.709Z", to_iso_8601(max, YMDhms_ms));
  EXPECT_EQ("2554-07-21T23:34:33Z", to_iso_8601(max, YMDhms));
  EXPECT_EQ("2554", to_iso_8601(max, Y));
  EXPECT_EQ("25540721T233433.709551615Z", to_iso_8601(max, BasicYMDhms_ns));
  EXPECT_EQ("25540721T233433.709551Z", to_iso_8601(max, BasicYMDhms_us));
  EXPECT_EQ("25540721T233433.709Z", to_iso_8601(max, BasicYMDhms_ms));
  EXPECT_EQ("25540721T233433Z", to_iso_8601(max, BasicYMDhms));

  // 2262-04-11T23:47:16.854775807Z is INT64_MAX nanoseconds.
  const real_time int64_max_ns{nanoseconds{9'223'372'036'854'775'807}};
  EXPECT_EQ("2262-04-11T23:47:16.854775807Z", to_iso_8601(int64_max_ns));
  EXPECT_EQ("2262-04-11T23:47:16.854775Z", to_iso_8601(int64_max_ns, YMDhms_us));
  EXPECT_EQ("2262-04-11T23:47:16.854Z", to_iso_8601(int64_max_ns, YMDhms_ms));
  // One more nanosecond.
  EXPECT_EQ("2262-04-11T23:47:16.854775808Z", to_iso_8601(int64_max_ns + 1ns));
  EXPECT_EQ("2262-04-11T23:47:16.854775Z",
            to_iso_8601(int64_max_ns + 1ns, YMDhms_us));

  EXPECT_EQ("1970-01-01T00:00:00.000000000Z", to_iso_8601(real_time::min()));
  EXPECT_EQ("1970-01-01T00:00:00.000000001Z",
            to_iso_8601(real_time::min() + 1ns));
  EXPECT_EQ("1970-01-01T00:00:00.000000Z",
            to_iso_8601(real_time::min() + 1ns, YMDhms_us));
}

// The iterator overload writes exactly the formatted text, returns the
// iterator one past it, and works with a plain char buffer as well as
// a back_inserter.
TEST(to_iso_8601, output_iterator) {
  std::string s = "prefix:";
  auto it = to_iso_8601(std::back_inserter(s), distinct, iso_8601_format::YMDhms);
  *it++ = '!';
  EXPECT_EQ("prefix:2345-06-07T08:09:10Z!", s);

  // A raw pointer. (Not the array itself: fmt's format_to has a
  // bounds-checked array overload that returns a format_to_result, not
  // an iterator.)
  char buf[40] = {};
  auto end = to_iso_8601(std::begin(buf), distinct,
                         iso_8601_format::BasicYMDhms_ms);
  EXPECT_EQ("23450607T080910.123Z"sv, std::string_view(buf, end));
  EXPECT_EQ(buf + 20, end);
  EXPECT_EQ('\0', *end);

  char ybuf[8] = {};
  EXPECT_EQ(ybuf + 4, to_iso_8601(std::begin(ybuf), distinct,
                                  iso_8601_format::Y));
  EXPECT_EQ("2345"sv, std::string_view(ybuf, 4));
}

// Output must not depend on the global locale. A grouping numpunct
// facet is installed globally (as a process would get from
// std::locale::global(std::locale("en_US.UTF-8"))) and the formatter
// must still emit "2345", not "2,345". iostream-based formatting fails
// this; fmt and std::format are locale-independent unless asked.
namespace {
struct grouping_numpunct : std::numpunct<char> {
  char do_thousands_sep() const override { return ','; }
  std::string do_grouping() const override { return "\3"; }
};

struct global_locale_guard {
  std::locale saved = std::locale();
  explicit global_locale_guard(const std::locale& l) { std::locale::global(l); }
  ~global_locale_guard() { std::locale::global(saved); }
};
} // namespace

TEST(to_iso_8601, locale_independent) {
  using enum iso_8601_format;
  global_locale_guard guard{std::locale(std::locale::classic(),
                                        new grouping_numpunct)};
  // Prove the guard actually installed a grouping locale, so a pass
  // below means something.
  {
    std::ostringstream check;
    check << 2345;
    ASSERT_EQ("2,345", check.str());
  }
  EXPECT_EQ("2345", to_iso_8601(distinct, Y));
  EXPECT_EQ(distinct_str, to_iso_8601(distinct, YMDhms_ns));
  EXPECT_EQ("2345-06-07T08:09:10.123456Z", to_iso_8601(distinct, YMDhms_us));
  EXPECT_EQ("2345-06-07T08:09:10.123Z", to_iso_8601(distinct, YMDhms_ms));
  EXPECT_EQ("23450607T080910.123456789Z", to_iso_8601(distinct, BasicYMDhms_ns));
  EXPECT_EQ("23450607T080910.123456Z", to_iso_8601(distinct, BasicYMDhms_us));
  EXPECT_EQ("23450607T080910.123Z", to_iso_8601(distinct, BasicYMDhms_ms));
}

// Formatter and parser must agree, in both directions, at every
// precision. Round-tripping alone cannot prove either side correct
// (they could share a bug), which is why the fixed-string tests above
// exist. But it does prove they share a convention.
namespace {
const real_time round_trip_fixtures[] = {
  real_time::min(), real_time::min() + 1ns, y2001, distinct,
  real_time::max() - 1ns, real_time::max()};
} // namespace

TEST(to_iso_8601, round_trip) {
  using enum iso_8601_format;
  for (const auto t : round_trip_fixtures) {
    SCOPED_TRACE(to_iso_8601(t));
    // Y and YM are covered by round_trip_date_only: chrono::years and
    // months are mean lengths, so time_point_cast is not "January 1".
    EXPECT_EQ(real_time(time_point_cast<chrono::days>(t)),
              from_iso_8601(to_iso_8601(t, YMD)));
    EXPECT_EQ(real_time(time_point_cast<hours>(t)),
              from_iso_8601(to_iso_8601(t, YMDh)));
    EXPECT_EQ(real_time(time_point_cast<minutes>(t)),
              from_iso_8601(to_iso_8601(t, YMDhm)));
    EXPECT_EQ(real_time(time_point_cast<seconds>(t)),
              from_iso_8601(to_iso_8601(t, YMDhms)));
    EXPECT_EQ(real_time(time_point_cast<milliseconds>(t)),
              from_iso_8601(to_iso_8601(t, YMDhms_ms)));
    EXPECT_EQ(real_time(time_point_cast<microseconds>(t)),
              from_iso_8601(to_iso_8601(t, YMDhms_us)));
    EXPECT_EQ(t, from_iso_8601(to_iso_8601(t, YMDhms_ns)));
  }
}

// chrono::years is 365.2425 days, so time_point_cast<years> is not
// "January 1". Do the Y/YM round trips with explicit expectations.
TEST(to_iso_8601, round_trip_date_only) {
  // 2001-01-01T00:00:00Z = 978307200, 2001-03-01T00:00:00Z = 983404800
  EXPECT_EQ(real_clock::from_time_t(978'307'200),
            from_iso_8601(to_iso_8601(y2001, iso_8601_format::Y)));
  EXPECT_EQ(real_clock::from_time_t(983'404'800),
            from_iso_8601(to_iso_8601(y2001, iso_8601_format::YM)));
  EXPECT_EQ(real_time(time_point_cast<chrono::days>(y2001)),
            from_iso_8601(to_iso_8601(y2001, iso_8601_format::YMD)));
  EXPECT_EQ(real_clock::from_time_t(978'307'200),
            from_iso_8601(to_iso_8601(y2001, iso_8601_format::BasicY),
                          iso_8601_mode::basic));
}

TEST(to_iso_8601, round_trip_now) {
  using enum iso_8601_format;
  const auto now = real_clock::now();

  EXPECT_EQ(real_time(time_point_cast<hours>(now)),
            from_iso_8601(to_iso_8601(now, YMDh)));
  EXPECT_EQ(real_time(time_point_cast<minutes>(now)),
            from_iso_8601(to_iso_8601(now, YMDhm)));
  EXPECT_EQ(real_time(time_point_cast<seconds>(now)),
            from_iso_8601(to_iso_8601(now, YMDhms)));
  EXPECT_EQ(real_time(time_point_cast<milliseconds>(now)),
            from_iso_8601(to_iso_8601(now, YMDhms_ms)));
  EXPECT_EQ(real_time(time_point_cast<microseconds>(now)),
            from_iso_8601(to_iso_8601(now, YMDhms_us)));
  EXPECT_EQ(now, from_iso_8601(to_iso_8601(now, YMDhms_ns)));
}

// What the formatter emits in basic format, the parser must read in
// basic mode, at every precision.
TEST(to_iso_8601, basic_format_round_trip) {
  using enum iso_8601_format;
  constexpr auto basic = iso_8601_mode::basic;
  const auto sigv4 = real_clock::from_time_t(1'369'353'600);
  EXPECT_EQ(sigv4, from_iso_8601(to_iso_8601(sigv4, BasicYMDhms), basic));

  for (const auto t : round_trip_fixtures) {
    SCOPED_TRACE(to_iso_8601(t, BasicYMDhms_ns));
    EXPECT_EQ(real_time(time_point_cast<chrono::days>(t)),
              from_iso_8601(to_iso_8601(t, BasicYMD), basic));
    EXPECT_EQ(real_time(time_point_cast<hours>(t)),
              from_iso_8601(to_iso_8601(t, BasicYMDh), basic));
    EXPECT_EQ(real_time(time_point_cast<minutes>(t)),
              from_iso_8601(to_iso_8601(t, BasicYMDhm), basic));
    EXPECT_EQ(real_time(time_point_cast<seconds>(t)),
              from_iso_8601(to_iso_8601(t, BasicYMDhms), basic));
    EXPECT_EQ(real_time(time_point_cast<milliseconds>(t)),
              from_iso_8601(to_iso_8601(t, BasicYMDhms_ms), basic));
    EXPECT_EQ(real_time(time_point_cast<microseconds>(t)),
              from_iso_8601(to_iso_8601(t, BasicYMDhms_us), basic));
    EXPECT_EQ(t, from_iso_8601(to_iso_8601(t, BasicYMDhms_ns), basic));
  }
}

// ---------------------------------------------------------------------
// from_iso_8601
// ---------------------------------------------------------------------

TEST(from_iso_8601, epoch) {
  const auto epoch = real_clock::from_time_t(0);

  EXPECT_EQ(epoch, from_iso_8601("1970"));
  EXPECT_EQ(epoch, from_iso_8601("1970-01"));
  EXPECT_EQ(epoch, from_iso_8601("1970-01-01"));
  EXPECT_EQ(epoch, from_iso_8601("1970-01-01T00Z"));
  EXPECT_EQ(epoch, from_iso_8601("1970-01-01T00:00Z"));
  EXPECT_EQ(epoch, from_iso_8601("1970-01-01T00:00:00Z"));
  EXPECT_EQ(epoch, from_iso_8601("1970-01-01T00:00:00.000000000Z"));
}

TEST(from_iso_8601, tz) {
  // Smallpox eradication declared: 1980-05-08T16:10+02:00 in Geneva
  const auto ref = ceph::real_clock::from_time_t(326643000);
  const auto ref_h = chrono::floor<hours>(ref);

  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T14Z"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T14-00:00"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T14+00:00"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T16+02:00"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T14-00"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T14+0000"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T16+02"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T16+0200"sv));

  EXPECT_EQ(ref, from_iso_8601("1980-05-08T14:10Z"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T14:10-00"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T14:10-00:00"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T14:10-0000"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T14:10+00"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T14:10+00:00"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T16:10+02"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T16:10+02:00"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T16:10+0200"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T12:10-02"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T12:10-02:00"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T12:10-0200"sv));

  const auto ref_5s = ref + 5s;
  EXPECT_EQ(ref_5s, from_iso_8601("1980-05-08T14:10:05Z"sv));
  EXPECT_EQ(ref_5s, from_iso_8601("1980-05-08T14:10:05-00:00"sv));
  EXPECT_EQ(ref_5s, from_iso_8601("1980-05-08T14:10:05-00"sv));
  EXPECT_EQ(ref_5s, from_iso_8601("1980-05-08T14:10:05-0000"sv));
  EXPECT_EQ(ref_5s, from_iso_8601("1980-05-08T14:10:05+00:00"sv));
  EXPECT_EQ(ref_5s, from_iso_8601("1980-05-08T14:10:05+0000"sv));
  EXPECT_EQ(ref_5s, from_iso_8601("1980-05-08T14:10:05+00"sv));
  EXPECT_EQ(ref_5s, from_iso_8601("1980-05-08T16:10:05+02:00"sv));
  EXPECT_EQ(ref_5s, from_iso_8601("1980-05-08T16:10:05+0200"sv));

  const auto ref_ns = ref + 5s + 30ns;
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030Z"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05.000000030Z"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030-00:00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030-00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030-0000"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05.000000030-00:00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030+00:00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030+00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030+0000"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05.000000030+00:00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T16:10:05,000000030+02:00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T16:10:05,000000030+02"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T16:10:05,000000030+0200"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T16:10:05.000000030+02:00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T16:10:05.000000030+02"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T16:10:05.000000030+0200"sv));

  // Offsets with a minutes component.
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T19:40+05:30"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T19:40+0530"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T10:40-03:30"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T22:55+08:45"sv));

  // Lax

  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10:05,000000030z"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08t14:10:05.000000030Z"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08t14:10:05.000000030Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08 14:10:05.000000030Z"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08 14:10:05.000000030Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10:05.000000030z"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05.000000030z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10:05.000000030"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05.000000030"sv, iso_8601_mode::lax));

  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08T14:10:05,000000030+00:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08t14:10:05,000000030+00:00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08t14:10:05,000000030+00:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08 14:10:05,000000030+00:00"sv));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08 14:10:05,000000030+00:00"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980-05-08T14"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T14"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10"sv));
  EXPECT_EQ(ref, from_iso_8601("1980-05-08T14:10"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980-05-08T14z"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08T14z"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980-05-08t16+02:00"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08t16+02:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08 16+02:00"sv));
  EXPECT_EQ(ref_h, from_iso_8601("1980-05-08 16+02:00"sv, iso_8601_mode::lax));

  // Lax is a superset, never a different language: a lowercase 'z'
  // alone does not make a 'T' optional, and vice versa, so the
  // combinations are also accepted.
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08t14:10:05.000000030z"sv, iso_8601_mode::lax));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08 14:10:05.000000030z"sv, iso_8601_mode::lax));
  EXPECT_EQ(ref_ns, from_iso_8601("1980-05-08 14:10:05.000000030"sv, iso_8601_mode::lax));

  // Garbage

  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10+24:00"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10+24:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10+15:00"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10+15:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-24:00"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-24:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-13:00"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-13:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08+02:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T+02:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14Zgarbage"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14Zgarbage"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14-00:00garbage"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14-00:00garbage"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14+00:00garbage"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14+00:00garbage"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T16+02:00garbage"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T16+02:00garbage"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-00:"sv));
  EXPECT_FALSE(from_iso_8601("1980-05-08T14:10-00:"sv, iso_8601_mode::lax));

  EXPECT_FALSE(from_iso_8601("1980T14:10Z"sv));
  EXPECT_FALSE(from_iso_8601("1980T14:10Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1980-05T14:10Z"sv));
  EXPECT_FALSE(from_iso_8601("1980-05T14:10Z"sv, iso_8601_mode::lax));
}

// The offset boundaries are inclusive: -12:00 and +14:00 are real
// zones (Baker Island, Line Islands). One minute past either is not.
TEST(from_iso_8601, offset_boundaries) {
  // 2001-03-09T19:36:07Z
  const auto ref = real_clock::from_time_t(984'166'567);
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T07:36:07-12:00"sv));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T07:36:07-1200"sv));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T07:36:07-12"sv));
  EXPECT_EQ(ref, from_iso_8601("2001-03-10T09:36:07+14:00"sv));
  EXPECT_EQ(ref, from_iso_8601("2001-03-10T09:36:07+1400"sv));
  EXPECT_EQ(ref, from_iso_8601("2001-03-10T09:36:07+14"sv));

  EXPECT_FALSE(from_iso_8601("2001-03-09T07:35:07-12:01"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T07:35:07-1201"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-10T09:37:07+14:01"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-10T09:37:07+1401"sv));

  // Offset minutes field is 00-59.
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+05:60"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+0560"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+05:99"sv));

  // Malformed offsets.
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07-"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+5"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+5:00"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+05:0"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+050"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+05:00:00"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+0a"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07Z+00:00"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+00:00Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07 Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07 +00:00"sv, iso_8601_mode::lax));
}

// An offset can move the instant across a day, month, or year
// boundary. Every existing tz test stays within one UTC day.
TEST(from_iso_8601, offset_crosses_boundaries) {
  // 1980-05-08T23:10Z = 326675400
  const auto late = real_clock::from_time_t(326'675'400);
  EXPECT_EQ(late, from_iso_8601("1980-05-09T01:10+02:00"sv));
  EXPECT_EQ(late, from_iso_8601("1980-05-08T23:10Z"sv));
  // 1980-05-09T01:10Z = 326682600
  const auto early = real_clock::from_time_t(326'682'600);
  EXPECT_EQ(early, from_iso_8601("1980-05-08T23:10-02:00"sv));

  // Across a year. 2000-12-31T23:00Z = 978303600
  const auto nye = real_clock::from_time_t(978'303'600);
  EXPECT_EQ(nye, from_iso_8601("2001-01-01T01:00+02:00"sv));
  // 2001-01-01T01:00Z = 978310800
  EXPECT_EQ(real_clock::from_time_t(978'310'800),
            from_iso_8601("2000-12-31T23:00-02:00"sv));

  // Across a leap day. 2000-02-29T23:00Z = 951865200
  EXPECT_EQ(real_clock::from_time_t(951'865'200),
            from_iso_8601("2000-03-01T01:00+02:00"sv));
}

TEST(from_iso_8601, epoch_timezone)
{
  // Pre-epoch
  EXPECT_FALSE(from_iso_8601("1970-01-01T00:00+05:00"sv));
  EXPECT_FALSE(from_iso_8601("1970-01-01T00:00+05:00"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("1970-01-01T00:00:00.000000001+00:01"sv));
  EXPECT_FALSE(from_iso_8601("1970-01-01T04:59:59.999999999+05:00"sv));
  EXPECT_EQ(real_time(0s), from_iso_8601("1970-01-01T05:00+05:00"sv));
  EXPECT_EQ(real_time(0s), from_iso_8601("1970-01-01T00:00-00:00"sv));
  EXPECT_FALSE(from_iso_8601("1969"sv));
  EXPECT_FALSE(from_iso_8601("1969-12-31"sv));
  EXPECT_FALSE(from_iso_8601("1969-12-31T23:59:59Z"sv));
  EXPECT_FALSE(from_iso_8601("1969-12-31T23:59:59.999999999Z"sv));
  // The bounds apply to the instant, not the written local date.
  // These are 1970-01-01T01:00:00Z and 1970-01-01T00:00:00Z, both
  // representable, written with a negative offset so the local date
  // falls in 1969.
  EXPECT_EQ(real_time(1h), from_iso_8601("1969-12-31T23:00:00-02:00"sv));
  EXPECT_EQ(real_time(0s), from_iso_8601("1969-12-31T22:00:00-02:00"sv));
  EXPECT_EQ(real_time(0s), from_iso_8601("1969-12-31T12:00-12:00"sv));
  // One nanosecond earlier is before the epoch and must still fail.
  EXPECT_FALSE(from_iso_8601("1969-12-31T21:59:59.999999999-02:00"sv));
  EXPECT_FALSE(from_iso_8601("0000"sv));
  EXPECT_FALSE(from_iso_8601("0001-01-01T00:00:00Z"sv));

  // Near the end of the epoch
  EXPECT_TRUE(from_iso_8601("2553-12-31T23:59:59.999999999-12:00"sv));
  EXPECT_TRUE(from_iso_8601("2554-01-01"sv));
  EXPECT_EQ(real_time::max(),
            from_iso_8601("2554-07-21T23:34:33.709551615Z"));
  // real_time::max() written with a positive offset, so the local
  // date is 07-22. The instant is in range, so this must parse.
  EXPECT_EQ(real_time::max(),
            from_iso_8601("2554-07-22T01:34:33.709551615+02:00"));
  EXPECT_EQ(real_time::max(),
            from_iso_8601("2554-07-22T13:34:33.709551615+14:00"));
  // Same instant, offset keeps the local date in range.
  EXPECT_EQ(real_time::max(),
            from_iso_8601("2554-07-21T21:34:33.709551615-02:00"));
  // One nanosecond later, however written, must fail.
  EXPECT_FALSE(from_iso_8601("2554-07-22T01:34:33.709551616+02:00"));
  EXPECT_FALSE(from_iso_8601("2554-07-21T23:34:33.709551616Z"));
  EXPECT_FALSE(from_iso_8601("2554-07-21T23:34:34Z"));
  EXPECT_FALSE(from_iso_8601("2554-07-21T23:35Z"));
  EXPECT_FALSE(from_iso_8601("2554-07-22"));
  EXPECT_FALSE(from_iso_8601("2554-07-21T23:34:33.709551615-00:01"));
  EXPECT_FALSE(from_iso_8601("2554-08"));
  EXPECT_FALSE(from_iso_8601("2555"sv));
  EXPECT_FALSE(from_iso_8601("9999-12-31T23:59:59Z"sv));
  EXPECT_FALSE(from_iso_8601("99999-12-31T23:59:59Z"sv));
}

TEST(from_iso_8601, distinct_fields) {
  const auto ref = y2001;
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000+00"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000+00:00"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000+0000"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000-00"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000-00:00"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000-0000"));

  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.800000000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.800000000+00"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.800000000+00:00"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.800000000+0000"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.800000000-00"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.800000000-00:00"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.800000000-0000"));

  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,80000000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,8000000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,80000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,8000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,80Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,8Z"));

  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.80000000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.8000000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.800000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.80000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.8000Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.800Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.80Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07.8Z"));

  EXPECT_EQ(ref, from_iso_8601("2001-03-10T05:06:07.80000000+09:30"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-10T05:06:07.8+09:30"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-10T05:06:07.80000000+0930"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-10T05:06:07.8+0930"));

  EXPECT_EQ(ref, from_iso_8601("2001-03-10T05:06:07.80000000+09:30", iso_8601_mode::lax));
  EXPECT_EQ(ref, from_iso_8601("2001-03-10t05:06:07.80000000+09:30", iso_8601_mode::lax));
  EXPECT_EQ(ref, from_iso_8601("2001-03-10 05:06:07.80000000+09:30", iso_8601_mode::lax));

  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000Z", iso_8601_mode::lax));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000z", iso_8601_mode::lax));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000", iso_8601_mode::lax));

  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,8Z", iso_8601_mode::lax));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,8z", iso_8601_mode::lax));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,8", iso_8601_mode::lax));

  // Rinderpest eradication declared in 2011-06-28
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000920110628Z"));
  EXPECT_EQ(ref, from_iso_8601("2001-03-09T19:36:07,800000000920110628Z", iso_8601_mode::lax));

  // Far past 2262, with every field distinct.
  EXPECT_EQ(distinct, from_iso_8601(distinct_str));
  EXPECT_EQ(distinct, from_iso_8601("2345-06-07T10:09:10.123456789+02:00"sv));
}

// Each fractional digit means what its position says it means: the
// parser must scale by position, not by count.
TEST(from_iso_8601, subsecond_positions) {
  const auto base = real_clock::from_time_t(984'166'567);
  EXPECT_EQ(base + 1ns, from_iso_8601("2001-03-09T19:36:07.000000001Z"));
  EXPECT_EQ(base + 10ns, from_iso_8601("2001-03-09T19:36:07.00000001Z"));
  EXPECT_EQ(base + 100ns, from_iso_8601("2001-03-09T19:36:07.0000001Z"));
  EXPECT_EQ(base + 1us, from_iso_8601("2001-03-09T19:36:07.000001Z"));
  EXPECT_EQ(base + 10us, from_iso_8601("2001-03-09T19:36:07.00001Z"));
  EXPECT_EQ(base + 100us, from_iso_8601("2001-03-09T19:36:07.0001Z"));
  EXPECT_EQ(base + 1ms, from_iso_8601("2001-03-09T19:36:07.001Z"));
  EXPECT_EQ(base + 10ms, from_iso_8601("2001-03-09T19:36:07.01Z"));
  EXPECT_EQ(base + 100ms, from_iso_8601("2001-03-09T19:36:07.1Z"));
  EXPECT_EQ(base + 123'456'789ns,
            from_iso_8601("2001-03-09T19:36:07.123456789Z"));
  // Zero-valued fraction of any length.
  EXPECT_EQ(base, from_iso_8601("2001-03-09T19:36:07.0Z"));
  EXPECT_EQ(base, from_iso_8601("2001-03-09T19:36:07.000000000Z"));
  EXPECT_EQ(base, from_iso_8601("2001-03-09T19:36:07.000000000000Z"));
  // Truncation beyond nine digits, not rounding.
  EXPECT_EQ(base + 999'999'999ns,
            from_iso_8601("2001-03-09T19:36:07.9999999999Z"));
  EXPECT_EQ(base + 123'456'789ns,
            from_iso_8601("2001-03-09T19:36:07.1234567899Z"));

  // A separator with no digits is not a fraction.
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07.Z"));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07,Z"));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07."sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07.+00:00"));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07.abcZ"));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07.1.2Z"));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07.1,2Z"));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07.-1Z"));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07.+1Z"));
  // ISO 8601 allows fractional hours and minutes. We do not.
  EXPECT_FALSE(from_iso_8601("2001-03-09T19.5Z"));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36.5Z"));
}

// Invalid calendar dates and out-of-range time fields.
TEST(from_iso_8601, calendar_validation) {
  // Valid leap days.
  EXPECT_EQ(real_clock::from_time_t(951'782'400),
            from_iso_8601("2000-02-29"sv));
  EXPECT_EQ(real_clock::from_time_t(1'709'164'800),
            from_iso_8601("2024-02-29"sv));
  // Century rule: 2100 is not a leap year.
  EXPECT_FALSE(from_iso_8601("2100-02-29"sv));
  EXPECT_FALSE(from_iso_8601("2100-02-29T00:00:00Z"sv));
  // Ordinary non-leap year.
  EXPECT_FALSE(from_iso_8601("2001-02-29"sv));
  EXPECT_FALSE(from_iso_8601("2001-02-30"sv));
  // 30-day months.
  EXPECT_FALSE(from_iso_8601("2001-04-31"sv));
  EXPECT_FALSE(from_iso_8601("2001-06-31"sv));
  EXPECT_FALSE(from_iso_8601("2001-09-31"sv));
  EXPECT_FALSE(from_iso_8601("2001-11-31"sv));
  EXPECT_TRUE(from_iso_8601("2001-04-30"sv));
  EXPECT_TRUE(from_iso_8601("2001-12-31"sv));
  // Zero and overflow.
  EXPECT_FALSE(from_iso_8601("2001-00-01"sv));
  EXPECT_FALSE(from_iso_8601("2001-13-01"sv));
  EXPECT_FALSE(from_iso_8601("2001-01-00"sv));
  EXPECT_FALSE(from_iso_8601("2001-01-32"sv));
  EXPECT_FALSE(from_iso_8601("2001-00"sv));
  EXPECT_FALSE(from_iso_8601("2001-13"sv));
  EXPECT_FALSE(from_iso_8601("2001-99-99"sv));

  // Time fields.
  EXPECT_FALSE(from_iso_8601("2001-03-09T24:00:00Z"sv)); // ISO allows, we don't
  EXPECT_FALSE(from_iso_8601("2001-03-09T24Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T25:00:00Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T99:00:00Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:60:00Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:99:00Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:60Z"sv)); // leap second
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:61Z"sv));
  EXPECT_TRUE(from_iso_8601("2001-03-09T23:59:59Z"sv));
  EXPECT_TRUE(from_iso_8601("2001-03-09T00:00:00Z"sv));
}

// Field widths are exact. Fewer digits is not "lenient", more digits
// is not "a bigger number".
TEST(from_iso_8601, field_widths) {
  EXPECT_FALSE(from_iso_8601("201-03-09"sv));
  EXPECT_FALSE(from_iso_8601("20011-03-09"sv));
  EXPECT_FALSE(from_iso_8601("2001-3-09"sv));
  EXPECT_FALSE(from_iso_8601("2001-003-09"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-9"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-009"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T9:36:07Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T019:36:07Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:6:07Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:036:07Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:7Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:007Z"sv));
  // Same in lax mode: lax relaxes delimiters, not widths.
  EXPECT_FALSE(from_iso_8601("2001-3-9 9:6:7"sv, iso_8601_mode::lax));

  // Signs and whitespace inside a field are not digits.
  EXPECT_FALSE(from_iso_8601("+2001-03-09"sv));
  EXPECT_FALSE(from_iso_8601("-2001-03-09"sv));
  EXPECT_FALSE(from_iso_8601("2001-+3-09"sv));
  EXPECT_FALSE(from_iso_8601("2001- 3-09"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T 9:36:07Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T1 :36:07Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:0 Z"sv, iso_8601_mode::lax));
}

// Structural garbage: wrong delimiters, missing pieces, trailing and
// leading junk, empty input.
TEST(from_iso_8601, structure) {
  EXPECT_FALSE(from_iso_8601(""sv));
  EXPECT_FALSE(from_iso_8601(""sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601(" "sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("T"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("-"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09TZ"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19::07Z"sv));
  EXPECT_FALSE(from_iso_8601("2001--03-09"sv));
  EXPECT_FALSE(from_iso_8601("2001-03--09"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09TT19Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09ZT19Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09Z"sv)); // date-only carries no zone
  EXPECT_FALSE(from_iso_8601("2001-03-09Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03Z"sv));
  EXPECT_FALSE(from_iso_8601("2001Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09+00:00"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07ZZ"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07zZ"sv, iso_8601_mode::lax));

  // Wrong delimiters.
  EXPECT_FALSE(from_iso_8601("2001/03/09"sv));
  EXPECT_FALSE(from_iso_8601("2001/03/09"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001.03.09"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19.36.07Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19-36-07Z"sv));
  EXPECT_FALSE(from_iso_8601("2001:03:09T19:36:07Z"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09_19:36:07Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09\t19:36:07Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09  19:36:07Z"sv, iso_8601_mode::lax));

  // Leading and trailing whitespace.
  EXPECT_FALSE(from_iso_8601(" 2001-03-09T19:36:07Z"sv));
  EXPECT_FALSE(from_iso_8601(" 2001-03-09T19:36:07Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07Z "sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07Z "sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07Z\n"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07\n"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09 "sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("2001 "sv, iso_8601_mode::lax));

  // Embedded NUL is just another non-digit.
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07Z\0"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07\0Z"sv));
  EXPECT_FALSE(from_iso_8601("20\0001-03-09"sv));

  // Non-ASCII digits and lookalikes.
  EXPECT_FALSE(from_iso_8601("２００１-03-09"sv));
  EXPECT_FALSE(from_iso_8601("2001‐03‐09"sv)); // U+2010 hyphen
  EXPECT_FALSE(from_iso_8601("2001-03-09Ｔ19:36:07Z"sv, iso_8601_mode::lax));

  // Other ISO 8601 forms we deliberately do not accept.
  EXPECT_FALSE(from_iso_8601("2001-068"sv));          // ordinal date
  EXPECT_FALSE(from_iso_8601("2001-W10-5"sv));        // week date
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07Z/P1D"sv)); // interval
  EXPECT_FALSE(from_iso_8601("P1D"sv));                // duration
  // Basic and mixed forms are covered by the basic_* and *_mixed
  // tests below.
}

// Reduced precision in strict mode requires a zone designator; in lax
// mode an unterminated time is UTC. Date-only forms never take one.
TEST(from_iso_8601, reduced_precision) {
  const auto day = real_clock::from_time_t(984'096'000); // 2001-03-09
  const auto hour = real_clock::from_time_t(984'164'400); // T19Z
  const auto minute = real_clock::from_time_t(984'166'560); // T19:36Z
  const auto second = real_clock::from_time_t(984'166'567); // T19:36:07Z

  EXPECT_EQ(day, from_iso_8601("2001-03-09"sv));
  EXPECT_EQ(hour, from_iso_8601("2001-03-09T19Z"sv));
  EXPECT_EQ(minute, from_iso_8601("2001-03-09T19:36Z"sv));
  EXPECT_EQ(second, from_iso_8601("2001-03-09T19:36:07Z"sv));
  EXPECT_EQ(second, from_iso_8601("2001-03-09T19:36:07.0Z"sv));

  EXPECT_FALSE(from_iso_8601("2001-03-09T19"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07.8"sv));

  EXPECT_EQ(day, from_iso_8601("2001-03-09"sv, iso_8601_mode::lax));
  EXPECT_EQ(hour, from_iso_8601("2001-03-09T19"sv, iso_8601_mode::lax));
  EXPECT_EQ(minute, from_iso_8601("2001-03-09T19:36"sv, iso_8601_mode::lax));
  EXPECT_EQ(second, from_iso_8601("2001-03-09T19:36:07"sv, iso_8601_mode::lax));
  EXPECT_EQ(hour, from_iso_8601("2001-03-09 19"sv, iso_8601_mode::lax));
  EXPECT_EQ(minute, from_iso_8601("2001-03-09 19:36"sv, iso_8601_mode::lax));
  EXPECT_EQ(second, from_iso_8601("2001-03-09 19:36:07"sv, iso_8601_mode::lax));

  // Offsets on reduced precision.
  EXPECT_EQ(hour, from_iso_8601("2001-03-09T21+02"sv));
  EXPECT_EQ(hour, from_iso_8601("2001-03-09T21+02:00"sv));
  EXPECT_EQ(minute, from_iso_8601("2001-03-09T21:36+02:00"sv));
  EXPECT_EQ(minute, from_iso_8601("2001-03-10T01:06+05:30"sv));
}

// All modes are valid arguments; the default is strict.
TEST(from_iso_8601, default_mode_is_strict) {
  EXPECT_EQ(from_iso_8601("2001-03-09T19:36:07"sv, iso_8601_mode::strict),
            from_iso_8601("2001-03-09T19:36:07"sv));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07"sv, iso_8601_mode::strict));
  EXPECT_TRUE(from_iso_8601("2001-03-09T19:36:07"sv, iso_8601_mode::lax));
  EXPECT_EQ(from_iso_8601("2001-03-09T19:36:07Z"sv, iso_8601_mode::strict),
            from_iso_8601("2001-03-09T19:36:07Z"sv, iso_8601_mode::lax));
  EXPECT_FALSE(from_iso_8601("20010309T193607Z"sv));
  EXPECT_TRUE(from_iso_8601("20010309T193607Z"sv, iso_8601_mode::basic));
}

// ---------------------------------------------------------------------
// from_iso_8601, basic format
//
// ISO 8601 basic format drops the '-' and ':' delimiters. The ladder
// of allowed precisions is
//
//     YYYY
//     YYYYMMDD
//     YYYYMMDDThh
//     YYYYMMDDThhmm
//     YYYYMMDDThhmmss
//     YYYYMMDDThhmmss.n+      (',' also accepted, as in extended)
//
// each time form followed by 'Z', ±hh or ±hhmm. There is no basic
// year-month: the standard requires 'YYYY-MM' even in basic documents
// because 'YYYYMM' collided with the (since withdrawn) truncated
// 'YYMMDD'. The 'T' is mandatory. Basic mode is as strict as strict
// mode: no lowercase, no space, no unterminated times.
// ---------------------------------------------------------------------

namespace {
constexpr auto basic = iso_8601_mode::basic;
constexpr auto lax = iso_8601_mode::lax;
} // namespace

TEST(from_iso_8601_basic, conformance) {
  const auto epoch = real_clock::from_time_t(0);
  EXPECT_EQ(epoch, from_iso_8601("1970"sv, basic));
  EXPECT_EQ(epoch, from_iso_8601("19700101"sv, basic));
  EXPECT_EQ(epoch, from_iso_8601("19700101T00Z"sv, basic));
  EXPECT_EQ(epoch, from_iso_8601("19700101T0000Z"sv, basic));
  EXPECT_EQ(epoch, from_iso_8601("19700101T000000Z"sv, basic));
  EXPECT_EQ(epoch, from_iso_8601("19700101T000000.000000000Z"sv, basic));
  EXPECT_EQ(epoch, from_iso_8601("19700101T000000,000000000Z"sv, basic));

  // The precision ladder with every field distinct.
  const auto day = real_clock::from_time_t(984'096'000); // 2001-03-09
  const auto hour = real_clock::from_time_t(984'164'400); // T19Z
  const auto minute = real_clock::from_time_t(984'166'560); // T19:36Z
  const auto second = real_clock::from_time_t(984'166'567); // T19:36:07Z
  EXPECT_EQ(real_clock::from_time_t(978'307'200), from_iso_8601("2001"sv, basic));
  EXPECT_EQ(day, from_iso_8601("20010309"sv, basic));
  EXPECT_EQ(hour, from_iso_8601("20010309T19Z"sv, basic));
  EXPECT_EQ(minute, from_iso_8601("20010309T1936Z"sv, basic));
  EXPECT_EQ(second, from_iso_8601("20010309T193607Z"sv, basic));
  EXPECT_EQ(second, from_iso_8601("20010309T193607.0Z"sv, basic));
  EXPECT_EQ(y2001, from_iso_8601("20010309T193607.8Z"sv, basic));
  EXPECT_EQ(y2001, from_iso_8601("20010309T193607,8Z"sv, basic));
  EXPECT_EQ(y2001, from_iso_8601("20010309T193607.800000000Z"sv, basic));
  EXPECT_EQ(y2001, from_iso_8601("20010309T193607.800000000920110628Z"sv, basic));

  // Past 2262, and the SigV4 documentation fixture.
  EXPECT_EQ(distinct, from_iso_8601("23450607T080910.123456789Z"sv, basic));
  EXPECT_EQ(real_clock::from_time_t(1'369'353'600),
            from_iso_8601("20130524T000000Z"sv, basic));

  // Fractional digits scale by position.
  const auto base = real_clock::from_time_t(984'166'567);
  EXPECT_EQ(base + 1ns, from_iso_8601("20010309T193607.000000001Z"sv, basic));
  EXPECT_EQ(base + 1us, from_iso_8601("20010309T193607.000001Z"sv, basic));
  EXPECT_EQ(base + 1ms, from_iso_8601("20010309T193607.001Z"sv, basic));
  EXPECT_EQ(base + 100ms, from_iso_8601("20010309T193607.1Z"sv, basic));
  EXPECT_EQ(base + 999'999'999ns,
            from_iso_8601("20010309T193607.9999999999Z"sv, basic));
}

TEST(from_iso_8601_basic, offsets) {
  // Smallpox eradication declared: 1980-05-08T16:10+02:00 in Geneva
  const auto ref = real_clock::from_time_t(326643000);
  const auto ref_h = chrono::floor<hours>(ref);

  EXPECT_EQ(ref_h, from_iso_8601("19800508T14Z"sv, basic));
  EXPECT_EQ(ref_h, from_iso_8601("19800508T14+00"sv, basic));
  EXPECT_EQ(ref_h, from_iso_8601("19800508T14-00"sv, basic));
  EXPECT_EQ(ref_h, from_iso_8601("19800508T14+0000"sv, basic));
  EXPECT_EQ(ref_h, from_iso_8601("19800508T14-0000"sv, basic));
  EXPECT_EQ(ref_h, from_iso_8601("19800508T16+02"sv, basic));
  EXPECT_EQ(ref_h, from_iso_8601("19800508T16+0200"sv, basic));
  EXPECT_EQ(ref_h, from_iso_8601("19800508T12-02"sv, basic));
  EXPECT_EQ(ref_h, from_iso_8601("19800508T12-0200"sv, basic));

  EXPECT_EQ(ref, from_iso_8601("19800508T1410Z"sv, basic));
  EXPECT_EQ(ref, from_iso_8601("19800508T1610+02"sv, basic));
  EXPECT_EQ(ref, from_iso_8601("19800508T1610+0200"sv, basic));
  EXPECT_EQ(ref, from_iso_8601("19800508T1210-0200"sv, basic));
  EXPECT_EQ(ref, from_iso_8601("19800508T1940+0530"sv, basic));
  EXPECT_EQ(ref, from_iso_8601("19800508T1040-0330"sv, basic));
  EXPECT_EQ(ref, from_iso_8601("19800508T2255+0845"sv, basic));

  const auto ref_ns = ref + 5s + 30ns;
  EXPECT_EQ(ref_ns, from_iso_8601("19800508T141005.000000030Z"sv, basic));
  EXPECT_EQ(ref_ns, from_iso_8601("19800508T141005,000000030Z"sv, basic));
  EXPECT_EQ(ref_ns, from_iso_8601("19800508T161005.000000030+02"sv, basic));
  EXPECT_EQ(ref_ns, from_iso_8601("19800508T161005.000000030+0200"sv, basic));
  EXPECT_EQ(ref_ns, from_iso_8601("19800508T121005,000000030-0200"sv, basic));

  // Crossing day, year and leap-day boundaries.
  EXPECT_EQ(real_clock::from_time_t(326'675'400), // 1980-05-08T23:10Z
            from_iso_8601("19800509T0110+0200"sv, basic));
  EXPECT_EQ(real_clock::from_time_t(326'682'600), // 1980-05-09T01:10Z
            from_iso_8601("19800508T2310-0200"sv, basic));
  EXPECT_EQ(real_clock::from_time_t(978'303'600), // 2000-12-31T23:00Z
            from_iso_8601("20010101T0100+0200"sv, basic));
  EXPECT_EQ(real_clock::from_time_t(951'865'200), // 2000-02-29T23:00Z
            from_iso_8601("20000301T0100+0200"sv, basic));

  // Inclusive boundaries at -12:00 and +14:00. 2001-03-09T19:36:07Z
  const auto bound = real_clock::from_time_t(984'166'567);
  EXPECT_EQ(bound, from_iso_8601("20010309T073607-1200"sv, basic));
  EXPECT_EQ(bound, from_iso_8601("20010309T073607-12"sv, basic));
  EXPECT_EQ(bound, from_iso_8601("20010310T093607+1400"sv, basic));
  EXPECT_EQ(bound, from_iso_8601("20010310T093607+14"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T073507-1201"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010310T093707+1401"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607+0560"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607+2400"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607-2400"sv, basic));

  // Malformed offsets.
  EXPECT_FALSE(from_iso_8601("20010309T193607+"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607-"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607+5"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607+050"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607+05000"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607+0a"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607Z+0000"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607+0000Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607ZZ"sv, basic));
  // An offset on a date-only form.
  EXPECT_FALSE(from_iso_8601("20010309Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309+0200"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001+0200"sv, basic));

  // Pre-epoch instants are rejected.
  EXPECT_FALSE(from_iso_8601("19700101T0000+0500"sv, basic));
  EXPECT_FALSE(from_iso_8601("19700101T000000.000000001+0001"sv, basic));
  EXPECT_EQ(real_time(0s), from_iso_8601("19700101T0500+0500"sv, basic));
}

TEST(from_iso_8601_basic, range_limits) {
  EXPECT_EQ(real_time::max(),
            from_iso_8601("25540721T233433.709551615Z"sv, basic));
  EXPECT_EQ(real_time::max(),
            from_iso_8601("25540721T213433.709551615-0200"sv, basic));
  EXPECT_FALSE(from_iso_8601("25540721T233433.709551616Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("25540721T233434Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("25540722"sv, basic));
  EXPECT_FALSE(from_iso_8601("2555"sv, basic));
  EXPECT_FALSE(from_iso_8601("1969"sv, basic));
  EXPECT_FALSE(from_iso_8601("19691231"sv, basic));
  EXPECT_FALSE(from_iso_8601("19691231T235959Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("99991231T235959Z"sv, basic));
}

// There is no basic year-month. Everything else on the ladder is a
// specific number of digits, and the ladder has no other rungs.
TEST(from_iso_8601_basic, reduced_precision) {
  EXPECT_TRUE(from_iso_8601("2001"sv, basic));
  EXPECT_FALSE(from_iso_8601("200103"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001-03"sv, basic));
  EXPECT_TRUE(from_iso_8601("20010309"sv, basic));
  EXPECT_TRUE(from_iso_8601("20010309T19Z"sv, basic));
  EXPECT_TRUE(from_iso_8601("20010309T1936Z"sv, basic));
  EXPECT_TRUE(from_iso_8601("20010309T193607Z"sv, basic));

  // Not on the ladder: wrong digit counts in the date.
  EXPECT_FALSE(from_iso_8601("2"sv, basic));
  EXPECT_FALSE(from_iso_8601("20"sv, basic));
  EXPECT_FALSE(from_iso_8601("200"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001030"sv, basic));
  EXPECT_FALSE(from_iso_8601("200103090"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001030919Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309193607Z"sv, basic));
  // Wrong digit counts in the time.
  EXPECT_FALSE(from_iso_8601("20010309T1Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T19360Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T1936071Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T019Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T0193607Z"sv, basic));
  // ISO 8601 allows fractional hours and minutes. We do not.
  EXPECT_FALSE(from_iso_8601("20010309T19.5Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T1936.5Z"sv, basic));
  // An empty fraction.
  EXPECT_FALSE(from_iso_8601("20010309T193607.Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607,Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607.+0200"sv, basic));

  // Reduced precision still needs a zone designator.
  EXPECT_FALSE(from_iso_8601("20010309T19"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T1936"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607.8"sv, basic));
}

TEST(from_iso_8601_basic, calendar_validation) {
  EXPECT_EQ(real_clock::from_time_t(951'782'400),
            from_iso_8601("20000229"sv, basic));
  EXPECT_EQ(real_clock::from_time_t(1'709'164'800),
            from_iso_8601("20240229"sv, basic));
  EXPECT_FALSE(from_iso_8601("21000229"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010229"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010431"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010631"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010931"sv, basic));
  EXPECT_FALSE(from_iso_8601("20011131"sv, basic));
  EXPECT_TRUE(from_iso_8601("20010430"sv, basic));
  EXPECT_TRUE(from_iso_8601("20011231"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010001"sv, basic));
  EXPECT_FALSE(from_iso_8601("20011301"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010100"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010132"sv, basic));
  EXPECT_FALSE(from_iso_8601("20019999"sv, basic));

  EXPECT_FALSE(from_iso_8601("20010309T240000Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T24Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T250000Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T196000Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193660Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193661Z"sv, basic));
  EXPECT_TRUE(from_iso_8601("20010309T235959Z"sv, basic));
  EXPECT_TRUE(from_iso_8601("20010309T000000Z"sv, basic));
}

// Basic mode is as strict as strict mode about everything that isn't
// a delimiter.
TEST(from_iso_8601_basic, strictness) {
  EXPECT_FALSE(from_iso_8601("20010309t193607Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309 193607Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309193607Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T19"sv, basic));

  EXPECT_FALSE(from_iso_8601(""sv, basic));
  EXPECT_FALSE(from_iso_8601("T"sv, basic));
  EXPECT_FALSE(from_iso_8601("Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309TZ"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309TT19Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309ZT19Z"sv, basic));
  EXPECT_FALSE(from_iso_8601(" 20010309T193607Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607Z "sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607Z\n"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309 "sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607Zgarbage"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607+0200garbage"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607Z\0"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T193607\0Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001\00309"sv, basic));
  EXPECT_FALSE(from_iso_8601("２００１0309"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309Ｔ193607Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("+20010309"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001 309"sv, basic));
  EXPECT_FALSE(from_iso_8601("20010309T19 607Z"sv, basic));

  // Other ISO 8601 basic forms we deliberately do not accept.
  EXPECT_FALSE(from_iso_8601("2001068"sv, basic));          // ordinal date
  EXPECT_FALSE(from_iso_8601("2001W105"sv, basic));         // week date
  EXPECT_FALSE(from_iso_8601("20010309T193607Z/P1D"sv, basic)); // interval
  EXPECT_FALSE(from_iso_8601("P1D"sv, basic));              // duration
}

// Basic mode reads only basic format. Every extended form is rejected,
// including the ones that are identical in both formats except for a
// delimiter. The year alone is the same string in both formats.
TEST(from_iso_8601_basic, rejects_extended) {
  EXPECT_FALSE(from_iso_8601("2001-03"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001-03-09"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07.8Z"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+02:00"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+0200"sv, basic));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19:36:07+02"sv, basic));
  EXPECT_FALSE(from_iso_8601(distinct_str, basic));
}

// Mixed forms: a basic date with an extended time, or the reverse, or
// a delimiter in one position but not its sibling, or an extended
// offset on a basic time. None of these is a representation in either
// format, so no mode that claims a format accepts them.
namespace {
constexpr std::string_view mixed_forms[] = {
  "2001-0309"sv,
  "200103-09"sv,
  "2001-03-09T193607Z"sv,
  "2001-03-09T1936Z"sv,
  "2001-03-09T19:3607Z"sv,
  "2001-03-09T1936:07Z"sv,
  "2001-03-09T193607.8Z"sv,
  "20010309T19:36:07Z"sv,
  "20010309T19:36Z"sv,
  "20010309T19:3607Z"sv,
  "20010309T1936:07Z"sv,
  "20010309T19:36:07.8Z"sv,
  "20010309T193607+02:00"sv,
  "20010309T193607.8+02:00"sv,
  "20010309T1936+02:00"sv,
  "20010309T19+02:00"sv,
  "2001-0309T19:36:07Z"sv,
  "200103-09T193607Z"sv,
};
} // namespace

TEST(from_iso_8601_basic, rejects_mixed) {
  for (const auto s : mixed_forms) {
    EXPECT_FALSE(from_iso_8601(s, basic)) << s;
  }
}

TEST(from_iso_8601, strict_rejects_mixed) {
  for (const auto s : mixed_forms) {
    EXPECT_FALSE(from_iso_8601(s)) << s;
  }
}

// Strict mode reads only extended format. The year alone is the same
// string in both formats and is accepted.
TEST(from_iso_8601, strict_rejects_basic) {
  EXPECT_TRUE(from_iso_8601("2001"sv));
  EXPECT_FALSE(from_iso_8601("20010309"sv));
  EXPECT_FALSE(from_iso_8601("20010309T19Z"sv));
  EXPECT_FALSE(from_iso_8601("20010309T1936Z"sv));
  EXPECT_FALSE(from_iso_8601("20010309T193607Z"sv));
  EXPECT_FALSE(from_iso_8601("20010309T193607.8Z"sv));
  EXPECT_FALSE(from_iso_8601("20010309T193607+0200"sv));
  EXPECT_FALSE(from_iso_8601("20010309T193607+02"sv));
  EXPECT_FALSE(from_iso_8601("20130524T000000Z"sv));
  EXPECT_FALSE(from_iso_8601(
    to_iso_8601(distinct, iso_8601_format::BasicYMDhms_ns)));
}

// Lax is extended format with relaxed separators between date and
// time and a relaxed zone designator. It is not a different dialect:
// basic format is rejected in lax mode exactly as in strict mode,
// with or without lax's own relaxations applied on top. The year alone
// is the same string in both formats and stays accepted.
TEST(from_iso_8601, lax_rejects_basic) {
  EXPECT_TRUE(from_iso_8601("2001"sv, lax));
  EXPECT_TRUE(from_iso_8601("2001-03-09T19:36:07Z"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309T19Z"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309T1936Z"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309T193607Z"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309T193607.8Z"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309T193607,8Z"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309T193607+0200"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309T193607+02"sv, lax));
  // Basic with lax's relaxations on top is still basic.
  EXPECT_FALSE(from_iso_8601("20010309T193607"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309t193607z"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309 193607"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309 193607Z"sv, lax));
  EXPECT_FALSE(from_iso_8601("20010309T193607z"sv, lax));
  EXPECT_FALSE(from_iso_8601("20130524T000000Z"sv, lax));
  EXPECT_FALSE(from_iso_8601(
    to_iso_8601(distinct, iso_8601_format::BasicYMDhms_ns), lax));
  // Nor does lax relax widths or the ladder.
  EXPECT_FALSE(from_iso_8601("200103"sv, lax));
  EXPECT_FALSE(from_iso_8601("2001039"sv, lax));
  EXPECT_FALSE(from_iso_8601("2001-03-09T19360"sv, lax));
}

// Lax does not accept mixed delimiters either: no writer produces
// them, so accepting them buys nothing.
TEST(from_iso_8601, lax_rejects_mixed) {
  for (const auto s : mixed_forms) {
    EXPECT_FALSE(from_iso_8601(s, lax)) << s;
  }
}
