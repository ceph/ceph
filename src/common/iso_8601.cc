// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "iso_8601.h"

#include <chrono>
#include <iomanip>
#include <sstream>

#include "common/ceph_time.h"
#include "include/ceph_assert.h"

#include "common/strtol.h"

namespace ceph {

namespace chrono = std::chrono;
using chrono::duration_cast;
using std::setw;
using std::size_t;
using std::stringstream;
using std::string;
using std::uint16_t;

using namespace std::literals;

namespace {
template <typename Field, typename Validate = decltype([](auto) { return true; })>
std::optional<Field> consume_bounded(std::string_view& remaining, size_t bound,
                                     Validate validate = {})
{
  auto view = remaining.substr(0, bound);
  if (view.size() != bound)
    return std::nullopt;
  if (auto val = parse<uint16_t>(view); val && validate(*val)) {
    remaining.remove_prefix(view.size());
    return Field(*val);
  }
  return std::nullopt;
}

std::optional<char> optional_delimiter(std::string_view& remaining,
                                       std::string_view valid)
{
  if (!remaining.empty() && valid.contains(remaining.front())) {
    auto c = remaining.front();
    remaining.remove_prefix(1);
    return c;
  }
  return std::nullopt;
}

inline auto less(std::uint16_t max)
{
  return [max](auto x) { return x < max; };
}

inline auto between(std::uint16_t min, std::uint16_t max)
{
  return [min, max](auto x) { return x > min && x < max; };
}

template <typename Rep, typename Period>
inline std::optional<real_time> bounded_sub(
    ceph::real_time time, chrono::duration<Rep, Period> decrement)
{
  auto delta = chrono::duration_cast<timespan>(chrono::abs(decrement));
  if (decrement > decrement.zero()) {
    if (time.time_since_epoch() < delta) return std::nullopt;
    time -= delta;
  } else {
    if (time > real_time::max() - delta) return std::nullopt;
    time += delta;
  }
  return time;
}

template <typename Rep, typename Period>
inline std::optional<real_time> bounded_add(
    ceph::real_time time, chrono::duration<Rep, Period> increment)
{
  auto delta = chrono::duration_cast<timespan>(chrono::abs(increment));
  if (increment > increment.zero()) {
    if (time > real_time::max() - delta) return std::nullopt;
    time += delta;
  } else {
    if (time.time_since_epoch() < delta) return std::nullopt;
    time -= delta;
  }
  return time;
}

std::optional<real_time> read_date(std::string_view& remaining)
{
  std::optional<chrono::day> day;
  std::optional<chrono::month> month;
  auto year = consume_bounded<chrono::year>(remaining, 4, between(1969, 2555));
  if (!year) return std::nullopt;
  if (optional_delimiter(remaining, "-")) {
    if (!(month = consume_bounded<chrono::month>(remaining, 2)))
      return std::nullopt;
    if (optional_delimiter(remaining, "-")) {
      if (!(day = consume_bounded<chrono::day>(remaining, 2)))
        return std::nullopt;
    }
  }
  if ((!month || !day) && !remaining.empty()) return std::nullopt;
  chrono::year_month_day ymd = (*year) / month.value_or(chrono::month(1)) /
                               day.value_or(chrono::day(1));
  if (!ymd.ok()) {
    return std::nullopt;
  }
  if (chrono::sys_days(ymd) > chrono::sys_days(2554y / 7 / 21d))
    // 21 July 2554 is the last day `ceph::real_time` can represent.
    return std::nullopt;
  // On Linux `system_clock::to_time_t` is not usable for large, unsigned values
  return real_clock::from_time_t(
      duration_cast<chrono::seconds>(chrono::sys_days(ymd).time_since_epoch())
          .count());
}

inline std::optional<chrono::nanoseconds> parse_nanoseconds(
    std::string_view& remaining)
{
  constexpr auto digits = "0123456789"sv;
  constexpr uint64_t pow10[] = {
      1,         10,         100,
      1'000,     10'000,     100'000,
      1'000'000, 10'000'000, 100'000'000};

  auto nend = remaining.find_first_not_of(digits);
  auto nv = remaining.substr(0, nend);

  if (nv.empty())
    return std::nullopt;
  // We only handle nanoseconds, but larger timestamps are valid, so
  // only parse nine but trim all provided.
  auto to_trim = nv.size();
  if (nv.size() > 9) {
    nv.remove_suffix(nv.size() - 9);
  }
  auto fractsecs = parse<uint64_t>(nv);
  if (!fractsecs)
    return std::nullopt;
  chrono::nanoseconds nanosecond(*fractsecs * pow10[9 - nv.size()]);
  remaining.remove_prefix(to_trim);
  return nanosecond;
}

std::optional<real_time> add_time(std::string_view& remaining,
                                  bool lax, real_time midnight)
{
  std::optional<real_time> time = midnight;

  // HH
  {
    auto hours = consume_bounded<chrono::hours>(remaining, 2, less(24));
    if (!hours) return std::nullopt;
    time = bounded_add(*time, *hours);
    if (!time) return std::nullopt;
  }

  // HH:MM
  if (optional_delimiter(remaining, ":")) {
    auto minutes = consume_bounded<chrono::minutes>(remaining, 2, less(60));
    if (!minutes)
      return std::nullopt;
    time = bounded_add(*time, *minutes);
    if (!time) return std::nullopt;

    // HH:MM:SS
    if (optional_delimiter(remaining, ":")) {
      auto seconds = consume_bounded<chrono::seconds>(remaining, 2, less(60));
      if (!seconds) return std::nullopt;
      time = bounded_add(*time, *seconds);
      if (!time) return std::nullopt;

      // HH::MM::SS.fff…
      if (optional_delimiter(remaining, ".,")) {
        auto nanoseconds = parse_nanoseconds(remaining);
        if (!nanoseconds) return std::nullopt;
        time = bounded_add(*time, *nanoseconds);
        if (!time) return std::nullopt;
      }
    }
  }
  chrono::minutes offset{0};
  // ±hh[:mm]
  if (auto sign = optional_delimiter(remaining, "+-"); sign) {
    short signum = *sign == '-' ? -1 : 1;
    auto hours = consume_bounded<chrono::hours>(remaining, 2);
    if (!hours)
      return std::nullopt;
    offset += *hours;
    if (auto colon = optional_delimiter(remaining, ":"); colon) {
      if (remaining.empty())
        return std::nullopt;
    }
    if (!remaining.empty()) {
      auto minutes = consume_bounded<chrono::minutes>(remaining, 2, less(60));
      if (!minutes) return std::nullopt;
      offset += *minutes;
    }
    offset *= signum;
    if (offset < -12h || offset > 14h) return std::nullopt;
  } else if (optional_delimiter(remaining, lax ? "Zz"sv : "Z"sv)) {
    // Explicit UTC
  } else if (lax && remaining.empty()) {
    // Allow unterminated
  } else {
    return std::nullopt;
  }
  return bounded_sub(*time, offset);
}
} // namespace

std::optional<real_time> from_iso_8601(const std::string_view s,
                                       const iso_8601_mode mode) noexcept
{
  auto remaining = s;
  ceph_assert(mode == iso_8601_mode::strict || mode == iso_8601_mode::lax);
  auto lax = (mode == iso_8601_mode::lax);

  auto time = read_date(remaining);
  if (!time) return std::nullopt;

  if (!remaining.empty()) {
    if (!optional_delimiter(remaining, lax ? "Tt "sv : "T"sv)) return std::nullopt;
    time = add_time(remaining, lax, *time);
  }
  if (!remaining.empty()) return std::nullopt;
  return time;
}

string to_iso_8601(const real_time t,
		   const iso_8601_format f,
                   std::string_view date_separator,
                   std::string_view time_separator) noexcept {
  ceph_assert(f >= iso_8601_format::Y &&
	      f <= iso_8601_format::YMDhmsn);
  stringstream out(std::ios_base::out);

  auto sec = real_clock::to_time_t(t);
  auto nsec =
      duration_cast<chrono::nanoseconds>(t.time_since_epoch() % (1s)).count();

  struct tm bt;
  gmtime_r(&sec, &bt);
  out.fill('0');

  out << 1900 + bt.tm_year;
  if (f == iso_8601_format::Y) {
    return out.str();
  }

  out << date_separator << setw(2) << bt.tm_mon + 1;
  if (f == iso_8601_format::YM) {
    return out.str();
  }

  out << date_separator << setw(2) << bt.tm_mday;
  if (f == iso_8601_format::YMD) {
    return out.str();
  }

  out << 'T' << setw(2) << bt.tm_hour;
  if (f == iso_8601_format::YMDh) {
    out << 'Z';
    return out.str();
  }

  out << time_separator << setw(2) << bt.tm_min;
  if (f == iso_8601_format::YMDhm) {
    out << 'Z';
    return out.str();
  }

  out << time_separator << setw(2) << bt.tm_sec;
  if (f == iso_8601_format::YMDhms) {
    out << 'Z';
    return out.str();
  }
  out << '.' << setw(9) << nsec << 'Z';
  return out.str();
}
} // namespace ceph
