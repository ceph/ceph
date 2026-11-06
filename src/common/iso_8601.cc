// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "iso_8601.h"

#include <cassert>
#include <expected>
#include <chrono>
#include <utility>

#include "common/ceph_time.h"

#include "common/strtol.h"

namespace ceph {
namespace chrono = std::chrono;
using namespace std::literals;
using enum iso_8601_mode;

namespace {
using duration = chrono::duration<__int128, std::nano>;
using time_point = chrono::sys_time<duration>;

constexpr auto digits = "0123456789"sv;

struct succeed {
  bool operator()(const auto&...) {
    return true;
  }
};

template <typename Field, typename Validate = succeed>
std::optional<Field> consume_bounded(std::string_view& remaining, std::size_t bound,
                                     Validate validate = {})
{
  auto view = remaining.substr(0, bound);
  if (view.size() != bound)
    return std::nullopt;
  if (auto val = parse<std::uint16_t>(view); val && validate(*val)) {
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

struct fail {};

template <typename Field, typename Validate = succeed>
auto consume_next_field(std::string_view& remaining, iso_8601_mode mode,
                        char separator, Validate validate = {})
    // We need a tristate to differentiate between "Something",
    // "Nothing", and "Error" and it was either this or
    // std::optional<std::optional<Field>>.
    //
    // This seemed marginally clearer.
    -> std::expected<std::optional<Field>, fail>
{
  if (remaining.empty()) {
    return std::nullopt;
  }

  if (mode == basic) {
    if (remaining.front() == separator) {
      return std::unexpected(fail{});
    }

    // So we don't try to process the timezone marker
    if (digits.contains(remaining.front())) {
      auto f = consume_bounded<Field>(remaining, 2, validate);
      if (!f) {
        return std::unexpected(fail{});
      } else {
        return f;
      }
    } else {
      return std::nullopt;
    }
    std::unreachable();
  }

  if (mode == strict || mode == lax) {
    if (remaining.front() == separator) {
      remaining.remove_prefix(1);
      auto f = consume_bounded<Field>(remaining, 2, validate);
      if (!f) {
        return std::unexpected(fail{});
      } else {
        return f;
      }
    } else {
      return std::nullopt;
    }
  }
  std::unreachable();
}

inline auto less(std::uint16_t max)
{
  return [max](auto x) { return x < max; };
}

inline auto between(std::uint16_t min, std::uint16_t max)
{
  return [min, max](auto x) { return x > min && x < max; };
}

std::optional<time_point> read_date(std::string_view& remaining, iso_8601_mode mode)
{
  std::optional<chrono::day> day;
  std::optional<chrono::month> month;
  auto year = consume_bounded<chrono::year>(remaining, 4, between(1968, 2555));
  if (!year) return std::nullopt;
  if (auto em = consume_next_field<chrono::month>(remaining, mode, '-'); !em) {
    return std::nullopt;
  } else if (*em) {
    month = **em;
    if (auto ed = consume_next_field<chrono::day>(remaining, mode, '-'); !ed) {
      return std::nullopt;
    } else if (*ed) {
      day = **ed;
    }
  }
  // ISO-8601 basic disallow YYYYMM as it clashes with the deprecated YYMMDD format.
  if (month && !day && mode == basic) return std::nullopt;
  if ((!month || !day) && !remaining.empty()) return std::nullopt;
  chrono::year_month_day ymd = (*year) / month.value_or(chrono::month(1)) /
                               day.value_or(chrono::day(1));
  if (!ymd.ok()) {
    return std::nullopt;
  }
  return chrono::time_point_cast<duration>(chrono::sys_days(ymd));
}

inline std::optional<chrono::nanoseconds> parse_nanoseconds(
    std::string_view& remaining)
{
  constexpr std::uint64_t pow10[] = {
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

std::optional<time_point> add_time(std::string_view& remaining,
                                   iso_8601_mode mode, time_point midnight)
{
  time_point time = midnight;

  // HH
  {
    auto hours = consume_bounded<chrono::hours>(remaining, 2, less(24));
    if (!hours) return std::nullopt;
    time += *hours;
  }

  // HH:MM
  if (auto minutes = consume_next_field<chrono::minutes>(remaining, mode, ':',
                                                         less(60));
      !minutes) {
    return std::nullopt;
  } else if (*minutes) {
    time += **minutes;
    // HH:MM:SS
    if (auto seconds = consume_next_field<chrono::seconds>(remaining, mode, ':',
                                                           less(60));
        !seconds) {
      return std::nullopt;
    } else if (*seconds) {
      time += **seconds;
      // HH::MM::SS.fff…
      if (optional_delimiter(remaining, ".,")) {
        auto nanoseconds = parse_nanoseconds(remaining);
        if (!nanoseconds) return std::nullopt;
        time += *nanoseconds;
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
      // `strict` mode allows the separator to be left out because a
      // surprising number of clients emit it, but basic doesn't allow
      // it ever.
      if (remaining.empty() || mode == basic)
        return std::nullopt;
    }
    if (!remaining.empty()) {
      auto minutes = consume_bounded<chrono::minutes>(remaining, 2, less(60));
      if (!minutes) return std::nullopt;
      offset += *minutes;
    }
    offset *= signum;
    if (offset < -12h || offset > 14h) return std::nullopt;
  } else if (optional_delimiter(remaining, mode == lax ? "Zz"sv : "Z"sv)) {
    // Explicit UTC
  } else if (mode == lax && remaining.empty()) {
    // Allow unterminated
  } else {
    return std::nullopt;
  }
  return time - offset;
}
} // namespace

std::optional<real_time> from_iso_8601(const std::string_view s,
                                       const iso_8601_mode mode) noexcept
{
  assert(mode >= iso_8601_mode::strict && mode <= iso_8601_mode::lax);
  auto remaining = s;
  constexpr auto epoch = time_point(duration(0));
  constexpr auto doomsday =  time_point(duration(timespan::max()));

  auto time = read_date(remaining, mode);
  if (!time) return std::nullopt;

  if (!remaining.empty()) {
    if (!optional_delimiter(remaining, mode == lax ? "Tt "sv : "T"sv)) return std::nullopt;
    time = add_time(remaining, mode, *time);
  }
  if (!time) return std::nullopt;
  if (!remaining.empty()) return std::nullopt;

  if (*time < epoch || *time > doomsday) {
    return std::nullopt;
  }
  return real_time(chrono::duration_cast<timespan>(time->time_since_epoch()));
}
} // namespace ceph
