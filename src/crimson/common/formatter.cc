// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "formatter.h"

#include <chrono>
#include <fmt/format.h>
#if FMT_VERSION >= 60000
#include <fmt/chrono.h>
#else
#include <fmt/time.h>
#endif


template <>
struct fmt::formatter<seastar::lowres_system_clock::time_point> {
  // ignore the format string
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const seastar::lowres_system_clock::time_point& t,
              FormatContext& ctx) const {
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(
      t.time_since_epoch() % std::chrono::seconds(1)).count();

    std::time_t time = seastar::lowres_system_clock::to_time_t(t);
    std::tm tm_local;
    if (!localtime_r(&time, &tm_local)) {
      throw fmt::format_error("time_t value out of range");
    }
    return fmt::format_to(ctx.out(), "{:%F %T} {:03d}", tm_local, milliseconds);
  }
};

namespace std {

ostream& operator<<(ostream& out,
                    const seastar::lowres_system_clock::time_point& t)
{
  return out << fmt::format("{}", t);
}

}
