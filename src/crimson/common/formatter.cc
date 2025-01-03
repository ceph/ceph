// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "formatter.h"

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
    std::time_t tt = std::chrono::duration_cast<std::chrono::seconds>(
      t.time_since_epoch()).count();
    auto milliseconds = (t.time_since_epoch() %
                         std::chrono::seconds(1)).count();
    return fmt::format_to(ctx.out(), "{:%Y-%m-%d %H:%M:%S} {:03d}",
                          fmt::localtime(tt), milliseconds);
  }
};

namespace std {

ostream& operator<<(ostream& out,
                    const seastar::lowres_system_clock::time_point& t)
{
  return out << fmt::format("{}", t);
}

}
