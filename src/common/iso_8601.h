// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_COMMON_ISO_8601_H
#define CEPH_COMMON_ISO_8601_H

#include <cassert>
#include <chrono>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <fmt/format.h>
#include <fmt/chrono.h>

#include "common/ceph_time.h"

namespace ceph {
/// Parsing mode for from_iso_8601
enum class iso_8601_mode {
  /// Enforces a superset of the W3C profile with the extensions:
  ///     - Timezone offsets
  ///     - hour-only offsets
  ///     - Basic 'hhmm' style undelimited offsets
  ///     - ',' as the fractional second separator as well as '.'
  ///     - Hour-only times (YYYY-MM-DDThhZ)
  strict,
  /// Use the ISO-8601 basic form without '-' and ':' delimiters.
  /// 'YYYYMM'-style dates are disallowed. So to enumerate specifically, we allow:
  ///     - YYYY
  ///     - YYYYMMDD
  ///     - YYYYMMDDThh
  ///     - YYYYMMDDThhmm
  ///     - YYYYMMDDThhmmss
  ///     - YYYYMMDDThhmmss.n[n…]
  /// Time formats require either 'Z' or an offset of the form ±hh[mm]
  basic,
  /// Allow the variations:
  ///     - lowercase 't' or space to separate time and date
  ///     - lowercase 'z' to specify UTC
  ///     - an unterminated date/time as UTC
  lax
};

// Parse a superset of the ISO 8601 time format
//
// \param[in] time The time to parse
// \param[in] mode The mode determines parsing behavior as described
//                 in \ref iso_8601_mode
//
// Contract:
//     - Resolution is supported to nanosecond granularity. Longer
//       fractional seconds are truncated to nanoseconds.
//     - All times must be representable as a sixty-four bit count of
//       nanoseconds since the epoch. That is, times within
//       [1970-01-01T00:00:00.000000000Z, 2554-07-21T23:34:33.709551615Z]
//     - No times without dates
//     - Offsets less than -12:00 or greater than +14:00 are rejected
//     - Invalid calendar dates are rejected
//     - Trailing characters, including whitespace, are not accepted.
//
// Partial times are handled thus:
//     - If there are no subseconds, they are assumed to be zero.
//     - If there are no seconds, they are assumed to be zero.
//     - If there are no minutes, they are assumed to be zero.
//     - If there is no time, it is assumed to midnight.
//     - If there is no day, it is assumed to be the first.
//     - If there is no month, it is assumed to be January.
//
// \return A ceph::real_time or std::nullopt if invalid.

std::optional<ceph::real_time> from_iso_8601(
    std::string_view time,
    iso_8601_mode mode = iso_8601_mode::strict) noexcept;

enum class iso_8601_format {
  // Extended ISO-8601 (Used in the W3C profile)
  Y, ///< YYYY
  YM, ///< YYYY-MM
  YMD, ///< YYYY-MM-DD
  YMDh, ///< YYYY-MM-DDThhZ
  YMDhm, ///< YY-MM-DDThh:mmZ
  YMDhms, ///< YY-MM-DDThh:mm:ssZ
  YMDhms_ms, ///< YY-MM-DDThh:mm:ss.fffZ
  YMDhms_us, ///< YY-MM-DDThh:mm:ss.ffffffZ
  YMDhms_ns, ///< YY-MM-DDThh:mm:ss.fffffffffZ
  // Basic ISO-8601 (Used in AWS Auth)
  BasicY, ///< /YYYY
  // ISO-8601 basic *forbids* YYYYMM dates as they are ambiguous with no-longer-allowed YYMMDD dates.
  BasicYMD, ///< YYYYMMDD
  BasicYMDh, ///< YYYYMMDDThhZ
  BasicYMDhm, ///< YYMMDDThhmmZ
  BasicYMDhms, ///< YYMMDDThhmmssZ
  BasicYMDhms_ms, ///< YYMMDDThhmmss.fffZ
  BasicYMDhms_us, ///< YYMMDDThhmmss.ffffffZ
  BasicYMDhms_ns, ///< YYMMDDThhmmss.fffffffffZ
};


template<typename OutputIt>
OutputIt to_iso_8601(OutputIt out, const ceph::real_time t,
                     const iso_8601_format f)
{
  using namespace std::literals;
  namespace chrono = std::chrono;
  using enum iso_8601_format;

  auto s = chrono::sys_seconds(chrono::seconds(real_clock::to_time_t(t)));
  auto ns = chrono::duration_cast<chrono::nanoseconds>(t.time_since_epoch() % 1s);

  switch (f) {
  // Extended
  case Y: // YYYY
    return fmt::format_to(out, "{:%Y}", s);

  case YM: // YYYY-MM
    return fmt::format_to(out, "{:%Y-%m}", s);

  case YMD: // YYYY-MM-DD
    return fmt::format_to(out, "{:%Y-%m-%d}", s);

  case YMDh: // YYYY-MM-DDThhZ
    return fmt::format_to(out, "{:%Y-%m-%dT%HZ}", s);

  case YMDhm: // YY-MM-DDThh:mmZ
    return fmt::format_to(out, "{:%Y-%m-%dT%H:%MZ}", s);

  case YMDhms: // YY-MM-DDThh:mm:ssZ
    return fmt::format_to(out, "{:%Y-%m-%dT%H:%M:%SZ}", s);

  case YMDhms_ms: // YY-MM-DDThh:mm:ss.fffZ
    return fmt::format_to(out, "{:%Y-%m-%dT%H:%M:%S}.{:03d}Z", s,
                   chrono::duration_cast<chrono::milliseconds>(ns).count());

  case YMDhms_us: // YY-MM-DDThh:mm:ss.ffffffZ
    return fmt::format_to(out, "{:%Y-%m-%dT%H:%M:%S}.{:06d}Z", s, chrono::duration_cast<chrono::microseconds>(ns).count());

  case YMDhms_ns: // YY-MM-DDThh:mm:ss.fffffffffZ
    return fmt::format_to(out, "{:%Y-%m-%dT%H:%M:%S}.{:09d}Z", s, ns.count());

  // Basic
  case BasicY: // YYYY
    return fmt::format_to(out, "{:%Y}", s);

  case BasicYMD: // YYYYMMDD
    return fmt::format_to(out, "{:%Y%m%d}", s);

  case BasicYMDh: // YYYYMMDDThhZ
    return fmt::format_to(out, "{:%Y%m%dT%HZ}", s);

  case BasicYMDhm: // YYMMDDThhmmZ
    return fmt::format_to(out, "{:%Y%m%dT%H%MZ}", s);

  case BasicYMDhms: // YYMMDDThhmmssZ
    return fmt::format_to(out, "{:%Y%m%dT%H%M%SZ}", s);

  case BasicYMDhms_ms: // YYMMDDThhmmss.fffZ
    return fmt::format_to(out, "{:%Y%m%dT%H%M%S}.{:03d}Z", s, chrono::duration_cast<chrono::milliseconds>(ns).count());

  case BasicYMDhms_us: //< YYMMDDThhmmss.ffffffZ
    return fmt::format_to(out, "{:%Y%m%dT%H%M%S}.{:06d}Z", s, chrono::duration_cast<chrono::microseconds>(ns).count());

  case BasicYMDhms_ns: //< YY-MM-DDThh:mm:ss.fffffffffZ
    return fmt::format_to(out, "{:%Y%m%dT%H%M%S}.{:09d}Z", s, ns.count());
  }
  std::unreachable();
}

inline std::string to_iso_8601(const ceph::real_time t,
                               const iso_8601_format f = iso_8601_format::YMDhms_ns)
{
  using namespace std::literals;
  std::string out;
  out.reserve("YYYY-MM-DDThh:mm:ss.nnnnnnnnnZ"sv.size());

  to_iso_8601(std::back_inserter(out), t, f);
  return out;
}
}

#endif
