// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_COMMON_ISO_8601_H
#define CEPH_COMMON_ISO_8601_H

#include <optional>
#include <string_view>

#include "common/ceph_time.h"

namespace ceph {

/// Parsing mode for from_iso_8601
enum class iso_8601_mode {
  /// Enforces a superset of the W3C profile with the extensions:
  ///     - Timezone offsets
  ///     - hour-only offsets
  ///     - Basic 'hhmm' style undelimited offsets
  ///     - ',' as the fractional second separator as well as '.'
  ///     - Hour-only times (YYYY-MM-DDTHHZ)
  strict,
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
  Y, YM, YMD, YMDh, YMDhm, YMDhms, YMDhmsn
};

std::string to_iso_8601(const ceph::real_time t,
			const iso_8601_format f = iso_8601_format::YMDhmsn,
                        std::string_view date_separator = "-",
                        std::string_view time_separator = ":")
  noexcept;

inline std::string to_iso_8601_no_separators(const ceph::real_time t,
                                             const iso_8601_format f = iso_8601_format::YMDhmsn)
  noexcept {
    return to_iso_8601(t, f, "", "");
  }
}

#endif
