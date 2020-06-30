// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_COMMON_ISO_8601_H
#define CEPH_COMMON_ISO_8601_H

#include <string_view>
#include <boost/optional.hpp>

#include "common/ceph_time.h"

namespace ceph {

// Here, we support the W3C profile of ISO 8601 with the following
// restrictions:
// -   Subsecond resolution is supported to nanosecond
//     granularity. Any number of digits between 1 and 9 may be
//     specified after the decimal point.
// -   All times must be UTC.
// -   All times must be representable as a sixty-four bit count of
//     nanoseconds since the epoch.
// -   Partial times are handled thus:
//     *    If there are no subseconds, they are assumed to be zero.
//     *    If there are no seconds, they are assumed to be zero.
//     *    If there are no minutes, they are assumed to be zero.
//     *    If there is no time, it is assumed to midnight.
//     *    If there is no day, it is assumed to be the first.
//     *    If there is no month, it is assumed to be January.
//
// If a date is invalid, boost::none is returned.

boost::optional<ceph::real_time> from_iso_8601(
  std::string_view s, const bool ws_terminates = true) noexcept;

enum class iso_8601_format {
  Y, YM, YMD, YMDh, YMDhm, YMDhms, YMDhmsn
};

std::string to_iso_8601(const ceph::real_time t,
			const iso_8601_format f = iso_8601_format::YMDhmsn)
  noexcept;
}

#endif
