// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/* N.B., this header defines fundamental serialized types.  Do not
 * include files which can only be compiled in radosgw or OSD
 * contexts (e.g., rgw_sal.h, rgw_common.h) */

#pragma once

#include <string>
#include <boost/container/flat_set.hpp>

namespace rgw::zone_features {

// zone feature names
inline constexpr std::string_view resharding = "resharding";
inline constexpr std::string_view compress_encrypted = "compress-encrypted";
inline constexpr std::string_view notification_v2 = "notification_v2";

// static list of features supported by this release
inline constexpr std::initializer_list<std::string_view> supported = {
    resharding,
    compress_encrypted,
    notification_v2,
};

inline constexpr bool supports(std::string_view feature) {
  for (auto i : supported) {
    if (feature.compare(i) == 0) {
      return true;
    }
  }
  return false;
}

// static list of features enabled by default on new zonegroups
inline constexpr std::initializer_list<std::string_view> enabled = {
    resharding,
    notification_v2,
};


// enable string_view overloads for find() contains() etc
struct feature_less : std::less<std::string_view> {
  using is_transparent = std::true_type;
};

using set = boost::container::flat_set<std::string, feature_less>;

} // namespace rgw::zone_features
