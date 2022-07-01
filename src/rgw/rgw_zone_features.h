// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <boost/container/flat_set.hpp>

namespace rgw::zone_features {

// zone feature names
inline constexpr std::string_view resharding = "resharding";

// static list of features supported by this release
inline constexpr std::initializer_list<std::string_view> supported = {
  resharding,
};

inline constexpr bool supports(std::string_view feature) {
  for (auto i : supported) {
    if (feature.compare(i) == 0) {
      return true;
    }
  }
  return false;
}


// enable string_view overloads for find() contains() etc
struct feature_less : std::less<std::string_view> {
  using is_transparent = std::true_type;
};

using set = boost::container::flat_set<std::string, feature_less>;

} // namespace rgw::zone_features
