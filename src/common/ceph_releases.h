// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <iosfwd>
#include <string_view>

#include "common/ceph_strings.h"

// the C++ version of CEPH_RELEASE_* defined by include/rados.h
enum class ceph_release_t : std::uint8_t {
  unknown = 0,
  argonaut,
  bobtail,
  cuttlefish,
  dumpling,
  emperor,
  firefly,
  giant,
  hammer,
  infernalis,
  jewel,
  kraken,
  luminous,
  mimic,
  nautilus,
  octopus,
  pacific,
  quincy,
  reef,
  squid,
  max,
};

std::ostream& operator<<(std::ostream& os, const ceph_release_t r);

inline bool operator!(ceph_release_t& r) {
  return (r < ceph_release_t::unknown ||
          r == ceph_release_t::unknown);
}

inline ceph_release_t& operator--(ceph_release_t& r) {
  r = static_cast<ceph_release_t>(static_cast<uint8_t>(r) - 1);
  return r;
}

inline ceph_release_t& operator++(ceph_release_t& r) {
  r = static_cast<ceph_release_t>(static_cast<uint8_t>(r) + 1);
  return r;
}

inline bool operator<(ceph_release_t lhs, ceph_release_t rhs) {
  // we used to use -1 for invalid release
  if (static_cast<int8_t>(lhs) < 0) {
    return true;
  } else if (static_cast<int8_t>(rhs) < 0) {
    return false;
  }
  return static_cast<uint8_t>(lhs) < static_cast<uint8_t>(rhs);
}

inline bool operator>(ceph_release_t lhs, ceph_release_t rhs) {
  // we used to use -1 for invalid release
  if (static_cast<int8_t>(lhs) < 0) {
    return false;
  } else if (static_cast<int8_t>(rhs) < 0) {
    return true;
  }
  return static_cast<uint8_t>(lhs) > static_cast<uint8_t>(rhs);
}

inline bool operator>=(ceph_release_t lhs, ceph_release_t rhs) {
  return !(lhs < rhs);
}

bool can_upgrade_from(ceph_release_t from_release,
		      std::string_view from_release_name,
		      std::ostream& err);

ceph_release_t ceph_release_from_name(std::string_view sv);
ceph_release_t ceph_release();

inline std::string_view to_string(ceph_release_t r) {
  return ceph_release_name(static_cast<int>(r));
}
template<typename IntType> IntType to_integer(ceph_release_t r) {
  return static_cast<IntType>(r);
}
