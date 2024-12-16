// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <boost/endian/conversion.hpp>

#include "int_types.h"

template<typename T>
inline T swab(T val) {
  return boost::endian::endian_reverse(val);
}

template<typename T>
struct ceph_le {
private:
  T v;
public:
  ceph_le() = default;
  explicit ceph_le(T nv)
    : v{boost::endian::native_to_little(nv)}
  {}
  ceph_le<T>& operator=(T nv) {
    v = boost::endian::native_to_little(nv);
    return *this;
  }
  constexpr operator T() const { return boost::endian::little_to_native(v); }
  friend inline bool operator==(ceph_le a, ceph_le b) {
    return a.v == b.v;
  }
} __attribute__ ((packed));

using ceph_le64 = ceph_le<__u64>;
using ceph_le32 = ceph_le<__u32>;
using ceph_le16 = ceph_le<__u16>;

using ceph_les64 = ceph_le<__s64>;
using ceph_les32 = ceph_le<__s32>;
using ceph_les16 = ceph_le<__s16>;

inline ceph_les64 init_les64(__s64 x) {
  ceph_les64 v;
  v = x;
  return v;
}
inline ceph_les32 init_les32(__s32 x) {
  ceph_les32 v;
  v = x;
  return v;
}
inline ceph_les16 init_les16(__s16 x) {
  ceph_les16 v;
  v = x;
  return v;
}
