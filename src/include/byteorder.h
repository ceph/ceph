// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <type_traits>
#include "acconfig.h"
#include "int_types.h"


#ifdef __GNUC__
template<typename T>
inline typename std::enable_if<sizeof(T) == sizeof(uint16_t), T>::type
swab(T val) {
  return __builtin_bswap16(val);
}
template<typename T>
inline typename std::enable_if<sizeof(T) == sizeof(uint32_t), T>::type
swab(T val) {
  return __builtin_bswap32(val);
}
template<typename T>
inline typename std::enable_if<sizeof(T) == sizeof(uint64_t), T>::type
swab(T val) {
  return __builtin_bswap64(val);
}
#else
template<typename T>
inline typename std::enable_if<sizeof(T) == sizeof(uint16_t), T>::type
swab(T val) {
  return (val >> 8) | (val << 8);
}
template<typename T>
inline typename std::enable_if<sizeof(T) == sizeof(uint32_t), T>::type
swab(T val) {
  return (( val >> 24) |
	  ((val >> 8)  & 0xff00) |
	  ((val << 8)  & 0xff0000) | 
	  ((val << 24)));
}
template<typename T>
inline typename std::enable_if<sizeof(T) == sizeof(uint64_t), T>::type
swab(T val) {
  return (( val >> 56) |
	  ((val >> 40) & 0xff00ull) |
	  ((val >> 24) & 0xff0000ull) |
	  ((val >> 8)  & 0xff000000ull) |
	  ((val << 8)  & 0xff00000000ull) |
	  ((val << 24) & 0xff0000000000ull) |
	  ((val << 40) & 0xff000000000000ull) |
	  ((val << 56)));
}
#endif

// mswab == maybe swab (if not LE)
#ifdef CEPH_BIG_ENDIAN
template<typename T>
inline T mswab(T val) {
  return swab(val);
}
#else
template<typename T>
inline T mswab(T val) {
  return val;
}
#endif

template<typename T>
struct ceph_le {
private:
  T v;
public:
  ceph_le<T>& operator=(T nv) {
    v = mswab(nv);
    return *this;
  }
  operator T() const { return mswab(v); }
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

inline ceph_le64 init_le64(__u64 x) {
  ceph_le64 v;
  v = x;
  return v;
}
inline ceph_le32 init_le32(__u32 x) {
  ceph_le32 v;
  v = x;
  return v;
}
inline ceph_le16 init_le16(__u16 x) {
  ceph_le16 v;
  v = x;
  return v;
}

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
