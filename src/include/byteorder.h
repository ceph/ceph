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

#define MAKE_LE_CLASS(bits)						\
  struct ceph_le##bits {						\
    __u##bits v;							\
    ceph_le##bits &operator=(__u##bits nv) {				\
      v = mswab(nv);							\
      return *this;							\
    }									\
    operator __u##bits() const { return mswab(v); }			\
  } __attribute__ ((packed));						\
  static inline bool operator==(ceph_le##bits a, ceph_le##bits b) {	\
    return a.v == b.v;							\
  }
  
MAKE_LE_CLASS(64)
MAKE_LE_CLASS(32)
MAKE_LE_CLASS(16)
#undef MAKE_LE_CLASS

inline __u64 init_le64(__u64 x) {
  return mswab<__u64>(x);
}
inline __u32 init_le32(__u32 x) {
  return mswab<__u32>(x);
}
inline __u16 init_le16(__u16 x) {
  return mswab<__u16>(x);
}

  /*
#define cpu_to_le64(x) (x)
#define cpu_to_le32(x) (x)
#define cpu_to_le16(x) (x)
  */
#define le64_to_cpu(x) ((uint64_t)x)
#define le32_to_cpu(x) ((__u32)x)
#define le16_to_cpu(x) ((__u16)x)
