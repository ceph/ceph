/*
 * byteorder.h
 *
 * LGPL 2
 */

#ifndef CEPH_BYTEORDER_H
#define CEPH_BYTEORDER_H

#include <sys/param.h>
#include "int_types.h"

#if defined(__APPLE__)
# if __DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN
#  define CEPH_LITTLE_ENDIAN
# elif __DARWIN_BYTE_ORDER == __DARWIN_BIG_ENDIAN
#  define CEPH_BIG_ENDIAN
# endif
#endif

#if defined(__FreeBSD__)
# if _BYTE_ORDER == _LITTLE_ENDIAN
#  define CEPH_LITTLE_ENDIAN
# elif _BYTE_ORDER == _BIG_ENDIAN
#  define CEPH_BIG_ENDIAN
# endif
#endif

#if defined(__linux__)
# if BYTE_ORDER == LITTLE_ENDIAN
#  define CEPH_LITTLE_ENDIAN
# elif BYTE_ORDER == BIG_ENDIAN
#  define CEPH_BIG_ENDIAN
# endif
#endif

#if defined(__sun) && defined(__SVR4)
# if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#  define CEPH_BIG_ENDIAN
# elif __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#  define CEPH_LITTLE_ENDIAN
# endif
#endif

#if defined(_AIX)
# define CEPH_BIG_ENDIAN
#endif




static __inline__ __u16 swab16(__u16 val) 
{
  return (val >> 8) | (val << 8);
}
static __inline__ __u32 swab32(__u32 val) 
{
  return (( val >> 24) |
	  ((val >> 8)  & 0xff00) |
	  ((val << 8)  & 0xff0000) | 
	  ((val << 24)));
}
static __inline__ uint64_t swab64(uint64_t val) 
{
  return (( val >> 56) |
	  ((val >> 40) & 0xff00ull) |
	  ((val >> 24) & 0xff0000ull) |
	  ((val >> 8)  & 0xff000000ull) |
	  ((val << 8)  & 0xff00000000ull) |
	  ((val << 24) & 0xff0000000000ull) |
	  ((val << 40) & 0xff000000000000ull) |
	  ((val << 56)));
}

// mswab == maybe swab (if not LE)
#ifdef CEPH_BIG_ENDIAN
# define mswab64(a) swab64(a)
# define mswab32(a) swab32(a)
# define mswab16(a) swab16(a)
#elif defined(CEPH_LITTLE_ENDIAN)
# define mswab64(a) (a)
# define mswab32(a) (a)
# define mswab16(a) (a)
#else
# error "Could not determine endianess"
#endif

#ifdef __cplusplus

#define MAKE_LE_CLASS(bits)						\
  struct ceph_le##bits {							\
    __u##bits v;							\
    ceph_le##bits &operator=(__u##bits nv) {				\
      v = mswab##bits(nv);						\
      return *this;							\
    }									\
    operator __u##bits() const { return mswab##bits(v); }		\
  } __attribute__ ((packed));						\
  static inline bool operator==(ceph_le##bits a, ceph_le##bits b) {		\
    return a.v == b.v;							\
  }
  
MAKE_LE_CLASS(64)
MAKE_LE_CLASS(32)
MAKE_LE_CLASS(16)
#undef MAKE_LE_CLASS

#endif /* __cplusplus */

#define init_le64(x) { (__u64)mswab64(x) }
#define init_le32(x) { (__u32)mswab32(x) }
#define init_le16(x) { (__u16)mswab16(x) }

  /*
#define cpu_to_le64(x) (x)
#define cpu_to_le32(x) (x)
#define cpu_to_le16(x) (x)
  */
#define le64_to_cpu(x) ((uint64_t)x)
#define le32_to_cpu(x) ((__u32)x)
#define le16_to_cpu(x) ((__u16)x)

#endif
