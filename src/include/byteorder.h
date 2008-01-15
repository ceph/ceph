/*
 * byteorder.h
 *
 * LGPL 2
 */

#ifndef _CEPH_BYTEORDER_H
#define _CEPH_BYTEORDER_H

typedef __u64 __le64;
typedef __u32 __le32;
typedef __u16 __le16;

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
static __inline__ __u64 swab64(__u64 val) 
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

#ifdef WORDS_BIGENDIAN
# define cpu_to_le64(x) swab64((x))
# define le64_to_cpu(x) swab64((x))
# define cpu_to_le32(x) swab32((x))
# define le32_to_cpu(x) swab32((x))
# define cpu_to_le16(x) swab16((x))
# define le16_to_cpu(x) swab16((x))
#else
# define cpu_to_le64(x) ((__u64)(x))
# define le64_to_cpu(x) ((__u64)(x))
# define cpu_to_le32(x) ((__u32)(x))
# define le32_to_cpu(x) ((__u32)(x))
# define cpu_to_le16(x) ((__u16)(x))
# define le16_to_cpu(x) ((__u16)(x))
#endif

#endif
