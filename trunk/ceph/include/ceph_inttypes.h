#ifndef __CEPH_INTTYPES_H
#define __CEPH_INTTYPES_H

/* 
 * these are int types defined in the kernel.
 * this header should be included prior to ceph_fs.h when used from userspace.
 * i suspect kernel_compat.h (or whatever) serves a similar purpose?
 */

typedef uint32_t __u64;
typedef uint32_t __u32;
typedef uint16_t __u16;
typedef uint8_t __u8;

typedef int32_t __s64;
typedef int32_t __s32;
typedef int16_t __s16;
typedef int8_t __s8;

typedef uint64_t u64;
typedef uint32_t u32;
typedef uint16_t u16;
typedef uint16_t u8;

typedef int64_t s64;
typedef int32_t s32;
typedef int16_t s16;
typedef int16_t s8;

#endif
