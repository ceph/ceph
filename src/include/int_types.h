#ifndef CEPH_INTTYPES_H
#define CEPH_INTTYPES_H

#include "acconfig.h"

#ifdef HAVE_LINUX_TYPES_H
#include <linux/types.h>
#endif

/*
 * Get 64b integers either from inttypes.h or glib.h
 */
#ifdef HAVE_INTTYPES_H
#  include <inttypes.h>
//#else
//#  ifdef HAVE_GLIB
//#    include <glib.h>
//#  endif
#endif

/*
 * C99 says inttypes.h includes stdint.h, but that's not true on all
 * systems. If it's there, include it always - just in case.
 */
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif

/*
 * Emergency replacements for PRI*64 modifiers. Some systems have
 * an inttypes.h that doesn't define all the PRI[doxu]64 macros.
 */
#if !defined(PRIu64)
#  if defined(HAVE_INTTYPES_H) || defined(HAVE_GLIB)
/* If we have inttypes or glib, assume we have 64-bit long long int */
#    define PRIu64 "llu"
#    define PRIi64 "lli"
#    define PRIx64 "llx"
#    define PRIX64 "llX"
#    define PRIo64 "llo"
#    define PRId64 "lld"
#  else
/* Assume that we don't have long long, so use long int modifiers */
#    define PRIu64 "lu"
#    define PRIi64 "li"
#    define PRIx64 "lx"
#    define PRIX64 "lX"
#    define PRIo64 "lo"
#    define PRId64 "ld"
#  endif
#endif

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#ifndef HAVE___U8
typedef uint8_t __u8;
#endif

#ifndef HAVE___S8
typedef int8_t __s8;
#endif

#ifndef HAVE___U16
typedef uint16_t __u16;
#endif

#ifndef HAVE___S16
typedef int16_t __s16;
#endif

#ifndef HAVE___U32
typedef uint32_t __u32;
#endif

#ifndef HAVE___S32
typedef int32_t __s32;
#endif

#ifndef HAVE___U64
typedef uint64_t __u64;
#endif

#ifndef HAVE___S64
typedef int64_t __s64;
#endif

#define __bitwise__

typedef __u16 __bitwise__ __le16;
typedef __u16 __bitwise__ __be16;
typedef __u32 __bitwise__ __le32;
typedef __u32 __bitwise__ __be32;
typedef __u64 __bitwise__ __le64;
typedef __u64 __bitwise__ __be64;

#endif
