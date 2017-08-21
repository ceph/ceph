#ifndef CEPH_CRUSH_COMPAT_H
#define CEPH_CRUSH_COMPAT_H

#include "include/int_types.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* asm-generic/bug.h */

#define BUG_ON(x) assert(!(x))

/* linux/kernel.h */

#define U8_MAX		((__u8)~0U)
#define S8_MAX		((__s8)(U8_MAX>>1))
#define S8_MIN		((__s8)(-S8_MAX - 1))
#define U16_MAX		((__u16)~0U)
#define S16_MAX		((__s16)(U16_MAX>>1))
#define S16_MIN		((__s16)(-S16_MAX - 1))
#define U32_MAX		((__u32)~0U)
#define S32_MAX		((__s32)(U32_MAX>>1))
#define S32_MIN		((__s32)(-S32_MAX - 1))
#define U64_MAX		((__u64)~0ULL)
#define S64_MAX		((__s64)(U64_MAX>>1))
#define S64_MIN		((__s64)(-S64_MAX - 1))

/* linux/math64.h */

#define div64_s64(dividend, divisor) ((dividend) / (divisor))

/* linux/slab.h */

#define kmalloc(size, flags) malloc(size)
#define kfree(x) do { if (x) free(x); } while (0)

#endif /* CEPH_CRUSH_COMPAT_H */
