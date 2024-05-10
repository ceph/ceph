// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#ifndef CEPH_INLINE_MEMORY_H
#define CEPH_INLINE_MEMORY_H

#include <cstdint>
#include <cstddef> // For size_t
#include <cstring> // for memcpy

#if defined(__GNUC__)

// optimize for the common case, which is very small copies
static inline void *maybe_inline_memcpy(void *dest, const void *src, size_t l,
				       size_t inline_len)
  __attribute__((always_inline));

void *maybe_inline_memcpy(void *dest, const void *src, size_t l,
			 size_t inline_len)
{
  if (l > inline_len) {
    return std::memcpy(dest, src, l);
  }
  switch (l) {
  case 8:
    return __builtin_memcpy(dest, src, 8);
  case 4:
    return __builtin_memcpy(dest, src, 4);
  case 3:
    return __builtin_memcpy(dest, src, 3);
  case 2:
    return __builtin_memcpy(dest, src, 2);
  case 1:
    return __builtin_memcpy(dest, src, 1);
  default:
    int cursor = 0;
    while (l >= sizeof(uint64_t)) {
      __builtin_memcpy((char*)dest + cursor, (char*)src + cursor,
		       sizeof(uint64_t));
      cursor += sizeof(uint64_t);
      l -= sizeof(uint64_t);
    }
    while (l >= sizeof(uint32_t)) {
      __builtin_memcpy((char*)dest + cursor, (char*)src + cursor,
		       sizeof(uint32_t));
      cursor += sizeof(uint32_t);
      l -= sizeof(uint32_t);
    }
    while (l > 0) {
      *((char*)dest + cursor) = *((char*)src + cursor);
      cursor++;
      l--;
    }
  }
  return dest;
}

#else

#define maybe_inline_memcpy(d, s, l, x) std::memcpy(d, s, l)

#endif


#if defined(__GNUC__) && defined(__x86_64__)

namespace ceph {
typedef unsigned uint128_t __attribute__ ((mode (TI)));
}
using ceph::uint128_t;

static inline bool mem_is_zero(const char *data, size_t len)
  __attribute__((always_inline));

bool mem_is_zero(const char *data, size_t len)
{
  // we do have XMM registers in x86-64, so if we need to check at least
  // 16 bytes, make use of them
  if (len / sizeof(uint128_t) > 0) {
    // align data pointer to 16 bytes, otherwise it'll segfault due to bug
    // in (at least some) GCC versions (using MOVAPS instead of MOVUPS).
    // check up to 15 first bytes while at it.
    while (((unsigned long long)data) & 15) {
      if (*(uint8_t*)data != 0) {
	return false;
      }
      data += sizeof(uint8_t);
      --len;
    }

    const char* data_start = data;
    const char* max128 = data + (len / sizeof(uint128_t))*sizeof(uint128_t);

    while (data < max128) {
      if (*(uint128_t*)data != 0) {
	return false;
      }
      data += sizeof(uint128_t);
    }
    len -= (data - data_start);
  }

  const char* max = data + len;
  const char* max32 = data + (len / sizeof(uint32_t))*sizeof(uint32_t);
  while (data < max32) {
    if (*(uint32_t*)data != 0) {
      return false;
    }
    data += sizeof(uint32_t);
  }
  while (data < max) {
    if (*(uint8_t*)data != 0) {
      return false;
    }
    data += sizeof(uint8_t);
  }
  return true;
}

#elif defined(__GNUC__) && defined(__aarch64__) && defined(__ARM_NEON) // gcc and aarch64 neon

#include <arm_neon.h>

static inline bool mem_is_zero(const char *data, size_t len) {
  const char *end = data + len;
  const char *end256 = data + (len / sizeof(uint64x2x2_t)) * sizeof(uint64x2x2_t);
  while (data < end256) {
    uint64x2x2_t value = vld1q_u64_x2((uint64_t *)data);
    if (value.val[0][0] != 0 || value.val[0][1] != 0 ||
        value.val[1][0] != 0 || value.val[1][1] != 0) {
      return false;
    }
    data += sizeof(uint64x2x2_t);
  }

  const char *end128 = data + sizeof(uint64x2_t);
  if (end128 < end) {
    uint64x2_t value = vld1q_u64((uint64_t *)data);
    if (value[0] != 0 || value[1] != 0) {
      return false;
    }
    data += sizeof(uint64x2_t);
  }

  const char *end64 = data + sizeof(uint64_t);
  if (end64 < end) {
    if(*(uint64_t *)data != 0) {
      return false;
    }
    data += sizeof(uint64_t);
  }

  while (data < end) {
    if (*data != 0) {
      return false;
    }
    ++data;
  }

  return true;
}

#else  // gcc and x86_64

static inline bool mem_is_zero(const char *data, size_t len) {
  const char *end = data + len;
  const char* end64 = data + (len / sizeof(uint64_t))*sizeof(uint64_t);

  while (data < end64) {
    if (*(uint64_t*)data != 0) {
      return false;
    }
    data += sizeof(uint64_t);
  }

  while (data < end) {
    if (*data != 0) {
      return false;
    }
    ++data;
  }
  return true;
}

#endif  // !x86_64

#endif
