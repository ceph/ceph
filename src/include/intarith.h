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

#ifndef CEPH_INTARITH_H
#define CEPH_INTARITH_H

#ifndef MIN
# define MIN(a,b) ((a) < (b) ? (a):(b))
#endif

#ifndef MAX
# define MAX(a,b) ((a) > (b) ? (a):(b))
#endif

#ifndef DIV_ROUND_UP
# define DIV_ROUND_UP(n, d)  (((n) + (d) - 1) / (d))
#endif

#ifndef ROUND_UP_TO
# define ROUND_UP_TO(n, d) ((n)%(d) ? ((n)+(d)-(n)%(d)) : (n))
#endif

#ifndef SHIFT_ROUND_UP
# define SHIFT_ROUND_UP(x,y) (((x)+(1<<(y))-1) >> (y))
#endif

// count trailing zeros.
// NOTE: the builtin is nondeterministic on 0 input
static inline unsigned ctz(unsigned v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_ctz(v);
}
static inline unsigned ctzl(unsigned long v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_ctzl(v);
}
static inline unsigned ctzll(unsigned long long v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_ctzll(v);
}

// count leading zeros
// NOTE: the builtin is nondeterministic on 0 input
static inline unsigned clz(unsigned v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_clz(v);
}
static inline unsigned clzl(unsigned long v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_clzl(v);
}
static inline unsigned clzll(unsigned long long v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_clzll(v);
}

// count bits (set + any 0's that follow)
static inline unsigned cbits(unsigned v) {
  if (v == 0)
    return 0;
  return (sizeof(v) * 8) - __builtin_clz(v);
}
static inline unsigned cbitsl(unsigned long v) {
  if (v == 0)
    return 0;
  return (sizeof(v) * 8) - __builtin_clzl(v);
}
static inline unsigned cbitsll(unsigned long long v) {
  if (v == 0)
    return 0;
  return (sizeof(v) * 8) - __builtin_clzll(v);
}

#endif
