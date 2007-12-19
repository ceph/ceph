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

#ifndef __EBOFS_CSUM_H
#define __EBOFS_CSUM_H

typedef __u64 csum_t;

/*
 * physically and logically aligned buffer.  yay.
 */
inline __u64 calc_csum(const char *start, int len) {
  // must be 64-bit aligned
  assert(((unsigned long)start & 7) == 0); 
  assert((len & 7) == 0);
  
  __u64 *p = (__u64*)start;
  __u64 *end = (__u64*)(start + len);
  __u64 csum = 0;
  while (p < end) {
    csum += *p;
    p++;
  }
  return csum;
}

/*
 * arbitrarily aligned buffer.  buffer alignment must match logical alignment.
 * i.e., buffer is aligned, but has non-aligned boundaries.
 */
inline __u64 calc_csum_unaligned(const char *start, int len) {
  const char *end = start + len;
  __u64 csum = 0;
  
  // front
  while (start < end && (unsigned long)start & 7) {
    csum += (__u64)(*start) << (8*(8 - ((unsigned long)start & 7)));
    start++;
  }
  if (start == end) 
    return csum;  

  // middle, aligned
  const char *fastend = end - 7;
  while (start < fastend) {
    csum += *(__u64*)start;
    start += sizeof(__u64);
  }

  // tail
  while (start < end) {
    csum += (__u64)(*start) << (8*(8 - ((unsigned long)start & 7)));
    start++;
  }
  return csum;
}


/*
 * arbitrarily aligned buffer, with arbitrary logical alignment
 */
inline __u64 calc_csum_realign(const char *start, int len, int off) {
  const char *end = start + len;
  __u64 csum = 0;
  
  if (((unsigned long)start & 7) == (off & 7))
    return calc_csum_unaligned(start, len);     // lucky us, start and off alignment matches.

  // do it the slow way.  yucky!
  while (start < end) {
    csum += (__u64)(*start) << (8*(8 - (off & 7)));
    start++; off++;
  }
  return csum;  
}

#endif
