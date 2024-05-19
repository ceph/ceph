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

#include "reverse.h"

uint32_t reverse_bits(uint32_t v) {
  if (v == 0)
    return v;

  /* reverse bits
   * swap odd and even bits
   */
  v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
  /* swap consecutive pairs */
  v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
  /* swap nibbles ... */
  v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
  /* swap bytes */
  v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
  /* swap 2-byte long pairs */
  v = ( v >> 16             ) | ( v               << 16);                                
  return v;
}

uint32_t reverse_nibbles(uint32_t retval) {
  /* reverse nibbles */
  retval = ((retval & 0x0f0f0f0f) << 4) | ((retval & 0xf0f0f0f0) >> 4);
  retval = ((retval & 0x00ff00ff) << 8) | ((retval & 0xff00ff00) >> 8);
  retval = ((retval & 0x0000ffff) << 16) | ((retval & 0xffff0000) >> 16);
  return retval;
}
