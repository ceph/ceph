// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_COMMON_H
#define CEPH_OSD_BLUESTORE_COMMON_H

#include "include/intarith.h"
#include "include/ceph_assert.h"

template <class Bitset, class Func>
void apply_for_bitset_range(uint64_t off,
  uint64_t len,
  uint64_t granularity,
  Bitset &bitset,
  Func f) {
  auto end = round_up_to(off + len, granularity) / granularity;
  ceph_assert(end <= bitset.size());
  uint64_t pos = off / granularity;
  while (pos < end) {
    f(pos, bitset);
    pos++;
  }
}

#endif
