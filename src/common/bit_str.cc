// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/bit_str.h"
#include "common/Formatter.h"
#include "include/ceph_assert.h"

static void _dump_bit_str(
    uint64_t bits,
    std::ostream *out,
    ceph::Formatter *f,
    std::function<const char*(uint64_t)> func,
    bool dump_bit_val)
{
  uint64_t b = bits;
  int cnt = 0;
  bool outted = false;

  while (b && cnt < 64) {
    uint64_t r = bits & (1ULL << cnt++);
    if (r) {
      if (out) {
        if (outted)
          *out << ",";
        *out << func(r);
        if (dump_bit_val) {
          *out << "(" << r << ")";
        }
      } else {
        ceph_assert(f != NULL);
        if (dump_bit_val) {
          f->dump_stream("bit_flag") << func(r)
                                     << "(" << r << ")";
        } else {
          f->dump_stream("bit_flag") << func(r);
        }
      }
      outted = true;
    }
    b >>= 1;
  }
  if (!outted && out)
      *out << "none";
}

void print_bit_str(
    uint64_t bits,
    std::ostream &out,
    const std::function<const char*(uint64_t)> &func,
    bool dump_bit_val)
{
  _dump_bit_str(bits, &out, NULL, func, dump_bit_val);
}

void dump_bit_str(
    uint64_t bits,
    ceph::Formatter *f,
    const std::function<const char*(uint64_t)> &func,
    bool dump_bit_val)
{
  _dump_bit_str(bits, NULL, f, func, dump_bit_val);
}
