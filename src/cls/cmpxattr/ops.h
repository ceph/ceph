// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include "types.h"
#include "include/encoding.h"
#include <time.h>
#include "include/utime.h"
#include "common/ceph_time.h"

namespace cls::cmpxattr {

  struct cmp_vals_set_vals_op {
    Mode mode;
    Op comparison;
    ComparisonMap cmp_pairs;
    std::map<std::string, ceph::bufferlist> set_pairs;
  };

  inline void encode(const cmp_vals_set_vals_op& o, ceph::bufferlist& bl, uint64_t f=0)
  {
    ENCODE_START(1, 1, bl);
    encode(o.mode, bl);
    encode(o.comparison, bl);
    encode(o.cmp_pairs, bl);
    encode(o.set_pairs, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(cmp_vals_set_vals_op& o, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(o.mode, bl);
    decode(o.comparison, bl);
    decode(o.cmp_pairs, bl);
    decode(o.set_pairs, bl);
    DECODE_FINISH(bl);
  }

  //===========================================================================
  struct lock_update_op {
    utime_t     max_lock_duration; // max duration for holding a lock
    std::string owner;
    std::string key_name;
  };

  inline void encode(const lock_update_op& o, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(o.max_lock_duration, bl);
    encode(o.owner, bl);
    encode(o.key_name, bl);
    ENCODE_FINISH(bl);
  }

  inline void decode(lock_update_op& o, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    decode(o.max_lock_duration, bl);
    decode(o.owner, bl);
    decode(o.key_name, bl);
    DECODE_FINISH(bl);
  }

} // namespace cls::cmpxattr
