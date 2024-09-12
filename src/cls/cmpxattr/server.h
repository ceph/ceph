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
#include "include/buffer.h"
#include "include/encoding.h"
#include <time.h>
#include <string>
#include "include/utime.h"
#include "common/ceph_time.h"
#include "common/Clock.h"

using namespace cls::cmpxattr;
struct named_time_lock_t {
  bool is_urgent_stop_msg() const {
    return( (urgent_msg == URGENT_MSG_STOP) ||
	    (urgent_msg == URGENT_MSG_PASUE) );
  }

  utime_t     creation_time = utime_t();
  utime_t     completion_time = utime_t();
  utime_t     max_lock_duration = utime_t();
  utime_t     lock_time = utime_t();
  uint64_t    progress_a = 0;
  uint64_t    progress_b = 0 ;
  std::string owner;
  std::string prev_owner;
  int32_t     urgent_msg = URGENT_MSG_NONE;
};

static inline void encode(const named_time_lock_t& ntl, ceph::bufferlist& bl)
{
  ENCODE_START(1, 1, bl);
  encode(ntl.creation_time, bl);
  encode(ntl.completion_time, bl);
  encode(ntl.max_lock_duration, bl);
  encode(ntl.lock_time, bl);
  encode(ntl.progress_a, bl);
  encode(ntl.progress_b, bl);
  encode(ntl.owner, bl);
  encode(ntl.prev_owner, bl);
  encode(ntl.urgent_msg, bl);
  ENCODE_FINISH(bl);
}

static inline void decode(named_time_lock_t& ntl, ceph::bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(ntl.creation_time, bl);
  decode(ntl.completion_time, bl);
  decode(ntl.max_lock_duration, bl);
  decode(ntl.lock_time, bl);
  decode(ntl.progress_a, bl);
  decode(ntl.progress_b, bl);
  decode(ntl.owner, bl);
  decode(ntl.prev_owner, bl);
  decode(ntl.urgent_msg, bl);
  DECODE_FINISH(bl);
}
