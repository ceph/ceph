// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "mon/PoolAvailability.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/Formatter.h"
#include "include/encoding_string.h"

PoolAvailability::PoolAvailability()
  :started_at(ceph_clock_now()),
   last_uptime(started_at),
   last_downtime(started_at) {}

void PoolAvailability::dump(ceph::Formatter *f) const {
  ceph_assert(f != NULL);
  f->dump_stream("pool_name") << pool_name;
  f->dump_stream("started_at") << started_at;
  f->dump_int("uptime", uptime);
  f->dump_stream("last_uptime") << last_uptime;
  f->dump_int("downtime", downtime);
  f->dump_stream("last_downtime") << last_downtime;
  f->dump_int("num_failures", num_failures);
  f->dump_bool("is_avail", is_avail);
}

void PoolAvailability::encode(ceph::buffer::list &bl) const {
  ENCODE_START(1, 1, bl);
  encode(pool_name, bl);
  encode(started_at, bl);
  encode(uptime, bl);
  encode(last_uptime, bl);
  encode(downtime, bl);
  encode(last_downtime, bl);
  encode(num_failures, bl);
  encode(is_avail, bl);
  ENCODE_FINISH(bl);
}

void PoolAvailability::decode(ceph::buffer::list::const_iterator &p) {
  DECODE_START(1, p);
  decode(pool_name, p);
  decode(started_at, p);
  decode(uptime, p);
  decode(last_uptime, p);
  decode(downtime, p);
  decode(last_downtime, p);
  decode(num_failures, p);
  decode(is_avail, p);
  DECODE_FINISH(p);
}

std::list<PoolAvailability> PoolAvailability::generate_test_instances() {
  std::list<PoolAvailability> o;
  o.emplace_back();
  o.back().started_at = utime_t(123, 456);
  o.back().last_uptime = utime_t(123, 456);
  o.back().last_downtime = utime_t(123, 456);
  o.emplace_back();
  o.back().pool_name = "foo";
  o.back().started_at = utime_t(123, 456);
  o.back().uptime = 100;
  o.back().last_uptime = utime_t(123, 456);
  o.back().downtime = 15;
  o.back().last_downtime = utime_t(123, 456);
  o.back().num_failures = 2;
  o.back().is_avail = true;
  return o;
}  
