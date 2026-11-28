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

#ifndef CEPH_MON_POOL_AVAILABILITY_H
#define CEPH_MON_POOL_AVAILABILITY_H

#include <cstdint>
#include <list>
#include <string>

#include "include/encoding.h"
#include "include/utime.h"

namespace ceph { class Formatter; }

struct PoolAvailability {
  std::string pool_name  = "";
  utime_t started_at;
  uint64_t uptime = 0;
  utime_t last_uptime;
  uint64_t downtime = 0;
  utime_t last_downtime;
  uint64_t num_failures = 0;
  bool is_avail = true;

  PoolAvailability();

  void dump(ceph::Formatter *f) const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &p);

  static std::list<PoolAvailability> generate_test_instances();
};
WRITE_CLASS_ENCODER(PoolAvailability)

#endif
