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

#ifndef CEPH_MON_MONITOR_DB_STORE_STATS_H
#define CEPH_MON_MONITOR_DB_STORE_STATS_H

#include <cstdint>
#include <list>

#include "include/encoding.h"
#include "include/utime.h"

namespace ceph { class Formatter; }

/**
 * monitor db store stats
 */
struct MonitorDBStoreStats {
  uint64_t bytes_total;
  uint64_t bytes_sst;
  uint64_t bytes_log;
  uint64_t bytes_misc;
  utime_t last_update;

  MonitorDBStoreStats() :
    bytes_total(0),
    bytes_sst(0),
    bytes_log(0),
    bytes_misc(0)
  {}

  void dump(ceph::Formatter *f) const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &p);

  static std::list<MonitorDBStoreStats> generate_test_instances();
};
WRITE_CLASS_ENCODER(MonitorDBStoreStats)

#endif
