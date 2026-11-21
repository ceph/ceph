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

#ifndef CEPH_MON_DATA_STATS_H
#define CEPH_MON_DATA_STATS_H

#include <list>

#include "include/encoding.h"
#include "include/util.h" // for ceph_data_stats_t
#include "include/utime.h"
#include "mon/MonitorDBStoreStats.h"

namespace ceph { class Formatter; }

struct DataStats {
  ceph_data_stats_t fs_stats;
  // data dir
  utime_t last_update;
  MonitorDBStoreStats store_stats;

  void dump(ceph::Formatter *f) const;
  static std::list<DataStats> generate_test_instances();

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &p);
};
WRITE_CLASS_ENCODER(DataStats)

#endif
