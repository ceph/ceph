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
#include "common/Formatter.h"

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

  void dump(ceph::Formatter *f) const {
    ceph_assert(f != NULL);
    f->dump_int("bytes_total", bytes_total);
    f->dump_int("bytes_sst", bytes_sst);
    f->dump_int("bytes_log", bytes_log);
    f->dump_int("bytes_misc", bytes_misc);
    f->dump_stream("last_updated") << last_update;
  }

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(bytes_total, bl);
    encode(bytes_sst, bl);
    encode(bytes_log, bl);
    encode(bytes_misc, bl);
    encode(last_update, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator &p) {
    DECODE_START(1, p);
    decode(bytes_total, p);
    decode(bytes_sst, p);
    decode(bytes_log, p);
    decode(bytes_misc, p);
    decode(last_update, p);
    DECODE_FINISH(p);
  }

  static std::list<MonitorDBStoreStats> generate_test_instances() {
    std::list<MonitorDBStoreStats> ls;
    ls.emplace_back();
    ls.emplace_back();
    ls.back().bytes_total = 1024*1024;
    ls.back().bytes_sst = 512*1024;
    ls.back().bytes_log = 256*1024;
    ls.back().bytes_misc = 256*1024;
    ls.back().last_update = utime_t();
    return ls;
  }
};
WRITE_CLASS_ENCODER(MonitorDBStoreStats)

#endif
