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

#include "mon/DataStats.h"
#include "common/Formatter.h"

void DataStats::dump(ceph::Formatter *f) const {
  ceph_assert(f != NULL);
  f->dump_int("kb_total", (fs_stats.byte_total/1024));
  f->dump_int("kb_used", (fs_stats.byte_used/1024));
  f->dump_int("kb_avail", (fs_stats.byte_avail/1024));
  f->dump_int("avail_percent", fs_stats.avail_percent);
  f->dump_stream("last_updated") << last_update;
  f->open_object_section("store_stats");
  store_stats.dump(f);
  f->close_section();
}

std::list<DataStats> DataStats::generate_test_instances() {
  std::list<DataStats> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().fs_stats.byte_total = 1024*1024;
  ls.back().fs_stats.byte_used = 512*1024;
  ls.back().fs_stats.byte_avail = 256*1024;
  ls.back().fs_stats.avail_percent = 50;
  ls.back().last_update = utime_t();
  ls.back().store_stats.bytes_total = 1024*1024;
  ls.back().store_stats.bytes_sst = 512*1024;
  ls.back().store_stats.bytes_log = 256*1024;
  ls.back().store_stats.bytes_misc = 256*1024;
  ls.back().store_stats.last_update = utime_t();
  return ls;
}

void DataStats::encode(ceph::buffer::list &bl) const {
  ENCODE_START(3, 1, bl);
  encode(fs_stats.byte_total, bl);
  encode(fs_stats.byte_used, bl);
  encode(fs_stats.byte_avail, bl);
  encode(fs_stats.avail_percent, bl);
  encode(last_update, bl);
  encode(store_stats, bl);
  ENCODE_FINISH(bl);
}

void DataStats::decode(ceph::buffer::list::const_iterator &p) {
  DECODE_START(3, p);
  // we moved from having fields in kb to fields in byte
  if (struct_v > 2) {
    decode(fs_stats.byte_total, p);
    decode(fs_stats.byte_used, p);
    decode(fs_stats.byte_avail, p);
  } else {
    uint64_t t;
    decode(t, p);
    fs_stats.byte_total = t*1024;
    decode(t, p);
    fs_stats.byte_used = t*1024;
    decode(t, p);
    fs_stats.byte_avail = t*1024;
  }
  decode(fs_stats.avail_percent, p);
  decode(last_update, p);
  if (struct_v > 1)
    decode(store_stats, p);

  DECODE_FINISH(p);
}
