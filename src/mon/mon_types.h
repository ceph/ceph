// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef CEPH_MON_TYPES_H
#define CEPH_MON_TYPES_H

#include "include/utime.h"
#include "include/util.h"
#include "common/Formatter.h"
#include "include/Context.h"
#include "mon/MonOpRequest.h"

#define PAXOS_PGMAP      0  // before osd, for pg kick to behave
#define PAXOS_MDSMAP     1
#define PAXOS_OSDMAP     2
#define PAXOS_LOG        3
#define PAXOS_MONMAP     4
#define PAXOS_AUTH       5
#define PAXOS_NUM        6

inline const char *get_paxos_name(int p) {
  switch (p) {
  case PAXOS_MDSMAP: return "mdsmap";
  case PAXOS_MONMAP: return "monmap";
  case PAXOS_OSDMAP: return "osdmap";
  case PAXOS_PGMAP: return "pgmap";
  case PAXOS_LOG: return "logm";
  case PAXOS_AUTH: return "auth";
  default: assert(0); return 0;
  }
}

#define CEPH_MON_ONDISK_MAGIC "ceph mon volume v012"

/**
 * leveldb store stats
 *
 * If we ever decide to support multiple backends for the monitor store,
 * we should then create an abstract class 'MonitorStoreStats' of sorts
 * and inherit it on LevelDBStoreStats.  I'm sure you'll figure something
 * out.
 */
struct LevelDBStoreStats {
  uint64_t bytes_total;
  uint64_t bytes_sst;
  uint64_t bytes_log;
  uint64_t bytes_misc;
  utime_t last_update;

  LevelDBStoreStats() :
    bytes_total(0),
    bytes_sst(0),
    bytes_log(0),
    bytes_misc(0)
  {}

  void dump(Formatter *f) const {
    assert(f != NULL);
    f->dump_int("bytes_total", bytes_total);
    f->dump_int("bytes_sst", bytes_sst);
    f->dump_int("bytes_log", bytes_log);
    f->dump_int("bytes_misc", bytes_misc);
    f->dump_stream("last_updated") << last_update;
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(bytes_total, bl);
    ::encode(bytes_sst, bl);
    ::encode(bytes_log, bl);
    ::encode(bytes_misc, bl);
    ::encode(last_update, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(bytes_total, p);
    ::decode(bytes_sst, p);
    ::decode(bytes_log, p);
    ::decode(bytes_misc, p);
    ::decode(last_update, p);
    DECODE_FINISH(p);
  }

  static void generate_test_instances(list<LevelDBStoreStats*>& ls) {
    ls.push_back(new LevelDBStoreStats);
    ls.push_back(new LevelDBStoreStats);
    ls.back()->bytes_total = 1024*1024;
    ls.back()->bytes_sst = 512*1024;
    ls.back()->bytes_log = 256*1024;
    ls.back()->bytes_misc = 256*1024;
    ls.back()->last_update = utime_t();
  }
};
WRITE_CLASS_ENCODER(LevelDBStoreStats)

// data stats

struct DataStats {
  ceph_data_stats_t fs_stats;
  // data dir
  utime_t last_update;
  LevelDBStoreStats store_stats;

  void dump(Formatter *f) const {
    assert(f != NULL);
    f->dump_int("kb_total", (fs_stats.byte_total/1024));
    f->dump_int("kb_used", (fs_stats.byte_used/1024));
    f->dump_int("kb_avail", (fs_stats.byte_avail/1024));
    f->dump_int("avail_percent", fs_stats.avail_percent);
    f->dump_stream("last_updated") << last_update;
    f->open_object_section("store_stats");
    store_stats.dump(f);
    f->close_section();
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(3, 1, bl);
    ::encode(fs_stats.byte_total, bl);
    ::encode(fs_stats.byte_used, bl);
    ::encode(fs_stats.byte_avail, bl);
    ::encode(fs_stats.avail_percent, bl);
    ::encode(last_update, bl);
    ::encode(store_stats, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    // we moved from having fields in kb to fields in byte
    if (struct_v > 2) {
      ::decode(fs_stats.byte_total, p);
      ::decode(fs_stats.byte_used, p);
      ::decode(fs_stats.byte_avail, p);
    } else {
      uint64_t t;
      ::decode(t, p);
      fs_stats.byte_total = t*1024;
      ::decode(t, p);
      fs_stats.byte_used = t*1024;
      ::decode(t, p);
      fs_stats.byte_avail = t*1024;
    }
    ::decode(fs_stats.avail_percent, p);
    ::decode(last_update, p);
    if (struct_v > 1)
      ::decode(store_stats, p);

    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(DataStats)

struct ScrubResult {
  map<string,uint32_t> prefix_crc;  ///< prefix -> crc
  map<string,uint64_t> prefix_keys; ///< prefix -> key count

  bool operator!=(const ScrubResult& other) {
    return prefix_crc != other.prefix_crc || prefix_keys != other.prefix_keys;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(prefix_crc, bl);
    ::encode(prefix_keys, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p) {
    DECODE_START(1, p);
    ::decode(prefix_crc, p);
    ::decode(prefix_keys, p);
    DECODE_FINISH(p);
  }
  void dump(Formatter *f) const {
    f->open_object_section("crc");
    for (map<string,uint32_t>::const_iterator p = prefix_crc.begin(); p != prefix_crc.end(); ++p)
      f->dump_unsigned(p->first.c_str(), p->second);
    f->close_section();
    f->open_object_section("keys");
    for (map<string,uint64_t>::const_iterator p = prefix_keys.begin(); p != prefix_keys.end(); ++p)
      f->dump_unsigned(p->first.c_str(), p->second);
    f->close_section();
  }
  static void generate_test_instances(list<ScrubResult*>& ls) {
    ls.push_back(new ScrubResult);
    ls.push_back(new ScrubResult);
    ls.back()->prefix_crc["foo"] = 123;
    ls.back()->prefix_keys["bar"] = 456;
  }
};
WRITE_CLASS_ENCODER(ScrubResult)

static inline ostream& operator<<(ostream& out, const ScrubResult& r) {
  return out << "ScrubResult(keys " << r.prefix_keys << " crc " << r.prefix_crc << ")";
}

/// for information like os, kernel, hostname, memory info, cpu model.
typedef map<string, string> Metadata;

struct C_MonOp : public Context
{
  MonOpRequestRef op;

  explicit C_MonOp(MonOpRequestRef o) :
    op(o) { }

  void finish(int r) {
    if (op && r == -ECANCELED) {
      op->mark_event("callback canceled");
    } else if (op && r == -EAGAIN) {
      op->mark_event("callback retry");
    } else if (op && r == 0) {
      op->mark_event("callback finished");
    }
    _finish(r);
  }

  void mark_op_event(const string &event) {
    if (op)
      op->mark_event(event);
  }

  virtual void _finish(int r) = 0;
};

#endif
