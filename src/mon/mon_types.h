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

// data stats

struct DataStats {
  // data dir
  uint64_t kb_total;
  uint64_t kb_used;
  uint64_t kb_avail;
  int latest_avail_percent;
  utime_t last_update;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(kb_total, bl);
    ::encode(kb_used, bl);
    ::encode(kb_avail, bl);
    ::encode(latest_avail_percent, bl);
    ::encode(last_update, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(kb_total, p);
    ::decode(kb_used, p);
    ::decode(kb_avail, p);
    ::decode(latest_avail_percent, p);
    ::decode(last_update, p);
    DECODE_FINISH(p);
  }
};

WRITE_CLASS_ENCODER(DataStats);

#endif
