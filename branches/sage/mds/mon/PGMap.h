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

#ifndef __PGMAP_H
#define __PGMAP_H

#include "osd/osd_types.h"

class PGMap {
public:
  // the map
  version_t version;
  hash_map<pg_t,pg_stat_t> pg_stat;

  class Incremental {
  public:
    version_t version;
    map<pg_t,pg_stat_t> pg_stat_updates;

    void _encode(bufferlist &bl) {
      ::_encode(version, bl);
      ::_encode(pg_stat_updates, bl);
    }
    void _decode(bufferlist& bl, int& off) {
      ::_decode(version, bl, off);
      ::_decode(pg_stat_updates, bl, off);
    }
  };

  void apply_incremental(Incremental& inc) {
    assert(inc.version == version+1);
    version++;
    for (map<pg_t,pg_stat_t>::iterator p = inc.pg_stat_updates.begin();
	 p != inc.pg_stat_updates.end();
	 ++p) {
      if (pg_stat.count(p->first))
	stat_sub(pg_stat[p->first]);
      pg_stat[p->first] = p->second;
      stat_add(p->second);
    }
  }

  // aggregate stats (soft state)
  hash_map<int,int> num_pg_by_state;
  int64_t num_pg;
  int64_t total_size;
  int64_t total_num_blocks;
  
  void stat_zero() {
    num_pg = 0;
    num_pg_by_state.clear();
    total_size = 0;
    total_num_blocks = 0;
  }
  void stat_add(pg_stat_t &s) {
    num_pg++;
    num_pg_by_state[s.state]++;
    total_size += s.size;
    total_num_blocks += s.num_blocks;
  }
  void stat_sub(pg_stat_t &s) {
    num_pg--;
    num_pg_by_state[s.state]--;
    total_size -= s.size;
    total_num_blocks -= s.num_blocks;
  }

  PGMap() : version(0), 
	    num_pg(0), total_size(0), total_num_blocks(0) {}

  void _encode(bufferlist &bl) {
    ::_encode(version, bl);
    ::_encode(pg_stat, bl);
  }
  void _decode(bufferlist& bl, int& off) {
    ::_decode(version, bl, off);
    ::_decode(pg_stat, bl, off);
    stat_zero();
    for (hash_map<pg_t,pg_stat_t>::iterator p = pg_stat.begin();
	 p != pg_stat.end();
	 ++p)
      stat_add(p->second);
  }
};

#endif
