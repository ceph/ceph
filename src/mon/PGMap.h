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
 
/*
 * Placement Group Map. Placement Groups are logical sets of objects
 * that are replicated by the same set of devices. pgid=(r,hash(o)&m)
 * where & is a bit-wise AND and m=2^k-1
 */

#ifndef __PGMAP_H
#define __PGMAP_H

#include "osd/osd_types.h"

class PGMap {
public:
  // the map
  version_t version;
  hash_map<pg_t,pg_stat_t> pg_stat;
  hash_map<int,osd_stat_t> osd_stat;

  class Incremental {
  public:
    version_t version;
    map<pg_t,pg_stat_t> pg_stat_updates;
    map<int,osd_stat_t> osd_stat_updates;

    void _encode(bufferlist &bl) {
      ::_encode(version, bl);
      ::_encode(pg_stat_updates, bl);
      ::_encode(osd_stat_updates, bl);
    }
    void _decode(bufferlist& bl, int& off) {
      ::_decode(version, bl, off);
      ::_decode(pg_stat_updates, bl, off);
      ::_decode(osd_stat_updates, bl, off);
    }

    Incremental() : version(0) {}
  };

  void apply_incremental(Incremental& inc) {
    assert(inc.version == version+1);
    version++;
    for (map<pg_t,pg_stat_t>::iterator p = inc.pg_stat_updates.begin();
	 p != inc.pg_stat_updates.end();
	 ++p) {
      if (pg_stat.count(p->first))
	stat_pg_sub(pg_stat[p->first]);
      pg_stat[p->first] = p->second;
      stat_pg_add(p->second);
    }
    for (map<int,osd_stat_t>::iterator p = inc.osd_stat_updates.begin();
	 p != inc.osd_stat_updates.end();
	 ++p) {
      if (osd_stat.count(p->first))
	stat_osd_sub(osd_stat[p->first]);
      osd_stat[p->first] = p->second;
      stat_osd_add(p->second);
    }
  }

  // aggregate stats (soft state)
  hash_map<int,int> num_pg_by_state;
  int64_t num_pg;
  int64_t total_pg_num_bytes;
  int64_t total_pg_num_blocks;
  int64_t total_pg_num_objects;
  int64_t num_osd;
  int64_t total_osd_num_blocks;
  int64_t total_osd_num_blocks_avail;
  int64_t total_osd_num_objects;
  
  void stat_zero() {
    num_pg = 0;
    num_pg_by_state.clear();
    total_pg_num_bytes = 0;
    total_pg_num_blocks = 0;
    total_pg_num_objects = 0;
    num_osd = 0;
    total_osd_num_blocks = 0;
    total_osd_num_blocks_avail = 0;
    total_osd_num_objects = 0;
  }
  void stat_pg_add(pg_stat_t &s) {
    num_pg++;
    num_pg_by_state[s.state]++;
    total_pg_num_bytes += s.num_bytes;
    total_pg_num_blocks += s.num_blocks;
    total_pg_num_objects += s.num_objects;
  }
  void stat_osd_add(osd_stat_t &s) {
    num_osd++;
    total_osd_num_blocks += s.num_blocks;
    total_osd_num_blocks_avail += s.num_blocks_avail;
    total_osd_num_objects += s.num_objects;
  }
  void stat_pg_sub(pg_stat_t &s) {
    num_pg--;
    num_pg_by_state[s.state]--;
    total_pg_num_bytes -= s.num_bytes;
    total_pg_num_blocks -= s.num_blocks;
    total_pg_num_objects -= s.num_objects;
  }
  void stat_osd_sub(osd_stat_t &s) {
    num_osd--;
    total_osd_num_blocks -= s.num_blocks;
    total_osd_num_blocks_avail -= s.num_blocks_avail;
    total_osd_num_objects -= s.num_objects;
  }

  PGMap() : version(0), 
	    num_pg(0), 
	    total_pg_num_bytes(0), 
	    total_pg_num_blocks(0), 
	    total_pg_num_objects(0), 
	    num_osd(0),
	    total_osd_num_blocks(0),
	    total_osd_num_blocks_avail(0),
	    total_osd_num_objects(0) {}

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
      stat_pg_add(p->second);
    for (hash_map<int,osd_stat_t>::iterator p = osd_stat.begin();
	 p != osd_stat.end();
	 ++p)
      stat_osd_add(p->second);
  }
};

#endif
