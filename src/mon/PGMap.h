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

#ifndef CEPH_PGMAP_H
#define CEPH_PGMAP_H

#include "common/debug.h"
#include "osd/osd_types.h"
#include "common/config.h"
#include <sstream>

class PGMap {
public:
  // the map
  version_t version;
  epoch_t last_osdmap_epoch;   // last osdmap epoch i applied to the pgmap
  epoch_t last_pg_scan;  // osdmap epoch
  hash_map<pg_t,pg_stat_t> pg_stat;
  hash_map<int,osd_stat_t> osd_stat;
  set<int> full_osds;
  set<int> nearfull_osds;

  class Incremental {
  public:
    version_t version;
    map<pg_t,pg_stat_t> pg_stat_updates;
    map<int,osd_stat_t> osd_stat_updates;
    set<int> osd_stat_rm;
    epoch_t osdmap_epoch;
    epoch_t pg_scan;  // osdmap epoch
    set<pg_t> pg_remove;
    float full_ratio;
    float nearfull_ratio;

    void encode(bufferlist &bl) const {
      __u8 v = 2;
      ::encode(v, bl);
      ::encode(version, bl);
      ::encode(pg_stat_updates, bl);
      ::encode(osd_stat_updates, bl);
      ::encode(osd_stat_rm, bl);
      ::encode(osdmap_epoch, bl);
      ::encode(pg_scan, bl);
      ::encode(full_ratio, bl);
      ::encode(nearfull_ratio, bl);
      ::encode(pg_remove, bl);
    }
    void decode(bufferlist::iterator &bl) {
      __u8 v;
      ::decode(v, bl);
      ::decode(version, bl);
      ::decode(pg_stat_updates, bl);
      ::decode(osd_stat_updates, bl);
      ::decode(osd_stat_rm, bl);
      ::decode(osdmap_epoch, bl);
      ::decode(pg_scan, bl);
      if (v >= 2) {
        ::decode(full_ratio, bl);
        ::decode(nearfull_ratio, bl);
      }
      ::decode(pg_remove, bl);
    }

    Incremental() : version(0), osdmap_epoch(0), pg_scan(0),
        full_ratio(0), nearfull_ratio(0) {}
  };


  // aggregate stats (soft state)
  hash_map<int,int> num_pg_by_state;
  int64_t num_pg, num_osd;
  hash_map<int,pool_stat_t> pg_pool_sum;
  pool_stat_t pg_sum;
  osd_stat_t osd_sum;

  float full_ratio;
  float nearfull_ratio;

  set<pg_t> creating_pgs;   // lru: front = new additions, back = recently pinged
  
  PGMap() : version(0),
	    last_osdmap_epoch(0), last_pg_scan(0),
	    num_pg(0),
	    num_osd(0),
	    full_ratio(((float)g_conf->mon_osd_full_ratio)/100),
	    nearfull_ratio(((float)g_conf->mon_osd_nearfull_ratio)/100) {}

  void apply_incremental(const Incremental& inc);
  void redo_full_sets();
  void stat_zero();
  void stat_pg_add(const pg_t &pgid, const pg_stat_t &s);
  void stat_pg_sub(const pg_t &pgid, const pg_stat_t &s);
  void stat_osd_add(const osd_stat_t &s);
  void stat_osd_sub(const osd_stat_t &s);
  
  void encode(bufferlist &bl);
  void decode(bufferlist::iterator &bl);

  void dump(Formatter *f) const; 
  void dump_basic(Formatter *f) const;
  void dump_pg_stats(Formatter *f) const;
  void dump_pool_stats(Formatter *f) const;
  void dump_osd_stats(Formatter *f) const;
  void dump(ostream& ss) const;

  void state_summary(ostream& ss) const;
  void recovery_summary(ostream& out) const;
  void print_summary(ostream& out) const;

};

inline ostream& operator<<(ostream& out, const PGMap& m) {
  m.print_summary(out);
  return out;
}

#endif
