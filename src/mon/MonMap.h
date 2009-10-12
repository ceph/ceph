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

#ifndef __MONMAP_H
#define __MONMAP_H

#include "include/err.h"

#include "msg/Message.h"
#include "include/types.h"
//#include "config.h"

class MonMap {
 public:
  epoch_t epoch;       // what epoch/version of the monmap
  ceph_fsid_t fsid;
  vector<entity_inst_t> mon_inst;
  utime_t last_changed;
  utime_t created;

  MonMap() : epoch(0) {
    memset(&fsid, 0, sizeof(fsid));
    last_changed = created = g_clock.now();
  }

  ceph_fsid_t& get_fsid() { return fsid; }

  unsigned size() {
    return mon_inst.size();
  }

  epoch_t get_epoch() { return epoch; }

  void add_mon(entity_inst_t inst) {
    mon_inst.push_back(inst);
  }

  void add(entity_addr_t a) {
    entity_inst_t i;
    i.addr = a;
    i.name = entity_name_t::MON(mon_inst.size());
    mon_inst.push_back(i);
  }
  bool remove(entity_addr_t a) {
    for (unsigned i=0; i<mon_inst.size(); i++) {
      if (mon_inst[i].addr == a) {
	for (; i < mon_inst.size()-1; i++) 
	  mon_inst[i].addr = mon_inst[i+1].addr;
	mon_inst.pop_back();
	return true;
      }
    }
    return false;
  }
  bool contains(entity_addr_t a) {
    for (unsigned i=0; i<mon_inst.size(); i++)
      if (mon_inst[i].addr == a) 
	return true;
    return false;
  }

  const entity_inst_t &get_inst(unsigned m) {
    assert(m < mon_inst.size());
    return mon_inst[m];
  }

  void encode(bufferlist& blist) {
    __u16 v = 1;
    ::encode(v, blist);
    ::encode_raw(fsid, blist);
    ::encode(epoch, blist);
    ::encode(mon_inst, blist);
    ::encode(last_changed, blist);
    ::encode(created, blist);
  }  
  void decode(bufferlist& blist) {
    bufferlist::iterator p = blist.begin();
    decode(p);
  }
  void decode(bufferlist::iterator &p) {
    __u16 v;
    ::decode(v, p);
    ::decode_raw(fsid, p);
    ::decode(epoch, p);
    ::decode(mon_inst, p);
    ::decode(last_changed, p);
    ::decode(created, p);
  }


  void generate_fsid() {
    for (int i=0; i<16; i++)
      fsid.fsid[i] = rand();
  }

  // read from/write to a file
  int write(const char *fn);
  int read(const char *fn);

  void print(ostream& out);
  void print_summary(ostream& out);
};

inline void encode(MonMap &m, bufferlist &bl) {
  m.encode(bl);
}
inline void decode(MonMap &m, bufferlist::iterator &p) {
  m.decode(p);
}

inline ostream& operator<<(ostream& out, MonMap& m) {
  m.print_summary(out);
  return out;
}

#endif
