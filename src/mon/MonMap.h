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

#ifndef CEPH_MONMAP_H
#define CEPH_MONMAP_H

#include "include/err.h"

#include "msg/Message.h"
#include "include/types.h"
//#include "config.h"

class MonMap {
 public:
  epoch_t epoch;       // what epoch/version of the monmap
  ceph_fsid_t fsid;
  map<string, entity_addr_t> mon_addr;
  utime_t last_changed;
  utime_t created;

  vector<string> rank_name;
  vector<entity_addr_t> rank_addr;
  
  void calc_ranks() {
    rank_name.resize(mon_addr.size());
    rank_addr.resize(mon_addr.size());
    unsigned i = 0;
    for (map<string,entity_addr_t>::iterator p = mon_addr.begin();
	 p != mon_addr.end();
	 p++, i++) {
      rank_name[i] = p->first;
      rank_addr[i] = p->second;
    }
  }

  MonMap() : epoch(0) {
    memset(&fsid, 0, sizeof(fsid));
    last_changed = created = g_clock.now();
  }

  ceph_fsid_t& get_fsid() { return fsid; }

  unsigned size() {
    return mon_addr.size();
  }

  const string& pick_random_mon() {
    unsigned n = rand() % rank_name.size();
    return rank_name[n];
  }
  const string& pick_random_mon_not(const string& butnot) {
    unsigned n = rand() % rank_name.size();
    if (rank_name[n] == butnot && rank_name.size() > 1) {
      if (n)
	n--;
      else
	n++;
    }
    return rank_name[n];
  }

  epoch_t get_epoch() { return epoch; }

  void add(const string &name, const entity_addr_t &addr) {
    assert(mon_addr.count(name) == 0);
    mon_addr[name] = addr;
    calc_ranks();
  }
  
  void remove(const string &name) {
    assert(mon_addr.count(name));
    mon_addr.erase(name);
    calc_ranks();
  }

  bool contains(const string& name) {
    return mon_addr.count(name);
  }

  bool contains(const entity_addr_t &a) {
    for (map<string,entity_addr_t>::iterator p = mon_addr.begin();
	 p != mon_addr.end();
	 p++)
      if (p->second == a)
	return true;
    return false;
  }

  int get_rank(const string& n) {
    for (unsigned i=0; i<rank_name.size(); i++)
      if (rank_name[i] == n)
	return i;
    return -1;
  }
  bool get_addr_name(entity_addr_t a, string& name) {
    for (map<string,entity_addr_t>::iterator p = mon_addr.begin();
	 p != mon_addr.end();
	 p++)
      if (p->second == a) {
	name = p->first;
	return true;
      }
    return false;
  }

  const entity_addr_t& get_addr(const string& n) {
    assert(mon_addr.count(n));
    return mon_addr[n];
  }
  const entity_addr_t& get_addr(unsigned m) {
    assert(m < rank_addr.size());
    return rank_addr[m];
  }
  entity_inst_t get_inst(const string& n) {
    assert(mon_addr.count(n));
    int m = get_rank(n);
    entity_inst_t i;
    i.addr = rank_addr[m];
    i.name = entity_name_t::MON(m);
    return i;
  }
  entity_inst_t get_inst(unsigned m) {
    assert(m < rank_addr.size());
    entity_inst_t i;
    i.addr = rank_addr[m];
    i.name = entity_name_t::MON(m);
    return i;
  }

  void encode(bufferlist& blist) {
    __u16 v = 2;
    ::encode(v, blist);
    ::encode_raw(fsid, blist);
    ::encode(epoch, blist);
    ::encode(mon_addr, blist);
    ::encode(last_changed, blist);
    ::encode(created, blist);
  }
  void encode_v1(bufferlist& blist) {
    __u16 v = 1;
    ::encode(v, blist);
    ::encode_raw(fsid, blist);
    ::encode(epoch, blist);
    vector<entity_inst_t> mon_inst(mon_addr.size());
    for (unsigned n = 0; n < mon_addr.size(); n++)
      mon_inst[n] = get_inst(n);
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
    if (v == 1) {
      vector<entity_inst_t> mon_inst;
      ::decode(mon_inst, p);
      for (unsigned i = 0; i < mon_inst.size(); i++) {
	char n[2];
	n[0] = '0' + i;
	n[1] = 0;
	string name = n;
	mon_addr[name] = mon_inst[i].addr;
      }
    } else
      ::decode(mon_addr, p);
    ::decode(last_changed, p);
    ::decode(created, p);
    calc_ranks();
  }

  void generate_fsid() {
    for (int i=0; i<16; i++)
      fsid.fsid[i] = rand();
  }

  // read from/write to a file
  int write(const char *fn);
  int read(const char *fn);

  void print(ostream& out) const;
  void print_summary(ostream& out) const;
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
