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
//#include "common/config.h"

namespace ceph {
  class Formatter;
}

class MonMap {
 public:
  epoch_t epoch;       // what epoch/version of the monmap
  uuid_d fsid;
  map<string, entity_addr_t> mon_addr;
  utime_t last_changed;
  utime_t created;

  map<entity_addr_t,string> addr_name;
  vector<string> rank_name;
  vector<entity_addr_t> rank_addr;

  void calc_ranks() {
    rank_name.resize(mon_addr.size());
    rank_addr.resize(mon_addr.size());
    addr_name.clear();
    for (map<string,entity_addr_t>::iterator p = mon_addr.begin();
	 p != mon_addr.end();
	 ++p) {
      assert(addr_name.count(p->second) == 0);
      addr_name[p->second] = p->first;
    }
    unsigned i = 0;
    for (map<entity_addr_t,string>::iterator p = addr_name.begin();
	 p != addr_name.end();
	 ++p, i++) {
      rank_name[i] = p->second;
      rank_addr[i] = p->first;
    }
  }

  MonMap() 
    : epoch(0) {
    memset(&fsid, 0, sizeof(fsid));
  }

  uuid_d& get_fsid() { return fsid; }

  unsigned size() {
    return mon_addr.size();
  }

  epoch_t get_epoch() { return epoch; }
  void set_epoch(epoch_t e) { epoch = e; }

  void list_addrs(list<entity_addr_t>& ls) const {
    for (map<string,entity_addr_t>::const_iterator p = mon_addr.begin();
	 p != mon_addr.end();
	 ++p)
      ls.push_back(p->second);
  }

  void add(const string &name, const entity_addr_t &addr) {
    assert(mon_addr.count(name) == 0);
    assert(addr_name.count(addr) == 0);
    mon_addr[name] = addr;
    calc_ranks();
  }
  
  void remove(const string &name) {
    assert(mon_addr.count(name));
    mon_addr.erase(name);
    calc_ranks();
  }

  void rename(string oldname, string newname) {
    assert(contains(oldname));
    assert(!contains(newname));
    mon_addr[newname] = mon_addr[oldname];
    mon_addr.erase(oldname);
    calc_ranks();
  }

  bool contains(const string& name) {
    return mon_addr.count(name);
  }

  bool contains(const entity_addr_t &a) {
    for (map<string,entity_addr_t>::iterator p = mon_addr.begin();
	 p != mon_addr.end();
	 ++p) {
      if (p->second == a)
	return true;
    }
    return false;
  }

  string get_name(unsigned n) const {
    assert(n < rank_name.size());
    return rank_name[n];
  }
  string get_name(const entity_addr_t& a) const {
    map<entity_addr_t,string>::const_iterator p = addr_name.find(a);
    if (p == addr_name.end())
      return string();
    else
      return p->second;
  }

  int get_rank(const string& n) {
    for (unsigned i=0; i<rank_name.size(); i++)
      if (rank_name[i] == n)
	return i;
    return -1;
  }
  int get_rank(const entity_addr_t& a) {
    for (unsigned i=0; i<rank_addr.size(); i++)
      if (rank_addr[i] == a)
	return i;
    return -1;
  }
  bool get_addr_name(const entity_addr_t& a, string& name) {
    if (addr_name.count(a) == 0)
      return false;
    name = addr_name[a];
    return true;
  }

  const entity_addr_t& get_addr(const string& n) {
    assert(mon_addr.count(n));
    return mon_addr[n];
  }
  const entity_addr_t& get_addr(unsigned m) {
    assert(m < rank_addr.size());
    return rank_addr[m];
  }
  void set_addr(const string& n, const entity_addr_t& a) {
    assert(mon_addr.count(n));
    mon_addr[n] = a;
    calc_ranks();
  }
  entity_inst_t get_inst(const string& n) {
    assert(mon_addr.count(n));
    int m = get_rank(n);
    assert(m >= 0); // vector can't take negative indicies
    entity_inst_t i;
    i.addr = rank_addr[m];
    i.name = entity_name_t::MON(m);
    return i;
  }
  entity_inst_t get_inst(unsigned m) const {
    assert(m < rank_addr.size());
    entity_inst_t i;
    i.addr = rank_addr[m];
    i.name = entity_name_t::MON(m);
    return i;
  }

  void encode(bufferlist& blist, uint64_t features) const;
  void decode(bufferlist& blist) {
    bufferlist::iterator p = blist.begin();
    decode(p);
  }
  void decode(bufferlist::iterator &p);

  void generate_fsid() {
    fsid.generate_random();
  }

  // read from/write to a file
  int write(const char *fn);
  int read(const char *fn);

  /**
   * build an initial bootstrap monmap from conf
   *
   * Build an initial bootstrap monmap from the config.  This will
   * try, in this order:
   *
   *   1 monmap   -- an explicitly provided monmap
   *   2 mon_host -- list of monitors
   *   3 config [mon.*] sections, and 'mon addr' fields in those sections
   *
   * @param cct context (and associated config)
   * @param errout ostream to send error messages too
   */
  int build_initial(CephContext *cct, ostream& errout);

  /**
   * build a monmap from a list of hosts or ips
   *
   * Resolve dns as needed.  Give mons dummy names.
   *
   * @param hosts  list of hosts, space or comma separated
   * @param prefix prefix to prepend to generated mon names
   * @return 0 for success, -errno on error
   */
  int build_from_host_list(std::string hosts, std::string prefix);

  /**
   * filter monmap given a set of initial members.
   *
   * Remove mons that aren't in the initial_members list.  Add missing
   * mons and give them dummy IPs (blank IPv4, with a non-zero
   * nonce). If the name matches my_name, then my_addr will be used in
   * place of a dummy addr.
   *
   * @param initial_members list of initial member names
   * @param my_name name of self, can be blank
   * @param my_addr my addr
   * @param removed optional pointer to set to insert removed mon addrs to
   */
  void set_initial_members(CephContext *cct,
			   list<std::string>& initial_members,
			   string my_name, const entity_addr_t& my_addr,
			   set<entity_addr_t> *removed);

  void print(ostream& out) const;
  void print_summary(ostream& out) const;
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(list<MonMap*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(MonMap)

inline ostream& operator<<(ostream &out, const MonMap &m) {
  m.print_summary(out);
  return out;
}

#endif
