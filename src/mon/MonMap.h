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

struct mon_info_t {
  /**
   * monitor name
   *
   * i.e., 'foo' in 'mon.foo'
   */
  string name;
  /**
   * monitor's public address
   *
   * public facing address, traditionally used to communicate with all clients
   * and other monitors.
   */
  entity_addr_t public_addr;

  mon_info_t(string &n, entity_addr_t& p_addr)
    : name(n), public_addr(p_addr)
  { }

  mon_info_t() { }


  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void print(ostream& out) const;
};
WRITE_CLASS_ENCODER(mon_info_t)
inline ostream& operator<<(ostream& out, const mon_info_t& mon) {
  mon.print(out);
  return out;
}


class MonMap {
 public:
  epoch_t epoch;       // what epoch/version of the monmap
  uuid_d fsid;
  utime_t last_changed;
  utime_t created;

  /* we don't use this anymore, but we keep it around to make
   * compatibility easier, as there may be monitors that do not
   * support our way to do monmaps (e.g., during an upgrade).
   *
   * This map is kept populated at all times with the public address of
   * each entry in 'mons'.
   *
   * This will be encoded with every map we produce just as well. We could
   * generate this during encoding, but seems like that's just wasting
   * cycles. And this barelly wastes any resources, so there's no big
   * tradeoff anyway.
   */
  map<string, entity_addr_t> mon_addr;

  map<string, mon_info_t> mons;
  map<entity_addr_t, mon_info_t> addr_mons;

  vector<mon_info_t> ranks;

  vector<string> rank_name;
  vector<entity_addr_t> rank_addr;


  /* Nasty kludge to make sure we understand older clients and that older
   * clients get up-to-date information on their monmaps.
   */
  void normalize_mon_addr(bool update) {

    if (mons.empty()) {
      /* must be an older version, or an empty monmap; copy from
       * mon_addr. */
      for (map<string, entity_addr_t>::iterator p = mon_addr.begin();
           p != mon_addr.end();
           ++p) {
        mon_info_t &m = mons[p->first];
        m.name = p->first;
        m.public_addr = p->second;
      }
    }

    if (!update)
      return;

    // copy whatever is on 'mons' to 'mon_addr'
    mon_addr.clear();
    for (map<string, mon_info_t>::iterator p = mons.begin();
         p != mons.end();
         ++p) {

      mon_addr[p->first] = p->second.public_addr;
    }
  }

  void calc_ranks() {

    ranks.resize(mons.size());
    addr_mons.clear();

    // used to order entries according to public_addr, because that's
    // how the ranks are expected to be ordered by.
    set<mon_info_t, rank_cmp> tmp;

    for (map<string,mon_info_t>::iterator p = mons.begin();
         p != mons.end();
         ++p) {
      mon_info_t &m = p->second;
      tmp.insert(m);

      // populate addr_mons
      assert(addr_mons.count(m.public_addr) == 0);
    }

    // map the set to the actual ranks etc
    unsigned i = 0;
    for (set<mon_info_t>::iterator p = tmp.begin();
         p != tmp.end();
         ++p, ++i) {
      ranks[i] = *p;
    }
  }

  MonMap() 
    : epoch(0) {
    memset(&fsid, 0, sizeof(fsid));
  }

  uuid_d& get_fsid() { return fsid; }

  unsigned size() {
    return mons.size();
  }

  epoch_t get_epoch() { return epoch; }
  void set_epoch(epoch_t e) { epoch = e; }

  /**
   * Obtain list of public facing addresses
   *
   * @param ls list to populate with the monitors' addresses
   */
  void list_addrs(list<entity_addr_t>& ls) const {
    for (map<string,mon_info_t>::const_iterator p = mons.begin();
         p != mons.end();
         ++p) {
      ls.push_back(p->second.public_addr);
    }
  }

  /**
   * Add new monitor to the monmap
   *
   * @param name Monitor name (i.e., 'foo' in 'mon.foo')
   * @param addr Monitor's public address
   */
  void add(const string &name, const entity_addr_t &addr) {
    assert(mons.count(name) == 0);
    assert(addr_mons.count(addr) == 0);
    mon_info_t &m = mons[name];
    m.name = name;
    m.public_addr = addr;
    normalize_mon_addr(true);
    calc_ranks();
  }
 
  /**
   * Remove monitor from the monmap
   *
   * @param name Monitor name (i.e., 'foo' in 'mon.foo')
   */
  void remove(const string &name) {
    assert(mons.count(name));
    mons.erase(name);
    normalize_mon_addr(true);
    calc_ranks();
  }

  /**
   * Rename monitor from @p oldname to @p newname
   *
   * @param oldname monitor's current name (i.e., 'foo' in 'mon.foo')
   * @param newname monitor's new name (i.e., 'bar' in 'mon.bar')
   */
  void rename(string oldname, string newname) {
    assert(contains(oldname));
    assert(!contains(newname));
    mons[newname] = mons[oldname];
    mons.erase(oldname);
    mons[newname].name = newname;
    normalize_mon_addr(true);
    calc_ranks();
  }

  bool contains(const string& name) {
    return mons.count(name);
  }

  /**
   * Check if monmap contains a monitor with address @p a
   *
   * @note checks for all addresses a monitor may have, public or otherwise.
   *
   * @param a monitor address
   * @returns true if monmap contains a monitor with address @p;
   *          false otherwise.
   */
  bool contains(const entity_addr_t &a) {
    for (map<string,mon_info_t>::iterator p = mons.begin();
         p != mons.end();
         ++p) {
      if (p->second.public_addr == a)
        return true;
    }
    return false;
  }

  string get_name(unsigned n) const {
    assert(n < ranks.size());
    return ranks[n].name;
  }
  string get_name(const entity_addr_t& a) const {
    map<entity_addr_t,mon_info_t>::const_iterator p = addr_mons.find(a);
    if (p == addr_mons.end())
      return string();
    else
      return p->second.name;
  }

  int get_rank(const string& n) {
    for (unsigned i = 0; i < ranks.size(); i++)
      if (ranks[i].name == n)
	return i;
    return -1;
  }
  int get_rank(const entity_addr_t& a) {
    for (unsigned i = 0; i < ranks.size(); i++)
      if (ranks[i].public_addr == a)
	return i;
    return -1;
  }
  bool get_addr_name(const entity_addr_t& a, string& name) {
    if (addr_mons.count(a) == 0)
      return false;
    name = addr_mons[a].name;
    return true;
  }

  const entity_addr_t& get_addr(const string& n) {
    assert(mons.count(n));
    return mons[n].public_addr;
  }
  const entity_addr_t& get_addr(unsigned m) {
    assert(m < ranks.size());
    return ranks[m].public_addr;
  }
  void set_addr(const string& n, const entity_addr_t& a) {
    assert(mons.count(n));
    mons[n].public_addr = a;
    normalize_mon_addr(true);
    calc_ranks();
  }
  entity_inst_t get_inst(const string& n) {
    assert(mons.count(n));
    int m = get_rank(n);
    assert(m >= 0); // vector can't take negative indicies
    return get_inst(m);
  }
  entity_inst_t get_inst(unsigned m) const {
    assert(m < ranks.size());
    entity_inst_t i;
    i.addr = ranks[m].public_addr;
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

private:
  struct rank_cmp {
    bool operator()(const mon_info_t &a, const mon_info_t &b) const {
      if (a.public_addr == b.public_addr)
        return a.name < b.name;
      return a.public_addr < b.public_addr;
    }
  };
};
WRITE_CLASS_ENCODER_FEATURES(MonMap)

inline ostream& operator<<(ostream& out, MonMap& m) {
  m.print_summary(out);
  return out;
}

#endif
