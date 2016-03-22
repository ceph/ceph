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
#include "mon/mon_types.h"
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

  map<string, mon_info_t> mon_info;
  map<entity_addr_t, string> addr_mons;

  vector<string> ranks;

  /**
   * Persistent Features are all those features that once set on a
   * monmap cannot, and should not, be removed. These will define the
   * non-negotiable features that a given monitor must support to
   * properly operate in a given quorum.
   *
   * Should be reserved for features that we really want to make sure
   * are sticky, and are important enough to tolerate not being able
   * to downgrade a monitor.
   */
  mon_feature_t persistent_features;
  /**
   * Optional Features are all those features that can be enabled or
   * disabled following a given criteria -- e.g., user-mandated via the
   * cli --, and act much like indicators of what the cluster currently
   * supports.
   *
   * They are by no means "optional" in the sense that monitors can
   * ignore them. Just that they are not persistent.
   */
  mon_feature_t optional_features;

  /**
   * Returns the set of features required by this monmap.
   *
   * The features required by this monmap is the union of all the
   * currently set persistent features and the currently set optional
   * features.
   *
   * @returns the set of features required by this monmap
   */
  mon_feature_t get_required_features() const {
    return (persistent_features | optional_features);
  }

  void sanitize_mons(map<string,entity_addr_t>& o) {

    // if mon_info is populated, it means we decoded a map encoded
    // by someone who understands the new format (i.e., is able to
    // encode 'mon_info'). This means they must also have provided
    // a properly populated 'mon_addr' (which we have dropped with
    // this patch), 'o' being the contents of said map. In this
    // case, 'o' must have the same number of entries as 'mon_info'.
    //
    // Also, for each entry in 'o', there has to be a matching
    // 'mon_info' entry, properly populated with a name and a matching
    // 'public_addr'.
    //
    // OTOH, if 'mon_info' is not populated, it means the one that
    // originally encoded the map does not know the new format, and
    // 'o' will be our only source of info about the monitors in the
    // cluster -- and we will use it to populate our 'mon_info' map.

    bool has_mon_info = false;
    if (mon_info.size() > 0) {
      assert(o.size() == mon_info.size());
      has_mon_info = true;
    }

    for (map<string, entity_addr_t>::const_iterator p = o.begin();
         p != o.end();
         ++p) {

      // make sure the info we have is accurate
      if (has_mon_info) {
        assert(mon_info.count(p->first));
        assert(mon_info[p->first].name == p->first);
        assert(mon_info[p->first].public_addr == p->second);
        continue;
      }

      mon_info_t &m = mon_info[p->first];
      m.name = p->first;
      m.public_addr = p->second;    
    }
  }

  void calc_ranks() {

    ranks.resize(mon_info.size());
    addr_mons.clear();

    // Used to order entries according to public_addr, because that's
    // how the ranks are expected to be ordered by. We may expand this
    // later on, according to some other criteria, by specifying a
    // different comparator.
    //
    // Please note that we use a 'set' here instead of resorting to
    // std::sort() because we need more info than that's available in
    // the vector. The vector will thus be ordered by, e.g., public_addr
    // while only containing the names of each individual monitor.
    // The only way of achieving this with std::sort() would be to first
    // insert every mon_info_t entry into a vector 'foo', std::sort() 'foo'
    // with custom comparison functions, and then copy each invidual entry
    // to a new vector. Unless there's a simpler way, we don't think the
    // added complexity makes up for the additional memory usage of a 'set'.
    set<mon_info_t, rank_cmp> tmp;

    for (map<string,mon_info_t>::iterator p = mon_info.begin();
         p != mon_info.end();
         ++p) {
      mon_info_t &m = p->second;
      tmp.insert(m);

      // populate addr_mons
      assert(addr_mons.count(m.public_addr) == 0);
      addr_mons[m.public_addr] = m.name;
    }

    // map the set to the actual ranks etc
    unsigned i = 0;
    for (set<mon_info_t>::iterator p = tmp.begin();
         p != tmp.end();
         ++p, ++i) {
      ranks[i] = p->name;
    }
  }

  MonMap()
    : epoch(0) {
    memset(&fsid, 0, sizeof(fsid));
  }

  uuid_d& get_fsid() { return fsid; }

  unsigned size() const {
    return mon_info.size();
  }

  epoch_t get_epoch() const { return epoch; }
  void set_epoch(epoch_t e) { epoch = e; }

  /**
   * Obtain list of public facing addresses
   *
   * @param ls list to populate with the monitors' addresses
   */
  void list_addrs(list<entity_addr_t>& ls) const {
    for (map<string,mon_info_t>::const_iterator p = mon_info.begin();
         p != mon_info.end();
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
    assert(mon_info.count(name) == 0);
    assert(addr_mons.count(addr) == 0);
    mon_info_t &m = mon_info[name];
    m.name = name;
    m.public_addr = addr;
    calc_ranks();
  }
 
  /**
   * Remove monitor from the monmap
   *
   * @param name Monitor name (i.e., 'foo' in 'mon.foo')
   */
  void remove(const string &name) {
    assert(mon_info.count(name));
    mon_info.erase(name);
    assert(mon_info.count(name) == 0);
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
    mon_info[newname] = mon_info[oldname];
    mon_info.erase(oldname);
    mon_info[newname].name = newname;
    calc_ranks();
  }

  bool contains(const string& name) const {
    return mon_info.count(name);
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
  bool contains(const entity_addr_t &a) const {
    for (map<string,mon_info_t>::const_iterator p = mon_info.begin();
         p != mon_info.end();
         ++p) {
      if (p->second.public_addr == a)
        return true;
    }
    return false;
  }

  string get_name(unsigned n) const {
    assert(n < ranks.size());
    return ranks[n];
  }
  string get_name(const entity_addr_t& a) const {
    map<entity_addr_t,string>::const_iterator p = addr_mons.find(a);
    if (p == addr_mons.end())
      return string();
    else
      return p->second;
  }

  int get_rank(const string& n) {
    for (unsigned i = 0; i < ranks.size(); i++)
      if (ranks[i] == n)
	return i;
    return -1;
  }
  int get_rank(const entity_addr_t& a) {
    string n = get_name(a);
    if (n.empty())
      return -1;

    for (unsigned i = 0; i < ranks.size(); i++)
      if (ranks[i] == n)
	return i;
    return -1;
  }
  bool get_addr_name(const entity_addr_t& a, string& name) {
    if (addr_mons.count(a) == 0)
      return false;
    name = addr_mons[a];
    return true;
  }

  const entity_addr_t& get_addr(const string& n) const {
    assert(mon_info.count(n));
    map<string,mon_info_t>::const_iterator p = mon_info.find(n);
    return p->second.public_addr;
  }
  const entity_addr_t& get_addr(unsigned m) const {
    assert(m < ranks.size());
    return get_addr(ranks[m]);
  }
  void set_addr(const string& n, const entity_addr_t& a) {
    assert(mon_info.count(n));
    mon_info[n].public_addr = a;
    calc_ranks();
  }
  entity_inst_t get_inst(const string& n) {
    assert(mon_info.count(n));
    int m = get_rank(n);
    assert(m >= 0); // vector can't take negative indicies
    return get_inst(m);
  }
  entity_inst_t get_inst(unsigned m) const {
    assert(m < ranks.size());
    entity_inst_t i;
    i.addr = get_addr(m);
    i.name = entity_name_t::MON(m);
    return i;
  }

  void encode(bufferlist& blist, uint64_t con_features) const;
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
