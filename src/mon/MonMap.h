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

#ifdef WITH_SEASTAR
#include <seastar/core/future.hh>
#endif

#include "common/config_fwd.h"

#include "include/err.h"
#include "include/types.h"

#include "mon/mon_types.h"
#include "msg/Message.h"


#ifdef WITH_SEASTAR
namespace ceph::common {
  class ConfigProxy;
}
#endif

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
   * monitor's public address(es)
   *
   * public facing address(es), used to communicate with all clients
   * and with other monitors.
   */
  entity_addrvec_t public_addrs;
  /**
   * the priority of the mon, the lower value the more preferred
   */
  uint16_t priority{0};

  // <REMOVE ME>
  mon_info_t(const string& n, const entity_addr_t& p_addr, uint16_t p)
    : name(n), public_addrs(p_addr), priority(p)
  {}
  // </REMOVE ME>

  mon_info_t(const string& n, const entity_addrvec_t& p_addrs, uint16_t p)
    : name(n), public_addrs(p_addrs), priority(p)
  {}
  mon_info_t(const string &n, const entity_addrvec_t& p_addrs)
    : name(n), public_addrs(p_addrs)
  { }

  mon_info_t() { }


  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::const_iterator& p);
  void print(ostream& out) const;
};
WRITE_CLASS_ENCODER_FEATURES(mon_info_t)

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

  // upgrade gate
  uint8_t min_mon_release = 0;

  void _add_ambiguous_addr(const string& name,
			   entity_addr_t addr,
			   int priority,
			   bool for_mkfs=false);

public:
  void calc_legacy_ranks();
  void calc_addr_mons() {
    // populate addr_mons
    addr_mons.clear();
    for (auto& p : mon_info) {
      for (auto& a : p.second.public_addrs.v) {
	addr_mons[a] = p.first;
      }
    }
  }

  MonMap()
    : epoch(0) {
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
    for (auto& i : mon_info) {
      for (auto& j : i.second.public_addrs.v) {
	ls.push_back(j);
      }
    }
  }

  /**
   * Add new monitor to the monmap
   *
   * @param m monitor info of the new monitor
   */
  void add(const mon_info_t& m) {
    ceph_assert(mon_info.count(m.name) == 0);
    for (auto& a : m.public_addrs.v) {
      ceph_assert(addr_mons.count(a) == 0);
    }
    mon_info[m.name] = m;
    if (get_required_features().contains_all(
	  ceph::features::mon::FEATURE_NAUTILUS)) {
      ranks.push_back(m.name);
      ceph_assert(ranks.size() == mon_info.size());
    } else {
      calc_legacy_ranks();
    }
    calc_addr_mons();
  }

  /**
   * Add new monitor to the monmap
   *
   * @param name Monitor name (i.e., 'foo' in 'mon.foo')
   * @param addr Monitor's public address
   */
  void add(const string &name, const entity_addrvec_t &addrv,
	   int priority=0) {
    add(mon_info_t(name, addrv, priority));
  }

  /**
   * Remove monitor from the monmap
   *
   * @param name Monitor name (i.e., 'foo' in 'mon.foo')
   */
  void remove(const string &name) {
    ceph_assert(mon_info.count(name));
    mon_info.erase(name);
    ceph_assert(mon_info.count(name) == 0);
    if (get_required_features().contains_all(
	  ceph::features::mon::FEATURE_NAUTILUS)) {
      ranks.erase(std::find(ranks.begin(), ranks.end(), name));
      ceph_assert(ranks.size() == mon_info.size());
    } else {
      calc_legacy_ranks();
    }
    calc_addr_mons();
  }

  /**
   * Rename monitor from @p oldname to @p newname
   *
   * @param oldname monitor's current name (i.e., 'foo' in 'mon.foo')
   * @param newname monitor's new name (i.e., 'bar' in 'mon.bar')
   */
  void rename(string oldname, string newname) {
    ceph_assert(contains(oldname));
    ceph_assert(!contains(newname));
    mon_info[newname] = mon_info[oldname];
    mon_info.erase(oldname);
    mon_info[newname].name = newname;
    if (get_required_features().contains_all(
	  ceph::features::mon::FEATURE_NAUTILUS)) {
      *std::find(ranks.begin(), ranks.end(), oldname) = newname;
      ceph_assert(ranks.size() == mon_info.size());
    } else {
      calc_legacy_ranks();
    }
    calc_addr_mons();
  }

  int set_rank(const string& name, int rank) {
    int oldrank = get_rank(name);
    if (oldrank < 0) {
      return -ENOENT;
    }
    if (rank < 0 || rank >= (int)ranks.size()) {
      return -EINVAL;
    }
    if (oldrank != rank) {
      ranks.erase(ranks.begin() + oldrank);
      ranks.insert(ranks.begin() + rank, name);
    }
    return 0;
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
  bool contains(const entity_addr_t &a, string *name=nullptr) const {
    for (auto& i : mon_info) {
      for (auto& j : i.second.public_addrs.v) {
	if (j == a) {
	  if (name) {
	    *name = i.first;
	  }
	  return true;
	}
      }
    }
    return false;
  }
  bool contains(const entity_addrvec_t &av, string *name=nullptr) const {
    for (auto& i : mon_info) {
      for (auto& j : i.second.public_addrs.v) {
	for (auto& k : av.v) {
	  if (j == k) {
	    if (name) {
	      *name = i.first;
	    }
	    return true;
	  }
	}
      }
    }
    return false;
  }

  string get_name(unsigned n) const {
    ceph_assert(n < ranks.size());
    return ranks[n];
  }
  string get_name(const entity_addr_t& a) const {
    map<entity_addr_t,string>::const_iterator p = addr_mons.find(a);
    if (p == addr_mons.end())
      return string();
    else
      return p->second;
  }
  string get_name(const entity_addrvec_t& av) const {
    for (auto& i : av.v) {
      map<entity_addr_t,string>::const_iterator p = addr_mons.find(i);
      if (p != addr_mons.end())
	return p->second;
    }
    return string();
  }

  int get_rank(const string& n) const {
    if (auto found = std::find(ranks.begin(), ranks.end(), n);
	found != ranks.end()) {
      return std::distance(ranks.begin(), found);
    } else {
      return -1;
    }
  }
  int get_rank(const entity_addr_t& a) const {
    string n = get_name(a);
    if (!n.empty()) {
      return get_rank(n);
    }
    return -1;
  }
  int get_rank(const entity_addrvec_t& av) const {
    string n = get_name(av);
    if (!n.empty()) {
      return get_rank(n);
    }
    return -1;
  }
  bool get_addr_name(const entity_addr_t& a, string& name) {
    if (addr_mons.count(a) == 0)
      return false;
    name = addr_mons[a];
    return true;
  }

  const entity_addrvec_t& get_addrs(const string& n) const {
    ceph_assert(mon_info.count(n));
    map<string,mon_info_t>::const_iterator p = mon_info.find(n);
    return p->second.public_addrs;
  }
  const entity_addrvec_t& get_addrs(unsigned m) const {
    ceph_assert(m < ranks.size());
    return get_addrs(ranks[m]);
  }
  void set_addrvec(const string& n, const entity_addrvec_t& a) {
    ceph_assert(mon_info.count(n));
    mon_info[n].public_addrs = a;
    calc_addr_mons();
  }

  void encode(bufferlist& blist, uint64_t con_features) const;
  void decode(bufferlist& blist) {
    auto p = std::cbegin(blist);
    decode(p);
  }
  void decode(bufferlist::const_iterator& p);

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
#ifdef WITH_SEASTAR
  seastar::future<> build_initial(const ceph::common::ConfigProxy& conf, bool for_mkfs);
#else
  int build_initial(CephContext *cct, bool for_mkfs, ostream& errout);
#endif
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
			   string my_name,
			   const entity_addrvec_t& my_addrs,
			   set<entity_addrvec_t> *removed);

  void print(ostream& out) const;
  void print_summary(ostream& out) const;
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(list<MonMap*>& o);
protected:
  /**
   * build a monmap from a list of ips
   *
   * Give mons dummy names.
   *
   * @param hosts  list of ips, space or comma separated
   * @param prefix prefix to prepend to generated mon names
   * @return 0 for success, -errno on error
   */
  int init_with_ips(const std::string& ips,
		    bool for_mkfs,
		    const std::string &prefix);
  /**
   * build a monmap from a list of hostnames
   *
   * Give mons dummy names.
   *
   * @param hosts  list of ips, space or comma separated
   * @param prefix prefix to prepend to generated mon names
   * @return 0 for success, -errno on error
   */
  int init_with_hosts(const std::string& hostlist,
		      bool for_mkfs,
		      const std::string& prefix);
  int init_with_config_file(const ConfigProxy& conf, std::ostream& errout);
#if WITH_SEASTAR
  seastar::future<> read_monmap(const std::string& monmap);
  /// try to build monmap with different settings, like
  /// mon_host, mon* sections, and mon_dns_srv_name
  seastar::future<> build_monmap(const ceph::common::ConfigProxy& conf, bool for_mkfs);
  /// initialize monmap by resolving given service name
  seastar::future<> init_with_dns_srv(bool for_mkfs, const std::string& name);
#else
  /// read from encoded monmap file
  int init_with_monmap(const std::string& monmap, std::ostream& errout);
  int init_with_dns_srv(CephContext* cct, std::string srv_name, bool for_mkfs,
			std::ostream& errout);
#endif
};
WRITE_CLASS_ENCODER_FEATURES(MonMap)

inline ostream& operator<<(ostream &out, const MonMap &m) {
  m.print_summary(out);
  return out;
}

#endif
