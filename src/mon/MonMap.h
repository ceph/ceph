// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#ifdef WITH_CRIMSON
#include <seastar/core/future.hh>
#endif

#include "common/config_fwd.h"
#include "common/ceph_releases.h"
#include "include/types.h" // for epoch_t
#include "include/utime.h" // for utime_t
#include "include/uuid.h" // for uuid_d

#include "mon/mon_feature_t.h"
#include "msg/msg_types.h" // for entity_addrvec_t

#include <iosfwd>
#include <map>
#include <set>
#include <string>
#include <vector>

class health_check_map_t;

#ifdef WITH_CRIMSON
namespace crimson::common {
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
  std::string name;
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
  uint16_t weight{0};

  ceph::real_clock::time_point time_added = ceph::real_clock::zero();

  /**
   * The location of the monitor, in CRUSH hierarchy terms
   */
  std::map<std::string,std::string> crush_loc;

  // <REMOVE ME>
  mon_info_t(const std::string& n, const entity_addr_t& p_addr, uint16_t p)
    : name(n), public_addrs(p_addr), priority(p)
  {}
  // </REMOVE ME>

  mon_info_t(const std::string& n, const entity_addrvec_t& p_addrs,
             uint16_t p, uint16_t w)
    : name(n), public_addrs(p_addrs), priority(p), weight(w)
  {}
  mon_info_t(const std::string &n, const entity_addrvec_t& p_addrs)
    : name(n), public_addrs(p_addrs)
  { }

  mon_info_t() = default;

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void print(std::ostream& out) const;
  void dump(ceph::Formatter *f) const;
  static std::list<mon_info_t> generate_test_instances();
};
WRITE_CLASS_ENCODER_FEATURES(mon_info_t)

class MonMap {
 public:
  epoch_t epoch{0};       // what epoch/version of the monmap
  uuid_d fsid;
  utime_t last_changed;
  utime_t created;

  std::map<std::string, mon_info_t> mon_info;
  std::map<entity_addr_t, std::string> addr_mons;

  std::vector<std::string> ranks;
  /* ranks which were removed when this map took effect.
     There should only be one at a time, but leave support
     for arbitrary numbers just to be safe. */
  std::set<unsigned> removed_ranks;

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
  ceph_release_t min_mon_release{ceph_release_t::unknown};

  void _add_ambiguous_addr(const std::string& name,
                           entity_addr_t addr,
                           int priority,
                           int weight,
                           bool for_mkfs);

  enum election_strategy {
			  // Keep in sync with ElectionLogic.h!
    CLASSIC = 1, // the original rank-based one
    DISALLOW = 2, // disallow a set from being leader
    CONNECTIVITY = 3 // includes DISALLOW, extends to prefer stronger connections
  };
  election_strategy strategy = CLASSIC;
  std::set<std::string> disallowed_leaders; // can't be leader under CONNECTIVITY/DISALLOW
  bool stretch_mode_enabled = false;
  std::string tiebreaker_mon;
  std::set<std::string> stretch_marked_down_mons; // can't be leader or taken proposal in CONNECTIVITY 
                                                  // seriously until fully recovered

public:
  void calc_legacy_ranks();
  void calc_addr_mons();

  MonMap() = default;

  uuid_d& get_fsid() { return fsid; }

  unsigned size() const {
    return mon_info.size();
  }

  unsigned min_quorum_size(unsigned total_mons=0) const {
    if (total_mons == 0) {
      total_mons = size();
    }
    return total_mons / 2 + 1;
  }

  epoch_t get_epoch() const { return epoch; }
  void set_epoch(epoch_t e) { epoch = e; }

  /**
   * Obtain list of public facing addresses
   *
   * @param ls list to populate with the monitors' addresses
   */
  void list_addrs(std::list<entity_addr_t>& ls) const;

  mon_info_t const& get(std::string const& name) const {
    return mon_info.at(name);
  }

  /**
   * Add new monitor to the monmap
   *
   * @param m monitor info of the new monitor
   */
  mon_info_t& add(mon_info_t&& m);

  /**
   * Add new monitor to the monmap
   *
   * @param name Monitor name (i.e., 'foo' in 'mon.foo')
   * @param addr Monitor's public address
   */
  mon_info_t& add(const std::string &name, const entity_addrvec_t &addrv,
	   uint16_t priority=0, uint16_t weight=0) {
    return add(mon_info_t(name, addrv, priority, weight));
  }

  /**
   * Remove monitor from the monmap
   *
   * @param name Monitor name (i.e., 'foo' in 'mon.foo')
   */
  void remove(const std::string &name);

  /**
   * Rename monitor from @p oldname to @p newname
   *
   * @param oldname monitor's current name (i.e., 'foo' in 'mon.foo')
   * @param newname monitor's new name (i.e., 'bar' in 'mon.bar')
   */
  void rename(std::string oldname, std::string newname);

  int set_rank(const std::string& name, int rank);

  bool contains(const std::string& name) const;

  /**
   * Check if monmap contains a monitor with address @p a
   *
   * @note checks for all addresses a monitor may have, public or otherwise.
   *
   * @param a monitor address
   * @returns true if monmap contains a monitor with address @p;
   *          false otherwise.
   */
  bool contains(const entity_addr_t &a, std::string *name=nullptr) const;
  bool contains(const entity_addrvec_t &av, std::string *name=nullptr) const;

  std::string get_name(unsigned n) const {
    ceph_assert(n < ranks.size());
    return ranks[n];
  }
  std::string get_name(const entity_addr_t& a) const;
  std::string get_name(const entity_addrvec_t& av) const;

  int get_rank(const std::string& n) const;
  int get_rank(const entity_addr_t& a) const;
  int get_rank(const entity_addrvec_t& av) const;
  bool get_addr_name(const entity_addr_t& a, std::string& name);

  const entity_addrvec_t& get_addrs(const std::string& n) const;
  const entity_addrvec_t& get_addrs(unsigned m) const;
  void set_addrvec(const std::string& n, const entity_addrvec_t& a);
  uint16_t get_priority(const std::string& n) const;
  uint16_t get_weight(const std::string& n) const;
  void set_weight(const std::string& n, uint16_t v);

  void encode(ceph::buffer::list& blist, uint64_t con_features) const;
  void decode(ceph::buffer::list& blist) {
    auto p = std::cbegin(blist);
    decode(p);
  }
  void decode(ceph::buffer::list::const_iterator& p);

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
   * @param errout std::ostream to send error messages too
   */
#ifdef WITH_CRIMSON
  seastar::future<> build_initial(const crimson::common::ConfigProxy& conf, bool for_mkfs);
#else
  int build_initial(CephContext *cct, bool for_mkfs, std::ostream& errout);
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
			   std::list<std::string>& initial_members,
			   std::string my_name,
			   const entity_addrvec_t& my_addrs,
			   std::set<entity_addrvec_t> *removed);

  void print(std::ostream& out) const;
  void print_summary(std::ostream& out) const;
  void dump(ceph::Formatter *f) const;
  void dump_summary(ceph::Formatter *f) const;

  void check_health(health_check_map_t *checks) const;

  static std::list<MonMap> generate_test_instances();
protected:
  /**
   * build a monmap from a list of entity_addrvec_t's
   *
   * Give mons dummy names.
   *
   * @param addrs  list of entity_addrvec_t's
   * @param prefix prefix to prepend to generated mon names
   */
  void init_with_addrs(const std::vector<entity_addrvec_t>& addrs,
                       bool for_mkfs,
                       std::string_view prefix);
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
		    std::string_view prefix);
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
		      std::string_view prefix);
  int init_with_config_file(const ConfigProxy& conf, std::ostream& errout);
#if WITH_CRIMSON
  seastar::future<> read_monmap(const std::string& monmap);
  /// try to build monmap with different settings, like
  /// mon_host, mon* sections, and mon_dns_srv_name
  seastar::future<> build_monmap(const crimson::common::ConfigProxy& conf, bool for_mkfs);
  /// initialize monmap by resolving given service name
  seastar::future<> init_with_dns_srv(bool for_mkfs, const std::string& name);
  /// initialize monmap with `mon_host` or `mon_host_override`
  bool maybe_init_with_mon_host(const std::string& mon_host, bool for_mkfs);
#else
  /// read from encoded monmap file
  int init_with_monmap(const std::string& monmap, std::ostream& errout);
  int init_with_dns_srv(CephContext* cct, std::string srv_name, bool for_mkfs,
			std::ostream& errout);
#endif
};
WRITE_CLASS_ENCODER_FEATURES(MonMap)

inline std::ostream& operator<<(std::ostream &out, const MonMap &m) {
  m.print_summary(out);
  return out;
}

#endif
