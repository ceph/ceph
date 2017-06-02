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
 
/* Metadata Server Monitor
 */

#ifndef CEPH_MDSMONITOR_H
#define CEPH_MDSMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "mds/FSMap.h"
#include "mds/MDSMap.h"
#include "PaxosService.h"
#include "msg/Messenger.h"
#include "messages/MMDSBeacon.h"

class MMonCommand;
class MMDSLoadTargets;
class MMDSMap;
class FileSystemCommandHandler;

#define MDS_HEALTH_PREFIX "mds_health"

class MDSMonitor : public PaxosService {
 public:
  MDSMonitor(Monitor *mn, Paxos *p, string service_name);

  // service methods
  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void init() override;
  void create_pending() override; 
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  // we don't require full versions; don't encode any.
  void encode_full(MonitorDBStore::TransactionRef t) override { }
  version_t get_trim_to() override;

  bool preprocess_query(MonOpRequestRef op) override;  // true if processed.
  bool prepare_update(MonOpRequestRef op) override;
  bool should_propose(double& delay) override;

  void on_active() override;
  void on_restart() override;

  void check_subs();
  void check_sub(Subscription *sub);

  const FSMap &get_pending() const { return pending_fsmap; }
  const FSMap &get_fsmap() const { return fsmap; }
  void dump_info(Formatter *f);
  int print_nodes(Formatter *f);

  /**
   * Return true if a blacklist was done (i.e. OSD propose needed)
   */
  bool fail_mds_gid(mds_gid_t gid);
 protected:
  // mds maps
  FSMap fsmap;           // current
  FSMap pending_fsmap;  // current + pending updates

  // my helpers
  void print_map(FSMap &m, int dbl=7);
  void update_logger();

  void _updated(MonOpRequestRef op);

  void _note_beacon(class MMDSBeacon *m);
  bool preprocess_beacon(MonOpRequestRef op);
  bool prepare_beacon(MonOpRequestRef op);

  bool preprocess_offload_targets(MonOpRequestRef op);
  bool prepare_offload_targets(MonOpRequestRef op);

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail,
		  CephContext *cct) const override;
  int fail_mds(std::ostream &ss, const std::string &arg);

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  int parse_role(
      const std::string &role_str,
      mds_role_t *role,
      std::ostream &ss);

  void modify_legacy_filesystem(
      std::function<void(std::shared_ptr<Filesystem> )> fn);
  int legacy_filesystem_command(
      MonOpRequestRef op,
      std::string const &prefix,
      map<string, cmd_vartype> &cmdmap,
      std::stringstream &ss);
  int filesystem_command(
      MonOpRequestRef op,
      std::string const &prefix,
      map<string, cmd_vartype> &cmdmap,
      std::stringstream &ss);

  // beacons
  struct beacon_info_t {
    utime_t stamp;
    uint64_t seq;
  };
  map<mds_gid_t, beacon_info_t> last_beacon;

  bool try_standby_replay(
      const MDSMap::mds_info_t& finfo,
      const Filesystem &leader_fs,
      const MDSMap::mds_info_t& ainfo);

  std::list<std::shared_ptr<FileSystemCommandHandler> > handlers;

  bool maybe_promote_standby(std::shared_ptr<Filesystem> fs);
  bool maybe_expand_cluster(std::shared_ptr<Filesystem> fs);
  void maybe_replace_gid(mds_gid_t gid, const beacon_info_t &beacon,
      bool *mds_propose, bool *osd_propose);
  void tick() override;     // check state, take actions

  int dump_metadata(const string& who, Formatter *f, ostream& err);

  void update_metadata(mds_gid_t gid, const Metadata& metadata);
  void remove_from_metadata(MonitorDBStore::TransactionRef t);
  int load_metadata(map<mds_gid_t, Metadata>& m);
  void count_metadata(const string& field, Formatter *f);

  // MDS daemon GID to latest health state from that GID
  std::map<uint64_t, MDSHealth> pending_daemon_health;
  std::set<uint64_t> pending_daemon_health_rm;

  map<mds_gid_t, Metadata> pending_metadata;

  mds_gid_t gid_from_arg(const std::string& arg, std::ostream& err);

  // When did the mon last call into our tick() method?  Used for detecting
  // when the mon was not updating us for some period (e.g. during slow
  // election) to reset last_beacon timeouts
  utime_t last_tick;
};

#endif
