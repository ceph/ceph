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

#include "include/types.h"
#include "PaxosFSMap.h"
#include "PaxosService.h"
#include "msg/Messenger.h"
#include "messages/MMDSBeacon.h"
#include "CommandHandler.h"

class FileSystemCommandHandler;

class MDSMonitor : public PaxosService, public PaxosFSMap, protected CommandHandler {
 public:
  MDSMonitor(Monitor *mn, Paxos *p, string service_name);

  // service methods
  void create_initial() override;
  void get_store_prefixes(std::set<string>& s) const override;
  void update_from_paxos(bool *need_bootstrap) override;
  void init() override;
  void create_pending() override; 
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  // we don't require full versions; don't encode any.
  void encode_full(MonitorDBStore::TransactionRef t) override { }
  version_t get_trim_to() const override;

  bool preprocess_query(MonOpRequestRef op) override;  // true if processed.
  bool prepare_update(MonOpRequestRef op) override;
  bool should_propose(double& delay) override;

  void on_active() override;
  void on_restart() override;

  void check_subs();
  void check_sub(Subscription *sub);

  void dump_info(Formatter *f);
  int print_nodes(Formatter *f);

  /**
   * Return true if a blacklist was done (i.e. OSD propose needed)
   */
  bool fail_mds_gid(FSMap &fsmap, mds_gid_t gid);

  bool is_leader() const override { return mon->is_leader(); }

 protected:
  // my helpers
  template<int dblV = 7>
  void print_map(const FSMap &m);

  void _updated(MonOpRequestRef op);

  void _note_beacon(class MMDSBeacon *m);
  bool preprocess_beacon(MonOpRequestRef op);
  bool prepare_beacon(MonOpRequestRef op);

  bool preprocess_offload_targets(MonOpRequestRef op);
  bool prepare_offload_targets(MonOpRequestRef op);

  int fail_mds(FSMap &fsmap, std::ostream &ss,
      const std::string &arg,
      MDSMap::mds_info_t *failed_info);

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  int filesystem_command(
      FSMap &fsmap,
      MonOpRequestRef op,
      std::string const &prefix,
      const cmdmap_t& cmdmap,
      std::stringstream &ss);

  // beacons
  struct beacon_info_t {
    mono_time stamp = mono_clock::zero();
    uint64_t seq = 0;
    beacon_info_t() {}
    beacon_info_t(mono_time stamp, uint64_t seq) : stamp(stamp), seq(seq) {}
  };
  map<mds_gid_t, beacon_info_t> last_beacon;

  std::list<std::shared_ptr<FileSystemCommandHandler> > handlers;

  bool maybe_promote_standby(FSMap& fsmap, Filesystem& fs);
  bool maybe_resize_cluster(FSMap &fsmap, fs_cluster_id_t fscid);
  void maybe_replace_gid(FSMap &fsmap, mds_gid_t gid,
      const MDSMap::mds_info_t& info, bool *mds_propose, bool *osd_propose);
  void tick() override;     // check state, take actions

  int dump_metadata(const FSMap &fsmap, const std::string &who, Formatter *f,
      ostream& err);

  void update_metadata(mds_gid_t gid, const Metadata& metadata);
  void remove_from_metadata(const FSMap &fsmap, MonitorDBStore::TransactionRef t);
  int load_metadata(map<mds_gid_t, Metadata>& m);
  void count_metadata(const std::string& field, Formatter *f);
public:
  void count_metadata(const std::string& field, map<string,int> *out);
protected:

  // MDS daemon GID to latest health state from that GID
  std::map<uint64_t, MDSHealth> pending_daemon_health;
  std::set<uint64_t> pending_daemon_health_rm;

  map<mds_gid_t, Metadata> pending_metadata;

  mds_gid_t gid_from_arg(const FSMap &fsmap, const std::string &arg, std::ostream& err);

  // When did the mon last call into our tick() method?  Used for detecting
  // when the mon was not updating us for some period (e.g. during slow
  // election) to reset last_beacon timeouts
  mono_time last_tick = mono_clock::zero();
};

#endif
