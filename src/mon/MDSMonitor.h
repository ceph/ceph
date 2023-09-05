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
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  MDSMonitor(Monitor &mn, Paxos &p, std::string service_name);

  // service methods
  void create_initial() override;
  void get_store_prefixes(std::set<std::string>& s) const override;
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

  bool should_print_status() const {
    auto& fs = get_fsmap();
    auto fs_count = fs.filesystem_count();
    auto standby_count = fs.get_num_standby();
    return fs_count > 0 || standby_count > 0;
  }

  void on_active() override;
  void on_restart() override;

  void check_subs();
  void check_sub(Subscription *sub);

  void dump_info(ceph::Formatter *f);
  int print_nodes(ceph::Formatter *f);

  /**
   * Return true if a blocklist was done (i.e. OSD propose needed)
   */
  bool fail_mds_gid(FSMap &fsmap, mds_gid_t gid);

  bool is_leader() const override { return mon.is_leader(); }

 protected:
  using mds_info_t = MDSMap::mds_info_t;

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
      const std::string &arg, mds_info_t *failed_info);

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
    ceph::mono_time stamp = ceph::mono_clock::zero();
    uint64_t seq = 0;
    beacon_info_t() {}
    beacon_info_t(ceph::mono_time stamp, uint64_t seq) : stamp(stamp), seq(seq) {}
  };
  std::map<mds_gid_t, beacon_info_t> last_beacon;

  std::list<std::shared_ptr<FileSystemCommandHandler> > handlers;

  bool maybe_promote_standby(FSMap& fsmap, const Filesystem& fs);
  bool maybe_resize_cluster(FSMap &fsmap, const Filesystem& fs);
  bool drop_mds(FSMap &fsmap, mds_gid_t gid, const mds_info_t* rep_info, bool* osd_propose);
  bool check_health(FSMap &fsmap, bool* osd_propose);
  void tick() override;     // check state, take actions

  int dump_metadata(const FSMap &fsmap, const std::string &who, ceph::Formatter *f,
		    std::ostream& err);

  void update_metadata(mds_gid_t gid, const Metadata& metadata);
  void remove_from_metadata(const FSMap &fsmap, MonitorDBStore::TransactionRef t);
  int load_metadata(std::map<mds_gid_t, Metadata>& m);
  void count_metadata(const std::string& field, ceph::Formatter *f);

public:
  void print_fs_summary(std::ostream& out) {
    get_fsmap().print_fs_summary(out);
  }
  void count_metadata(const std::string& field, std::map<std::string,int> *out);
  void get_versions(std::map<std::string, std::list<std::string>> &versions);

protected:
  // MDS daemon GID to latest health state from that GID
  std::map<uint64_t, MDSHealth> pending_daemon_health;
  std::set<uint64_t> pending_daemon_health_rm;

  std::map<mds_gid_t, Metadata> pending_metadata;

  mds_gid_t gid_from_arg(const FSMap &fsmap, const std::string &arg, std::ostream& err);

  // When did the mon last call into our tick() method?  Used for detecting
  // when the mon was not updating us for some period (e.g. during slow
  // election) to reset last_beacon timeouts
  ceph::mono_time last_tick = ceph::mono_clock::zero();

private:
  time last_fsmap_struct_flush = clock::zero();
  bool check_fsmap_struct_version = true;
};

#endif
