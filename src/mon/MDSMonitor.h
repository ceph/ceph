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
#include "msg/Messenger.h"

#include "mds/MDSMap.h"

#include "PaxosService.h"
#include "Session.h"

#include "messages/MMDSBeacon.h"

class MMDSGetMap;
class MMonCommand;
class MMDSLoadTargets;

#define MDS_HEALTH_PREFIX "mds_health"

class MDSMonitor : public PaxosService {
 public:
  // mds maps
  MDSMap mdsmap;          // current
  bufferlist mdsmap_bl;   // encoded

  MDSMap pending_mdsmap;  // current + pending updates

  // my helpers
  void print_map(MDSMap &m, int dbl=7);

  class C_Updated : public Context {
    MDSMonitor *mm;
    MonOpRequestRef op;
  public:
    C_Updated(MDSMonitor *a, MonOpRequestRef c) :
      mm(a), op(c) {}
    void finish(int r) {
      if (r >= 0)
	mm->_updated(op);   // success
      else if (r == -ECANCELED) {
	mm->mon->no_reply(op);
      } else {
	mm->dispatch(op);        // try again
      }
    }
  };

  void create_new_fs(MDSMap &m, const std::string &name, int metadata_pool, int data_pool);

  version_t get_trim_to();

  // service methods
  void create_initial();
  void update_from_paxos(bool *need_bootstrap);
  void init();
  void create_pending(); 
  void encode_pending(MonitorDBStore::TransactionRef t);
  // we don't require full versions; don't encode any.
  virtual void encode_full(MonitorDBStore::TransactionRef t) { }

  void update_logger();

  void _updated(MonOpRequestRef op);
 
  bool preprocess_query(MonOpRequestRef op);  // true if processed.
  bool prepare_update(MonOpRequestRef op);
  bool should_propose(double& delay);

  void on_active();

  void _note_beacon(class MMDSBeacon *m);
  bool preprocess_beacon(MonOpRequestRef op);
  bool prepare_beacon(MonOpRequestRef op);

  bool preprocess_offload_targets(MonOpRequestRef op);
  bool prepare_offload_targets(MonOpRequestRef op);

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) const;
  int fail_mds(std::ostream &ss, const std::string &arg);
  void fail_mds_gid(mds_gid_t gid);

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);
  int management_command(
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

  bool try_standby_replay(MDSMap::mds_info_t& finfo, MDSMap::mds_info_t& ainfo);

public:
  MDSMonitor(Monitor *mn, Paxos *p, string service_name)
    : PaxosService(mn, p, service_name)
  {
  }

  void tick();     // check state, take actions

  void dump_info(Formatter *f);
  int dump_metadata(const string& who, Formatter *f, ostream& err);
  int print_nodes(Formatter *f);

  void check_subs();
  void check_sub(Subscription *sub);

private:
  void update_metadata(mds_gid_t gid, const Metadata& metadata);
  void remove_from_metadata(MonitorDBStore::TransactionRef t);
  int load_metadata(map<mds_gid_t, Metadata>& m);

  // MDS daemon GID to latest health state from that GID
  std::map<uint64_t, MDSHealth> pending_daemon_health;
  std::set<uint64_t> pending_daemon_health_rm;

  map<mds_gid_t, Metadata> pending_metadata;

  int _check_pool(const int64_t pool_id, std::stringstream *ss) const;
  mds_gid_t gid_from_arg(const std::string& arg, std::ostream& err);
};

#endif
