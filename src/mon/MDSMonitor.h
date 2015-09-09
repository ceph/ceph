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
    MMDSBeacon *m;
  public:
    C_Updated(MDSMonitor *a, MMDSBeacon *c) :
      mm(a), m(c) {}
    void finish(int r) {
      if (r >= 0)
	mm->_updated(m);   // success
      else if (r == -ECANCELED) {
	mm->mon->no_reply(m);
	m->put();
      } else {
	mm->dispatch((PaxosServiceMessage*)m);        // try again
      }
    }
  };

  void create_new_fs(MDSMap &m, int metadata_pool, int data_pool);

  version_t get_trim_to();

  // service methods
  void create_initial();
  void update_from_paxos(bool *need_bootstrap);
  void create_pending(); 
  void encode_pending(MonitorDBStore::Transaction *t);
  // we don't require full versions; don't encode any.
  virtual void encode_full(MonitorDBStore::Transaction *t) { }

  void update_logger();

  void _updated(MMDSBeacon *m);
 
  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);
  bool should_propose(double& delay);

  void on_active();

  void _note_beacon(class MMDSBeacon *m);
  bool preprocess_beacon(class MMDSBeacon *m);
  bool prepare_beacon(class MMDSBeacon *m);

  bool preprocess_offload_targets(MMDSLoadTargets *m);
  bool prepare_offload_targets(MMDSLoadTargets *m);

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) const;
  int fail_mds(std::ostream &ss, const std::string &arg);
  void fail_mds_gid(uint64_t gid);

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  // beacons
  struct beacon_info_t {
    utime_t stamp;
    uint64_t seq;
  };
  map<uint64_t, beacon_info_t> last_beacon;

  bool try_standby_replay(MDSMap::mds_info_t& finfo, MDSMap::mds_info_t& ainfo);

public:
  MDSMonitor(Monitor *mn, Paxos *p, string service_name)
    : PaxosService(mn, p, service_name)
  {
  }

  void tick();     // check state, take actions

  void dump_info(Formatter *f);

  void check_subs();
  void check_sub(Subscription *sub);

};

#endif
