// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

/*
 * The Monmap Monitor is used to track the monitors in the cluster.
 */

#ifndef CEPH_MONMAPMONITOR_H
#define CEPH_MONMAPMONITOR_H

#include <map>
#include <set>

using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "PaxosService.h"
#include "MonMap.h"
#include "MonitorDBStore.h"

class MMonGetMap;
class MMonMap;
class MMonCommand;
class MMonJoin;

class MonmapMonitor : public PaxosService {
 public:
  MonmapMonitor(Monitor *mn, Paxos *p, const string& service_name)
    : PaxosService(mn, p, service_name)
  {
  }
  MonMap pending_map; //the pending map awaiting passage

  void create_initial();

  void update_from_paxos(bool *need_bootstrap);

  void create_pending();

  void encode_pending(MonitorDBStore::TransactionRef t);
  // we always encode the full map; we have no use for full versions
  virtual void encode_full(MonitorDBStore::TransactionRef t) { }

  void on_active();
  void apply_mon_features(const mon_feature_t& features);

  void dump_info(Formatter *f);

  bool preprocess_query(MonOpRequestRef op);
  bool prepare_update(MonOpRequestRef op);

  bool preprocess_join(MonOpRequestRef op);
  bool prepare_join(MonOpRequestRef op);

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail,
		  CephContext *cct) const override;

  int get_monmap(bufferlist &bl);

  /*
   * Since monitors are pretty
   * important, this implementation will just write 0.0.
   */
  bool should_propose(double& delay);

  void tick();

 private:
  bufferlist monmap_bl;

  class C_ApplyFeatures : public Context {
    MonmapMonitor *svc;
    mon_feature_t features;
  public:
    C_ApplyFeatures(MonmapMonitor *s, const mon_feature_t& f) :
      svc(s), features(f) { }
    void finish(int r) {
      if (r >= 0) {
        svc->apply_mon_features(features);
      } else if (r == -EAGAIN || r == -ECANCELED) {
        // discard features if we're no longer on the quorum that
        // established them in the first place.
        return;
      } else {
        assert(0 == "bad C_ApplyFeatures return value");
      }
    }
  };

};


#endif
