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

  void update_from_paxos();

  void create_pending();

  void encode_pending(MonitorDBStore::Transaction *t);
  // we always encode the full map; we have no use for full versions
  virtual void encode_full(MonitorDBStore::Transaction *t) { }

  void on_active();

  void dump_info(Formatter *f);

  bool preprocess_query(PaxosServiceMessage *m);
  bool prepare_update(PaxosServiceMessage *m);

  bool preprocess_join(MMonJoin *m);
  bool prepare_join(MMonJoin *m);

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) const;

  /*
   * Since monitors are pretty
   * important, this implementation will just write 0.0.
   */
  bool should_propose(double& delay);

  void tick();

 private:
  bufferlist monmap_bl;
};


#endif
