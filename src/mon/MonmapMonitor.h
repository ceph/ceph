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

#include "include/types.h"
#include "msg/Messenger.h"

#include "PaxosService.h"
#include "MonMap.h"
#include "MonitorDBStore.h"

class MonmapMonitor : public PaxosService {
 public:
  MonmapMonitor(Monitor *mn, Paxos *p, const string& service_name)
    : PaxosService(mn, p, service_name)
  {
  }
  MonMap pending_map; //the pending map awaiting passage

  void create_initial() override;

  void update_from_paxos(bool *need_bootstrap) override;

  void create_pending() override;

  void encode_pending(MonitorDBStore::TransactionRef t) override;
  // we always encode the full map; we have no use for full versions
  void encode_full(MonitorDBStore::TransactionRef t) override { }

  void on_active() override;
  void apply_mon_features(const mon_feature_t& features,
			  int min_mon_release);

  void dump_info(Formatter *f);

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  bool preprocess_join(MonOpRequestRef op);
  bool prepare_join(MonOpRequestRef op);

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  int get_monmap(bufferlist &bl);

  /*
   * Since monitors are pretty
   * important, this implementation will just write 0.0.
   */
  bool should_propose(double& delay) override;

  void check_sub(Subscription *sub);

private:
  void check_subs();
  bufferlist monmap_bl;
};


#endif
