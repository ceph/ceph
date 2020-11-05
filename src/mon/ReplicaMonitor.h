// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#ifndef CEPH_REPLICA_MONITOR_H
#define CEPH_REPLICA_MONITOR_H

#include "ReplicaDaemonMap.h"
#include "PaxosService.h"

constexpr std::string_view REPLICAMAP_DB_PREFIX{"replicamap_db"};

class ReplicaMonitor : public PaxosService
{
public:
  ReplicaMonitor(Monitor& monitor, Paxos& paxos, std::string service_name);
  void init() override;
  bool is_leader() {
    return mon.is_leader();
  }

  //service pure virtual function
  void create_initial() override;
  void update_from_paxos(bool *need_boostrap) override;
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef mon_dbstore_tran) override;
  //Do we need full version? If not, empty implementation
  void encode_full(MonitorDBStore::TransactionRef mon_dbstore_tran) override;
  //return true if being processed
  bool preprocess_query(MonOpRequestRef mon_op_req) override;
  bool prepare_update(MonOpRequestRef mon_op_req) override;
  void on_restart() override;

  void check_sub(Subscription *sub);
private:
  /* The trusted ReplicaDaemonMap is at the ReplicMonitor attached with Leader Monitor */
  ReplicaDaemonMap cur_cache_replicadaemon_map;  /* cache replicadaemon_map at current epoch */
  /* cache replicadaemon_map pending to be updated to MonitorDBStore */
  ReplicaDaemonMap pending_cache_replicadaemon_map;
};

#endif // defined CEPH_REPLICA_MONITOR_H
