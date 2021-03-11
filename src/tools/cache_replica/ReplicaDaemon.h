// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#ifndef CEPH_REPLICA_H
#define CEPH_REPLICA_H

#include <string_view>
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "mon/MonClient.h"
#include "mon/ReplicaDaemonMap.h"
#include "common/LogClient.h"
#include "common/errno.h"

class ReplicaDaemon : public Dispatcher {
public:
  ReplicaDaemon(std::string_view name,
                Messenger* msgr_public,
                MonClient *mon_client,
                boost::asio::io_context& ioctx);
  int init();
  void ms_fast_dispatch(Message *m) override;
  bool ms_dispatch(Message *m) override;
  // There's no need to keep below function here.
  // Reason: ReplicaDaemon doesn't need ReplicaMap anymore
  void update_state_from_replicadaemon_map(ReplicaDaemonMap& replicadaemon_map_ref);

protected:
  std::string name;
  Messenger *msgr_public;
  MonClient *mon_client;
  ReplicaDaemonState self_state;
  boost::asio::io_context& ioctx;
  LogClient log_client;
  LogChannelRef clog;

private:
  std::unique_ptr<ReplicaDaemonMap> replicadaemon_map; // ReplicaDaemon do not need ReplicaDaemonMap
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override;
  bool ms_handle_refused(Connection *con) override;
};

#endif // defined CEPH_REPLICA_H
