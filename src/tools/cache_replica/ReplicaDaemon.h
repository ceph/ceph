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
private:
  bool clean_cache();
  bool init_connection();

public:
  ReplicaDaemon(std::string_view name,
                int32_t daemon_id,
                std::string& rnic_addr,
                Messenger* msgr_public,
                MonClient *mon_client,
                boost::asio::io_context& ioctx);
  int init();
  void ms_fast_dispatch(Message *m) override;
  bool ms_dispatch(Message *m) override;

protected:
  std::string name;
  Messenger *msgr_public;
  MonClient *mon_client;
  ReplicaDaemonInfo self_state;
  boost::asio::io_context& ioctx;
  LogClient log_client;
  LogChannelRef clog;

private:
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override;
  bool ms_handle_refused(Connection *con) override;
};

#endif // defined CEPH_REPLICA_H
