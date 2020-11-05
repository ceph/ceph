// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#include "ReplicaDaemonMap.h"

void ReplicaDaemonState::print_state(std::ostream& oss) const
{
  oss << "commit at epoch: " << commit_epoch << ", "
      << "state : " << (daemon_status == STATE_BOOTING ? "daemon booting" :
                       daemon_status == STATE_ACTIVE   ? "daemon active" :
                       daemon_status == STATE_STOPPING ? "daemon stopping" :
                       daemon_status == STATE_DOWN     ? "daemon down" :
                       "wrong state") << ", "
      << "replica addr: " << replica_route_addr
      << std::endl;
}

ReplicaDaemonMap::ReplicaDaemonMap()
{
}

void ReplicaDaemonMap::encode(bufferlist& replicadaemon_map_bl, uint64_t features) const
{
  using ceph::encode;
  ENCODE_START(0, 0, replicadaemon_map_bl);

  encode(epoch, replicadaemon_map_bl);
  encode(replicadaemons_state, replicadaemon_map_bl);

  ENCODE_FINISH(replicadaemon_map_bl);
}

void ReplicaDaemonMap::decode(bufferlist::const_iterator& replicadaemon_map_bl_it)
{
  using ceph::decode;
  DECODE_START(0, replicadaemon_map_bl_it);

  decode(epoch, replicadaemon_map_bl_it);
  decode(replicadaemons_state, replicadaemon_map_bl_it);

  DECODE_FINISH(replicadaemon_map_bl_it);
}

void ReplicaDaemonMap::update_daemonmap(const ReplicaDaemonState& new_daemon_state) {
  bool replicadaemon_state_exist = false;
  for (auto& replicadaemon_state : replicadaemons_state) {
    if (replicadaemon_state.replica_route_addr.legacy_equals(
        new_daemon_state.replica_route_addr)) {
        replicadaemon_state_exist = true;
        replicadaemon_state.daemon_status = new_daemon_state.daemon_status;
        break;
    }
  }
  if (!replicadaemon_state_exist) {
    replicadaemons_state.push_back(new_daemon_state);
  }
}

void ReplicaDaemonMap::print_map(std::ostream& oss) const
{
  oss << "commit at epoch: " << epoch << std::endl;
  for (auto& per_state: replicadaemons_state) {
    oss << "  ";
    per_state.print_state(oss);
  }
}
