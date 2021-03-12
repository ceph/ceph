// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#ifndef CEPH_REPLICADAEMONMAP_H
#define CEPH_REPLICADAEMONMAP_H

#include "include/types.h"
#include "msg/msg_types.h"

struct ReplicaDaemonInfo {
  version_t commit_epoch = 0;
  int32_t daemon_id;
  int32_t rnic_bind_port;
  std::string rnic_addr;
  uint64_t free_size;

  void encode(bufferlist& replicadaemon_state_bl, uint64_t features = 0) const {
    ENCODE_START(0, 0, replicadaemon_state_bl);
    encode(commit_epoch, replicadaemon_state_bl);
    encode(daemon_id, replicadaemon_state_bl);
    encode(rnic_bind_port, replicadaemon_state_bl);
    encode(rnic_addr, replicadaemon_state_bl, features);
    encode(free_size, replicadaemon_state_bl, features);
    ENCODE_FINISH(replicadaemon_state_bl);
  }

  void decode(bufferlist::const_iterator& replicadaemon_state_bl_it) {
    DECODE_START(0, replicadaemon_state_bl_it);
    decode(commit_epoch, replicadaemon_state_bl_it);
    decode(daemon_id, replicadaemon_state_bl_it);
    decode(rnic_bind_port, replicadaemon_state_bl_it);
    decode(rnic_addr, replicadaemon_state_bl_it);
    decode(free_size, replicadaemon_state_bl_it);
    DECODE_FINISH(replicadaemon_state_bl_it);
  }

  void set_rnic_bind_port(int32_t rnic_bind_port) {
    this->rnic_bind_port = rnic_bind_port;
  }

  void set_free_size(uint64_t free_size) {
    this->free_size = free_size;
  }

  void print_state(std::ostream& oss) const;
};
WRITE_CLASS_ENCODER(ReplicaDaemonInfo)

class ReplicaDaemonMap {
public:
  ReplicaDaemonMap();

  void encode(bufferlist& replicadaemon_map_bl, uint64_t features) const;
  void decode(bufferlist::const_iterator& replicadaemon_map_bl_it);
  void decode(bufferlist& replicadaemon_map_bl) {
    auto bl_it = replicadaemon_map_bl.cbegin();
    decode(bl_it);
  }

  epoch_t get_epoch() const {
    return epoch;
  }
  void set_epoch(epoch_t epoch) {
    this->epoch = epoch;
  }

  void update_daemonmap(const ReplicaDaemonInfo& new_daemon_state);
  void print_map(std::ostream& oss) const;

  bool empty() {
    return replicadaemons_state.empty();
  }

private:
  epoch_t epoch = 0;
  std::vector<ReplicaDaemonInfo> replicadaemons_state; // Let's change it to be map:
  // std::map<std::pair<pool, rbd_image>, std::tuple<size, replicated>>;
};
WRITE_CLASS_ENCODER_FEATURES(ReplicaDaemonMap)

#endif // defined CEPH_REPLICADAEMONMAP_H
