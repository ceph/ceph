// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#ifndef CEPH_MREPLICABLINK_H
#define CEPH_MREPLICABLINK_H

#include "include/types.h"
#include "msg/Message.h"
#include "mon/ReplicaDaemonMap.h"
#include "messages/PaxosServiceMessage.h"

class MReplicaDaemonBlink : public PaxosServiceMessage {
private:
  static constexpr int HEADER_VERSION = 0;
  static constexpr int COMPAT_VERSION = 0;
  ReplicaDaemonState replicadaemon_state;

public:
  MReplicaDaemonBlink() :
    PaxosServiceMessage{MSG_REPLICADAEMON_BLINK, 0, HEADER_VERSION, COMPAT_VERSION} {
  }
  MReplicaDaemonBlink(ReplicaDaemonState &replicadaemon_state) :
    PaxosServiceMessage{MSG_REPLICADAEMON_BLINK, replicadaemon_state.commit_epoch,
                        HEADER_VERSION, COMPAT_VERSION},
    replicadaemon_state(replicadaemon_state) {
  }

  ~MReplicaDaemonBlink() override {
  }

public:
  const ReplicaDaemonState& get_replicadaemon_stateref(void) const {
    return replicadaemon_state;
  }
  void update_replicadaemon_status(ReplicaDaemonStatus new_status) {
    replicadaemon_state.update_replicadaemon_status(new_status);
  }
  void print(std::ostream& out) const override {
    replicadaemon_state.print_state(out);
  }

  std::string_view get_type_name() const override {
    return "replica_blink";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(replicadaemon_state, payload, features);
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(replicadaemon_state, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif // defined CEPH_MREPLICABLINK_H
