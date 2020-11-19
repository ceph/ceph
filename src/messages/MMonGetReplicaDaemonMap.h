// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#ifndef CEPH_MMONGETREPLICAMAP_H
#define CEPH_MMONGETREPLICAMAP_H

#include "include/types.h"
#include "msg/Message.h"
#include "messages/PaxosServiceMessage.h"

class MMonGetReplicaDaemonMap : public PaxosServiceMessage {
private:
  static constexpr int HEADER_VERSION = 0;
  static constexpr int COMPAT_VERSION = 0;

public:
  MMonGetReplicaDaemonMap()
    : PaxosServiceMessage{CEPH_MSG_MON_GET_REPLICADAEMONMAP, 0,
                          HEADER_VERSION, COMPAT_VERSION} {
  }

  ~MMonGetReplicaDaemonMap() override {
  }

public:
  std::string_view get_type_name() const override {
    return "mon_get_replicadaemon_map";
  }
  void print(std::ostream& out) const override {
    out << "mon_get_replicadaemon_map";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif // CEPH_MMONGETREPLICAMAP_H
