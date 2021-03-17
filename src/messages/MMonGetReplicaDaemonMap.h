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

struct ReqReplicaDaemonInfo {
  int32_t replicas;
  uint64_t replica_size;

  ReqReplicaDaemonInfo(int32_t replicas = 0, uint64_t replica_size = 0):
    replicas(replicas), replica_size(replica_size) {
  }

  void encode(bufferlist& req_bl, uint64_t features = 0) const {
    using ceph::encode;
    ENCODE_START(0, 0, req_bl);

    encode(replicas, req_bl);
    encode(replica_size, req_bl);

    ENCODE_FINISH(req_bl);
  }

  void decode(bufferlist::const_iterator& req_bl_it) {
    using ceph::encode;
    DECODE_START(0, req_bl_it);

    decode(replicas, req_bl_it);
    decode(replica_size, req_bl_it);

    DECODE_FINISH(req_bl_it);
  }

  void decode(bufferlist& req_bl) {
    auto bl_it = req_bl.cbegin();
    decode(bl_it);
  }
};
WRITE_CLASS_ENCODER(ReqReplicaDaemonInfo)

class MMonGetReplicaDaemonMap : public PaxosServiceMessage {
private:
  static constexpr int HEADER_VERSION = 0;
  static constexpr int COMPAT_VERSION = 0;

public:
  ReqReplicaDaemonInfo req_daemon_info;
  MMonGetReplicaDaemonMap(int32_t replicas = 0, uint64_t replica_size = 0)
    : PaxosServiceMessage{CEPH_MSG_MON_GET_REPLICADAEMONMAP, 0,
                          HEADER_VERSION, COMPAT_VERSION},
      req_daemon_info(replicas, replica_size) {
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
    header.version = HEADER_VERSION;
    header.compat_version = COMPAT_VERSION;
    using ceph::encode;
    paxos_encode();
    encode(req_daemon_info, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(req_daemon_info, p);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif // CEPH_MMONGETREPLICAMAP_H
