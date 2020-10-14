// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#ifndef CEPH_MREPLICADAEMON_MAP_H
#define CEPH_MREPLICADAEMON_MAP_H

#include "msg/Message.h"
#include "mon/ReplicaDaemonMap.h"

class MReplicaDaemonMap : public Message {
private:
  ReplicaDaemonMap replicadaemon_map;

public:
  const ReplicaDaemonMap& get_map() const {
    return replicadaemon_map;
  }

  MReplicaDaemonMap() :
    Message{CEPH_MSG_REPLICADAEMON_MAP} {
  }
  MReplicaDaemonMap(ReplicaDaemonMap &replicadaemon_map) :
    Message{CEPH_MSG_REPLICADAEMON_MAP},
    replicadaemon_map(replicadaemon_map) {
  }

  std::string_view get_type_name() const override {
    return "replicamap";
  }

  ~MReplicaDaemonMap() override {
  }
public:
  void print(std::ostream& out) const override {
    out << get_type_name() << "(e " << replicadaemon_map.get_epoch() << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(replicadaemon_map, p);
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(replicadaemon_map, payload, features);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif // defined CEPH_MREPLICADAEMON_MAP_H
