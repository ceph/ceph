// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_MMON_HEALTH_H
#define CEPH_MMON_HEALTH_H

#include "msg/Message.h"
#include "messages/MMonQuorumService.h"
#include "mon/mon_types.h"

class MMonHealth : public MessageInstance<MMonHealth, MMonQuorumService> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 1;

  int service_type = 0;
  int service_op = 0;

  // service specific data
  DataStats data_stats;

  MMonHealth() : MessageInstance(MSG_MON_HEALTH, HEAD_VERSION) { }

private:
  ~MMonHealth() override { }

public:
  std::string_view get_type_name() const override { return "mon_health"; }
  void print(ostream &o) const override {
    o << "mon_health("
      << " e " << get_epoch()
      << " r " << get_round()
      << " )";
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    service_decode(p);
    decode(service_type, p);
    decode(service_op, p);
    decode(data_stats, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    service_encode();
    encode(service_type, payload);
    encode(service_op, payload);
    encode(data_stats, payload);
  }

};

#endif /* CEPH_MMON_HEALTH_H */
