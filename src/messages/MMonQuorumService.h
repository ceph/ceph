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
#ifndef CEPH_MMON_QUORUM_SERVICE_H
#define CEPH_MMON_QUORUM_SERVICE_H

#include "msg/Message.h"

class MMonQuorumService : public Message {
public:
  epoch_t epoch = 0;
  version_t round = 0;

protected:
  MMonQuorumService(int type, int head)
    : Message{type, head, 1}
  {}
  ~MMonQuorumService() override {}

public:
  void set_epoch(epoch_t e) {
    epoch = e;
  }

  void set_round(version_t r) {
    round = r;
  }

  epoch_t get_epoch() const {
    return epoch;
  }

  version_t get_round() const {
    return round;
  }

  void service_encode() {
    using ceph::encode;
    encode(epoch, payload);
    encode(round, payload);
  }

  void service_decode(ceph::buffer::list::const_iterator &p) {
    using ceph::decode;
    decode(epoch, p);
    decode(round, p);
  }

  void encode_payload(uint64_t features) override {
    ceph_abort_msg("MMonQuorumService message must always be a base class");
  }

  void decode_payload() override {
    ceph_abort_msg("MMonQuorumService message must always be a base class");
  }

  std::string_view get_type_name() const override { return "quorum_service"; }
};

#endif /* CEPH_MMON_QUORUM_SERVICE_H */
