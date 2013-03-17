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

struct MMonQuorumService : public Message
{
  epoch_t epoch;
  version_t round;

  MMonQuorumService(int type, int head=1, int compat=1) :
    Message(type, head, compat),
    epoch(0),
    round(0)
  { }

protected:
  ~MMonQuorumService() { }

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
    ::encode(epoch, payload);
    ::encode(round, payload);
  }

  void service_decode(bufferlist::iterator &p) {
    ::decode(epoch, p);
    ::decode(round, p);
  }

  void encode_payload(uint64_t features) {
    assert(0 == "MMonQuorumService message must always be a base class");
  }

  void decode_payload() {
    assert(0 == "MMonQuorumService message must always be a base class");
  }

  const char *get_type_name() const { return "quorum_service"; }
};

#endif /* CEPH_MMON_QUORUM_SERVICE_H */
