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

struct MMonHealth : public MMonQuorumService
{
  static const int HEAD_VERSION = 1;

  enum {
    OP_TELL = 1,
  };

  int service_type;
  int service_op;

  // service specific data
  DataStats data_stats;

  MMonHealth() : MMonQuorumService(MSG_MON_HEALTH, HEAD_VERSION) { }
  MMonHealth(uint32_t type, int op = 0) :
    MMonQuorumService(MSG_MON_HEALTH, HEAD_VERSION),
    service_type(type),
    service_op(op)
  { }

private:
  ~MMonHealth() { }

public:
  const char *get_type_name() const { return "mon_health"; }
  const char *get_service_op_name() const {
    switch (service_op) {
    case OP_TELL: return "tell";
    }
    return "???";
  }
  void print(ostream &o) const {
    o << "mon_health( service " << get_service_type()
      << " op " << get_service_op_name()
      << " e " << get_epoch() << " r " << get_round()
      << " )";
  }

  int get_service_type() const {
    return service_type;
  }

  int get_service_op() {
    return service_op;
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    service_decode(p);
    ::decode(service_type, p);
    ::decode(service_op, p);
    ::decode(data_stats, p);
  }

  void encode_payload(uint64_t features) {
    service_encode();
    ::encode(service_type, payload);
    ::encode(service_op, payload);
    ::encode(data_stats, payload);
  }

};
REGISTER_MESSAGE(MMonHealth, MSG_MON_HEALTH);
#endif /* CEPH_MMON_HEALTH_H */
