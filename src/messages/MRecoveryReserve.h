// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MRECOVERY_H
#define CEPH_MRECOVERY_H

#include "msg/Message.h"

class MRecoveryReserve : public Message {
  static const int HEAD_VERSION = 3;
  static const int COMPAT_VERSION = 2;
public:
  spg_t pgid;
  epoch_t query_epoch;
  enum {
    REQUEST = 0,   // primary->replica: please reserve slot
    GRANT = 1,     // replica->primary: ok, i reserved it
    RELEASE = 2,   // primary->replica: release the slot i reserved before
    REVOKE = 3,    // replica->primary: i'm taking back the slot i gave you
  };
  uint32_t type;
  uint32_t priority = 0;

  MRecoveryReserve()
    : Message(MSG_OSD_RECOVERY_RESERVE, HEAD_VERSION, COMPAT_VERSION),
      query_epoch(0), type(-1) {}
  MRecoveryReserve(int type,
		   spg_t pgid,
		   epoch_t query_epoch,
		   unsigned prio = 0)
    : Message(MSG_OSD_RECOVERY_RESERVE, HEAD_VERSION, COMPAT_VERSION),
      pgid(pgid), query_epoch(query_epoch),
      type(type), priority(prio) {}

  const char *get_type_name() const override {
    return "MRecoveryReserve";
  }

  void print(ostream& out) const override {
    out << "MRecoveryReserve(" << pgid;
    switch (type) {
    case REQUEST:
      out << " REQUEST";
      break;
    case GRANT:
      out << " GRANT";
      break;
    case RELEASE:
      out << " RELEASE";
      break;
    case REVOKE:
      out << " REVOKE";
      break;
    }
    out << " e" << query_epoch << ")";
    if (type == REQUEST) out << ", prio: " << priority;
    return;
  }

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    decode(pgid.pgid, p);
    decode(query_epoch, p);
    decode(type, p);
    decode(pgid.shard, p);
    if (header.version >= 3) {
      decode(priority, p);
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid.pgid, payload);
    encode(query_epoch, payload);
    encode(type, payload);
    encode(pgid.shard, payload);
    encode(priority, payload);
  }
};

#endif
