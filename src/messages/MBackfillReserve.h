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

#ifndef CEPH_MBACKFILL_H
#define CEPH_MBACKFILL_H

#include "msg/Message.h"

class MBackfillReserve : public Message {
  static const int HEAD_VERSION = 4;
  static const int COMPAT_VERSION = 4;
public:
  spg_t pgid;
  epoch_t query_epoch;
  enum {
    REQUEST = 0,   // primary->replica: please reserve a slot
    GRANT = 1,     // replica->primary: ok, i reserved it
    REJECT = 2,    // replica->primary: sorry, try again later (*)
    RELEASE = 3,   // primary->replcia: release the slot i reserved before
    TOOFULL = 4,   // replica->primary: too full, stop backfilling
    REVOKE = 5,    // replica->primary: i'm taking back the slot i gave you
    // (*) NOTE: prior to luminous, REJECT was overloaded to also mean release
  };
  uint32_t type;
  uint32_t priority;

  MBackfillReserve()
    : Message(MSG_OSD_BACKFILL_RESERVE, HEAD_VERSION, COMPAT_VERSION),
      query_epoch(0), type(-1), priority(-1) {}
  MBackfillReserve(int type,
		   spg_t pgid,
		   epoch_t query_epoch, unsigned prio = -1)
    : Message(MSG_OSD_BACKFILL_RESERVE, HEAD_VERSION, COMPAT_VERSION),
      pgid(pgid), query_epoch(query_epoch),
      type(type), priority(prio) {}

  const char *get_type_name() const override {
    return "MBackfillReserve";
  }

  void print(ostream& out) const override {
    out << "MBackfillReserve ";
    switch (type) {
    case REQUEST:
      out << "REQUEST ";
      break;
    case GRANT:
      out << "GRANT "; 
      break;
    case REJECT:
      out << "REJECT ";
      break;
    case RELEASE:
      out << "RELEASE ";
      break;
    case TOOFULL:
      out << "TOOFULL ";
      break;
    case REVOKE:
      out << "REVOKE ";
      break;
    }
    out << " pgid: " << pgid << ", query_epoch: " << query_epoch;
    if (type == REQUEST) out << ", prio: " << priority;
    return;
  }

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    decode(pgid.pgid, p);
    decode(query_epoch, p);
    decode(type, p);
    decode(priority, p);
    decode(pgid.shard, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    if (!HAVE_FEATURE(features, RECOVERY_RESERVATION_2)) {
      header.version = 3;
      header.compat_version = 3;
      encode(pgid.pgid, payload);
      encode(query_epoch, payload);
      encode((type == RELEASE || type == TOOFULL || type == REVOKE) ?
	       REJECT : type, payload);
      encode(priority, payload);
      encode(pgid.shard, payload);
      return;
    }
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    encode(pgid.pgid, payload);
    encode(query_epoch, payload);
    encode(type, payload);
    encode(priority, payload);
    encode(pgid.shard, payload);
  }
};

#endif
