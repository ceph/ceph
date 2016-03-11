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
  static const int HEAD_VERSION = 3;
  static const int COMPAT_VERSION = 1;
public:
  spg_t pgid;
  epoch_t query_epoch;
  enum {
    REQUEST = 0,
    GRANT = 1,
    REJECT = 2,
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

  const char *get_type_name() const {
    return "MBackfillReserve";
  }

  void print(ostream& out) const {
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
    }
    out << " pgid: " << pgid << ", query_epoch: " << query_epoch;
    if (type == REQUEST) out << ", prio: " << priority;
    return;
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(pgid.pgid, p);
    ::decode(query_epoch, p);
    ::decode(type, p);
    if (header.version > 1)
      ::decode(priority, p);
    else
      priority = 0;
    if (header.version >= 3)
      ::decode(pgid.shard, p);
    else
      pgid.shard = shard_id_t::NO_SHARD;

  }

  void encode_payload(uint64_t features) {
    ::encode(pgid.pgid, payload);
    ::encode(query_epoch, payload);
    ::encode(type, payload);
    ::encode(priority, payload);
    ::encode(pgid.shard, payload);
  }
};
REGISTER_MESSAGE(MBackfillReserve, MSG_OSD_BACKFILL_RESERVE);
#endif
