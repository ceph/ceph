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
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;
public:
  pg_t pgid;
  epoch_t query_epoch;
  enum {
    REQUEST = 0,
    GRANT = 1,
    REJECT = 2,
  };
  int type;
  MBackfillReserve(int type,
		      pg_t pgid,
		      epoch_t query_epoch)
    : Message(MSG_OSD_BACKFILL_RESERVE, HEAD_VERSION, COMPAT_VERSION),
      pgid(pgid), query_epoch(query_epoch),
      type(type) {}

  MBackfillReserve() :
    Message(MSG_OSD_BACKFILL_RESERVE, HEAD_VERSION, COMPAT_VERSION) {}

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
    }
    out << " pgid: " << pgid << ", query_epoch: " << query_epoch;
    return;
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(pgid, p);
    ::decode(query_epoch, p);
    ::decode(type, p);
  }

  void encode_payload(uint64_t features) {
    ::encode(pgid, payload);
    ::encode(query_epoch, payload);
    ::encode(type, payload);
  }
};

#endif
