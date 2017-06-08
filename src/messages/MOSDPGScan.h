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

#ifndef CEPH_MOSDPGSCAN_H
#define CEPH_MOSDPGSCAN_H

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDPGScan : public Message {

  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;

public:
  enum {
    OP_SCAN_GET_DIGEST = 1,      // just objects and versions
    OP_SCAN_DIGEST = 2,          // result
  };
  const char *get_op_name(int o) const {
    switch (o) {
    case OP_SCAN_GET_DIGEST: return "get_digest";
    case OP_SCAN_DIGEST: return "digest";
    default: return "???";
    }
  }

  __u32 op;
  epoch_t map_epoch, query_epoch;
  pg_shard_t from;
  spg_t pgid;
  hobject_t begin, end;

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(map_epoch, p);
    ::decode(query_epoch, p);
    ::decode(pgid.pgid, p);
    ::decode(begin, p);
    ::decode(end, p);

    // handle hobject_t format upgrade
    if (!begin.is_max() && begin.pool == -1)
      begin.pool = pgid.pool();
    if (!end.is_max() && end.pool == -1)
      end.pool = pgid.pool();

    if (header.version >= 2) {
      ::decode(from, p);
      ::decode(pgid.shard, p);
    } else {
      from = pg_shard_t(
	get_source().num(),
	shard_id_t::NO_SHARD);
      pgid.shard = shard_id_t::NO_SHARD;
    }
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(op, payload);
    ::encode(map_epoch, payload);
    ::encode(query_epoch, payload);
    ::encode(pgid.pgid, payload);
    ::encode(begin, payload);
    ::encode(end, payload);
    ::encode(from, payload);
    ::encode(pgid.shard, payload);
  }

  MOSDPGScan() : Message(MSG_OSD_PG_SCAN, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDPGScan(__u32 o, pg_shard_t from,
	     epoch_t e, epoch_t qe, spg_t p, hobject_t be, hobject_t en)
    : Message(MSG_OSD_PG_SCAN, HEAD_VERSION, COMPAT_VERSION),
      op(o),
      map_epoch(e), query_epoch(e),
      from(from),
      pgid(p),
      begin(be), end(en) {
  }
private:
  ~MOSDPGScan() {}

public:
  const char *get_type_name() const { return "pg_scan"; }
  void print(ostream& out) const {
    out << "pg_scan(" << get_op_name(op)
	<< " " << pgid
	<< " " << begin << "-" << end
	<< " e " << map_epoch << "/" << query_epoch
	<< ")";
  }
};

#endif
