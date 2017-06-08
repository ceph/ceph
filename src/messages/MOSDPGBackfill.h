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

#ifndef CEPH_MOSDPGBACKFILL_H
#define CEPH_MOSDPGBACKFILL_H

#include "msg/Message.h"
#include "osd/osd_types.h"

class MOSDPGBackfill : public Message {
  static const int HEAD_VERSION = 3;
  static const int COMPAT_VERSION = 1;
public:
  enum {
    OP_BACKFILL_PROGRESS = 2,
    OP_BACKFILL_FINISH = 3,
    OP_BACKFILL_FINISH_ACK = 4,
  };
  const char *get_op_name(int o) const {
    switch (o) {
    case OP_BACKFILL_PROGRESS: return "progress";
    case OP_BACKFILL_FINISH: return "finish";
    case OP_BACKFILL_FINISH_ACK: return "finish_ack";
    default: return "???";
    }
  }

  __u32 op;
  epoch_t map_epoch, query_epoch;
  spg_t pgid;
  hobject_t last_backfill;
  bool compat_stat_sum;
  pg_stat_t stats;

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(map_epoch, p);
    ::decode(query_epoch, p);
    ::decode(pgid.pgid, p);
    ::decode(last_backfill, p);

    // For compatibility with version 1
    ::decode(stats.stats, p);

    if (header.version >= 2) {
      ::decode(stats, p);
    } else {
      compat_stat_sum = true;
    }

    // Handle hobject_t format change
    if (!last_backfill.is_max() &&
	last_backfill.pool == -1)
      last_backfill.pool = pgid.pool();
    if (header.version >= 3)
      ::decode(pgid.shard, p);
    else
      pgid.shard = shard_id_t::NO_SHARD;
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(op, payload);
    ::encode(map_epoch, payload);
    ::encode(query_epoch, payload);
    ::encode(pgid.pgid, payload);
    ::encode(last_backfill, payload);

    // For compatibility with version 1
    ::encode(stats.stats, payload);

    ::encode(stats, payload);

    ::encode(pgid.shard, payload);
  }

  MOSDPGBackfill() :
    Message(MSG_OSD_PG_BACKFILL, HEAD_VERSION, COMPAT_VERSION),
    compat_stat_sum(false) {}
  MOSDPGBackfill(__u32 o, epoch_t e, epoch_t qe, spg_t p)
    : Message(MSG_OSD_PG_BACKFILL, HEAD_VERSION, COMPAT_VERSION),
      op(o),
      map_epoch(e), query_epoch(e),
      pgid(p),
      compat_stat_sum(false) {}
private:
  ~MOSDPGBackfill() {}

public:
  const char *get_type_name() const { return "pg_backfill"; }
  void print(ostream& out) const {
    out << "pg_backfill(" << get_op_name(op)
	<< " " << pgid
	<< " e " << map_epoch << "/" << query_epoch
	<< " lb " << last_backfill
	<< ")";
  }
};

#endif
