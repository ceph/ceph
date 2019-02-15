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

#include "MOSDFastDispatchOp.h"

class MOSDPGBackfill : public MessageInstance<MOSDPGBackfill, MOSDFastDispatchOp> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 3;
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

  __u32 op = 0;
  epoch_t map_epoch = 0, query_epoch = 0;
  spg_t pgid;
  hobject_t last_backfill;
  pg_stat_t stats;

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return query_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(op, p);
    decode(map_epoch, p);
    decode(query_epoch, p);
    decode(pgid.pgid, p);
    decode(last_backfill, p);

    // For compatibility with version 1
    decode(stats.stats, p);

    decode(stats, p);

    // Handle hobject_t format change
    if (!last_backfill.is_max() &&
	last_backfill.pool == -1)
      last_backfill.pool = pgid.pool();
    decode(pgid.shard, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(map_epoch, payload);
    encode(query_epoch, payload);
    encode(pgid.pgid, payload);
    encode(last_backfill, payload);

    // For compatibility with version 1
    encode(stats.stats, payload);

    encode(stats, payload);

    encode(pgid.shard, payload);
  }

  MOSDPGBackfill()
    : MessageInstance(MSG_OSD_PG_BACKFILL, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDPGBackfill(__u32 o, epoch_t e, epoch_t qe, spg_t p)
    : MessageInstance(MSG_OSD_PG_BACKFILL, HEAD_VERSION, COMPAT_VERSION),
      op(o),
      map_epoch(e), query_epoch(e),
      pgid(p) {}
private:
  ~MOSDPGBackfill() override {}

public:
  std::string_view get_type_name() const override { return "pg_backfill"; }
  void print(ostream& out) const override {
    out << "pg_backfill(" << get_op_name(op)
	<< " " << pgid
	<< " e " << map_epoch << "/" << query_epoch
	<< " lb " << last_backfill
	<< ")";
  }
};

#endif
