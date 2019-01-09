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

#include "MOSDFastDispatchOp.h"

class MOSDPGScan : public MessageInstance<MOSDPGScan, MOSDFastDispatchOp> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 2;

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

  __u32 op = 0;
  epoch_t map_epoch = 0, query_epoch = 0;
  pg_shard_t from;
  spg_t pgid;
  hobject_t begin, end;

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
    decode(begin, p);
    decode(end, p);

    // handle hobject_t format upgrade
    if (!begin.is_max() && begin.pool == -1)
      begin.pool = pgid.pool();
    if (!end.is_max() && end.pool == -1)
      end.pool = pgid.pool();

    decode(from, p);
    decode(pgid.shard, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(op, payload);
    encode(map_epoch, payload);
    if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      // pre-nautilus OSDs do not set last_peering_reset properly
      encode(map_epoch, payload);
    } else {
      encode(query_epoch, payload);
    }
    encode(pgid.pgid, payload);
    encode(begin, payload);
    encode(end, payload);
    encode(from, payload);
    encode(pgid.shard, payload);
  }

  MOSDPGScan()
    : MessageInstance(MSG_OSD_PG_SCAN, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDPGScan(__u32 o, pg_shard_t from,
	     epoch_t e, epoch_t qe, spg_t p, hobject_t be, hobject_t en)
    : MessageInstance(MSG_OSD_PG_SCAN, HEAD_VERSION, COMPAT_VERSION),
      op(o),
      map_epoch(e), query_epoch(qe),
      from(from),
      pgid(p),
      begin(be), end(en) {
  }
private:
  ~MOSDPGScan() override {}

public:
  std::string_view get_type_name() const override { return "pg_scan"; }
  void print(ostream& out) const override {
    out << "pg_scan(" << get_op_name(op)
	<< " " << pgid
	<< " " << begin << "-" << end
	<< " e " << map_epoch << "/" << query_epoch
	<< ")";
  }
};

#endif
