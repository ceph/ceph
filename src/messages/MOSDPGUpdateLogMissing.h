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


#ifndef CEPH_MOSDPGUPDATELOGMISSING_H
#define CEPH_MOSDPGUPDATELOGMISSING_H

#include "MOSDFastDispatchOp.h"

class MOSDPGUpdateLogMissing : public MessageInstance<MOSDPGUpdateLogMissing, MOSDFastDispatchOp> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 1;


public:
  epoch_t map_epoch = 0, min_epoch = 0;
  spg_t pgid;
  shard_id_t from;
  ceph_tid_t rep_tid = 0;
  mempool::osd_pglog::list<pg_log_entry_t> entries;
  // piggybacked osd/pg state
  eversion_t pg_trim_to; // primary->replica: trim to here
  eversion_t pg_roll_forward_to; // primary->replica: trim rollback info to here

  epoch_t get_epoch() const { return map_epoch; }
  spg_t get_pgid() const { return pgid; }
  epoch_t get_query_epoch() const { return map_epoch; }
  ceph_tid_t get_tid() const { return rep_tid; }

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDPGUpdateLogMissing()
    : MessageInstance(MSG_OSD_PG_UPDATE_LOG_MISSING, HEAD_VERSION,
			 COMPAT_VERSION) { }
  MOSDPGUpdateLogMissing(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    spg_t pgid,
    shard_id_t from,
    epoch_t epoch,
    epoch_t min_epoch,
    ceph_tid_t rep_tid,
    eversion_t pg_trim_to,
    eversion_t pg_roll_forward_to)
    : MessageInstance(MSG_OSD_PG_UPDATE_LOG_MISSING, HEAD_VERSION,
			 COMPAT_VERSION),
      map_epoch(epoch),
      min_epoch(min_epoch),
      pgid(pgid),
      from(from),
      rep_tid(rep_tid),
      entries(entries),
      pg_trim_to(pg_trim_to),
      pg_roll_forward_to(pg_roll_forward_to)
  {}

private:
  ~MOSDPGUpdateLogMissing() override {}

public:
  std::string_view get_type_name() const override { return "PGUpdateLogMissing"; }
  void print(ostream& out) const override {
    out << "pg_update_log_missing(" << pgid << " epoch " << map_epoch
	<< "/" << min_epoch
	<< " rep_tid " << rep_tid
	<< " entries " << entries
	<< " trim_to " << pg_trim_to
	<< " roll_forward_to " << pg_roll_forward_to
	<< ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(map_epoch, payload);
    encode(pgid, payload);
    encode(from, payload);
    encode(rep_tid, payload);
    encode(entries, payload);
    encode(min_epoch, payload);
    encode(pg_trim_to, payload);
    encode(pg_roll_forward_to, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(map_epoch, p);
    decode(pgid, p);
    decode(from, p);
    decode(rep_tid, p);
    decode(entries, p);
    if (header.version >= 2) {
      decode(min_epoch, p);
    } else {
      min_epoch = map_epoch;
    }
    if (header.version >= 3) {
      decode(pg_trim_to, p);
      decode(pg_roll_forward_to, p);
    }
  }
};

#endif
