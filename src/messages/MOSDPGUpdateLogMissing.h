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

class MOSDPGUpdateLogMissing final : public MOSDFastDispatchOp {
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

  /**
   * pg_committed_to
   *
   * Propagates PeeringState::pg_committed_to to replicas as with
   * MOSDRepOp, ECSubWrite
   *
   * Historical Note: Prior to early 2024, this field was named
   * pg_roll_forward_to. pg_committed_to is a safe value to rollforward to as
   * it is a conservative bound on versions that can become divergent.  Switching
   * it to be populated by pg_committed_to rather than mlcod mirrors MOSDRepOp
   * and upgrade cases in both directions should be safe as mlcod is <= pct
   * and replicas (both ec and replicated) only actually rely on versions <= this
   * field being non-divergent. This note may be removed in main after U is
   * released.
   */
  eversion_t pg_committed_to;

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
    : MOSDFastDispatchOp{MSG_OSD_PG_UPDATE_LOG_MISSING, HEAD_VERSION,
			 COMPAT_VERSION} {}
  MOSDPGUpdateLogMissing(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    spg_t pgid,
    shard_id_t from,
    epoch_t epoch,
    epoch_t min_epoch,
    ceph_tid_t rep_tid,
    eversion_t pg_trim_to,
    eversion_t pg_committed_to)
    : MOSDFastDispatchOp{MSG_OSD_PG_UPDATE_LOG_MISSING, HEAD_VERSION,
			 COMPAT_VERSION},
      map_epoch(epoch),
      min_epoch(min_epoch),
      pgid(pgid),
      from(from),
      rep_tid(rep_tid),
      entries(entries),
      pg_trim_to(pg_trim_to),
      pg_committed_to(pg_committed_to)
  {}

private:
  ~MOSDPGUpdateLogMissing() final {}

public:
  std::string_view get_type_name() const override { return "PGUpdateLogMissing"; }
  void print(std::ostream& out) const override {
    out << "pg_update_log_missing(" << pgid << " epoch " << map_epoch
	<< "/" << min_epoch
	<< " rep_tid " << rep_tid
	<< " entries " << entries
	<< " trim_to " << pg_trim_to
	<< " pg_committed_to " << pg_committed_to
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
    encode(pg_committed_to, payload);
  }
  void decode_payload() override {
    using ceph::decode;
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
      decode(pg_committed_to, p);
    }
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
