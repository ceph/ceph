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

class MOSDPGUpdateLogMissing : public MOSDFastDispatchOp {

  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;


public:
  epoch_t map_epoch, min_epoch;
  spg_t pgid;
  shard_id_t from;
  ceph_tid_t rep_tid;
  mempool::osd_pglog::list<pg_log_entry_t> entries;

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
    : MOSDFastDispatchOp(MSG_OSD_PG_UPDATE_LOG_MISSING, HEAD_VERSION,
			 COMPAT_VERSION) { }
  MOSDPGUpdateLogMissing(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    spg_t pgid,
    shard_id_t from,
    epoch_t epoch,
    epoch_t min_epoch,
    ceph_tid_t rep_tid)
    : MOSDFastDispatchOp(MSG_OSD_PG_UPDATE_LOG_MISSING, HEAD_VERSION,
			 COMPAT_VERSION),
      map_epoch(epoch),
      min_epoch(min_epoch),
      pgid(pgid),
      from(from),
      rep_tid(rep_tid),
      entries(entries) {}

private:
  ~MOSDPGUpdateLogMissing() override {}

public:
  const char *get_type_name() const override { return "PGUpdateLogMissing"; }
  void print(ostream& out) const override {
    out << "pg_update_log_missing(" << pgid << " epoch " << map_epoch
	<< "/" << min_epoch
	<< " rep_tid " << rep_tid
	<< " entries " << entries << ")";
  }

  void encode_payload(uint64_t features) override {
    ::encode(map_epoch, payload);
    ::encode(pgid, payload);
    ::encode(from, payload);
    ::encode(rep_tid, payload);
    ::encode(entries, payload);
    ::encode(min_epoch, payload);
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    ::decode(map_epoch, p);
    ::decode(pgid, p);
    ::decode(from, p);
    ::decode(rep_tid, p);
    ::decode(entries, p);
    if (header.version >= 2) {
      ::decode(min_epoch, p);
    } else {
      min_epoch = map_epoch;
    }
  }
};

#endif
