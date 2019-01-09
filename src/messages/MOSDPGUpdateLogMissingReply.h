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


#ifndef CEPH_MOSDPGUPDATELOGMISSINGREPLY_H
#define CEPH_MOSDPGUPDATELOGMISSINGREPLY_H

#include "MOSDFastDispatchOp.h"

class MOSDPGUpdateLogMissingReply : public MessageInstance<MOSDPGUpdateLogMissingReply, MOSDFastDispatchOp> {
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
  // piggybacked osd state
  eversion_t last_complete_ondisk;

  epoch_t get_epoch() const { return map_epoch; }
  spg_t get_pgid() const { return pgid; }
  epoch_t get_query_epoch() const { return map_epoch; }
  ceph_tid_t get_tid() const { return rep_tid; }
  pg_shard_t get_from() const {
    return pg_shard_t(get_source().num(), from);
  }
  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDPGUpdateLogMissingReply()
    : MessageInstance(
      MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY,
      HEAD_VERSION,
      COMPAT_VERSION)
      {}
  MOSDPGUpdateLogMissingReply(
    spg_t pgid,
    shard_id_t from,
    epoch_t epoch,
    epoch_t min_epoch,
    ceph_tid_t rep_tid,
    eversion_t last_complete_ondisk)
    : MessageInstance(
        MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY,
        HEAD_VERSION,
        COMPAT_VERSION),
      map_epoch(epoch),
      min_epoch(min_epoch),
      pgid(pgid),
      from(from),
      rep_tid(rep_tid),
      last_complete_ondisk(last_complete_ondisk)
    {}

private:
  ~MOSDPGUpdateLogMissingReply() override {}

public:
  std::string_view get_type_name() const override { return "PGUpdateLogMissingReply"; }
  void print(ostream& out) const override {
    out << "pg_update_log_missing_reply(" << pgid << " epoch " << map_epoch
	<< "/" << min_epoch
	<< " rep_tid " << rep_tid
	<< " lcod " << last_complete_ondisk << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(map_epoch, payload);
    encode(pgid, payload);
    encode(from, payload);
    encode(rep_tid, payload);
    encode(min_epoch, payload);
    encode(last_complete_ondisk, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(map_epoch, p);
    decode(pgid, p);
    decode(from, p);
    decode(rep_tid, p);
    if (header.version >= 2) {
      decode(min_epoch, p);
    } else {
      min_epoch = map_epoch;
    }
    if (header.version >= 3) {
      decode(last_complete_ondisk, p);
    }
  }
};

#endif
