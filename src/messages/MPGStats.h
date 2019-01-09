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

#ifndef CEPH_MPGSTATS_H
#define CEPH_MPGSTATS_H

#include "osd/osd_types.h"
#include "messages/PaxosServiceMessage.h"

class MPGStats : public MessageInstance<MPGStats, PaxosServiceMessage> {
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;
public:
  friend factory;

  uuid_d fsid;
  map<pg_t, pg_stat_t> pg_stat;
  osd_stat_t osd_stat;
  map<int64_t, store_statfs_t> pool_stat;
  epoch_t epoch = 0;
  utime_t had_map_for;
  
  MPGStats() : MessageInstance(MSG_PGSTATS, 0, HEAD_VERSION, COMPAT_VERSION) {}
  MPGStats(const uuid_d& f, epoch_t e, utime_t had)
    : MessageInstance(MSG_PGSTATS, 0, HEAD_VERSION, COMPAT_VERSION),
      fsid(f),
      epoch(e),
      had_map_for(had)
  {}

private:
  ~MPGStats() override {}

public:
  std::string_view get_type_name() const override { return "pg_stats"; }
  void print(ostream& out) const override {
    out << "pg_stats(" << pg_stat.size() << " pgs tid " << get_tid() << " v " << version << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(osd_stat, payload, features);
    encode(pg_stat, payload);
    encode(epoch, payload);
    encode(had_map_for, payload);
    encode(pool_stat, payload, features);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(osd_stat, p);
    decode(pg_stat, p);
    decode(epoch, p);
    decode(had_map_for, p);
    if (header.version >= 2)
      decode(pool_stat, p);
  }
};

#endif
