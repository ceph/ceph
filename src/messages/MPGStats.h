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

class MPGStats final : public PaxosServiceMessage {
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

public:
  uuid_d fsid;
  std::map<pg_t, pg_stat_t> pg_stat;
  osd_stat_t osd_stat;
  std::map<int64_t, store_statfs_t> pool_stat;
  epoch_t epoch = 0;

  MPGStats() : PaxosServiceMessage{MSG_PGSTATS, 0, HEAD_VERSION, COMPAT_VERSION} {}
  MPGStats(const uuid_d& f, epoch_t e)
    : PaxosServiceMessage{MSG_PGSTATS, 0, HEAD_VERSION, COMPAT_VERSION},
      fsid(f),
      epoch(e)
  {}

private:
  ~MPGStats() final {}

public:
  std::string_view get_type_name() const override { return "pg_stats"; }
  void print(std::ostream& out) const override {
    out << "pg_stats(" << pg_stat.size() << " pgs seq " << osd_stat.seq << " v " << version << ")";
  }
  void dump_stats(ceph::Formatter *f) const {
    f->open_object_section("stats");
    {
      f->open_array_section("pg_stat");
      for(const auto& [_pg, _stat] : pg_stat) {
        f->open_object_section("pg_stat");
        _pg.dump(f);
        _stat.dump(f);
        f->close_section();
      }
      f->close_section();

      f->dump_object("osd_stat", osd_stat);

      f->open_array_section("pool_stat");
      for(const auto& [_id, _stat] : pool_stat) {
        f->open_object_section("pool");
        f->dump_int("poolid", _id);
        _stat.dump(f);
        f->close_section();
      }
      f->close_section();
    }
    f->close_section();
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(osd_stat, payload, features);
    encode(pg_stat, payload);
    encode(epoch, payload);
    encode(utime_t{}, payload);
    encode(pool_stat, payload, features);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(osd_stat, p);
    if (osd_stat.num_osds == 0) {
      // for the benefit of legacy OSDs who don't set this field
      osd_stat.num_osds = 1;
    }
    decode(pg_stat, p);
    decode(epoch, p);
    utime_t dummy;
    decode(dummy, p);
    if (header.version >= 2)
      decode(pool_stat, p);
  }
};

#endif
