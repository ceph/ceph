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


#ifndef CEPH_MGETPOOLSTATSREPLY_H
#define CEPH_MGETPOOLSTATSREPLY_H

#include "osd/osd_types.h" // for pool_stat_t

class MGetPoolStatsReply final : public PaxosServiceMessage {
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

public:
  uuid_d fsid;
  boost::container::flat_map<std::string, pool_stat_t> pool_stats;
  bool per_pool = false;

  MGetPoolStatsReply() : PaxosServiceMessage{MSG_GETPOOLSTATSREPLY, 0,
					     HEAD_VERSION, COMPAT_VERSION} {}
  MGetPoolStatsReply(uuid_d& f, ceph_tid_t t, version_t v) :
    PaxosServiceMessage{MSG_GETPOOLSTATSREPLY, v,
			HEAD_VERSION, COMPAT_VERSION},
    fsid(f) {
    set_tid(t);
  }

private:
  ~MGetPoolStatsReply() final {}

public:
  std::string_view get_type_name() const override { return "getpoolstats"; }
  void print(std::ostream& out) const override {
    out << "getpoolstatsreply(" << get_tid();
    if (per_pool)
      out << " per_pool";
    out << " v" << version <<  ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(pool_stats, payload, features);
    encode(per_pool, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(pool_stats, p);
    if (header.version >= 2) {
      decode(per_pool, p);
    } else {
      per_pool = false;
    }
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
