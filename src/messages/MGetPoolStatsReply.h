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

class MGetPoolStatsReply : public MessageInstance<MGetPoolStatsReply, PaxosServiceMessage> {
public:
  friend factory;

  uuid_d fsid;
  std::map<std::string,pool_stat_t> pool_stats;

  MGetPoolStatsReply() : MessageInstance(MSG_GETPOOLSTATSREPLY, 0) {}
  MGetPoolStatsReply(uuid_d& f, ceph_tid_t t, version_t v) :
    MessageInstance(MSG_GETPOOLSTATSREPLY, v),
    fsid(f) {
    set_tid(t);
  }

private:
  ~MGetPoolStatsReply() override {}

public:
  std::string_view get_type_name() const override { return "getpoolstats"; }
  void print(std::ostream& out) const override {
    out << "getpoolstatsreply(" << get_tid() << " v" << version <<  ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(pool_stats, payload, features);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(pool_stats, p);
  }
};

#endif
