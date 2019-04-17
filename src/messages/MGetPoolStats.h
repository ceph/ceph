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


#ifndef CEPH_MGETPOOLSTATS_H
#define CEPH_MGETPOOLSTATS_H

#include "messages/PaxosServiceMessage.h"

class MGetPoolStats : public MessageInstance<MGetPoolStats, PaxosServiceMessage> {
public:
  friend factory;

  uuid_d fsid;
  std::list<std::string> pools;

  MGetPoolStats() : MessageInstance(MSG_GETPOOLSTATS, 0) {}
  MGetPoolStats(const uuid_d& f, ceph_tid_t t, std::list<std::string>& ls, version_t l) :
    MessageInstance(MSG_GETPOOLSTATS, l),
    fsid(f), pools(ls) {
    set_tid(t);
  }

private:
  ~MGetPoolStats() override {}

public:
  std::string_view get_type_name() const override { return "getpoolstats"; }
  void print(std::ostream& out) const override {
    out << "getpoolstats(" << get_tid() << " " << pools << " v" << version << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(pools, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(pools, p);
  }
};

#endif
