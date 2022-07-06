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

#ifndef CEPH_MMONJOIN_H
#define CEPH_MMONJOIN_H

#include "messages/PaxosServiceMessage.h"

class MMonJoin final : public PaxosServiceMessage {
public:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 2;

  uuid_d fsid;
  std::string name;
  entity_addrvec_t addrs;
  /* The location members are for stretch mode. crush_loc is the location
   * (generally just a "datacenter=<foo>" statement) of the monitor. The
   * force_loc is whether the mon cluster should replace a previously-known
   * location. Generally the monitor will force an update if it's given a
   * location from the CLI on boot-up, and then never force again (so that it
   * can be moved/updated via the ceph tool from elsewhere). */
  std::map<std::string,std::string> crush_loc;
  bool force_loc{false};

  MMonJoin() : PaxosServiceMessage{MSG_MON_JOIN, 0, HEAD_VERSION, COMPAT_VERSION} {}
  MMonJoin(uuid_d &f, std::string n, const entity_addrvec_t& av)
    : PaxosServiceMessage{MSG_MON_JOIN, 0, HEAD_VERSION, COMPAT_VERSION},
      fsid(f), name(n), addrs(av)
  { }
  MMonJoin(uuid_d &f, std::string n, const entity_addrvec_t& av,
	   const std::map<std::string,std::string>& cloc, bool force)
    : PaxosServiceMessage{MSG_MON_JOIN, 0, HEAD_VERSION, COMPAT_VERSION},
      fsid(f), name(n), addrs(av), crush_loc(cloc), force_loc(force)
  { }
  
private:
  ~MMonJoin() final {}

public:
  std::string_view get_type_name() const override { return "mon_join"; }
  void print(std::ostream& o) const override {
    o << "mon_join(" << name << " " << addrs << " " << crush_loc << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(name, payload);
    assert(HAVE_FEATURE(features, SERVER_NAUTILUS));
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    encode(addrs, payload, features);
    encode(crush_loc, payload);
    encode(force_loc, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(name, p);
    assert(header.version > 1);
    decode(addrs, p);
    if (header.version >= 3) {
      decode(crush_loc, p);
      decode(force_loc, p);
    }
  }
};

#endif
