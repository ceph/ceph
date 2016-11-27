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

#ifndef CEPH_MMGRBEACON_H
#define CEPH_MMGRBEACON_H

#include "messages/PaxosServiceMessage.h"

#include "include/types.h"


class MMgrBeacon : public PaxosServiceMessage {

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

protected:
  uint64_t gid;
  entity_addr_t server_addr;
  bool available;
  std::string name;

public:
  MMgrBeacon()
    : PaxosServiceMessage(MSG_MGR_BEACON, 0, HEAD_VERSION, COMPAT_VERSION),
      gid(0), available(false)
  {
  }

  MMgrBeacon(uint64_t gid_, const std::string &name_,
             entity_addr_t server_addr_, bool available_)
    : PaxosServiceMessage(MSG_MGR_BEACON, 0, HEAD_VERSION, COMPAT_VERSION),
      gid(gid_), server_addr(server_addr_), available(available_), name(name_)
  {
  }

  uint64_t get_gid() const { return gid; }
  entity_addr_t get_server_addr() const { return server_addr; }
  bool get_available() const { return available; }
  const std::string& get_name() const { return name; }

private:
  ~MMgrBeacon() {}

public:

  const char *get_type_name() const { return "mgrbeacon"; }

  void print(ostream& out) const {
    out << get_type_name() << " mgr." << name << "(" << gid << ", "
        << server_addr << ", " << available << ")";
  }

  void encode_payload(uint64_t features) {
    paxos_encode();
    ::encode(server_addr, payload, features);
    ::encode(gid, payload);
    ::encode(available, payload);
    ::encode(name, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(server_addr, p);
    ::decode(gid, p);
    ::decode(available, p);
    ::decode(name, p);
  }
};

#endif
