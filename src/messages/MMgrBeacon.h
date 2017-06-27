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

  static const int HEAD_VERSION = 3;
  static const int COMPAT_VERSION = 1;

protected:
  uint64_t gid;
  entity_addr_t server_addr;
  bool available;
  std::string name;
  uuid_d fsid;
  std::set<std::string> available_modules;

public:
  MMgrBeacon()
    : PaxosServiceMessage(MSG_MGR_BEACON, 0, HEAD_VERSION, COMPAT_VERSION),
      gid(0), available(false)
  {
  }

  MMgrBeacon(const uuid_d& fsid_, uint64_t gid_, const std::string &name_,
             entity_addr_t server_addr_, bool available_,
	     const std::set<std::string>& module_list)
    : PaxosServiceMessage(MSG_MGR_BEACON, 0, HEAD_VERSION, COMPAT_VERSION),
      gid(gid_), server_addr(server_addr_), available(available_), name(name_),
      fsid(fsid_), available_modules(module_list)
  {
  }

  uint64_t get_gid() const { return gid; }
  entity_addr_t get_server_addr() const { return server_addr; }
  bool get_available() const { return available; }
  const std::string& get_name() const { return name; }
  const uuid_d& get_fsid() const { return fsid; }
  std::set<std::string>& get_available_modules() { return available_modules; }

private:
  ~MMgrBeacon() override {}

public:

  const char *get_type_name() const override { return "mgrbeacon"; }

  void print(ostream& out) const override {
    out << get_type_name() << " mgr." << name << "(" << fsid << ","
	<< gid << ", " << server_addr << ", " << available
	<< ")";
  }

  void encode_payload(uint64_t features) override {
    paxos_encode();
    ::encode(server_addr, payload, features);
    ::encode(gid, payload);
    ::encode(available, payload);
    ::encode(name, payload);
    ::encode(fsid, payload);
    ::encode(available_modules, payload);
  }
  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(server_addr, p);
    ::decode(gid, p);
    ::decode(available, p);
    ::decode(name, p);
    if (header.version >= 2) {
      ::decode(fsid, p);
    }
    if (header.version >= 3) {
      ::decode(available_modules, p);
    }
  }
};

#endif
