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


#ifndef CEPH_MMONPROBE_H
#define CEPH_MMONPROBE_H

#include "msg/Message.h"
#include "mon/MonMap.h"

class MMonProbe : public Message {
public:
  enum {
    OP_PROBE = 1,
    OP_REPLY = 2,
    OP_SLURP = 3,
    OP_SLURP_LATEST = 4,
    OP_DATA = 5,
  };

  static const char *get_opname(int o) {
    switch (o) {
    case OP_PROBE: return "probe";
    case OP_REPLY: return "reply";
    case OP_SLURP: return "slurp";
    case OP_SLURP_LATEST: return "slurp_latest";
    case OP_DATA: return "data";
    default: assert(0); return 0;
    }
  }
  
  uuid_d fsid;
  int32_t op;
  string name;
  set<int32_t> quorum;
  bufferlist monmap_bl;
  map<string, version_t> paxos_versions;

  string machine_name;
  map<string, map<version_t,bufferlist> > paxos_values;
  bufferlist latest_value;
  version_t latest_version, newest_version, oldest_version;

  MMonProbe() : Message(MSG_MON_PROBE) {}
  MMonProbe(const uuid_d& f, int o, const string& n)
    : Message(MSG_MON_PROBE), fsid(f), op(o), name(n),
      latest_version(0), newest_version(0), oldest_version(0) {}
private:
  ~MMonProbe() {}

public:  
  const char *get_type_name() { return "mon_probe"; }
  void print(ostream& out) {
    out << "mon_probe(" << get_opname(op) << " " << fsid << " name " << name;
    if (quorum.size())
      out << " quorum " << quorum;
    if (paxos_versions.size())
      out << " versions " << paxos_versions;
    if (machine_name.length())
      out << " machine_name " << machine_name << " " << oldest_version << "-" << newest_version;
    out << ")";
  }
  
  void encode_payload(CephContext *cct, uint64_t features) {
    ::encode(fsid, payload);
    ::encode(op, payload);
    ::encode(name, payload);
    ::encode(quorum, payload);
    ::encode(monmap_bl, payload);
    ::encode(paxos_versions, payload);
    ::encode(machine_name, payload);
    ::encode(oldest_version, payload);
    ::encode(newest_version, payload);
    ::encode(paxos_values, payload);
    ::encode(latest_value, payload);
    ::encode(latest_version, payload);
  }
  void decode_payload(CephContext *cct) {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(op, p);
    ::decode(name, p);
    ::decode(quorum, p);
    ::decode(monmap_bl, p);
    ::decode(paxos_versions, p);
    ::decode(machine_name, p);
    ::decode(oldest_version, p);
    ::decode(newest_version, p);
    ::decode(paxos_values, p);
    ::decode(latest_value, p);
    ::decode(latest_version, p);
  }
};

#endif
