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

#ifndef __MLOG_H
#define __MLOG_H

#include "include/LogEntry.h"
#include "messages/PaxosServiceMessage.h"

class MLog : public PaxosServiceMessage {
public:
  ceph_fsid_t fsid;
  deque<LogEntry> entries;
  
  MLog() : PaxosServiceMessage(MSG_LOG, 0) {}
  MLog(ceph_fsid_t& f, deque<LogEntry>& e) : PaxosServiceMessage(MSG_LOG, 0), fsid(f), entries(e) { }
  MLog(ceph_fsid_t& f) : PaxosServiceMessage(MSG_LOG, 0), fsid(f) {}
private:
  ~MLog() {}

public:
  const char *get_type_name() { return "log"; }
  void print(ostream& out) {
    out << "log(";
    if (entries.size())
      out << entries.size() << " entries";
    out << ")";
  }

  void encode_payload() {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(entries, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(entries, p);
  }
};

#endif
