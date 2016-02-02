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

#ifndef CEPH_MLOG_H
#define CEPH_MLOG_H

#include "common/LogEntry.h"
#include "messages/PaxosServiceMessage.h"

#include <deque>

class MLog : public PaxosServiceMessage {
public:
  uuid_d fsid;
  std::deque<LogEntry> entries;
  
  MLog() : PaxosServiceMessage(MSG_LOG, 0) {}
  MLog(const uuid_d& f, const std::deque<LogEntry>& e)
    : PaxosServiceMessage(MSG_LOG, 0), fsid(f), entries(e) { }
  MLog(const uuid_d& f) : PaxosServiceMessage(MSG_LOG, 0), fsid(f) { }
private:
  ~MLog() {}

public:
  const char *get_type_name() const { return "log"; }
  void print(ostream& out) const {
    out << "log(";
    if (entries.size())
      out << entries.size() << " entries from seq " << entries.front().seq
	  << " at " << entries.front().stamp;
    out << ")";
  }

  void encode_payload(uint64_t features) {
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
