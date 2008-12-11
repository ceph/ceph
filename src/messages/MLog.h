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

class MLog : public Message {
public:
  ceph_fsid fsid;
  deque<LogEntry> entries;
  version_t last;
  
  MLog() : Message(MSG_PGSTATS) {}
  MLog(ceph_fsid& f, deque<LogEntry>& e) : 
    Message(MSG_LOG), fsid(f), entries(e), last(0) { }
  MLog(ceph_fsid& f, version_t l) : 
    Message(MSG_LOG), fsid(f), last(l) {}

  const char *get_type_name() { return "log"; }
  void print(ostream& out) {
    out << "log";
  }

  void encode_payload() {
    ::encode(fsid, payload);
    ::encode(entries, payload);
    ::encode(last, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(fsid, p);
    ::decode(entries, p);
    ::decode(last, p);
  }
};

#endif
