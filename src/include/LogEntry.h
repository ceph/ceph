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

#ifndef __CEPH_LOG_H
#define __CEPH_LOG_H

#include "include/types.h"
#include "include/encoding.h"

struct LogEntry {
  entity_inst_t who;
  utime_t stamp;
  __u64 seq;
  __u8 level;
  string msg;

  void encode(bufferlist& bl) const {
    ::encode(who, bl);
    ::encode(stamp, bl);
    ::encode(seq, bl);
    ::encode(level, bl);
    ::encode(msg, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(who, bl);
    ::decode(stamp, bl);
    ::decode(seq, bl);
    ::decode(level, bl);
    ::decode(msg, bl);
  }
};
WRITE_CLASS_ENCODER(LogEntry)

inline ostream& operator<<(ostream& out, const LogEntry& e)
{
  return out << e.stamp << " " << e.who << " : " << e.seq << " : " << (int)e.level << " : " << e.msg;
}

#endif
