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

typedef enum {
  LOG_DEBUG = 0,
  LOG_INFO = 1,
  LOG_SEC = 2,
  LOG_WARN = 3,
  LOG_ERROR = 4,
} log_type;

struct LogEntry {
  entity_inst_t who;
  utime_t stamp;
  __u64 seq;
  log_type type;
  string msg;

  void encode(bufferlist& bl) const {
    __u16 t = type;
    ::encode(who, bl);
    ::encode(stamp, bl);
    ::encode(seq, bl);
    ::encode(t, bl);
    ::encode(msg, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u16 t;
    ::decode(who, bl);
    ::decode(stamp, bl);
    ::decode(seq, bl);
    ::decode(t, bl);
    type = (log_type)t;
    ::decode(msg, bl);
  }
};
WRITE_CLASS_ENCODER(LogEntry)

inline ostream& operator<<(ostream& out, const log_type& t)
{
  switch (t) {
  case LOG_DEBUG:
    return out << "[DBG]";
  case LOG_INFO:
    return out << "[INF]";
  case LOG_WARN:
    return out << "[WRN]";
  case LOG_ERROR:
    return out << "[ERR]";
  case LOG_SEC:
    return out << "[SEC]";
  default:
    return out << "[???]";
  }
}

inline ostream& operator<<(ostream& out, const LogEntry& e)
{
  return out << e.stamp << " " << e.who << " " << e.seq << " : " << e.type << " " << e.msg;
}

#endif
