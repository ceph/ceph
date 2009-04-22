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

struct LogEntryKey {
  entity_inst_t who;
  utime_t stamp;
  __u64 seq;

  LogEntryKey() {}
  LogEntryKey(entity_inst_t w, utime_t t, __u64 s) : who(w), stamp(t), seq(s) {}

  void encode(bufferlist& bl) const {
    ::encode(who, bl);
    ::encode(stamp, bl);
    ::encode(seq, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(who, bl);
    ::decode(stamp, bl);
    ::decode(seq, bl);
  }
};
WRITE_CLASS_ENCODER(LogEntryKey)

static inline bool operator==(const LogEntryKey& l, const LogEntryKey& r) {
  return l.who == r.who && l.stamp == r.stamp && l.seq == r.seq;
}

struct LogEntry {
  entity_inst_t who;
  utime_t stamp;
  __u64 seq;
  log_type type;
  string msg;

  LogEntryKey key() const { return LogEntryKey(who, stamp, seq); }

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

struct LogSummary {
  version_t version;
  list<LogEntry> tail;

  LogSummary() : version(0) {}

  void add(const LogEntry& e) {
    tail.push_back(e);
    while (tail.size() > 50)
      tail.pop_front();
  }
  bool contains(LogEntryKey k) const {
    for (list<LogEntry>::const_iterator p = tail.begin();
	 p != tail.end();
	 p++)
      if (p->key() == k) return true;
    return false;
  }

  void encode(bufferlist& bl) const {
    ::encode(version, bl);
    ::encode(tail, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(version, bl);
    ::decode(tail, bl);
  }
};
WRITE_CLASS_ENCODER(LogSummary)

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
