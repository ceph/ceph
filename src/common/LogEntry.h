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

#ifndef CEPH_LOGENTRY_H
#define CEPH_LOGENTRY_H

#include "include/types.h"
#include "include/utime.h"
#include "include/encoding.h"
#include "msg/msg_types.h" // for entity_inst_t

namespace ceph {
  class Formatter;
}

typedef enum {
  CLOG_DEBUG = 0,
  CLOG_INFO = 1,
  CLOG_SEC = 2,
  CLOG_WARN = 3,
  CLOG_ERROR = 4,
  CLOG_UNKNOWN = -1,
} clog_type;

static const std::string CLOG_CHANNEL_NONE    = "none";
static const std::string CLOG_CHANNEL_DEFAULT = "cluster";
static const std::string CLOG_CHANNEL_CLUSTER = "cluster";
static const std::string CLOG_CHANNEL_AUDIT   = "audit";

// this is the key name used in the config options for the default, e.g.
//   default=true foo=false bar=false
static const std::string CLOG_CONFIG_DEFAULT_KEY = "default";

/*
 * Given a clog log_type, return the equivalent syslog priority
 */
int clog_type_to_syslog_level(clog_type t);

clog_type string_to_clog_type(const string& s);
int string_to_syslog_level(string s);
int string_to_syslog_facility(string s);

string clog_type_to_string(clog_type t);


struct LogEntryKey {
  entity_inst_t who;
  utime_t stamp;
  uint64_t seq;

  LogEntryKey() : seq(0) {}
  LogEntryKey(const entity_inst_t& w, utime_t t, uint64_t s) : who(w), stamp(t), seq(s) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<LogEntryKey*>& o);
};
WRITE_CLASS_ENCODER(LogEntryKey)

static inline bool operator==(const LogEntryKey& l, const LogEntryKey& r) {
  return l.who == r.who && l.stamp == r.stamp && l.seq == r.seq;
}

struct LogEntry {
  entity_inst_t who;
  utime_t stamp;
  uint64_t seq;
  clog_type prio;
  string msg;
  string channel;

  LogEntry() : seq(0), prio(CLOG_DEBUG) {}

  LogEntryKey key() const { return LogEntryKey(who, stamp, seq); }

  void log_to_syslog(string level, string facility);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<LogEntry*>& o);
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
  bool contains(const LogEntryKey& k) const {
    for (list<LogEntry>::const_iterator p = tail.begin();
	 p != tail.end();
	 ++p)
      if (p->key() == k)
	return true;
    return false;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<LogSummary*>& o);
};
WRITE_CLASS_ENCODER(LogSummary)

inline ostream& operator<<(ostream& out, clog_type t)
{
  switch (t) {
  case CLOG_DEBUG:
    return out << "[DBG]";
  case CLOG_INFO:
    return out << "[INF]";
  case CLOG_SEC:
    return out << "[SEC]";
  case CLOG_WARN:
    return out << "[WRN]";
  case CLOG_ERROR:
    return out << "[ERR]";
  default:
    return out << "[???]";
  }
}

inline ostream& operator<<(ostream& out, const LogEntry& e)
{
  return out << e.stamp << " " << e.who << " " << e.seq << " : "
             << e.channel << " " << e.prio << " " << e.msg;
}

#endif
