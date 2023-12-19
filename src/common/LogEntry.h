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

#include <fmt/format.h>

#include "include/utime.h"
#include "msg/msg_fmt.h"
#include "msg/msg_types.h"
#include "common/entity_name.h"
#include "ostream_temp.h"
#include "LRUSet.h"

namespace ceph {
  class Formatter;
}

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

clog_type string_to_clog_type(const std::string& s);
int string_to_syslog_level(std::string s);
int string_to_syslog_facility(std::string s);

std::string clog_type_to_string(clog_type t);


struct LogEntryKey {
private:
  uint64_t _hash = 0;

  void _calc_hash() {
    std::hash<entity_name_t> h;
    _hash = seq + h(rank);
  }

  entity_name_t rank;
  utime_t stamp;
  uint64_t seq = 0;

public:
  LogEntryKey() {}
  LogEntryKey(const entity_name_t& w, utime_t t, uint64_t s)
    : rank(w), stamp(t), seq(s) {
    _calc_hash();
  }

  uint64_t get_hash() const {
    return _hash;
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<LogEntryKey*>& o);

  friend bool operator==(const LogEntryKey& l, const LogEntryKey& r) {
    return l.rank == r.rank && l.stamp == r.stamp && l.seq == r.seq;
  }

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(rank, bl);
    encode(stamp, bl);
    encode(seq, bl);
  }
  void decode(bufferlist::const_iterator &p) {
    using ceph::decode;
    decode(rank, p);
    decode(stamp, p);
    decode(seq, p);
  }
};
WRITE_CLASS_ENCODER(LogEntryKey)


namespace std {
template<> struct hash<LogEntryKey> {
  size_t operator()(const LogEntryKey& r) const {
    return r.get_hash();
  }
};
} // namespace std

struct LogEntry {
  EntityName name;
  entity_name_t rank;
  entity_addrvec_t addrs;
  utime_t stamp;
  uint64_t seq;
  clog_type prio;
  std::string msg;
  std::string channel;

  LogEntry() : seq(0), prio(CLOG_DEBUG) {}

  LogEntryKey key() const { return LogEntryKey(rank, stamp, seq); }

  void log_to_syslog(std::string level, std::string facility) const;

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<LogEntry*>& o);
  static clog_type str_to_level(std::string const &str);
  static std::string_view level_to_str(clog_type t) {
    switch (t) {
    case CLOG_DEBUG:
      return "DBG";
    case CLOG_INFO:
      return "INF";
    case CLOG_SEC:
      return "SEC";
    case CLOG_WARN:
      return "WRN";
    case CLOG_ERROR:
      return "ERR";
    case CLOG_UNKNOWN:
      return "UNKNOWN";
    }
    return "???";
  }
};
WRITE_CLASS_ENCODER_FEATURES(LogEntry)

struct LogSummary {
  version_t version;

  // ---- pre-quincy ----
  // channel -> [(seq#, entry), ...]
  std::map<std::string,std::list<std::pair<uint64_t,LogEntry>>> tail_by_channel;
  uint64_t seq = 0;
  ceph::unordered_set<LogEntryKey> keys;

  // ---- quincy+ ----
  LRUSet<LogEntryKey> recent_keys;
  std::map<std::string, std::pair<uint64_t,uint64_t>> channel_info; // channel -> [begin, end)

  LogSummary() : version(0) {}

  void build_ordered_tail_legacy(std::list<LogEntry> *tail) const;

  void add_legacy(const LogEntry& e) {
    keys.insert(e.key());
    tail_by_channel[e.channel].push_back(std::make_pair(++seq, e));
  }
  void prune(size_t max) {
    for (auto& i : tail_by_channel) {
      while (i.second.size() > max) {
	keys.erase(i.second.front().second.key());
	i.second.pop_front();
      }
    }
    recent_keys.prune(max);
  }
  bool contains(const LogEntryKey& k) const {
    return keys.count(k) || recent_keys.contains(k);
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<LogSummary*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(LogSummary)

inline std::ostream& operator<<(std::ostream& out, const clog_type t)
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

inline std::ostream& operator<<(std::ostream& out, const LogEntry& e)
{
  return out << e.stamp << " " << e.name << " (" << e.rank << ") "
	     << e.seq << " : "
             << e.channel << " " << e.prio << " " << e.msg;
}

template <> struct fmt::formatter<EntityName> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const EntityName& e, FormatContext& ctx) {
    return formatter<std::string_view>::format(e.to_str(), ctx);
  }
};

template <> struct fmt::formatter<LogEntry> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const LogEntry& e, FormatContext& ctx) {
    return fmt::format_to(ctx.out(), "{} {} ({}) {} : {} [{}] {}",
                          e.stamp, e.name, e.rank, e.seq, e.channel,
                          LogEntry::level_to_str(e.prio), e.msg);
  }
};

#endif
