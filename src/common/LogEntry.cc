// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
//
#include <syslog.h>
#include <boost/algorithm/string/predicate.hpp>

#include "LogEntry.h"
#include "Formatter.h"
#include "include/stringify.h"

using std::list;
using std::map;
using std::make_pair;
using std::pair;
using std::string;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;

// ----
// LogEntryKey

void LogEntryKey::dump(Formatter *f) const
{
  f->dump_stream("rank") << rank;
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("seq", seq);
}

void LogEntryKey::generate_test_instances(list<LogEntryKey*>& o)
{
  o.push_back(new LogEntryKey);
  o.push_back(new LogEntryKey(entity_name_t::CLIENT(1234), utime_t(1,2), 34));
}

clog_type LogEntry::str_to_level(std::string const &str)
{
  std::string level_str = str;
  std::transform(level_str.begin(), level_str.end(), level_str.begin(),
      [](char c) {return std::tolower(c);});

  if (level_str == "debug") {
    return CLOG_DEBUG;
  } else if (level_str == "info") {
    return CLOG_INFO;
  } else if (level_str == "sec") {
    return CLOG_SEC;
  } else if (level_str == "warn" || level_str == "warning") {
    return CLOG_WARN;
  } else if (level_str == "error" || level_str == "err") {
    return CLOG_ERROR;
  } else {
    return CLOG_UNKNOWN;
  }
}

// ----

int clog_type_to_syslog_level(clog_type t)
{
  switch (t) {
    case CLOG_DEBUG:
      return LOG_DEBUG;
    case CLOG_INFO:
      return LOG_INFO;
    case CLOG_WARN:
      return LOG_WARNING;
    case CLOG_ERROR:
      return LOG_ERR;
    case CLOG_SEC:
      return LOG_CRIT;
    default:
      ceph_abort();
      return 0;
  }
}

clog_type string_to_clog_type(const string& s)
{
  if (boost::iequals(s, "debug") ||
      boost::iequals(s, "dbg"))
    return CLOG_DEBUG;
  if (boost::iequals(s, "info") ||
      boost::iequals(s, "inf"))
    return CLOG_INFO;
  if (boost::iequals(s, "warning") ||
      boost::iequals(s, "warn") ||
      boost::iequals(s, "wrn"))
    return CLOG_WARN;
  if (boost::iequals(s, "error") ||
      boost::iequals(s, "err"))
    return CLOG_ERROR;
  if (boost::iequals(s, "security") ||
      boost::iequals(s, "sec"))
    return CLOG_SEC;

  return CLOG_UNKNOWN;
}

int string_to_syslog_level(string s)
{
  if (boost::iequals(s, "debug"))
    return LOG_DEBUG;
  if (boost::iequals(s, "info") ||
      boost::iequals(s, "notice"))
    return LOG_INFO;
  if (boost::iequals(s, "warning") ||
      boost::iequals(s, "warn"))
    return LOG_WARNING;
  if (boost::iequals(s, "error") ||
      boost::iequals(s, "err"))
    return LOG_ERR;
  if (boost::iequals(s, "crit") ||
      boost::iequals(s, "critical") ||
      boost::iequals(s, "emerg"))
    return LOG_CRIT;

  // err on the side of noise!
  return LOG_DEBUG;
}

int string_to_syslog_facility(string s)
{
  if (boost::iequals(s, "auth"))
    return LOG_AUTH;
  if (boost::iequals(s, "authpriv"))
    return LOG_AUTHPRIV;
  if (boost::iequals(s, "cron"))
    return LOG_CRON;
  if (boost::iequals(s, "daemon"))
    return LOG_DAEMON;
  if (boost::iequals(s, "ftp"))
    return LOG_FTP;
  if (boost::iequals(s, "kern"))
    return LOG_KERN;
  if (boost::iequals(s, "local0"))
    return LOG_LOCAL0;
  if (boost::iequals(s, "local1"))
    return LOG_LOCAL1;
  if (boost::iequals(s, "local2"))
    return LOG_LOCAL2;
  if (boost::iequals(s, "local3"))
    return LOG_LOCAL3;
  if (boost::iequals(s, "local4"))
    return LOG_LOCAL4;
  if (boost::iequals(s, "local5"))
    return LOG_LOCAL5;
  if (boost::iequals(s, "local6"))
    return LOG_LOCAL6;
  if (boost::iequals(s, "local7"))
    return LOG_LOCAL7;
  if (boost::iequals(s, "lpr"))
    return LOG_LPR;
  if (boost::iequals(s, "mail"))
    return LOG_MAIL;
  if (boost::iequals(s, "news"))
    return LOG_NEWS;
  if (boost::iequals(s, "syslog"))
    return LOG_SYSLOG;
  if (boost::iequals(s, "user"))
    return LOG_USER;
  if (boost::iequals(s, "uucp"))
    return LOG_UUCP;

  // default to USER
  return LOG_USER;
}

string clog_type_to_string(clog_type t)
{
  switch (t) {
    case CLOG_DEBUG:
      return "debug";
    case CLOG_INFO:
      return "info";
    case CLOG_WARN:
      return "warn";
    case CLOG_ERROR:
      return "err";
    case CLOG_SEC:
      return "crit";
    default:
      ceph_abort();
  }
}

void LogEntry::log_to_syslog(string level, string facility) const
{
  int min = string_to_syslog_level(level);
  int l = clog_type_to_syslog_level(prio);
  if (l <= min) {
    int f = string_to_syslog_facility(facility);
    syslog(l | f, "%s %s %llu : %s",
	   name.to_cstr(),
	   stringify(rank).c_str(),
	   (long long unsigned)seq,
	   msg.c_str());
  }
}

void LogEntry::encode(bufferlist& bl, uint64_t features) const
{
  assert(HAVE_FEATURE(features, SERVER_NAUTILUS));
  ENCODE_START(5, 5, bl);
  __u16 t = prio;
  encode(name, bl);
  encode(rank, bl);
  encode(addrs, bl, features);
  encode(stamp, bl);
  encode(seq, bl);
  encode(t, bl);
  encode(msg, bl);
  encode(channel, bl);
  ENCODE_FINISH(bl);
}

void LogEntry::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 2, 2, bl);
  if (struct_v < 5) {
    __u16 t;
    entity_inst_t who;
    decode(who, bl);
    rank = who.name;
    addrs.v.clear();
    addrs.v.push_back(who.addr);
    decode(stamp, bl);
    decode(seq, bl);
    decode(t, bl);
    prio = (clog_type)t;
    decode(msg, bl);
    if (struct_v >= 3) {
      decode(channel, bl);
    } else {
      // prior to having logging channels we only had a cluster log.
      // Ensure we keep that appearance when the other party has no
      // clue of what a 'channel' is.
      channel = CLOG_CHANNEL_CLUSTER;
    }
    if (struct_v >= 4) {
      decode(name, bl);
    }
  } else {
    __u16 t;
    decode(name, bl);
    decode(rank, bl);
    decode(addrs, bl);
    decode(stamp, bl);
    decode(seq, bl);
    decode(t, bl);
    prio = (clog_type)t;
    decode(msg, bl);
    decode(channel, bl);
  }
  DECODE_FINISH(bl);
}

void LogEntry::dump(Formatter *f) const
{
  f->dump_stream("name") << name;
  f->dump_stream("rank") << rank;
  f->dump_object("addrs", addrs);
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("seq", seq);
  f->dump_string("channel", channel);
  f->dump_stream("priority") << prio;
  f->dump_string("message", msg);
}

void LogEntry::generate_test_instances(list<LogEntry*>& o)
{
  o.push_back(new LogEntry);
}


// -----

void LogSummary::build_ordered_tail_legacy(list<LogEntry> *tail) const
{
  tail->clear();
  // channel -> (begin, end)
  map<string,pair<list<pair<uint64_t,LogEntry>>::const_iterator,
		  list<pair<uint64_t,LogEntry>>::const_iterator>> pos;
  for (auto& i : tail_by_channel) {
    pos.emplace(i.first, make_pair(i.second.begin(), i.second.end()));
  }
  while (true) {
    uint64_t min_seq = 0;
    list<pair<uint64_t,LogEntry>>::const_iterator *minp = 0;
    for (auto& i : pos) {
      if (i.second.first == i.second.second) {
	continue;
      }
      if (min_seq == 0 || i.second.first->first < min_seq) {
	min_seq = i.second.first->first;
	minp = &i.second.first;
      }
    }
    if (min_seq == 0) {
      break; // done
    }
    tail->push_back((*minp)->second);
    ++(*minp);
  }
}

void LogSummary::encode(bufferlist& bl, uint64_t features) const
{
  assert(HAVE_FEATURE(features, SERVER_MIMIC));
  ENCODE_START(4, 3, bl);
  encode(version, bl);
  encode(seq, bl);
  encode(tail_by_channel, bl, features);
  encode(channel_info, bl);
  recent_keys.encode(bl);
  ENCODE_FINISH(bl);
}

void LogSummary::decode(bufferlist::const_iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(4, 2, 2, bl);
  decode(version, bl);
  decode(seq, bl);
  decode(tail_by_channel, bl);
  if (struct_v >= 4) {
    decode(channel_info, bl);
    recent_keys.decode(bl);
  }
  DECODE_FINISH(bl);
  keys.clear();
  for (auto& i : tail_by_channel) {
    for (auto& e : i.second) {
      keys.insert(e.second.key());
    }
  }
}

void LogSummary::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->open_object_section("tail_by_channel");
  for (auto& i : tail_by_channel) {
    f->open_object_section(i.first.c_str());
    for (auto& j : i.second) {
      string s = stringify(j.first);
      f->dump_object(s.c_str(), j.second);
    }
    f->close_section();
  }
  f->close_section();
}

void LogSummary::generate_test_instances(list<LogSummary*>& o)
{
  o.push_back(new LogSummary);
  // more!
}
