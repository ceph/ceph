
#include <syslog.h>

#include <boost/algorithm/string.hpp>

#include "LogEntry.h"
#include "Formatter.h"

#include "include/stringify.h"



// ----
// LogEntryKey

void LogEntryKey::encode(bufferlist& bl) const
{
  ::encode(who, bl);
  ::encode(stamp, bl);
  ::encode(seq, bl);
}

void LogEntryKey::decode(bufferlist::iterator& bl)
{
  ::decode(who, bl);
  ::decode(stamp, bl);
  ::decode(seq, bl);
}

void LogEntryKey::dump(Formatter *f) const
{
  f->dump_stream("who") << who;
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("seq", seq);
}

void LogEntryKey::generate_test_instances(list<LogEntryKey*>& o)
{
  o.push_back(new LogEntryKey);
  o.push_back(new LogEntryKey(entity_inst_t(), utime_t(1,2), 34));
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
      assert(0);
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
      assert(0);
      return 0;
  }
}

void LogEntry::log_to_syslog(string level, string facility)
{
  int min = string_to_syslog_level(level);
  int l = clog_type_to_syslog_level(prio);
  if (l <= min) {
    int f = string_to_syslog_facility(facility);
    syslog(l | f, "%s %llu : %s",
	   stringify(who).c_str(),
	   (long long unsigned)seq,
	   msg.c_str());
  }
}

void LogEntry::encode(bufferlist& bl) const
{
  ENCODE_START(3, 2, bl);
  __u16 t = prio;
  ::encode(who, bl);
  ::encode(stamp, bl);
  ::encode(seq, bl);
  ::encode(t, bl);
  ::encode(msg, bl);
  ::encode(channel, bl);
  ENCODE_FINISH(bl);
}

void LogEntry::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  __u16 t;
  ::decode(who, bl);
  ::decode(stamp, bl);
  ::decode(seq, bl);
  ::decode(t, bl);
  prio = (clog_type)t;
  ::decode(msg, bl);
  if (struct_v >= 3) {
    ::decode(channel, bl);
  } else {
    // prior to having logging channels we only had a cluster log.
    // Ensure we keep that appearance when the other party has no
    // clue of what a 'channel' is.
    channel = CLOG_CHANNEL_CLUSTER;
  }
  DECODE_FINISH(bl);
}

void LogEntry::dump(Formatter *f) const
{
  f->dump_stream("who") << who;
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

void LogSummary::encode(bufferlist& bl) const
{
  ENCODE_START(2, 2, bl);
  ::encode(version, bl);
  ::encode(tail, bl);
  ENCODE_FINISH(bl);
}

void LogSummary::decode(bufferlist::iterator& bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
  ::decode(version, bl);
  ::decode(tail, bl);
  DECODE_FINISH(bl);
}

void LogSummary::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->open_array_section("tail");
  for (list<LogEntry>::const_iterator p = tail.begin(); p != tail.end(); ++p) {
    f->open_object_section("entry");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
}

void LogSummary::generate_test_instances(list<LogSummary*>& o)
{
  o.push_back(new LogSummary);
  // more!
}
