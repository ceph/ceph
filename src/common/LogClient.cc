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



#include "include/types.h"

#include "msg/Messenger.h"
#include "msg/Message.h"

#include "messages/MLog.h"
#include "messages/MLogAck.h"
#include "mon/MonMap.h"
#include "mon/MonClient.h"

#include <iostream>
#include <errno.h>
#include <sys/stat.h>
#include <syslog.h>

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN

#include "common/LogClient.h"

#include "common/config.h"

/*
 * Given a clog log_type, return the equivalent syslog priority
 */
static inline int clog_type_to_syslog_prio(clog_type t)
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

LogClientTemp::LogClientTemp(clog_type type_, LogClient &parent_)
  : type(type_), parent(parent_)
{
}

LogClientTemp::LogClientTemp(const LogClientTemp &rhs)
  : type(rhs.type), parent(rhs.parent)
{
  // don't want to-- nor can we-- copy the ostringstream
}

LogClientTemp::~LogClientTemp()
{
  if (ss.peek() != EOF)
    parent.do_log(type, ss);
}

void LogClient::do_log(clog_type type, std::stringstream& ss)
{
  while (!ss.eof()) {
    string s;
    getline(ss, s);
    if (!s.empty())
      do_log(type, s);
  }
}

void LogClient::do_log(clog_type type, const std::string& s)
{
  Mutex::Locker l(log_lock);
  dout(0) << "log " << type << " : " << s << dendl;
  LogEntry e;
  e.who = messenger->get_myinst();
  e.stamp = g_clock.now();
  e.seq = ++last_log;
  e.type = type;
  e.msg = s;

  // log to syslog?
  if (g_conf->clog_to_syslog) {
    ostringstream oss;
    oss << e;
    string str(oss.str());
    syslog(clog_type_to_syslog_prio(e.type) | LOG_USER, "%s", str.c_str());
  }

  // log to monitor?
  if (g_conf->clog_to_monitors) {
    log_queue.push_back(e);

    // if we are a monitor, queue for ourselves, synchronously
    if (is_mon) {
      assert(messenger->get_myname().is_mon());
      dout(10) << "send_log to self" << dendl;
      Message *log = _get_mon_log_message();
      messenger->send_message(log, messenger->get_myinst());
    }
  }
}

void LogClient::reset_session()
{
  Mutex::Locker l(log_lock);
  last_log_sent = last_log - log_queue.size();
}

Message *LogClient::get_mon_log_message()
{
  Mutex::Locker l(log_lock);
  return _get_mon_log_message();
}

Message *LogClient::_get_mon_log_message()
{
   if (log_queue.empty())
     return NULL;

  // only send entries that haven't been sent yet during this mon
  // session!  monclient needs to call reset_session() on mon session
  // reset for this to work right.

  if (last_log_sent == last_log)
    return NULL;

  unsigned i = last_log - last_log_sent;
  dout(10) << " log_queue is " << log_queue.size() << " last_log " << last_log << " sent " << last_log_sent
	   << " i " << i << dendl;
  std::deque<LogEntry> o(i);
  std::deque<LogEntry>::reverse_iterator p = log_queue.rbegin();
  while (i > 0) {
    i--;
    o[i] = *p++;
    dout(10) << " will send " << o[i] << dendl;
  }

  last_log_sent = last_log;
  
  MLog *log = new MLog(monmap->get_fsid());
  log->entries.swap(o);

  return log;
}

void LogClient::handle_log_ack(MLogAck *m)
{
  Mutex::Locker l(log_lock);
  dout(10) << "handle_log_ack " << *m << dendl;

  version_t last = m->last;

  deque<LogEntry>::iterator q = log_queue.begin();
  while (q != log_queue.end()) {
    const LogEntry &entry(*q);
    if (entry.seq > last)
      break;
    dout(10) << " logged " << entry << dendl;
    q = log_queue.erase(q);
  }
  m->put();
}

bool LogClient::ms_dispatch(Message *m)
{
  dout(20) << "dispatch " << m << dendl;

  switch (m->get_type()) {
  case MSG_LOGACK:
    handle_log_ack((MLogAck*)m);
    return true;
  }
  return false;
}
