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

#define dout_subsys ceph_subsys_monc

LogClient::LogClient(CephContext *cct, Messenger *m, MonMap *mm,
		     enum logclient_flag_t flags)
  : cct(cct), messenger(m), monmap(mm), is_mon(flags & FLAG_MON),
    log_lock("LogClient::log_lock"), last_log_sent(0), last_log(0)
{
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
  ldout(cct,0) << "log " << type << " : " << s << dendl;
  LogEntry e;
  e.who = messenger->get_myinst();
  e.stamp = ceph_clock_now(cct);
  e.seq = ++last_log;
  e.type = type;
  e.msg = s;

  // log to syslog?
  if (cct->_conf->clog_to_syslog) {
    e.log_to_syslog(cct->_conf->clog_to_syslog_level,
		    cct->_conf->clog_to_syslog_facility);
  }

  // log to monitor?
  if (cct->_conf->clog_to_monitors) {
    log_queue.push_back(e);

    // if we are a monitor, queue for ourselves, synchronously
    if (is_mon) {
      assert(messenger->get_myname().is_mon());
      ldout(cct,10) << "send_log to self" << dendl;
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

bool LogClient::are_pending()
{
  Mutex::Locker l(log_lock);
  return last_log > last_log_sent;
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

  // limit entries per message
  unsigned num_unsent = last_log - last_log_sent;
  unsigned num_send;
  if (cct->_conf->mon_client_max_log_entries_per_message > 0)
    num_send = MIN(num_unsent, (unsigned)cct->_conf->mon_client_max_log_entries_per_message);
  else
    num_send = num_unsent;

  ldout(cct,10) << " log_queue is " << log_queue.size() << " last_log " << last_log << " sent " << last_log_sent
		<< " num " << log_queue.size()
		<< " unsent " << num_unsent
		<< " sending " << num_send << dendl;
  assert(num_unsent <= log_queue.size());
  std::deque<LogEntry>::iterator p = log_queue.begin();
  std::deque<LogEntry> o;
  while (p->seq < last_log_sent) {
    ++p;
    assert(p != log_queue.end());
  }
  while (num_send--) {
    assert(p != log_queue.end());
    o.push_back(*p);
    last_log_sent = p->seq;
    ldout(cct,10) << " will send " << *p << dendl;
    ++p;
  }
  
  MLog *log = new MLog(monmap->get_fsid());
  log->entries.swap(o);

  return log;
}

void LogClient::handle_log_ack(MLogAck *m)
{
  Mutex::Locker l(log_lock);
  ldout(cct,10) << "handle_log_ack " << *m << dendl;

  version_t last = m->last;

  deque<LogEntry>::iterator q = log_queue.begin();
  while (q != log_queue.end()) {
    const LogEntry &entry(*q);
    if (entry.seq > last)
      break;
    ldout(cct,10) << " logged " << entry << dendl;
    q = log_queue.erase(q);
  }
  m->put();
}

