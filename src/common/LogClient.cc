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

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN

#include "common/LogClient.h"

#include "config.h"

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
  log_queue.push_back(e);
  
  if (is_synchronous)
    _send_log();
}

void LogClient::send_log()
{
  Mutex::Locker l(log_lock);
  _send_log();
}

void LogClient::_send_log()
{
  if (log_queue.empty())
    return;
  MLog *log = new MLog(monmap->get_fsid(), log_queue);

  if (monc) {
    monc->send_mon_message(log);
  } else {
    // if we are a monitor, queue for ourselves
    assert(messenger->get_myname().is_mon());
    dout(10) << "send_log to self" << dendl;
    messenger->send_message(log, messenger->get_myinst());
  }
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


void LogClient::ms_handle_connect(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    dout(10) << "ms_handle_connect on mon " << con->get_peer_addr() << ", resending pending log events" << dendl;
    send_log();
  }
}
