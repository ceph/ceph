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

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN

#include "common/LogClient.h"

#include "config.h"

void LogClient::log(log_type type, const char *s)
{
  string str(s);
  log(type, str);
}

void LogClient::log(log_type type, stringstream& ss)
{
  while (!ss.eof()) {
    string s;
    getline(ss, s);
    log(type, s);
  }
}

void LogClient::log(log_type type, string& s)
{
  Mutex::Locker l(log_lock);
  dout(0) << "log " << (log_type)type << " : " << s << dendl;
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

  if (mon < 0) {
    if (messenger->get_myname().is_mon())
      mon = messenger->get_myname().num();  // if we are a monitor, queue for ourselves
    else
      mon = rand() % monmap->mon_inst.size();
  }

  dout(10) << "send_log to mon" << mon << dendl;
  messenger->send_message(log, monmap->get_inst(mon));
}

void LogClient::handle_log_ack(MLogAck *m)
{
  Mutex::Locker l(log_lock);
  dout(10) << "handle_log_ack " << *m << dendl;

  version_t last = m->last;
  while (log_queue.size() && log_queue.begin()->seq <= last) {
    dout(10) << " logged " << log_queue.front() << dendl;
    log_queue.pop_front();
  }
  delete m;
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


