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

#ifndef CEPH_LOGCLIENT_H
#define CEPH_LOGCLIENT_H

#include "common/LogEntry.h"
#include "common/Mutex.h"
#include "msg/Dispatcher.h"

#include <iosfwd>
#include <sstream>

class LogClient;
class MLog;
class MLogAck;
class Messenger;
class MonClient;
class MonMap;

class LogClientTemp
{
public:
  LogClientTemp(clog_type type_, LogClient &parent_);
  LogClientTemp(const LogClientTemp &rhs);
  ~LogClientTemp();

  template<typename T>
  std::ostream& operator<<(const T& rhs)
  {
    return ss << rhs;
  }

private:
  clog_type type;
  LogClient &parent;
  stringstream ss;
};

class LogClient : public Dispatcher
{
public:
  enum logclient_flag_t {
    NO_FLAGS = 0,
    FLAG_SYNC = 0x1,
  };

  LogClient(Messenger *m, MonMap *mm, MonClient *mc, enum logclient_flag_t flags) :
    messenger(m), monmap(mm), monc(mc), is_synchronous(flags & FLAG_SYNC),
    log_lock("LogClient::log_lock"), last_log(0) { }

  void send_log();
  void handle_log_ack(MLogAck *m);
  void set_synchronous(bool sync) { is_synchronous = sync; }

  LogClientTemp debug() {
    return LogClientTemp(CLOG_DEBUG, *this);
  }
  void debug(std::stringstream &s) {
    do_log(CLOG_DEBUG, s);
  }
  LogClientTemp info() {
    return LogClientTemp(CLOG_INFO, *this);
  }
  void info(std::stringstream &s) {
    do_log(CLOG_INFO, s);
  }
  LogClientTemp warn() {
    return LogClientTemp(CLOG_WARN, *this);
  }
  void warn(std::stringstream &s) {
    do_log(CLOG_WARN, s);
  }
  LogClientTemp error() {
    return LogClientTemp(CLOG_ERROR, *this);
  }
  void error(std::stringstream &s) {
    do_log(CLOG_ERROR, s);
  }
  LogClientTemp sec() {
    return LogClientTemp(CLOG_SEC, *this);
  }
  void sec(std::stringstream &s) {
    do_log(CLOG_SEC, s);
  }

private:
  void do_log(clog_type type, std::stringstream& ss);
  void do_log(clog_type type, const std::string& s);
  bool ms_dispatch(Message *m);
  void _send_log();
  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}

  Messenger *messenger;
  MonMap *monmap;
  MonClient *monc;
  bool is_synchronous;
  Mutex log_lock;
  version_t last_log;
  std::deque<LogEntry> log_queue;

  friend class LogClientTemp;
};

#endif
