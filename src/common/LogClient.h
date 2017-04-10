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

#include <iosfwd>
#include <sstream>

class LogClient;
class MLog;
class MLogAck;
class Messenger;
class MonMap;
class Message;
struct uuid_d;
struct Connection;

class LogChannel;

namespace ceph {
namespace log {
  class Graylog;
}
}

int parse_log_client_options(CephContext *cct,
			     map<string,string> &log_to_monitors,
			     map<string,string> &log_to_syslog,
			     map<string,string> &log_channels,
			     map<string,string> &log_prios,
			     map<string,string> &log_to_graylog,
			     map<string,string> &log_to_graylog_host,
			     map<string,string> &log_to_graylog_port,
			     uuid_d &fsid,
			     string &host);

class LogClientTemp
{
public:
  LogClientTemp(clog_type type_, LogChannel &parent_);
  LogClientTemp(const LogClientTemp &rhs);
  ~LogClientTemp();

  template<typename T>
  std::ostream& operator<<(const T& rhs)
  {
    return ss << rhs;
  }

private:
  clog_type type;
  LogChannel &parent;
  stringstream ss;
};

/** Manage where we output to and at which priority
 *
 * Not to be confused with the LogClient, which is the almighty coordinator
 * of channels.  We just deal with the boring part of the logging: send to
 * syslog, send to file, generate LogEntry and queue it for the LogClient.
 *
 * Past queueing the LogEntry, the LogChannel is done with the whole thing.
 * LogClient will deal with sending and handling of LogEntries.
 */
class LogChannel
{
public:

  LogChannel(CephContext *cct, LogClient *lc, const std::string &channel);
  LogChannel(CephContext *cct, LogClient *lc,
             const std::string &channel,
             const std::string &facility,
             const std::string &prio);

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

  void set_log_to_monitors(bool v) {
    log_to_monitors = v;
  }
  void set_log_to_syslog(bool v) {
    log_to_syslog = v;
  }
  void set_log_channel(const std::string& v) {
    log_channel = v;
  }
  void set_log_prio(const std::string& v) {
    log_prio = v;
  }
  void set_syslog_facility(const std::string& v) {
    syslog_facility = v;
  }
  std::string get_log_prio() { return log_prio; }
  std::string get_log_channel() { return log_channel; }
  std::string get_syslog_facility() { return syslog_facility; }
  bool must_log_to_syslog() { return log_to_syslog; }
  /**
   * Do we want to log to syslog?
   *
   * @return true if log_to_syslog is true and both channel and prio
   *         are not empty; false otherwise.
   */
  bool do_log_to_syslog() {
    return must_log_to_syslog() &&
          !log_prio.empty() && !log_channel.empty();
  }
  bool must_log_to_monitors() { return log_to_monitors; }

  bool do_log_to_graylog() {
    return (graylog != nullptr);
  }

  typedef shared_ptr<LogChannel> Ref;

  /**
   * update config values from parsed k/v map for each config option
   *
   * Pick out the relevant value based on our channel.
   */
  void update_config(map<string,string> &log_to_monitors,
		     map<string,string> &log_to_syslog,
		     map<string,string> &log_channels,
		     map<string,string> &log_prios,
		     map<string,string> &log_to_graylog,
		     map<string,string> &log_to_graylog_host,
		     map<string,string> &log_to_graylog_port,
		     uuid_d &fsid,
		     string &host);

  void do_log(clog_type prio, std::stringstream& ss);
  void do_log(clog_type prio, const std::string& s);

private:
  CephContext *cct;
  LogClient *parent;
  Mutex channel_lock;
  std::string log_channel;
  std::string log_prio;
  std::string syslog_facility;
  bool log_to_syslog;
  bool log_to_monitors;
  shared_ptr<ceph::log::Graylog> graylog;


  friend class LogClientTemp;
};

typedef LogChannel::Ref LogChannelRef;

class LogClient
{
public:
  enum logclient_flag_t {
    NO_FLAGS = 0,
    FLAG_MON = 0x1,
  };

  LogClient(CephContext *cct, Messenger *m, MonMap *mm,
	    enum logclient_flag_t flags);
  virtual ~LogClient() {
    channels.clear();
  }

  bool handle_log_ack(MLogAck *m);
  Message *get_mon_log_message(bool flush);
  bool are_pending();

  LogChannelRef create_channel() {
    return create_channel(CLOG_CHANNEL_DEFAULT);
  }

  LogChannelRef create_channel(const std::string& name) {
    LogChannelRef c;
    if (channels.count(name))
      c = channels[name];
    else {
      c = LogChannelRef(new LogChannel(cct, this, name));
      channels[name] = c;
    }
    return c;
  }

  void destroy_channel(const std::string& name) {
    if (channels.count(name))
      channels.erase(name);
  }

  void shutdown() {
    channels.clear();
  }

  version_t queue(LogEntry &entry);

private:
  Message *_get_mon_log_message();
  void _send_to_mon();

  CephContext *cct;
  Messenger *messenger;
  MonMap *monmap;
  bool is_mon;
  Mutex log_lock;
  version_t last_log_sent;
  version_t last_log;
  std::deque<LogEntry> log_queue;

  std::map<std::string, LogChannelRef> channels;

};
#endif
