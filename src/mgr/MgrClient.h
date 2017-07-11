// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef MGR_CLIENT_H_
#define MGR_CLIENT_H_

#include "msg/Dispatcher.h"
#include "mon/MgrMap.h"

#include "msg/Connection.h"

#include "common/perf_counters.h"
#include "common/Timer.h"
#include "common/CommandTable.h"

class MMgrMap;
class MMgrConfigure;
class Messenger;
class MCommandReply;
class MPGStats;

class MgrSessionState
{
  public:
  // Which performance counters have we already transmitted schema for?
  std::set<std::string> declared;

  // Our connection to the mgr
  ConnectionRef con;
};

class MgrCommand : public CommandOp
{
  public:

  MgrCommand(ceph_tid_t t) : CommandOp(t) {}
  MgrCommand() : CommandOp() {}
};

class MgrClient : public Dispatcher
{
protected:
  CephContext *cct;
  MgrMap map;
  Messenger *msgr;

  unique_ptr<MgrSessionState> session;

  Mutex lock = {"MgrClient::lock"};

  uint32_t stats_period = 0;
  SafeTimer timer;

  CommandTable<MgrCommand> command_table;

  utime_t last_connect_attempt;

  Context *report_callback = nullptr;
  Context *connect_retry_callback = nullptr;

  // If provided, use this to compose an MPGStats to send with
  // our reports (hook for use by OSD)
  std::function<MPGStats*()> pgstats_cb;

  // for service registration and beacon
  bool service_daemon = false;
  bool daemon_dirty_status = false;
  std::string service_name, daemon_name;
  std::map<std::string,std::string> daemon_metadata;
  std::map<std::string,std::string> daemon_status;

  void reconnect();
  void _send_open();

public:
  MgrClient(CephContext *cct_, Messenger *msgr_);

  void set_messenger(Messenger *msgr_) { msgr = msgr_; }

  void init();
  void shutdown();

  bool ms_dispatch(Message *m) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;

  bool handle_mgr_map(MMgrMap *m);
  bool handle_mgr_configure(MMgrConfigure *m);
  bool handle_command_reply(MCommandReply *m);

  void send_report();
  void send_pgstats();

  void set_pgstats_cb(std::function<MPGStats*()> cb_)
  {
    Mutex::Locker l(lock);
    pgstats_cb = cb_;
  }

  int start_command(const vector<string>& cmd, const bufferlist& inbl,
		    bufferlist *outbl, string *outs,
		    Context *onfinish);

  int service_daemon_register(
    const std::string& service,
    const std::string& name,
    const std::map<std::string,std::string>& metadata);
  int service_daemon_update_status(
    const std::map<std::string,std::string>& status);
};

#endif

