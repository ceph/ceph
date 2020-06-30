
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

#include <boost/variant.hpp>

#include "msg/Connection.h"
#include "msg/Dispatcher.h"
#include "mon/MgrMap.h"
#include "mgr/DaemonHealthMetric.h"

#include "messages/MMgrReport.h"
#include "mgr/MetricTypes.h"

#include "common/perf_counters.h"
#include "common/Timer.h"
#include "common/CommandTable.h"

class MMgrMap;
class MMgrConfigure;
class MMgrClose;
class Messenger;
class MCommandReply;
class MPGStats;
class MonMap;

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
  std::string name;
  bool tell = false;

  explicit MgrCommand(ceph_tid_t t) : CommandOp(t) {}
  MgrCommand() : CommandOp() {}
};

class MgrClient : public Dispatcher
{
protected:
  CephContext *cct;
  MgrMap map;
  Messenger *msgr;
  MonMap *monmap;

  std::unique_ptr<MgrSessionState> session;

  ceph::mutex lock = ceph::make_mutex("MgrClient::lock");
  ceph::condition_variable shutdown_cond;

  uint32_t stats_period = 0;
  uint32_t stats_threshold = 0;
  SafeTimer timer;

  CommandTable<MgrCommand> command_table;

  using clock_t = ceph::real_clock;
  clock_t::time_point last_connect_attempt;

  uint64_t last_config_bl_version = 0;

  Context *report_callback = nullptr;
  Context *connect_retry_callback = nullptr;

  // If provided, use this to compose an MPGStats to send with
  // our reports (hook for use by OSD)
  std::function<MPGStats*()> pgstats_cb;
  std::function<void(const ConfigPayload &)> set_perf_queries_cb;
  std::function<MetricPayload()> get_perf_report_cb;

  // for service registration and beacon
  bool service_daemon = false;
  bool daemon_dirty_status = false;
  bool task_dirty_status = false;
  std::string service_name, daemon_name;
  std::map<std::string,std::string> daemon_metadata;
  std::map<std::string,std::string> daemon_status;
  std::map<std::string,std::string> task_status;
  std::vector<DaemonHealthMetric> daemon_health_metrics;

  void reconnect();
  void _send_open();

  // In pre-luminous clusters, the ceph-mgr service is absent or optional,
  // so we must not block in start_command waiting for it.
  bool mgr_optional = false;

public:
  MgrClient(CephContext *cct_, Messenger *msgr_, MonMap *monmap);

  void set_messenger(Messenger *msgr_) { msgr = msgr_; }

  void init();
  void shutdown();

  void set_mgr_optional(bool optional_) {mgr_optional = optional_;}

  bool ms_dispatch2(const ceph::ref_t<Message>& m) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;

  bool handle_mgr_map(ceph::ref_t<MMgrMap> m);
  bool handle_mgr_configure(ceph::ref_t<MMgrConfigure> m);
  bool handle_mgr_close(ceph::ref_t<MMgrClose> m);
  bool handle_command_reply(
    uint64_t tid,
    ceph::buffer::list& data,
    const std::string& rs,
    int r);

  void set_perf_metric_query_cb(
    std::function<void(const ConfigPayload &)> cb_set,
    std::function<MetricPayload()> cb_get)
  {
      std::lock_guard l(lock);
      set_perf_queries_cb = cb_set;
      get_perf_report_cb = cb_get;
  }

  void send_pgstats();
  void set_pgstats_cb(std::function<MPGStats*()>&& cb_)
  {
    std::lock_guard l(lock);
    pgstats_cb = std::move(cb_);
  }

  int start_command(
    const std::vector<std::string>& cmd, const ceph::buffer::list& inbl,
    ceph::buffer::list *outbl, std::string *outs,
    Context *onfinish);
  int start_tell_command(
    const std::string& name,
    const std::vector<std::string>& cmd, const ceph::buffer::list& inbl,
    ceph::buffer::list *outbl, std::string *outs,
    Context *onfinish);

  int service_daemon_register(
    const std::string& service,
    const std::string& name,
    const std::map<std::string,std::string>& metadata);
  int service_daemon_update_status(
    std::map<std::string,std::string>&& status);
  int service_daemon_update_task_status(
    std::map<std::string,std::string> &&task_status);
  void update_daemon_health(std::vector<DaemonHealthMetric>&& metrics);

  bool is_initialized() const { return initialized; }

private:
  void handle_config_payload(const OSDConfigPayload &payload) {
    if (set_perf_queries_cb) {
      set_perf_queries_cb(payload);
    }
  }

  void handle_config_payload(const MDSConfigPayload &payload) {
    if (set_perf_queries_cb) {
      set_perf_queries_cb(payload);
    }
  }

  void handle_config_payload(const UnknownConfigPayload &payload) {
    ceph_abort();
  }

  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    MgrClient *mgrc;

    HandlePayloadVisitor(MgrClient *mgrc)
      : mgrc(mgrc) {
    }

    template <typename ConfigPayload>
    inline void operator()(const ConfigPayload &payload) const {
      mgrc->handle_config_payload(payload);
    }
  };

  void _send_stats();
  void _send_pgstats();
  void _send_report();

  bool initialized = false;
};

#endif
