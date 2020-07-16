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

#ifndef DAEMON_SERVER_H_
#define DAEMON_SERVER_H_

#include "PyModuleRegistry.h"

#include <set>
#include <string>
#include <boost/variant.hpp>

#include "common/ceph_mutex.h"
#include "common/LogClient.h"
#include "common/Timer.h"

#include <msg/Messenger.h>
#include <mon/MonClient.h>

#include "ServiceMap.h"
#include "MgrSession.h"
#include "DaemonState.h"
#include "MetricCollector.h"
#include "OSDPerfMetricCollector.h"
#include "MDSPerfMetricCollector.h"

class MMgrReport;
class MMgrOpen;
class MMgrClose;
class MMonMgrReport;
class MCommand;
class MMgrCommand;
struct MonCommand;
class CommandContext;
struct OSDPerfMetricQuery;
struct MDSPerfMetricQuery;

/**
 * Server used in ceph-mgr to communicate with Ceph daemons like
 * MDSs and OSDs.
 */
class DaemonServer : public Dispatcher, public md_config_obs_t
{
protected:
  boost::scoped_ptr<Throttle> client_byte_throttler;
  boost::scoped_ptr<Throttle> client_msg_throttler;
  boost::scoped_ptr<Throttle> osd_byte_throttler;
  boost::scoped_ptr<Throttle> osd_msg_throttler;
  boost::scoped_ptr<Throttle> mds_byte_throttler;
  boost::scoped_ptr<Throttle> mds_msg_throttler;
  boost::scoped_ptr<Throttle> mon_byte_throttler;
  boost::scoped_ptr<Throttle> mon_msg_throttler;

  Messenger *msgr;
  MonClient *monc;
  Finisher  &finisher;
  DaemonStateIndex &daemon_state;
  ClusterState &cluster_state;
  PyModuleRegistry &py_modules;
  LogChannelRef clog, audit_clog;

  // Connections for daemons, and clients with service names set
  // (i.e. those MgrClients that are allowed to send MMgrReports)
  std::set<ConnectionRef> daemon_connections;

  /// connections for osds
  ceph::unordered_map<int,set<ConnectionRef>> osd_cons;

  ServiceMap pending_service_map;  // uncommitted

  epoch_t pending_service_map_dirty = 0;

  ceph::mutex lock = ceph::make_mutex("DaemonServer");

  static void _generate_command_map(cmdmap_t& cmdmap,
                                    map<string,string> &param_str_map);
  static const MonCommand *_get_mgrcommand(const string &cmd_prefix,
                                           const std::vector<MonCommand> &commands);
  bool _allowed_command(
    MgrSession *s, const string &service, const string &module,
    const string &prefix, const cmdmap_t& cmdmap,
    const map<string,string>& param_str_map,
    const MonCommand *this_cmd);

private:
  friend class ReplyOnFinish;
  bool _reply(MCommand* m,
	      int ret, const std::string& s, const bufferlist& payload);

  void _prune_pending_service_map();

  utime_t started_at;
  std::atomic<bool> pgmap_ready;
  std::set<int32_t> reported_osds;
  void maybe_ready(int32_t osd_id);

  SafeTimer timer;
  bool shutting_down;
  Context *tick_event;
  void tick();
  void schedule_tick_locked(double delay_sec);

  class OSDPerfMetricCollectorListener : public MetricListener {
  public:
    OSDPerfMetricCollectorListener(DaemonServer *server)
      : server(server) {
    }
    void handle_query_updated() override {
      server->handle_osd_perf_metric_query_updated();
    }
  private:
    DaemonServer *server;
  };
  OSDPerfMetricCollectorListener osd_perf_metric_collector_listener;
  OSDPerfMetricCollector osd_perf_metric_collector;
  void handle_osd_perf_metric_query_updated();

  class MDSPerfMetricCollectorListener : public MetricListener {
  public:
    MDSPerfMetricCollectorListener(DaemonServer *server)
      : server(server) {
    }
    void handle_query_updated() override {
      server->handle_mds_perf_metric_query_updated();
    }
  private:
    DaemonServer *server;
  };
  MDSPerfMetricCollectorListener mds_perf_metric_collector_listener;
  MDSPerfMetricCollector mds_perf_metric_collector;
  void handle_mds_perf_metric_query_updated();

  void handle_metric_payload(const OSDMetricPayload &payload) {
    osd_perf_metric_collector.process_reports(payload);
  }

  void handle_metric_payload(const MDSMetricPayload &payload) {
    mds_perf_metric_collector.process_reports(payload);
  }

  void handle_metric_payload(const UnknownMetricPayload &payload) {
    ceph_abort();
  }

  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    DaemonServer *server;

    HandlePayloadVisitor(DaemonServer *server)
      : server(server) {
    }

    template <typename MetricPayload>
    inline void operator()(const MetricPayload &payload) const {
      server->handle_metric_payload(payload);
    }
  };

  void update_task_status(DaemonKey key,
			  const std::map<std::string,std::string>& task_status);

public:
  int init(uint64_t gid, entity_addrvec_t client_addrs);
  void shutdown();

  entity_addrvec_t get_myaddrs() const;

  DaemonServer(MonClient *monc_,
               Finisher &finisher_,
	       DaemonStateIndex &daemon_state_,
	       ClusterState &cluster_state_,
	       PyModuleRegistry &py_modules_,
	       LogChannelRef cl,
	       LogChannelRef auditcl);
  ~DaemonServer() override;

  bool ms_dispatch2(const ceph::ref_t<Message>& m) override;
  int ms_handle_authentication(Connection *con) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;

  void fetch_missing_metadata(const DaemonKey& key, const entity_addr_t& addr);
  bool handle_open(const ceph::ref_t<MMgrOpen>& m);
  bool handle_close(const ceph::ref_t<MMgrClose>& m);
  bool handle_report(const ceph::ref_t<MMgrReport>& m);
  bool handle_command(const ceph::ref_t<MCommand>& m);
  bool handle_command(const ceph::ref_t<MMgrCommand>& m);
  bool _handle_command(std::shared_ptr<CommandContext>& cmdctx);
  void send_report();
  void got_service_map();
  void got_mgr_map();
  void adjust_pgs();

  void _send_configure(ConnectionRef c);

  MetricQueryID add_osd_perf_query(
      const OSDPerfMetricQuery &query,
      const std::optional<OSDPerfMetricLimit> &limit);
  int remove_osd_perf_query(MetricQueryID query_id);
  int get_osd_perf_counters(MetricQueryID query_id,
                            std::map<OSDPerfMetricKey, PerformanceCounters> *c);

  virtual const char** get_tracked_conf_keys() const override;
  virtual void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override;

  void schedule_tick(double delay_sec);

  void log_access_denied(std::shared_ptr<CommandContext>& cmdctx,
                         MgrSession* session, std::stringstream& ss);
  void dump_pg_ready(ceph::Formatter *f);
};

#endif

