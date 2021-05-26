// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include "ActivePyModule.h"

#include "common/Finisher.h"
#include "common/ceph_mutex.h"

#include "PyFormatter.h"

#include "osdc/Objecter.h"
#include "client/Client.h"
#include "common/LogClient.h"
#include "mon/MgrMap.h"
#include "mon/MonCommand.h"
#include "mon/mon_types.h"
#include "mon/ConfigMap.h"
#include "mgr/TTLCache.h"

#include "DaemonState.h"
#include "ClusterState.h"
#include "OSDPerfMetricTypes.h"

class health_check_map_t;
class DaemonServer;
class MgrSession;
class ModuleCommand;
class PyModuleRegistry;

class ActivePyModules
{
  // module class instances not yet created
  std::set<std::string, std::less<>> pending_modules;
  // module class instances already created
  std::map<std::string, std::shared_ptr<ActivePyModule>> modules;
  PyModuleConfig &module_config;
  bool have_local_config_map = false;
  std::map<std::string, std::string> store_cache;
  ConfigMap config_map;  ///< derived from store_cache config/ keys
  DaemonStateIndex &daemon_state;
  ClusterState &cluster_state;
  MonClient &monc;
  LogChannelRef clog, audit_clog;
  Objecter &objecter;
  Client   &client;
  Finisher &finisher;
  TTLCache<string, PyObject*> ttl_cache;
public:
  Finisher cmd_finisher;
private:
  DaemonServer &server;
  PyModuleRegistry &py_module_registry;

  map<std::string,ProgressEvent> progress_events;

  mutable ceph::mutex lock = ceph::make_mutex("ActivePyModules::lock");

public:
  ActivePyModules(
    PyModuleConfig &module_config,
    std::map<std::string, std::string> store_data,
    bool mon_provides_kv_sub,
    DaemonStateIndex &ds, ClusterState &cs, MonClient &mc,
    LogChannelRef clog_, LogChannelRef audit_clog_, Objecter &objecter_, Client &client_,
    Finisher &f, DaemonServer &server, PyModuleRegistry &pmr);

  ~ActivePyModules();

  // FIXME: wrap for send_command?
  MonClient &get_monc() {return monc;}
  Objecter  &get_objecter() {return objecter;}
  Client    &get_client() {return client;}
  PyObject *cacheable_get_python(const std::string &what);
  PyObject *get_python(const std::string &what);
  PyObject *get_server_python(const std::string &hostname);
  PyObject *list_servers_python();
  PyObject *get_metadata_python(
    const std::string &svc_type, const std::string &svc_id);
  PyObject *get_daemon_status_python(
    const std::string &svc_type, const std::string &svc_id);
  PyObject *get_counter_python(
    const std::string &svc_type,
    const std::string &svc_id,
    const std::string &path);
  PyObject *get_latest_counter_python(
    const std::string &svc_type,
    const std::string &svc_id,
    const std::string &path);
  PyObject *get_perf_schema_python(
     const std::string &svc_type,
     const std::string &svc_id);
  PyObject *get_context();
  PyObject *get_osdmap();
  /// @note @c fct is not allowed to acquire locks when holding GIL
  PyObject *with_perf_counters(
      std::function<void(
        PerfCounterInstance& counter_instance,
        PerfCounterType& counter_type,
        PyFormatter& f)> fct,
      const std::string &svc_name,
      const std::string &svc_id,
      const std::string &path) const;

  MetricQueryID add_osd_perf_query(
      const OSDPerfMetricQuery &query,
      const std::optional<OSDPerfMetricLimit> &limit);
  void remove_osd_perf_query(MetricQueryID query_id);
  PyObject *get_osd_perf_counters(MetricQueryID query_id);

  MetricQueryID add_mds_perf_query(
      const MDSPerfMetricQuery &query,
      const std::optional<MDSPerfMetricLimit> &limit);
  void remove_mds_perf_query(MetricQueryID query_id);
  PyObject *get_mds_perf_counters(MetricQueryID query_id);

  bool get_store(const std::string &module_name,
      const std::string &key, std::string *val) const;
  PyObject *get_store_prefix(const std::string &module_name,
			      const std::string &prefix) const;
  void set_store(const std::string &module_name,
      const std::string &key, const std::optional<std::string> &val);

  bool get_config(const std::string &module_name,
      const std::string &key, std::string *val) const;
  std::pair<int, std::string> set_config(const std::string &module_name,
      const std::string &key, const std::optional<std::string> &val);

  PyObject *get_typed_config(const std::string &module_name,
			     const std::string &key,
			     const std::string &prefix = "") const;
  PyObject *get_foreign_config(
    const std::string& who,
    const std::string& name);

  void set_health_checks(const std::string& module_name,
			 health_check_map_t&& checks);
  void get_health_checks(health_check_map_t *checks);

  void update_progress_event(const std::string& evid,
			     const std::string& desc,
			     float progress,
			     bool add_to_ceph_s);
  void complete_progress_event(const std::string& evid);
  void clear_all_progress_events();
  void get_progress_events(std::map<std::string,ProgressEvent>* events);

  void register_client(std::string_view name, std::string addrs);
  void unregister_client(std::string_view name, std::string addrs);

  void config_notify();

  void set_uri(const std::string& module_name, const std::string &uri);
  void set_device_wear_level(const std::string& devid, float wear_level);

  int handle_command(
    const ModuleCommand& module_command,
    const MgrSession& session,
    const cmdmap_t &cmdmap,
    const bufferlist &inbuf,
    std::stringstream *ds,
    std::stringstream *ss);

  std::map<std::string, std::string> get_services() const;

  void update_kv_data(
    const std::string prefix,
    bool incremental,
    const map<std::string, std::optional<bufferlist>, std::less<>>& data);
  void _refresh_config_map();

  // Public so that MonCommandCompletion can use it
  // FIXME: for send_command completion notifications,
  // send it to only the module that sent the command, not everyone
  void notify_all(const std::string &notify_type,
                  const std::string &notify_id);
  void notify_all(const LogEntry &log_entry);

  bool is_pending(std::string_view name) const {
    return pending_modules.count(name) > 0;
  }
  bool module_exists(const std::string &name) const
  {
    return modules.count(name) > 0;
  }

  bool method_exists(
      const std::string &module_name,
      const std::string &method_name) const
  {
    return modules.at(module_name)->method_exists(method_name);
  }

  PyObject *dispatch_remote(
      const std::string &other_module,
      const std::string &method,
      PyObject *args,
      PyObject *kwargs,
      std::string *err);

  int init();
  void shutdown();

  void start_one(PyModuleRef py_module);

  void dump_server(const std::string &hostname,
                   const DaemonStateCollection &dmc,
                   Formatter *f);

  void cluster_log(const std::string &channel, clog_type prio,
    const std::string &message);

  bool inject_python_on() const;
  void update_cache_metrics();
};

