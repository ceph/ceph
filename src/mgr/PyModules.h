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

#ifndef PY_MODULES_H_
#define PY_MODULES_H_

#include "MgrPyModule.h"

#include "common/Finisher.h"
#include "common/Mutex.h"
#include "common/Thread.h"

#include "osdc/Objecter.h"
#include "client/Client.h"
#include "common/LogClient.h"
#include "mon/MgrMap.h"
#include "mon/MonCommand.h"

#include "DaemonState.h"
#include "ClusterState.h"

class ServeThread;
class health_check_map_t;

class PyModules
{
  std::map<std::string, std::unique_ptr<MgrPyModule>> modules;
  std::map<std::string, std::unique_ptr<ServeThread>> serve_threads;
  DaemonStateIndex &daemon_state;
  ClusterState &cluster_state;
  MonClient &monc;
  LogChannelRef clog;
  Objecter &objecter;
  Client   &client;
  Finisher &finisher;

  mutable Mutex lock{"PyModules::lock"};

  std::string get_site_packages();

  PyThreadState *pMainThreadState = nullptr;

public:
  static std::string config_prefix;

  PyModules(DaemonStateIndex &ds, ClusterState &cs, MonClient &mc,
            LogChannelRef clog_, Objecter &objecter_, Client &client_,
            Finisher &f);

  ~PyModules();

  // FIXME: wrap for send_command?
  MonClient &get_monc() {return monc;}
  Objecter  &get_objecter() {return objecter;}
  Client    &get_client() {return client;}

  PyObject *get_python(const std::string &what);
  PyObject *get_server_python(const std::string &hostname);
  PyObject *list_servers_python();
  PyObject *get_metadata_python(
    std::string const &handle,
    const std::string &svc_name, const std::string &svc_id);
  PyObject *get_daemon_status_python(
    std::string const &handle,
    const std::string &svc_name, const std::string &svc_id);
  PyObject *get_counter_python(
    std::string const &handle,
    const std::string &svc_name,
    const std::string &svc_id,
    const std::string &path);
  PyObject *get_perf_schema_python(
     const std::string &handle,
     const std::string svc_type,
     const std::string &svc_id);
  PyObject *get_context();
  PyObject *get_osdmap();

  std::map<std::string, std::string> config_cache;

  // Python command definitions, including callback
  std::vector<ModuleCommand> get_py_commands() const;

  // Monitor command definitions, suitable for CLI
  std::vector<MonCommand> get_commands() const;

  void insert_config(const std::map<std::string, std::string> &new_config);

  // Public so that MonCommandCompletion can use it
  // FIXME: for send_command completion notifications,
  // send it to only the module that sent the command, not everyone
  void notify_all(const std::string &notify_type,
                  const std::string &notify_id);
  void notify_all(const LogEntry &log_entry);

  int init();
  void start();
  void shutdown();

  void dump_server(const std::string &hostname,
                   const DaemonStateCollection &dmc,
                   Formatter *f);

  bool get_config(const std::string &handle,
      const std::string &key, std::string *val) const;
  PyObject *get_config_prefix(const std::string &handle,
			      const std::string &prefix) const;
  void set_config(const std::string &handle,
      const std::string &key, const boost::optional<std::string> &val);

  void set_health_checks(const std::string& handle,
			 health_check_map_t&& checks);
  void get_health_checks(health_check_map_t *checks);

  void log(const std::string &handle,
           int level, const std::string &record);

  static void list_modules(std::set<std::string> *modules);
};

#endif

