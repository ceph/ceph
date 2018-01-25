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
#include "common/Mutex.h"

#include "osdc/Objecter.h"
#include "client/Client.h"
#include "common/LogClient.h"
#include "mon/MgrMap.h"
#include "mon/MonCommand.h"

#include "DaemonState.h"
#include "ClusterState.h"

class health_check_map_t;

typedef std::map<std::string, std::string> PyModuleConfig;

class ActivePyModules
{

  std::map<std::string, std::unique_ptr<ActivePyModule>> modules;
  PyModuleConfig config_cache;
  DaemonStateIndex &daemon_state;
  ClusterState &cluster_state;
  MonClient &monc;
  LogChannelRef clog;
  Objecter &objecter;
  Client   &client;
  Finisher &finisher;


  mutable Mutex lock{"ActivePyModules::lock"};

public:
  ActivePyModules(PyModuleConfig const &config_,
            DaemonStateIndex &ds, ClusterState &cs, MonClient &mc,
            LogChannelRef clog_, Objecter &objecter_, Client &client_,
            Finisher &f);

  ~ActivePyModules();

  // FIXME: wrap for send_command?
  MonClient &get_monc() {return monc;}
  Objecter  &get_objecter() {return objecter;}
  Client    &get_client() {return client;}

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
  PyObject *get_perf_schema_python(
     const std::string svc_type,
     const std::string &svc_id);
  PyObject *get_context();
  PyObject *get_osdmap();

  bool get_config(const std::string &module_name,
      const std::string &key, std::string *val) const;
  PyObject *get_config_prefix(const std::string &module_name,
			      const std::string &prefix) const;
  void set_config(const std::string &module_name,
      const std::string &key, const boost::optional<std::string> &val);

  void set_health_checks(const std::string& module_name,
			 health_check_map_t&& checks);
  void get_health_checks(health_check_map_t *checks);

  void set_uri(const std::string& module_name, const std::string &uri);

  int handle_command(
    const std::string &module_name,
    const cmdmap_t &cmdmap,
    std::stringstream *ds,
    std::stringstream *ss);

  std::map<std::string, std::string> get_services() const;

  // Public so that MonCommandCompletion can use it
  // FIXME: for send_command completion notifications,
  // send it to only the module that sent the command, not everyone
  void notify_all(const std::string &notify_type,
                  const std::string &notify_id);
  void notify_all(const LogEntry &log_entry);

  int init();
  void shutdown();

  int start_one(PyModuleRef py_module);

  void dump_server(const std::string &hostname,
                   const DaemonStateCollection &dmc,
                   Formatter *f);
};

