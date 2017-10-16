// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#pragma once

// Python.h comes first because otherwise it clobbers ceph's assert
#include "Python.h"

#include <string>
#include <map>
#include <memory>

#include "common/LogClient.h"

#include "ActivePyModules.h"
#include "StandbyPyModules.h"

class PyModule
{
private:
  const std::string module_name;
  std::string get_site_packages();

public:
  SafeThreadState pMyThreadState;
  PyObject *pClass = nullptr;
  PyObject *pStandbyClass = nullptr;

  PyModule(const std::string &module_name_)
    : module_name(module_name_)
  {
  }

  ~PyModule();

  int load(PyThreadState *pMainThreadState);

  std::string get_name() const {
    return module_name;
  }
};

/**
 * This class is responsible for setting up the python runtime environment
 * and importing the python modules.
 *
 * It is *not* responsible for constructing instances of their BaseMgrModule
 * subclasses.
 */
class PyModuleRegistry
{
private:
  mutable Mutex lock{"PyModuleRegistry::lock"};

  LogChannelRef clog;

  std::map<std::string, std::unique_ptr<PyModule>> modules;

  std::unique_ptr<ActivePyModules> active_modules;
  std::unique_ptr<StandbyPyModules> standby_modules;

  PyThreadState *pMainThreadState;

  // We have our own copy of MgrMap, because we are constructed
  // before ClusterState exists.
  MgrMap mgr_map;

public:
  static std::string config_prefix;

  static void list_modules(std::set<std::string> *modules);

  PyModuleRegistry(LogChannelRef clog_)
    : clog(clog_)
  {}

  bool handle_mgr_map(const MgrMap &mgr_map_)
  {
    Mutex::Locker l(lock);

    bool modules_changed = mgr_map_.modules != mgr_map.modules;
    mgr_map = mgr_map_;

    if (standby_modules != nullptr) {
      standby_modules->handle_mgr_map(mgr_map_);
    }

    return modules_changed;
  }

  bool is_initialized() const
  {
    return mgr_map.epoch > 0;
  }

  int init(const MgrMap &map);

  void active_start(
                PyModuleConfig &config_,
                DaemonStateIndex &ds, ClusterState &cs, MonClient &mc,
                LogChannelRef clog_, Objecter &objecter_, Client &client_,
                Finisher &f);
  void standby_start(
      MonClient *monc);

  bool is_standby_running() const
  {
    return standby_modules != nullptr;
  }

  void active_shutdown();
  void shutdown();

  template<typename Callback, typename...Args>
  void with_active_modules(Callback&& cb, Args&&...args) const
  {
    Mutex::Locker l(lock);
    assert(active_modules != nullptr);

    std::forward<Callback>(cb)(*active_modules, std::forward<Args>(args)...);
  }

  // FIXME: breaking interface so that I don't have to go rewrite all
  // the places that call into these (for now)
  // >>>
  void notify_all(const std::string &notify_type,
                  const std::string &notify_id)
  {
    if (active_modules) {
      active_modules->notify_all(notify_type, notify_id);
    }
  }

  void notify_all(const LogEntry &log_entry)
  {
    if (active_modules) {
      active_modules->notify_all(log_entry);
    }
  }

  std::vector<MonCommand> get_commands() const
  {
    assert(active_modules);
    return active_modules->get_commands();
  }
  std::vector<ModuleCommand> get_py_commands() const
  {
    assert(active_modules);
    return active_modules->get_py_commands();
  }
  void get_health_checks(health_check_map_t *checks)
  {
    assert(active_modules);
    active_modules->get_health_checks(checks);
  }
  std::map<std::string, std::string> get_services() const
  {
    assert(active_modules);
    return active_modules->get_services();
  }
  // <<< (end of ActivePyModules cheeky call-throughs)
};
