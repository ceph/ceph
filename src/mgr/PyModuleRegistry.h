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

// First because it includes Python.h
#include "PyModule.h"

#include <string>
#include <map>
#include <set>
#include <memory>

#include "common/LogClient.h"

#include "ActivePyModules.h"
#include "StandbyPyModules.h"

class MgrSession;

/**
 * This class is responsible for setting up the python runtime environment
 * and importing the python modules.
 *
 * It is *not* responsible for constructing instances of their BaseMgrModule
 * subclasses: that is the job of ActiveMgrModule, which consumes the class
 * references that we load here.
 */
class PyModuleRegistry
{
private:
  mutable ceph::mutex lock = ceph::make_mutex("PyModuleRegistry::lock");
  LogChannelRef clog;

  std::map<std::string, PyModuleRef> modules;
  std::multimap<std::string, entity_addrvec_t> clients;

  std::unique_ptr<ActivePyModules> active_modules;
  std::unique_ptr<StandbyPyModules> standby_modules;

  PyThreadState *pMainThreadState;

  // We have our own copy of MgrMap, because we are constructed
  // before ClusterState exists.
  MgrMap mgr_map;

  static std::string get_site_packages();
  /**
   * Discover python modules from local disk
   */
  std::vector<std::string> probe_modules(const std::string &path) const;

  PyModuleConfig module_config;

public:
  void handle_config(const std::string &k, const std::string &v);
  void handle_config_notify();

  void update_kv_data(
    const std::string prefix,
    bool incremental,
    const map<std::string, std::optional<bufferlist>, std::less<>>& data) {
    ceph_assert(active_modules);
    active_modules->update_kv_data(prefix, incremental, data);
  }

  /**
   * Get references to all modules (whether they have loaded and/or
   * errored) or not.
   */
  auto get_modules() const
  {
    std::vector<PyModuleRef> modules_out;
    std::lock_guard l(lock);
    for (const auto &i : modules) {
      modules_out.push_back(i.second);
    }

    return modules_out;
  }

  explicit PyModuleRegistry(LogChannelRef clog_)
    : clog(clog_)
  {}

  /**
   * @return true if the mgrmap has changed such that the service needs restart
   */
  bool handle_mgr_map(const MgrMap &mgr_map_);

  bool have_standby_modules() const {
    return !!standby_modules;
  }

  void init();

  void upgrade_config(
      MonClient *monc,
      const std::map<std::string, std::string> &old_config);

  void active_start(
                DaemonStateIndex &ds, ClusterState &cs,
                const std::map<std::string, std::string> &kv_store,
		bool mon_provides_kv_sub,
                MonClient &mc, LogChannelRef clog_, LogChannelRef audit_clog_,
                Objecter &objecter_, Client &client_, Finisher &f,
                DaemonServer &server);
  void standby_start(MonClient &mc, Finisher &f);

  bool is_standby_running() const
  {
    return standby_modules != nullptr;
  }

  std::vector<MonCommand> get_commands() const;
  std::vector<ModuleCommand> get_py_commands() const;

  /**
   * Get the specified module. The module does not have to be
   * loaded or runnable.
   *
   * Returns an empty reference if it does not exist.
   */
  PyModuleRef get_module(const std::string &module_name)
  {
    std::lock_guard l(lock);
    auto module_iter = modules.find(module_name);
    if (module_iter == modules.end()) {
        return {};
    }
    return module_iter->second;
  }

  /**
   * Pass through command to the named module for execution.
   *
   * The command must exist in the COMMANDS reported by the module.  If it
   * doesn't then this will abort.
   *
   * If ActivePyModules has not been instantiated yet then this will
   * return EAGAIN.
   */
  int handle_command(
    const ModuleCommand& module_command,
    const MgrSession& session,
    const cmdmap_t &cmdmap,
    const bufferlist &inbuf,
    std::stringstream *ds,
    std::stringstream *ss);

  /**
   * Pass through health checks reported by modules, and report any
   * modules that have failed (i.e. unhandled exceptions in serve())
   */
  void get_health_checks(health_check_map_t *checks);

  void get_progress_events(map<std::string,ProgressEvent> *events) {
    if (active_modules) {
      active_modules->get_progress_events(events);
    }
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

  bool should_notify(const std::string& name,
		     const std::string& notify_type) {
    return modules.at(name)->should_notify(notify_type);
  }

  std::map<std::string, std::string> get_services() const
  {
    ceph_assert(active_modules);
    return active_modules->get_services();
  }

  void register_client(std::string_view name, entity_addrvec_t addrs, bool replace)
  {
    std::lock_guard l(lock);
    auto n = std::string(name);
    if (replace) {
      clients.erase(n);
    }
    clients.emplace(n, std::move(addrs));
  }
  void unregister_client(std::string_view name, const entity_addrvec_t& addrs)
  {
    std::lock_guard l(lock);
    auto itp = clients.equal_range(std::string(name));
    for (auto it = itp.first; it != itp.second; ++it) {
      if (it->second == addrs) {
        clients.erase(it);
        return;
      }
    }
  }

  auto get_clients() const
  {
    std::lock_guard l(lock);
    return clients;
  }

  bool is_module_active(const std::string &name) {
    ceph_assert(active_modules);
    return active_modules->module_exists(name);
  }

  auto& get_active_module_finisher(const std::string &name) {
    ceph_assert(active_modules);
    return active_modules->get_module_finisher(name);
  }

  // <<< (end of ActivePyModules cheeky call-throughs)
};
