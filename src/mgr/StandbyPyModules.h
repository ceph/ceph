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

#pragma once

#include "PythonCompat.h"

#include <string>
#include <map>

#include "common/Thread.h"
#include "common/Mutex.h"

#include "mgr/Gil.h"
#include "mon/MonClient.h"
#include "mon/MgrMap.h"
#include "mgr/PyModuleRunner.h"

typedef std::map<std::string, std::string> PyModuleConfig;

/**
 * State that is read by all modules running in standby mode
 */
class StandbyPyModuleState
{
  mutable Mutex lock{"StandbyPyModuleState::lock"};

  MgrMap mgr_map;
  PyModuleConfig config_cache;

  mutable Cond config_loaded;

public:

  bool is_config_loaded = false;

  void set_mgr_map(const MgrMap &mgr_map_)
  {
    Mutex::Locker l(lock);

    mgr_map = mgr_map_;
  }

  void loaded_config(const PyModuleConfig &config_)
  {
    Mutex::Locker l(lock);

    config_cache = config_;
    is_config_loaded = true;
    config_loaded.Signal();
  }

  template<typename Callback, typename...Args>
  void with_mgr_map(Callback&& cb, Args&&...args) const
  {
    Mutex::Locker l(lock);
    std::forward<Callback>(cb)(mgr_map, std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  auto with_config(Callback&& cb, Args&&... args) const ->
    decltype(cb(config_cache, std::forward<Args>(args)...)) {
    Mutex::Locker l(lock);

    if (!is_config_loaded) {
      config_loaded.Wait(lock);
    }

    return std::forward<Callback>(cb)(config_cache, std::forward<Args>(args)...);
  }
};


class StandbyPyModule : public PyModuleRunner
{
  StandbyPyModuleState &state;

  public:

  StandbyPyModule(
      StandbyPyModuleState &state_,
      PyModuleRef py_module_,
      LogChannelRef clog_)
    :
      PyModuleRunner(py_module_, clog_),
      state(state_)
  {
  }

  bool get_config(const std::string &key, std::string *value) const;
  std::string get_active_uri() const;

  int load();
};

class StandbyPyModules
{
private:
  mutable Mutex lock{"StandbyPyModules::lock"};
  std::map<std::string, std::unique_ptr<StandbyPyModule>> modules;

  MonClient *monc;

  StandbyPyModuleState state;

  void load_config();
  class LoadConfigThread : public Thread
  {
    protected:
      MonClient *monc;
      StandbyPyModuleState *state;
    public:
    LoadConfigThread(MonClient *monc_, StandbyPyModuleState *state_)
      : monc(monc_), state(state_)
    {}
    void *entry() override;
  };

  LoadConfigThread load_config_thread;

  LogChannelRef clog;

public:

  StandbyPyModules(
      MonClient *monc_,
      const MgrMap &mgr_map_,
      LogChannelRef clog_);

  int start_one(PyModuleRef py_module);

  void shutdown();

  void handle_mgr_map(const MgrMap &mgr_map)
  {
    state.set_mgr_map(mgr_map);
  }

};
