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

class Finisher;

/**
 * State that is read by all modules running in standby mode
 */
class StandbyPyModuleState
{
  mutable Mutex lock{"StandbyPyModuleState::lock"};

  MgrMap mgr_map;
  PyModuleConfig &module_config;
  MonClient &monc;

public:

  
  StandbyPyModuleState(PyModuleConfig &module_config_, MonClient &monc_)
    : module_config(module_config_), monc(monc_)
  {}

  void set_mgr_map(const MgrMap &mgr_map_)
  {
    std::lock_guard l(lock);

    mgr_map = mgr_map_;
  }

  // MonClient does all its own locking so we're happy to hand out
  // references.
  MonClient &get_monc() {return monc;};

  template<typename Callback, typename...Args>
  void with_mgr_map(Callback&& cb, Args&&...args) const
  {
    std::lock_guard l(lock);
    std::forward<Callback>(cb)(mgr_map, std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  auto with_config(Callback&& cb, Args&&... args) const ->
    decltype(cb(module_config, std::forward<Args>(args)...)) {
    std::lock_guard l(lock);

    return std::forward<Callback>(cb)(module_config, std::forward<Args>(args)...);
  }
};


class StandbyPyModule : public PyModuleRunner
{
  StandbyPyModuleState &state;

  public:

  StandbyPyModule(
      StandbyPyModuleState &state_,
      const PyModuleRef &py_module_,
      LogChannelRef clog_)
    :
      PyModuleRunner(py_module_, clog_),
      state(state_)
  {
  }

  bool get_config(const std::string &key, std::string *value) const;
  bool get_store(const std::string &key, std::string *value) const;
  std::string get_active_uri() const;

  int load();
};

class StandbyPyModules
{
private:
  mutable Mutex lock{"StandbyPyModules::lock"};
  std::map<std::string, std::unique_ptr<StandbyPyModule>> modules;

  StandbyPyModuleState state;

  LogChannelRef clog;

  Finisher &finisher;

public:

  StandbyPyModules(
      const MgrMap &mgr_map_,
      PyModuleConfig &module_config,
      LogChannelRef clog_,
      MonClient &monc,
      Finisher &f);

  void start_one(PyModuleRef py_module);

  void shutdown();

  void handle_mgr_map(const MgrMap &mgr_map)
  {
    state.set_mgr_map(mgr_map);
  }

};
