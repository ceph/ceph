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

//typedef std::map<std::string, std::string> PyModuleConfig;

/**
 * State that is read by all modules running in standby mode
 */
class StandbyPyModuleState
{
  mutable Mutex lock{"StandbyPyModuleState::lock"};

  MgrMap mgr_map;
  //PyModuleConfig config_cache;

  

public:

  PyModuleConfig &module_config;
  
  StandbyPyModuleState(PyModuleConfig &module_config_)
    : module_config(module_config_)
  {}

  void set_mgr_map(const MgrMap &mgr_map_)
  {
    Mutex::Locker l(lock);

    mgr_map = mgr_map_;
  }

  template<typename Callback, typename...Args>
  void with_mgr_map(Callback&& cb, Args&&...args) const
  {
    Mutex::Locker l(lock);
    std::forward<Callback>(cb)(mgr_map, std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  auto with_config(Callback&& cb, Args&&... args) const ->
    decltype(cb(module_config, std::forward<Args>(args)...)) {
    Mutex::Locker l(lock);

    return std::forward<Callback>(cb)(module_config, std::forward<Args>(args)...);
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

  LogChannelRef clog;

public:

  StandbyPyModules(
      MonClient *monc_,
      const MgrMap &mgr_map_,
      PyModuleConfig &module_config,
      LogChannelRef clog_);

  int start_one(PyModuleRef py_module);

  void shutdown();

  void handle_mgr_map(const MgrMap &mgr_map)
  {
    state.set_mgr_map(mgr_map);
  }

};
