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

#include "StandbyPyModules.h"

#include "common/Finisher.h"
#include "common/debug.h"
#include "common/errno.h"

#include "mgr/MgrContext.h"
#include "mgr/Gil.h"


#include <boost/python.hpp>
#include "include/ceph_assert.h"  // boost clobbers this

// For ::config_prefix
#include "PyModuleRegistry.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "


StandbyPyModules::StandbyPyModules(
    const MgrMap &mgr_map_,
    PyModuleConfig &module_config,
    LogChannelRef clog_,
    MonClient &monc_,
    Finisher &f)
    : state(module_config, monc_),
      clog(clog_),
      finisher(f)
{
  state.set_mgr_map(mgr_map_);
}

// FIXME: completely identical to ActivePyModules
void StandbyPyModules::shutdown()
{
  std::lock_guard locker(lock);

  // Signal modules to drop out of serve() and/or tear down resources
  for (auto &i : modules) {
    auto module = i.second.get();
    const auto& name = i.first;
    dout(10) << "waiting for module " << name << " to shutdown" << dendl;
    lock.Unlock();
    module->shutdown();
    lock.Lock();
    dout(10) << "module " << name << " shutdown" << dendl;
  }

  // For modules implementing serve(), finish the threads where we
  // were running that.
  for (auto &i : modules) {
    lock.Unlock();
    dout(10) << "joining thread for module " << i.first << dendl;
    i.second->thread.join();
    dout(10) << "joined thread for module " << i.first << dendl;
    lock.Lock();
  }

  modules.clear();
}

void StandbyPyModules::start_one(PyModuleRef py_module)
{
  std::lock_guard l(lock);
  const auto name = py_module->get_name();

  ceph_assert(modules.count(name) == 0);

  modules[name].reset(new StandbyPyModule(state, py_module, clog));
  auto standby_module = modules.at(name).get();

  // Send all python calls down a Finisher to avoid blocking
  // C++ code, and avoid any potential lock cycles.
  finisher.queue(new FunctionContext([this, standby_module, name](int) {
    int r = standby_module->load();
    if (r != 0) {
      derr << "Failed to run module in standby mode ('" << name << "')"
           << dendl;
      std::lock_guard l(lock);
      modules.erase(name);
    } else {
      dout(4) << "Starting thread for " << name << dendl;
      standby_module->thread.create(standby_module->get_thread_name());
    }
  }));
}

int StandbyPyModule::load()
{
  Gil gil(py_module->pMyThreadState, true);

  // We tell the module how we name it, so that it can be consistent
  // with us in logging etc.
  auto pThisPtr = PyCapsule_New(this, nullptr, nullptr);
  ceph_assert(pThisPtr != nullptr);
  auto pModuleName = PyString_FromString(get_name().c_str());
  ceph_assert(pModuleName != nullptr);
  auto pArgs = PyTuple_Pack(2, pModuleName, pThisPtr);
  Py_DECREF(pThisPtr);
  Py_DECREF(pModuleName);

  pClassInstance = PyObject_CallObject(py_module->pStandbyClass, pArgs);
  Py_DECREF(pArgs);
  if (pClassInstance == nullptr) {
    derr << "Failed to construct class in '" << get_name() << "'" << dendl;
    derr << handle_pyerror() << dendl;
    return -EINVAL;
  } else {
    dout(1) << "Constructed class from module: " << get_name() << dendl;
    return 0;
  }
}

bool StandbyPyModule::get_config(const std::string &key,
                                 std::string *value) const
{
  const std::string global_key = PyModule::config_prefix
    + get_name() + "/" + key;

  dout(4) << __func__ << " key: " << global_key << dendl;
 
  return state.with_config([global_key, value](const PyModuleConfig &config){
    if (config.config.count(global_key)) {
      *value = config.config.at(global_key);
      return true;
    } else {
      return false;
    }
  });
}

bool StandbyPyModule::get_store(const std::string &key,
                                std::string *value) const
{

  const std::string global_key = PyModule::config_prefix
    + get_name() + "/" + key;

  dout(4) << __func__ << " key: " << global_key << dendl;

  // Active modules use a cache of store values (kept up to date
  // as writes pass through the active mgr), but standbys
  // fetch values synchronously to get an up to date value.
  // It's an acceptable cost because standby modules should not be
  // doing a lot.
  
  MonClient &monc = state.get_monc();

  std::ostringstream cmd_json;
  cmd_json << "{\"prefix\": \"config-key get\", \"key\": \""
           << global_key << "\"}";

  bufferlist outbl;
  std::string outs;
  C_SaferCond c;
  monc.start_mon_command(
      {cmd_json.str()},
      {},
      &outbl,
      &outs,
      &c);

  int r = c.wait();
  if (r == -ENOENT) {
    return false;
  } else if (r != 0) {
    // This is some internal error, not meaningful to python modules,
    // so let them just see no value.
    derr << __func__ << " error fetching store key '" << global_key << "': "
         << cpp_strerror(r) << " " << outs << dendl;
    return false;
  } else {
    *value = outbl.to_str();
    return true;
  }
}

std::string StandbyPyModule::get_active_uri() const
{
  std::string result;
  state.with_mgr_map([&result, this](const MgrMap &mgr_map){
    auto iter = mgr_map.services.find(get_name());
    if (iter != mgr_map.services.end()) {
      result = iter->second;
    }
  });

  return result;
}

