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

#include "common/debug.h"

#include "mgr/MgrContext.h"
#include "mgr/Gil.h"


#include <boost/python.hpp>
#include "include/assert.h"  // boost clobbers this

// For ::config_prefix
#include "PyModuleRegistry.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "


StandbyPyModules::StandbyPyModules(MonClient *monc_, const MgrMap &mgr_map_,
    LogChannelRef clog_)
    : monc(monc_), load_config_thread(monc, &state), clog(clog_)
{
  state.set_mgr_map(mgr_map_);
}

// FIXME: completely identical to ActivePyModules
void StandbyPyModules::shutdown()
{
  Mutex::Locker locker(lock);

  if (!state.is_config_loaded && load_config_thread.is_started()) {
    // FIXME: handle cases where initial load races with shutdown
    // this is actually not super rare because 
    assert(0);
    //load_config_thread.kill(SIGKILL);
  }

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

int StandbyPyModules::start_one(PyModuleRef py_module)
{
  Mutex::Locker l(lock);
  const std::string &module_name = py_module->get_name();

  assert(modules.count(module_name) == 0);

  modules[module_name].reset(new StandbyPyModule(
      state,
      py_module, clog));

  if (modules.size() == 1) {
    load_config_thread.create("LoadConfig");
  }

  int r = modules[module_name]->load();
  if (r != 0) {
    modules.erase(module_name);
    return r;
  } else {
    dout(4) << "Starting thread for " << module_name << dendl;
    // Giving Thread the module's module_name member as its
    // char* thread name: thread must not outlive module class lifetime.
    modules[module_name]->thread.create(
        modules[module_name]->get_name().c_str());
    return 0;
  }
}

int StandbyPyModule::load()
{
  Gil gil(py_module->pMyThreadState, true);

  // We tell the module how we name it, so that it can be consistent
  // with us in logging etc.
  auto pThisPtr = PyCapsule_New(this, nullptr, nullptr);
  assert(pThisPtr != nullptr);
  auto pModuleName = PyString_FromString(get_name().c_str());
  assert(pModuleName != nullptr);
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

void *StandbyPyModules::LoadConfigThread::entry()
{
  dout(10) << "listing keys" << dendl;
  JSONCommand cmd;
  cmd.run(monc, "{\"prefix\": \"config-key ls\"}");
  cmd.wait();
  assert(cmd.r == 0);

  std::map<std::string, std::string> loaded;
  
  for (auto &key_str : cmd.json_result.get_array()) {
    std::string const key = key_str.get_str();
    dout(20) << "saw key '" << key << "'" << dendl;

    const std::string config_prefix = PyModuleRegistry::config_prefix;

    if (key.substr(0, config_prefix.size()) == config_prefix) {
      dout(20) << "fetching '" << key << "'" << dendl;
      Command get_cmd;
      std::ostringstream cmd_json;
      cmd_json << "{\"prefix\": \"config-key get\", \"key\": \"" << key << "\"}";
      get_cmd.run(monc, cmd_json.str());
      get_cmd.wait();
      assert(get_cmd.r == 0);
      loaded[key] = get_cmd.outbl.to_str();
    }
  }
  state->loaded_config(loaded);

  return nullptr;
}

bool StandbyPyModule::get_config(const std::string &key,
                                 std::string *value) const
{
  PyThreadState *tstate = PyEval_SaveThread();
  PyEval_RestoreThread(tstate);

  const std::string global_key = PyModuleRegistry::config_prefix
    + get_name() + "/" + key;

  dout(4) << __func__ << "key: " << global_key << dendl;

  return state.with_config([global_key, value](const PyModuleConfig &config){
    if (config.count(global_key)) {
      *value = config.at(global_key);
      return true;
    } else {
      return false;
    }
  });
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

