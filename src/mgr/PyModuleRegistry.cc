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


#include "include/stringify.h"
#include "common/errno.h"

#include "BaseMgrModule.h"
#include "PyOSDMap.h"
#include "BaseMgrStandbyModule.h"
#include "Gil.h"

#include "ActivePyModules.h"

#include "PyModuleRegistry.h"

// definition for non-const static member
std::string PyModuleRegistry::config_prefix;



#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

#undef dout_prefix
#define dout_prefix *_dout << "mgr[py] "



void PyModuleRegistry::init()
{
  Mutex::Locker locker(lock);

  // namespace in config-key prefixed by "mgr/"
  config_prefix = std::string(g_conf->name.get_type_str()) + "/";

  // Set up global python interpreter
#if PY_MAJOR_VERSION >= 3
#define WCHAR(s) L ## #s
  Py_SetProgramName(const_cast<wchar_t*>(WCHAR(PYTHON_EXECUTABLE)));
#undef WCHAR
#else
  Py_SetProgramName(const_cast<char*>(PYTHON_EXECUTABLE));
#endif
  // Add more modules
  if (g_conf->get_val<bool>("daemonize")) {
    PyImport_AppendInittab("ceph_logger", PyModule::init_ceph_logger);
  }
  PyImport_AppendInittab("ceph_module", PyModule::init_ceph_module);
  Py_InitializeEx(0);

  // Let CPython know that we will be calling it back from other
  // threads in future.
  if (! PyEval_ThreadsInitialized()) {
    PyEval_InitThreads();
  }

  // Drop the GIL and remember the main thread state (current
  // thread state becomes NULL)
  pMainThreadState = PyEval_SaveThread();
  assert(pMainThreadState != nullptr);

  std::list<std::string> failed_modules;

  std::set<std::string> module_names = probe_modules();
  // Load python code
  for (const auto& module_name : module_names) {
    dout(1) << "Loading python module '" << module_name << "'" << dendl;

    // Everything starts disabled, set enabled flag on module
    // when we see first MgrMap
    auto mod = std::make_shared<PyModule>(module_name);
    int r = mod->load(pMainThreadState);
    if (r != 0) {
      // Don't use handle_pyerror() here; we don't have the GIL
      // or the right thread state (this is deliberate).
      derr << "Error loading module '" << module_name << "': "
        << cpp_strerror(r) << dendl;
      failed_modules.push_back(module_name);
      // Don't drop out here, load the other modules
    } else {
      // Success!
      modules[module_name] = std::move(mod);
    }
  }

  if (!failed_modules.empty()) {
    clog->error() << "Failed to load ceph-mgr modules: " << joinify(
        failed_modules.begin(), failed_modules.end(), std::string(", "));
  }
}

bool PyModuleRegistry::handle_mgr_map(const MgrMap &mgr_map_)
{
  Mutex::Locker l(lock);

  if (mgr_map.epoch == 0) {
    mgr_map = mgr_map_;

    // First time we see MgrMap, set the enabled flags on modules
    // This should always happen before someone calls standby_start
    // or active_start
    for (const auto &[module_name, module] : modules) {
      const bool enabled = (mgr_map.modules.count(module_name) > 0);
      module->set_enabled(enabled);
    }

    return false;
  } else {
    bool modules_changed = mgr_map_.modules != mgr_map.modules;
    mgr_map = mgr_map_;

    if (standby_modules != nullptr) {
      standby_modules->handle_mgr_map(mgr_map_);
    }

    return modules_changed;
  }
}

void PyModuleRegistry::standby_start(MonClient *monc)
{
  Mutex::Locker l(lock);
  assert(active_modules == nullptr);
  assert(standby_modules == nullptr);

  // Must have seen a MgrMap by this point, in order to know
  // which modules should be enabled
  assert(mgr_map.epoch > 0);

  dout(4) << "Starting modules in standby mode" << dendl;

  standby_modules.reset(new StandbyPyModules(monc, mgr_map, clog));

  std::set<std::string> failed_modules;
  for (const auto &i : modules) {
    if (!i.second->is_enabled()) {
      continue;
    }

    if (i.second->pStandbyClass) {
      dout(4) << "starting module " << i.second->get_name() << dendl;
      int r = standby_modules->start_one(i.second);
      if (r != 0) {
        derr << "failed to start module '" << i.second->get_name()
             << "'" << dendl;;
        failed_modules.insert(i.second->get_name());
        // Continue trying to load any other modules
      }
    } else {
      dout(4) << "skipping module '" << i.second->get_name() << "' because "
                 "it does not implement a standby mode" << dendl;
    }
  }

  if (!failed_modules.empty()) {
    clog->error() << "Failed to execute ceph-mgr module(s) in standby mode: "
        << joinify(failed_modules.begin(), failed_modules.end(),
                   std::string(", "));
  }
}

void PyModuleRegistry::active_start(
            PyModuleConfig &config_,
            DaemonStateIndex &ds, ClusterState &cs, MonClient &mc,
            LogChannelRef clog_, Objecter &objecter_, Client &client_,
            Finisher &f)
{
  Mutex::Locker locker(lock);

  dout(4) << "Starting modules in active mode" << dendl;

  assert(active_modules == nullptr);

  // Must have seen a MgrMap by this point, in order to know
  // which modules should be enabled
  assert(mgr_map.epoch > 0);

  if (standby_modules != nullptr) {
    standby_modules->shutdown();
    standby_modules.reset();
  }

  active_modules.reset(new ActivePyModules(
              config_, ds, cs, mc, clog_, objecter_, client_, f));

  for (const auto &i : modules) {
    if (!i.second->is_enabled()) {
      continue;
    }

    dout(4) << "Starting " << i.first << dendl;
    int r = active_modules->start_one(i.second);
    if (r != 0) {
      derr << "Failed to run module in active mode ('" << i.first << "')"
           << dendl;
    }
  }
}

void PyModuleRegistry::active_shutdown()
{
  Mutex::Locker locker(lock);

  if (active_modules != nullptr) {
    active_modules->shutdown();
    active_modules.reset();
  }
}

void PyModuleRegistry::shutdown()
{
  Mutex::Locker locker(lock);

  if (standby_modules != nullptr) {
    standby_modules->shutdown();
    standby_modules.reset();
  }

  // Ideally, now, we'd be able to do this for all modules:
  //
  //    Py_EndInterpreter(pMyThreadState);
  //    PyThreadState_Swap(pMainThreadState);
  //
  // Unfortunately, if the module has any other *python* threads active
  // at this point, Py_EndInterpreter() will abort with:
  //
  //    Fatal Python error: Py_EndInterpreter: not the last thread
  //
  // This can happen when using CherryPy in a module, becuase CherryPy
  // runs an extra thread as a timeout monitor, which spends most of its
  // life inside a time.sleep(60).  Unless you are very, very lucky with
  // the timing calling this destructor, that thread will still be stuck
  // in a sleep, and Py_EndInterpreter() will abort.
  //
  // This could of course also happen with a poorly written module which
  // made no attempt to clean up any additional threads it created.
  //
  // The safest thing to do is just not call Py_EndInterpreter(), and
  // let Py_Finalize() kill everything after all modules are shut down.

  modules.clear();

  PyEval_RestoreThread(pMainThreadState);
  Py_Finalize();
}

std::set<std::string> PyModuleRegistry::probe_modules() const
{
  std::string path = g_conf->get_val<std::string>("mgr_module_path");

  DIR *dir = opendir(path.c_str());
  if (!dir) {
    return {};
  }

  std::set<std::string> modules_out;
  struct dirent *entry = NULL;
  while ((entry = readdir(dir)) != NULL) {
    string n(entry->d_name);
    string fn = path + "/" + n;
    struct stat st;
    int r = ::stat(fn.c_str(), &st);
    if (r == 0 && S_ISDIR(st.st_mode)) {
      string initfn = fn + "/module.py";
      r = ::stat(initfn.c_str(), &st);
      if (r == 0) {
	modules_out.insert(n);
      }
    }
  }
  closedir(dir);

  return modules_out;
}

int PyModuleRegistry::handle_command(
  std::string const &module_name,
  const cmdmap_t &cmdmap,
  std::stringstream *ds,
  std::stringstream *ss)
{
  if (active_modules) {
    return active_modules->handle_command(module_name, cmdmap, ds, ss);
  } else {
    // We do not expect to be called before active modules is up, but
    // it's straightfoward to handle this case so let's do it.
    return -EAGAIN;
  }
}

std::vector<ModuleCommand> PyModuleRegistry::get_py_commands() const
{
  Mutex::Locker l(lock);

  std::vector<ModuleCommand> result;
  for (const auto& i : modules) {
    i.second->get_commands(&result);
  }

  return result;
}

std::vector<MonCommand> PyModuleRegistry::get_commands() const
{
  std::vector<ModuleCommand> commands = get_py_commands();
  std::vector<MonCommand> result;
  for (auto &pyc: commands) {
    result.push_back({pyc.cmdstring, pyc.helpstring, "mgr",
                        pyc.perm, "cli", MonCommand::FLAG_MGR});
  }
  return result;
}

void PyModuleRegistry::get_health_checks(health_check_map_t *checks)
{
  Mutex::Locker l(lock);

  // Only the active mgr reports module issues
  if (active_modules) {
    active_modules->get_health_checks(checks);

    std::map<std::string, std::string> dependency_modules;
    std::map<std::string, std::string> failed_modules;

    /*
     * Break up broken modules into two categories:
     *  - can_run=false: the module is working fine but explicitly
     *    telling you that a dependency is missing.  Advise the user to
     *    read the message from the module and install what's missing.
     *  - failed=true or loaded=false: something unexpected is broken,
     *    either at runtime (from serve()) or at load time.  This indicates
     *    a bug and the user should be guided to inspect the mgr log
     *    to investigate and gather evidence.
     */

    for (const auto &i : modules) {
      auto module = i.second;
      if (module->is_enabled() && !module->get_can_run()) {
        dependency_modules[module->get_name()] = module->get_error_string();
      } else if ((module->is_enabled() && !module->is_loaded())
              || module->is_failed()) {
        failed_modules[module->get_name()] = module->get_error_string();
      }
    }

    if (!dependency_modules.empty()) {
      std::ostringstream ss;
      if (dependency_modules.size() == 1) {
        auto iter = dependency_modules.begin();
        ss << "Module '" << iter->first << "' has failed dependency: "
           << iter->second;
      } else if (dependency_modules.size() > 1) {
        ss << dependency_modules.size() << " modules have failed dependencies";
      }
      checks->add("MGR_MODULE_DEPENDENCY", HEALTH_WARN, ss.str());
    }

    if (!failed_modules.empty()) {
      std::ostringstream ss;
      if (failed_modules.size() == 1) {
        auto iter = failed_modules.begin();
        ss << "Module '" << iter->first << "' has failed: "
           << iter->second;
      } else if (failed_modules.size() > 1) {
        ss << failed_modules.size() << " modules have failed";
      }
      checks->add("MGR_MODULE_ERROR", HEALTH_ERR, ss.str());
    }
  }
}

