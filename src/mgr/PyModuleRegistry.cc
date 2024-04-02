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

#include "PyModuleRegistry.h"

#include <filesystem>
#include <boost/scope_exit.hpp>

#include "include/stringify.h"
#include "common/errno.h"
#include "common/split.h"

#include "BaseMgrModule.h"
#include "PyOSDMap.h"
#include "BaseMgrStandbyModule.h"
#include "Gil.h"
#include "MgrContext.h"
#include "mgr/mgr_commands.h"

#include "ActivePyModules.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

#undef dout_prefix
#define dout_prefix *_dout << "mgr[py] "

namespace fs = std::filesystem;

std::set<std::string> obsolete_modules = {
  "orchestrator_cli",
};

void PyModuleRegistry::init()
{
  std::lock_guard locker(lock);

  // Set up global python interpreter
#define WCHAR(s) L ## #s
#if PY_VERSION_HEX >= 0x03080000
  PyConfig py_config;
  // do not enable isolated mode, otherwise we would not be able to have access
  // to the site packages. since we cannot import any module before initializing
  // the interpreter, we would not be able to use "site" module for retrieving
  // the path to site packager. we import "site" module for retrieving
  // sitepackages in Python < 3.8 though, this does not apply to the
  // initialization with PyConfig.
  PyConfig_InitPythonConfig(&py_config);
  BOOST_SCOPE_EXIT_ALL(&py_config) {
    PyConfig_Clear(&py_config);
  };
#if PY_VERSION_HEX >= 0x030b0000
  py_config.safe_path = 0;
#endif
  py_config.parse_argv = 0;
  py_config.configure_c_stdio = 0;
  py_config.install_signal_handlers = 0;
  py_config.pathconfig_warnings = 0;
  // site_import is 1 by default, but set it explicitly as we do import
  // site packages manually for Python < 3.8
  py_config.site_import = 1;

  PyStatus status;
  status = PyConfig_SetString(&py_config, &py_config.program_name, WCHAR(MGR_PYTHON_EXECUTABLE));
  ceph_assertf(!PyStatus_Exception(status), "PyConfig_SetString: %s:%s", status.func, status.err_msg);
  // Some python modules do not cope with an unpopulated argv, so lets
  // fake one.  This step also picks up site-packages into sys.path.
  const wchar_t* argv[] = {L"ceph-mgr"};
  status = PyConfig_SetArgv(&py_config, 1, (wchar_t *const *)argv);
  ceph_assertf(!PyStatus_Exception(status), "PyConfig_SetArgv: %s:%s", status.func, status.err_msg);
  // Add more modules
  if (g_conf().get_val<bool>("daemonize")) {
    PyImport_AppendInittab("ceph_logger", PyModule::init_ceph_logger);
  }
  PyImport_AppendInittab("ceph_module", PyModule::init_ceph_module);
  // Configure sys.path to include mgr_module_path
  auto pythonpath_env = g_conf().get_val<std::string>("mgr_module_path");
  if (const char* pythonpath = getenv("PYTHONPATH")) {
    pythonpath_env += ":";
    pythonpath_env += pythonpath;
  }
  status = PyConfig_SetBytesString(&py_config, &py_config.pythonpath_env, pythonpath_env.data());
  ceph_assertf(!PyStatus_Exception(status), "PyConfig_SetBytesString: %s:%s", status.func, status.err_msg);
  dout(10) << "set PYTHONPATH to " << std::quoted(pythonpath_env) << dendl;
  status = Py_InitializeFromConfig(&py_config);
  ceph_assertf(!PyStatus_Exception(status), "Py_InitializeFromConfig: %s:%s", status.func, status.err_msg);
#else
  Py_SetProgramName(const_cast<wchar_t*>(WCHAR(MGR_PYTHON_EXECUTABLE)));
  // Add more modules
  if (g_conf().get_val<bool>("daemonize")) {
    PyImport_AppendInittab("ceph_logger", PyModule::init_ceph_logger);
  }
  PyImport_AppendInittab("ceph_module", PyModule::init_ceph_module);
  Py_InitializeEx(0);
  const wchar_t *argv[] = {L"ceph-mgr"};
  PySys_SetArgv(1, (wchar_t**)argv);

  std::string paths = (g_conf().get_val<std::string>("mgr_module_path") + ':' +
		       get_site_packages() + ':');
  std::wstring sys_path(begin(paths), end(paths));
  sys_path += Py_GetPath();
  PySys_SetPath(const_cast<wchar_t*>(sys_path.c_str()));
  dout(10) << "Computed sys.path '"
	   << std::string(begin(sys_path), end(sys_path)) << "'" << dendl;
#endif // PY_VERSION_HEX >= 0x03080000
#undef WCHAR

#if PY_VERSION_HEX < 0x03090000
  // Let CPython know that we will be calling it back from other
  // threads in future.
  if (! PyEval_ThreadsInitialized()) {
    PyEval_InitThreads();
  }
#endif
  // Drop the GIL and remember the main thread state (current
  // thread state becomes NULL)
  pMainThreadState = PyEval_SaveThread();
  ceph_assert(pMainThreadState != nullptr);

  std::list<std::string> failed_modules;

  const std::string module_path = g_conf().get_val<std::string>("mgr_module_path");
  auto module_names = probe_modules(module_path);
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
    }

    // Record the module even if the load failed, so that we can
    // report its loading error
    modules[module_name] = std::move(mod);
  }
  if (module_names.empty()) {
    clog->error() << "No ceph-mgr modules found in " << module_path;
  }
  if (!failed_modules.empty()) {
    clog->error() << "Failed to load ceph-mgr modules: " << joinify(
        failed_modules.begin(), failed_modules.end(), std::string(", "));
  }
}

bool PyModuleRegistry::handle_mgr_map(const MgrMap &mgr_map_)
{
  std::lock_guard l(lock);

  if (mgr_map.epoch == 0) {
    mgr_map = mgr_map_;

    // First time we see MgrMap, set the enabled flags on modules
    // This should always happen before someone calls standby_start
    // or active_start
    for (const auto &[module_name, module] : modules) {
      const bool enabled = (mgr_map.modules.count(module_name) > 0);
      module->set_enabled(enabled);
      const bool always_on = (mgr_map.get_always_on_modules().count(module_name) > 0);
      module->set_always_on(always_on);
    }

    return false;
  } else {
    bool modules_changed = mgr_map_.modules != mgr_map.modules ||
      mgr_map_.always_on_modules != mgr_map.always_on_modules;
    mgr_map = mgr_map_;

    if (standby_modules != nullptr) {
      standby_modules->handle_mgr_map(mgr_map_);
    }

    return modules_changed;
  }
}



void PyModuleRegistry::standby_start(MonClient &mc, Finisher &f)
{
  std::lock_guard l(lock);
  ceph_assert(active_modules == nullptr);
  ceph_assert(standby_modules == nullptr);

  // Must have seen a MgrMap by this point, in order to know
  // which modules should be enabled
  ceph_assert(mgr_map.epoch > 0);

  dout(4) << "Starting modules in standby mode" << dendl;

  standby_modules.reset(new StandbyPyModules(
        mgr_map, module_config, clog, mc, f));

  std::set<std::string> failed_modules;
  for (const auto &i : modules) {
    if (!(i.second->is_enabled() && i.second->get_can_run())) {
      // report always_on modules with a standby mode that won't run
      if (i.second->is_always_on() && i.second->pStandbyClass) {
        failed_modules.insert(i.second->get_name());
      }
      continue;
    }

    if (i.second->pStandbyClass) {
      dout(4) << "starting module " << i.second->get_name() << dendl;
      standby_modules->start_one(i.second);
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
            DaemonStateIndex &ds, ClusterState &cs,
            const std::map<std::string, std::string> &kv_store,
	    bool mon_provides_kv_sub,
            MonClient &mc, LogChannelRef clog_, LogChannelRef audit_clog_,
            Objecter &objecter_, Client &client_, Finisher &f,
            DaemonServer &server)
{
  std::lock_guard locker(lock);

  dout(4) << "Starting modules in active mode" << dendl;

  ceph_assert(active_modules == nullptr);

  // Must have seen a MgrMap by this point, in order to know
  // which modules should be enabled
  ceph_assert(mgr_map.epoch > 0);

  if (standby_modules != nullptr) {
    standby_modules->shutdown();
    standby_modules.reset();
  }

  active_modules.reset(
    new ActivePyModules(
      module_config,
      kv_store, mon_provides_kv_sub,
      ds, cs, mc,
      clog_, audit_clog_, objecter_, client_, f, server,
      *this));

  for (const auto &i : modules) {
    // Anything we're skipping because of !can_run will be flagged
    // to the user separately via get_health_checks
    if (!(i.second->is_enabled() && i.second->is_loaded())) {
      continue;
    }

    dout(4) << "Starting " << i.first << dendl;
    active_modules->start_one(i.second);
  }
}

std::string PyModuleRegistry::get_site_packages()
{
  std::stringstream site_packages;

  // CPython doesn't auto-add site-packages dirs to sys.path for us,
  // but it does provide a module that we can ask for them.
  auto site_module = PyImport_ImportModule("site");
  ceph_assert(site_module);

  auto site_packages_fn = PyObject_GetAttrString(site_module, "getsitepackages");
  if (site_packages_fn != nullptr) {
    auto site_packages_list = PyObject_CallObject(site_packages_fn, nullptr);
    ceph_assert(site_packages_list);

    auto n = PyList_Size(site_packages_list);
    for (Py_ssize_t i = 0; i < n; ++i) {
      if (i != 0) {
        site_packages << ":";
      }
      site_packages << PyUnicode_AsUTF8(PyList_GetItem(site_packages_list, i));
    }

    Py_DECREF(site_packages_list);
    Py_DECREF(site_packages_fn);
  } else {
    // Fall back to generating our own site-packages paths by imitating
    // what the standard site.py does.  This is annoying but it lets us
    // run inside virtualenvs :-/

    auto site_packages_fn = PyObject_GetAttrString(site_module, "addsitepackages");
    ceph_assert(site_packages_fn);

    auto known_paths = PySet_New(nullptr);
    auto pArgs = PyTuple_Pack(1, known_paths);
    PyObject_CallObject(site_packages_fn, pArgs);
    Py_DECREF(pArgs);
    Py_DECREF(known_paths);
    Py_DECREF(site_packages_fn);

    auto sys_module = PyImport_ImportModule("sys");
    ceph_assert(sys_module);
    auto sys_path = PyObject_GetAttrString(sys_module, "path");
    ceph_assert(sys_path);

    dout(1) << "sys.path:" << dendl;
    auto n = PyList_Size(sys_path);
    bool first = true;
    for (Py_ssize_t i = 0; i < n; ++i) {
      dout(1) << "  " << PyUnicode_AsUTF8(PyList_GetItem(sys_path, i)) << dendl;
      if (first) {
        first = false;
      } else {
        site_packages << ":";
      }
      site_packages << PyUnicode_AsUTF8(PyList_GetItem(sys_path, i));
    }

    Py_DECREF(sys_path);
    Py_DECREF(sys_module);
  }

  Py_DECREF(site_module);

  return site_packages.str();
}

std::vector<std::string> PyModuleRegistry::probe_modules(const std::string &path) const
{
  const auto opt = g_conf().get_val<std::string>("mgr_disabled_modules");
  const auto disabled_modules = ceph::split(opt);

  std::vector<std::string> modules;
  for (const auto& entry: fs::directory_iterator(path)) {
    if (!fs::is_directory(entry)) {
      continue;
    }
    const std::string name = entry.path().filename();
    if (std::count(disabled_modules.begin(), disabled_modules.end(), name)) {
      dout(10) << "ignoring disabled module " << name << dendl;
      continue;
    }
    auto module_path = entry.path() / "module.py";
    if (fs::exists(module_path)) {
      modules.push_back(name);
    }
  }
  return modules;
}

int PyModuleRegistry::handle_command(
  const ModuleCommand& module_command,
  const MgrSession& session,
  const cmdmap_t &cmdmap,
  const bufferlist &inbuf,
  std::stringstream *ds,
  std::stringstream *ss)
{
  if (active_modules) {
    return active_modules->handle_command(module_command, session, cmdmap,
                                          inbuf, ds, ss);
  } else {
    // We do not expect to be called before active modules is up, but
    // it's straightfoward to handle this case so let's do it.
    return -EAGAIN;
  }
}

std::vector<ModuleCommand> PyModuleRegistry::get_py_commands() const
{
  std::lock_guard l(lock);

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
    uint64_t flags = MonCommand::FLAG_MGR;
    if (pyc.polling) {
      flags |= MonCommand::FLAG_POLL;
    }
    result.push_back({pyc.cmdstring, pyc.helpstring, "mgr",
                        pyc.perm, flags});
  }
  return result;
}

void PyModuleRegistry::get_health_checks(health_check_map_t *checks)
{
  std::lock_guard l(lock);

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
              || (module->is_failed() && module->get_can_run())) {
        // - Unloadable modules are only reported if they're enabled,
        //   to avoid spamming users about modules they don't have the
        //   dependencies installed for because they don't use it.
        // - Failed modules are only reported if they passed the can_run
        //   checks (to avoid outputting two health messages about a
        //   module that said can_run=false but we tried running it anyway)
        failed_modules[module->get_name()] = module->get_error_string();
      }
    }

    // report failed always_on modules as health errors
    for (const auto& name : mgr_map.get_always_on_modules()) {
      if (obsolete_modules.count(name)) {
	continue;
      }
      if (active_modules->is_pending(name)) {
	continue;
      }
      if (!active_modules->module_exists(name)) {
        if (failed_modules.find(name) == failed_modules.end() &&
            dependency_modules.find(name) == dependency_modules.end()) {
          failed_modules[name] = "Not found or unloadable";
        }
      }
    }

    if (!dependency_modules.empty()) {
      std::ostringstream ss;
      if (dependency_modules.size() == 1) {
        auto iter = dependency_modules.begin();
        ss << "Module '" << iter->first << "' has failed dependency: "
           << iter->second;
      } else if (dependency_modules.size() > 1) {
        ss << dependency_modules.size()
	   << " mgr modules have failed dependencies";
      }
      auto& d = checks->add("MGR_MODULE_DEPENDENCY", HEALTH_WARN, ss.str(),
			    dependency_modules.size());
      for (auto& i : dependency_modules) {
	std::ostringstream ss;
        ss << "Module '" << i.first << "' has failed dependency: " << i.second;
	d.detail.push_back(ss.str());
      }
    }

    if (!failed_modules.empty()) {
      std::ostringstream ss;
      if (failed_modules.size() == 1) {
        auto iter = failed_modules.begin();
        ss << "Module '" << iter->first << "' has failed: " << iter->second;
      } else if (failed_modules.size() > 1) {
        ss << failed_modules.size() << " mgr modules have failed";
      }
      auto& d = checks->add("MGR_MODULE_ERROR", HEALTH_ERR, ss.str(),
			    failed_modules.size());
      for (auto& i : failed_modules) {
	std::ostringstream ss;
        ss << "Module '" << i.first << "' has failed: " << i.second;
	d.detail.push_back(ss.str());
      }
    }
  }
}

void PyModuleRegistry::handle_config(const std::string &k, const std::string &v)
{
  std::lock_guard l(module_config.lock);

  if (!v.empty()) {
    // removing value to hide sensitive data going into mgr logs
    // leaving this for debugging purposes
    // dout(10) << "Loaded module_config entry " << k << ":" << v << dendl;
    dout(10) << "Loaded module_config entry " << k << ":" << dendl;
    module_config.config[k] = v;
  } else {
    module_config.config.erase(k);
  }
}

void PyModuleRegistry::handle_config_notify()
{
  std::lock_guard l(lock);
  if (active_modules) {
    active_modules->config_notify();
  }
}
