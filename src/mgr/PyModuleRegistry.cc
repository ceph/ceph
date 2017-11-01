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
#include "common/backport14.h"

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

namespace {
  PyObject* log_write(PyObject*, PyObject* args) {
    char* m = nullptr;
    if (PyArg_ParseTuple(args, "s", &m)) {
      auto len = strlen(m);
      if (len && m[len-1] == '\n') {
	m[len-1] = '\0';
      }
      dout(4) << m << dendl;
    }
    Py_RETURN_NONE;
  }

  PyObject* log_flush(PyObject*, PyObject*){
    Py_RETURN_NONE;
  }

  static PyMethodDef log_methods[] = {
    {"write", log_write, METH_VARARGS, "write stdout and stderr"},
    {"flush", log_flush, METH_VARARGS, "flush"},
    {nullptr, nullptr, 0, nullptr}
  };
}

#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "



std::string PyModule::get_site_packages()
{
  std::stringstream site_packages;

  // CPython doesn't auto-add site-packages dirs to sys.path for us,
  // but it does provide a module that we can ask for them.
  auto site_module = PyImport_ImportModule("site");
  assert(site_module);

  auto site_packages_fn = PyObject_GetAttrString(site_module, "getsitepackages");
  if (site_packages_fn != nullptr) {
    auto site_packages_list = PyObject_CallObject(site_packages_fn, nullptr);
    assert(site_packages_list);

    auto n = PyList_Size(site_packages_list);
    for (Py_ssize_t i = 0; i < n; ++i) {
      if (i != 0) {
        site_packages << ":";
      }
      site_packages << PyString_AsString(PyList_GetItem(site_packages_list, i));
    }

    Py_DECREF(site_packages_list);
    Py_DECREF(site_packages_fn);
  } else {
    // Fall back to generating our own site-packages paths by imitating
    // what the standard site.py does.  This is annoying but it lets us
    // run inside virtualenvs :-/

    auto site_packages_fn = PyObject_GetAttrString(site_module, "addsitepackages");
    assert(site_packages_fn);

    auto known_paths = PySet_New(nullptr);
    auto pArgs = PyTuple_Pack(1, known_paths);
    PyObject_CallObject(site_packages_fn, pArgs);
    Py_DECREF(pArgs);
    Py_DECREF(known_paths);
    Py_DECREF(site_packages_fn);

    auto sys_module = PyImport_ImportModule("sys");
    assert(sys_module);
    auto sys_path = PyObject_GetAttrString(sys_module, "path");
    assert(sys_path);

    dout(1) << "sys.path:" << dendl;
    auto n = PyList_Size(sys_path);
    bool first = true;
    for (Py_ssize_t i = 0; i < n; ++i) {
      dout(1) << "  " << PyString_AsString(PyList_GetItem(sys_path, i)) << dendl;
      if (first) {
        first = false;
      } else {
        site_packages << ":";
      }
      site_packages << PyString_AsString(PyList_GetItem(sys_path, i));
    }

    Py_DECREF(sys_path);
    Py_DECREF(sys_module);
  }

  Py_DECREF(site_module);

  return site_packages.str();
}

int PyModuleRegistry::init(const MgrMap &map)
{
  Mutex::Locker locker(lock);

  // Don't try and init me if you don't really have a map
  assert(map.epoch > 0);

  mgr_map = map;

  // namespace in config-key prefixed by "mgr/"
  config_prefix = std::string(g_conf->name.get_type_str()) + "/";

  // Set up global python interpreter
  Py_SetProgramName(const_cast<char*>(PYTHON_EXECUTABLE));
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

  // Load python code
  for (const auto& module_name : mgr_map.modules) {
    dout(1) << "Loading python module '" << module_name << "'" << dendl;
    auto mod = ceph::make_unique<PyModule>(module_name);
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

  return 0;
}


int PyModule::load(PyThreadState *pMainThreadState)
{
  assert(pMainThreadState != nullptr);

  // Configure sub-interpreter and construct C++-generated python classes
  {
    SafeThreadState sts(pMainThreadState);
    Gil gil(sts);

    auto thread_state = Py_NewInterpreter();
    if (thread_state == nullptr) {
      derr << "Failed to create python sub-interpreter for '" << module_name << '"' << dendl;
      return -EINVAL;
    } else {
      pMyThreadState.set(thread_state);
      // Some python modules do not cope with an unpopulated argv, so lets
      // fake one.  This step also picks up site-packages into sys.path.
      const char *argv[] = {"ceph-mgr"};
      PySys_SetArgv(1, (char**)argv);

      if (g_conf->daemonize) {
        auto py_logger = Py_InitModule("ceph_logger", log_methods);
#if PY_MAJOR_VERSION >= 3
        PySys_SetObject("stderr", py_logger);
        PySys_SetObject("stdout", py_logger);
#else
        PySys_SetObject(const_cast<char*>("stderr"), py_logger);
        PySys_SetObject(const_cast<char*>("stdout"), py_logger);
#endif
      }

      // Configure sys.path to include mgr_module_path
      std::string sys_path = std::string(Py_GetPath()) + ":" + get_site_packages()
                             + ":" + g_conf->get_val<std::string>("mgr_module_path");
      dout(10) << "Computed sys.path '" << sys_path << "'" << dendl;

      PySys_SetPath(const_cast<char*>(sys_path.c_str()));
    }

    PyMethodDef ModuleMethods[] = {
      {nullptr}
    };

    // Initialize module
    PyObject *ceph_module = Py_InitModule("ceph_module", ModuleMethods);
    assert(ceph_module != nullptr);

    auto load_class = [ceph_module](const char *name, PyTypeObject *type)
    {
      type->tp_new = PyType_GenericNew;
      if (PyType_Ready(type) < 0) {
          assert(0);
      }
      Py_INCREF(type);

      PyModule_AddObject(ceph_module, name, (PyObject *)type);
    };

    load_class("BaseMgrModule", &BaseMgrModuleType);
    load_class("BaseMgrStandbyModule", &BaseMgrStandbyModuleType);
    load_class("BasePyOSDMap", &BasePyOSDMapType);
    load_class("BasePyOSDMapIncremental", &BasePyOSDMapIncrementalType);
    load_class("BasePyCRUSH", &BasePyCRUSHType);
  }

  // Environment is all good, import the external module
  {
    Gil gil(pMyThreadState);

    // Load the module
    PyObject *pName = PyString_FromString(module_name.c_str());
    auto pModule = PyImport_Import(pName);
    Py_DECREF(pName);
    if (pModule == nullptr) {
      derr << "Module not found: '" << module_name << "'" << dendl;
      derr << handle_pyerror() << dendl;
      return -ENOENT;
    }

    // Find the class
    // TODO: let them call it what they want instead of just 'Module'
    pClass = PyObject_GetAttrString(pModule, (const char*)"Module");
    if (pClass == nullptr) {
      derr << "Class not found in module '" << module_name << "'" << dendl;
      derr << handle_pyerror() << dendl;
      return -EINVAL;
    }

    pStandbyClass = PyObject_GetAttrString(pModule,
                                           (const char*)"StandbyModule");
    if (pStandbyClass) {
      dout(4) << "Standby mode available in module '" << module_name
              << "'" << dendl;
    } else {
      dout(4) << "Standby mode not provided by module '" << module_name
              << "'" << dendl;
      PyErr_Clear();
    }

    Py_DECREF(pModule);
  }

  return 0;
} 

PyModule::~PyModule()
{
  if (pMyThreadState.ts != nullptr) {
    Gil gil(pMyThreadState, true);
    Py_XDECREF(pClass);
    Py_XDECREF(pStandbyClass);
  }
}

void PyModuleRegistry::standby_start(MonClient *monc)
{
  Mutex::Locker l(lock);
  assert(active_modules == nullptr);
  assert(standby_modules == nullptr);
  assert(is_initialized());

  dout(4) << "Starting modules in standby mode" << dendl;

  standby_modules.reset(new StandbyPyModules(monc, mgr_map));

  std::set<std::string> failed_modules;
  for (const auto &i : modules) {
    if (i.second->pStandbyClass) {
      dout(4) << "starting module " << i.second->get_name() << dendl;
      int r = standby_modules->start_one(i.first,
              i.second->pStandbyClass,
              i.second->pMyThreadState);
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
  assert(is_initialized());

  if (standby_modules != nullptr) {
    standby_modules->shutdown();
    standby_modules.reset();
  }

  active_modules.reset(new ActivePyModules(
              config_, ds, cs, mc, clog_, objecter_, client_, f));

  for (const auto &i : modules) {
    dout(4) << "Starting " << i.first << dendl;
    int r = active_modules->start_one(i.first,
            i.second->pClass,
            i.second->pMyThreadState);
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

static void _list_modules(
  const std::string path,
  std::set<std::string> *modules)
{
  DIR *dir = opendir(path.c_str());
  if (!dir) {
    return;
  }
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
	modules->insert(n);
      }
    }
  }
  closedir(dir);
}

void PyModuleRegistry::list_modules(std::set<std::string> *modules)
{
  _list_modules(g_conf->get_val<std::string>("mgr_module_path"), modules);
}

