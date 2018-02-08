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

#include "BaseMgrModule.h"
#include "BaseMgrStandbyModule.h"
#include "PyOSDMap.h"

#include "PyModule.h"

#include "common/debug.h"
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

#undef dout_prefix
#define dout_prefix *_dout << "mgr[py] "

// Courtesy of http://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
#include <boost/python.hpp>
#include "include/assert.h"  // boost clobbers this
// decode a Python exception into a string
std::string handle_pyerror()
{
    using namespace boost::python;
    using namespace boost;

    PyObject *exc, *val, *tb;
    object formatted_list, formatted;
    PyErr_Fetch(&exc, &val, &tb);
    handle<> hexc(exc), hval(allow_null(val)), htb(allow_null(tb));
    object traceback(import("traceback"));
    if (!tb) {
        object format_exception_only(traceback.attr("format_exception_only"));
        formatted_list = format_exception_only(hexc, hval);
    } else {
        object format_exception(traceback.attr("format_exception"));
        formatted_list = format_exception(hexc,hval, htb);
    }
    formatted = str("").join(formatted_list);
    return extract<std::string>(formatted);
}


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

#if PY_MAJOR_VERSION >= 3
  static PyModuleDef ceph_logger_module = {
    PyModuleDef_HEAD_INIT,
    "ceph_logger",
    nullptr,
    -1,
    log_methods,
  };
#endif
}


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

#if PY_MAJOR_VERSION >= 3
PyObject* PyModule::init_ceph_logger()
{
  auto py_logger = PyModule_Create(&ceph_logger_module);
  PySys_SetObject("stderr", py_logger);
  PySys_SetObject("stdout", py_logger);
  return py_logger;
}
#else
void PyModule::init_ceph_logger()
{
  auto py_logger = Py_InitModule("ceph_logger", log_methods);
  PySys_SetObject(const_cast<char*>("stderr"), py_logger);
  PySys_SetObject(const_cast<char*>("stdout"), py_logger);
}
#endif

#if PY_MAJOR_VERSION >= 3
PyObject* PyModule::init_ceph_module()
#else
void PyModule::init_ceph_module()
#endif
{
  static PyMethodDef module_methods[] = {
    {nullptr, nullptr, 0, nullptr}
  };
#if PY_MAJOR_VERSION >= 3
  static PyModuleDef ceph_module_def = {
    PyModuleDef_HEAD_INIT,
    "ceph_module",
    nullptr,
    -1,
    module_methods,
    nullptr,
    nullptr,
    nullptr,
    nullptr
  };
  PyObject *ceph_module = PyModule_Create(&ceph_module_def);
#else
  PyObject *ceph_module = Py_InitModule("ceph_module", module_methods);
#endif
  assert(ceph_module != nullptr);
  std::map<const char*, PyTypeObject*> classes{
    {{"BaseMgrModule", &BaseMgrModuleType},
     {"BaseMgrStandbyModule", &BaseMgrStandbyModuleType},
     {"BasePyOSDMap", &BasePyOSDMapType},
     {"BasePyOSDMapIncremental", &BasePyOSDMapIncrementalType},
     {"BasePyCRUSH", &BasePyCRUSHType}}
  };
  for (auto [name, type] : classes) {
    type->tp_new = PyType_GenericNew;
    if (PyType_Ready(type) < 0) {
      assert(0);
    }
    Py_INCREF(type);

    PyModule_AddObject(ceph_module, name, (PyObject *)type);
  }
#if PY_MAJOR_VERSION >= 3
  return ceph_module;
#endif
}

int PyModule::load(PyThreadState *pMainThreadState)
{
  assert(pMainThreadState != nullptr);

  // Configure sub-interpreter
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
#if PY_MAJOR_VERSION >= 3
      const wchar_t *argv[] = {L"ceph-mgr"};
      PySys_SetArgv(1, (wchar_t**)argv);
#else
      const char *argv[] = {"ceph-mgr"};
      PySys_SetArgv(1, (char**)argv);
#endif
      // Configure sys.path to include mgr_module_path
      string paths = (":" + get_site_packages() +
		      ":" + g_conf->get_val<std::string>("mgr_module_path"));
#if PY_MAJOR_VERSION >= 3
      wstring sys_path(Py_GetPath() + wstring(begin(paths), end(paths)));
      PySys_SetPath(const_cast<wchar_t*>(sys_path.c_str()));
      dout(10) << "Computed sys.path '"
	       << string(begin(sys_path), end(sys_path)) << "'" << dendl;
#else
      string sys_path(Py_GetPath() + paths);
      PySys_SetPath(const_cast<char*>(sys_path.c_str()));
      dout(10) << "Computed sys.path '" << sys_path << "'" << dendl;
#endif
    }
  }
  // Environment is all good, import the external module
  {
    Gil gil(pMyThreadState);

    int r;
    r = load_subclass_of("MgrModule", &pClass);
    if (r) {
      derr << "Class not found in module '" << module_name << "'" << dendl;
      return r;
    }

    r = load_commands();
    if (r != 0) {
      std::ostringstream oss;
      oss << "Missing COMMAND attribute in module '" << module_name << "'";
      error_string = oss.str();
      derr << oss.str() << dendl;
      return r;
    }

    // We've imported the module and found a MgrModule subclass, at this
    // point the module is considered loaded.  It might still not be
    // runnable though, can_run populated later...
    loaded = true;

    r = load_subclass_of("MgrStandbyModule", &pStandbyClass);
    if (!r) {
      dout(4) << "Standby mode available in module '" << module_name
              << "'" << dendl;
    } else {
      dout(4) << "Standby mode not provided by module '" << module_name
              << "'" << dendl;
    }

    // Populate can_run by interrogating the module's callback that
    // may check for dependencies etc
    PyObject *pCanRunTuple = PyObject_CallMethod(pClass,
      const_cast<char*>("can_run"), const_cast<char*>("()"));
    if (pCanRunTuple != nullptr) {
      if (PyTuple_Check(pCanRunTuple) && PyTuple_Size(pCanRunTuple) == 2) {
        PyObject *pCanRun = PyTuple_GetItem(pCanRunTuple, 0);
        PyObject *can_run_str = PyTuple_GetItem(pCanRunTuple, 1);
        if (!PyBool_Check(pCanRun) || !PyString_Check(can_run_str)) {
          derr << "Module " << get_name()
               << " returned wrong type in can_run" << dendl;
          can_run = false;
        } else {
          can_run = (pCanRun == Py_True);
          if (!can_run) {
            error_string = PyString_AsString(can_run_str);
            dout(4) << "Module " << get_name()
                    << " reported that it cannot run: "
                    << error_string << dendl;
          }
        }
      } else {
        derr << "Module " << get_name()
             << " returned wrong type in can_run" << dendl;
        can_run = false;
      }

      Py_DECREF(pCanRunTuple);
    } else {
      derr << "Exception calling can_run on " << get_name() << dendl;
      derr << handle_pyerror() << dendl;
      can_run = false;
    }
  }
  return 0;
}

int PyModule::load_commands()
{
  // Don't need a Gil here -- this is called from load(),
  // which already has one.
  PyObject *command_list = PyObject_GetAttrString(pClass, "COMMANDS");
  if (command_list == nullptr) {
    // Even modules that don't define command should still have the COMMANDS
    // from the MgrModule definition.  Something is wrong!
    derr << "Module " << get_name() << " has missing COMMANDS member" << dendl;
    return -EINVAL;
  }
  if (!PyObject_TypeCheck(command_list, &PyList_Type)) {
    // Relatively easy mistake for human to make, e.g. defining COMMANDS
    // as a {} instead of a []
    derr << "Module " << get_name() << " has COMMANDS member of wrong type ("
            "should be a list)" << dendl;
    return -EINVAL;
  }
  const size_t list_size = PyList_Size(command_list);
  for (size_t i = 0; i < list_size; ++i) {
    PyObject *command = PyList_GetItem(command_list, i);
    assert(command != nullptr);

    ModuleCommand item;

    PyObject *pCmd = PyDict_GetItemString(command, "cmd");
    assert(pCmd != nullptr);
    item.cmdstring = PyString_AsString(pCmd);

    dout(20) << "loaded command " << item.cmdstring << dendl;

    PyObject *pDesc = PyDict_GetItemString(command, "desc");
    assert(pDesc != nullptr);
    item.helpstring = PyString_AsString(pDesc);

    PyObject *pPerm = PyDict_GetItemString(command, "perm");
    assert(pPerm != nullptr);
    item.perm = PyString_AsString(pPerm);

    item.module_name = module_name;

    commands.push_back(item);
  }
  Py_DECREF(command_list);

  dout(10) << "loaded " << commands.size() << " commands" << dendl;

  return 0;
}

int PyModule::load_subclass_of(const char* base_class, PyObject** py_class)
{
  // load the base class
  PyObject *mgr_module = PyImport_ImportModule("mgr_module");
  if (!mgr_module) {
    derr << "Module not found: 'mgr_module'" << dendl;
    derr << handle_pyerror() << dendl;
    return -EINVAL;
  }
  auto mgr_module_type = PyObject_GetAttrString(mgr_module, base_class);
  Py_DECREF(mgr_module);
  if (!mgr_module_type) {
    derr << "Unable to import MgrModule from mgr_module" << dendl;
    derr << handle_pyerror() << dendl;
    return -EINVAL;
  }

  // find the sub class
  PyObject *plugin_module = PyImport_ImportModule(module_name.c_str());
  if (!plugin_module) {
    derr << "Module not found: '" << module_name << "'" << dendl;
    derr << handle_pyerror() << dendl;
    return -ENOENT;
  }
  auto locals = PyModule_GetDict(plugin_module);
  Py_DECREF(plugin_module);
  PyObject *key, *value;
  Py_ssize_t pos = 0;
  *py_class = nullptr;
  while (PyDict_Next(locals, &pos, &key, &value)) {
    if (!PyType_Check(value)) {
      continue;
    }
    if (!PyObject_IsSubclass(value, mgr_module_type)) {
      continue;
    }
    if (PyObject_RichCompareBool(value, mgr_module_type, Py_EQ)) {
      continue;
    }
    auto class_name = PyString_AsString(key);
    if (*py_class) {
      derr << __func__ << ": ignoring '"
	   << module_name << "." << class_name << "'"
	   << ": only one '" << base_class
	   << "' class is loaded from each plugin" << dendl;
      continue;
    }
    *py_class = value;
    dout(4) << __func__ << ": found class: '"
	    << module_name << "." << class_name << "'" << dendl;
  }
  Py_DECREF(mgr_module_type);

  return *py_class ? 0 : -EINVAL;
}

PyModule::~PyModule()
{
  if (pMyThreadState.ts != nullptr) {
    Gil gil(pMyThreadState, true);
    Py_XDECREF(pClass);
    Py_XDECREF(pStandbyClass);
  }
}

