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
#include "MgrContext.h"
#include "PyUtil.h"

#include "PyModule.h"

#include "include/stringify.h"
#include "common/BackTrace.h"
#include "global/signal_handler.h"

#include "common/debug.h"
#include "common/errno.h"
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

#undef dout_prefix
#define dout_prefix *_dout << "mgr[py] "

// definition for non-const static member
std::string PyModule::mgr_store_prefix = "mgr/";

// Courtesy of http://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
#define BOOST_BIND_GLOBAL_PLACEHOLDERS
// Boost apparently can't be bothered to fix its own usage of its own
// deprecated features.
#include <boost/python/extract.hpp>
#include <boost/python/import.hpp>
#include <boost/python/object.hpp>
#undef BOOST_BIND_GLOBAL_PLACEHOLDERS
#include <boost/algorithm/string/predicate.hpp>
#include "include/ceph_assert.h"  // boost clobbers this


using std::string;
using std::wstring;

// decode a Python exception into a string
std::string handle_pyerror(
  bool crash_dump,
  std::string module,
  std::string caller)
{
  using namespace boost::python;
  using namespace boost;

  PyObject *exc, *val, *tb;
  object formatted_list, formatted;
  PyErr_Fetch(&exc, &val, &tb);
  PyErr_NormalizeException(&exc, &val, &tb);
  handle<> hexc(exc), hval(allow_null(val)), htb(allow_null(tb));

  object traceback(import("traceback"));
  if (!tb) {
    object format_exception_only(traceback.attr("format_exception_only"));
    try {
      formatted_list = format_exception_only(hexc, hval);
    } catch (error_already_set const &) {
      // error while processing exception object
      // returning only the exception string value
      PyObject *name_attr = PyObject_GetAttrString(exc, "__name__");
      std::stringstream ss;
      ss << PyUnicode_AsUTF8(name_attr) << ": " << PyUnicode_AsUTF8(val);
      Py_XDECREF(name_attr);
      ss << "\nError processing exception object: " << peek_pyerror();
      return ss.str();
    }
  } else {
    object format_exception(traceback.attr("format_exception"));
    try {
      formatted_list = format_exception(hexc, hval, htb);
    } catch (error_already_set const &) {
      // error while processing exception object
      // returning only the exception string value
      PyObject *name_attr = PyObject_GetAttrString(exc, "__name__");
      std::stringstream ss;
      ss << PyUnicode_AsUTF8(name_attr) << ": " << PyUnicode_AsUTF8(val);
      Py_XDECREF(name_attr);
      ss << "\nError processing exception object: " << peek_pyerror();
      return ss.str();
    }
  }
  formatted = str("").join(formatted_list);

  if (!module.empty()) {
    std::list<std::string> bt_strings;
    std::map<std::string, std::string> extra;
    
    extra["mgr_module"] = module;
    extra["mgr_module_caller"] = caller;
    PyObject *name_attr = PyObject_GetAttrString(exc, "__name__");
    extra["mgr_python_exception"] = stringify(PyUnicode_AsUTF8(name_attr));
    Py_XDECREF(name_attr);

    PyObject *l = get_managed_object(formatted_list, boost::python::tag);
    if (PyList_Check(l)) {
      // skip first line, which is: "Traceback (most recent call last):\n"
      for (unsigned i = 1; i < PyList_Size(l); ++i) {
	PyObject *val = PyList_GET_ITEM(l, i);
	std::string s = PyUnicode_AsUTF8(val);
	s.resize(s.size() - 1);  // strip off newline character
	bt_strings.push_back(s);
      }
    }
    PyBackTrace bt(bt_strings);
    
    char crash_path[PATH_MAX];
    generate_crash_dump(crash_path, bt, &extra);
  }
  
  return extract<std::string>(formatted);
}

/**
 * Get the single-line exception message, without clearing any
 * exception state.
 */
std::string peek_pyerror()
{
  PyObject *ptype, *pvalue, *ptraceback;
  PyErr_Fetch(&ptype, &pvalue, &ptraceback);
  ceph_assert(ptype);
  ceph_assert(pvalue);
  PyObject *pvalue_str = PyObject_Str(pvalue);
  std::string exc_msg = PyUnicode_AsUTF8(pvalue_str);
  Py_DECREF(pvalue_str);
  PyErr_Restore(ptype, pvalue, ptraceback);

  return exc_msg;
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

  static PyModuleDef ceph_logger_module = {
    PyModuleDef_HEAD_INIT,
    "ceph_logger",
    nullptr,
    -1,
    log_methods,
  };
}

PyModuleConfig::PyModuleConfig() = default;  

PyModuleConfig::PyModuleConfig(PyModuleConfig &mconfig)
  : config(mconfig.config)
{}

PyModuleConfig::~PyModuleConfig() = default;


std::pair<int, std::string> PyModuleConfig::set_config(
    MonClient *monc,
    const std::string &module_name,
    const std::string &key, const std::optional<std::string>& val)
{
  const std::string global_key = "mgr/" + module_name + "/" + key;
  Command set_cmd;
  {
    std::ostringstream cmd_json;
    JSONFormatter jf;
    jf.open_object_section("cmd");
    if (val) {
      jf.dump_string("prefix", "config set");
      jf.dump_string("value", *val);
    } else {
      jf.dump_string("prefix", "config rm");
    }
    jf.dump_string("who", "mgr");
    jf.dump_string("name", global_key);
    jf.close_section();
    jf.flush(cmd_json);
    set_cmd.run(monc, cmd_json.str());
  }
  set_cmd.wait();

  if (set_cmd.r == 0) {
    std::lock_guard l(lock);
    if (val) {
      config[global_key] = *val;
    } else {
      config.erase(global_key);
    }
    return {0, ""};
  } else {
    if (val) {
      dout(0) << "`config set mgr " << global_key << " " << val << "` failed: "
        << cpp_strerror(set_cmd.r) << dendl;
    } else {
      dout(0) << "`config rm mgr " << global_key << "` failed: "
        << cpp_strerror(set_cmd.r) << dendl;
    }
    dout(0) << "mon returned " << set_cmd.r << ": " << set_cmd.outs << dendl;
    return {set_cmd.r, set_cmd.outs};
  }
}

std::string PyModule::get_site_packages()
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

PyObject* PyModule::init_ceph_logger()
{
  auto py_logger = PyModule_Create(&ceph_logger_module);
  PySys_SetObject("stderr", py_logger);
  PySys_SetObject("stdout", py_logger);
  return py_logger;
}

PyObject* PyModule::init_ceph_module()
{
  static PyMethodDef module_methods[] = {
    {nullptr, nullptr, 0, nullptr}
  };
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
  ceph_assert(ceph_module != nullptr);
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
      ceph_abort();
    }
    Py_INCREF(type);

    PyModule_AddObject(ceph_module, name, (PyObject *)type);
  }
  return ceph_module;
}

int PyModule::load(PyThreadState *pMainThreadState)
{
  ceph_assert(pMainThreadState != nullptr);

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
      const wchar_t *argv[] = {L"ceph-mgr"};
      PySys_SetArgv(1, (wchar_t**)argv);
      // Configure sys.path to include mgr_module_path
      string paths = (g_conf().get_val<std::string>("mgr_module_path") + ':' +
                      get_site_packages() + ':');
      wstring sys_path(wstring(begin(paths), end(paths)) + Py_GetPath());
      PySys_SetPath(const_cast<wchar_t*>(sys_path.c_str()));
      dout(10) << "Computed sys.path '"
	       << string(begin(sys_path), end(sys_path)) << "'" << dendl;
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
      derr << "Missing or invalid COMMANDS attribute in module '"
          << module_name << "'" << dendl;
      error_string = "Missing or invalid COMMANDS attribute";
      return r;
    }

    register_options(pClass);
    r = load_options();
    if (r != 0) {
      derr << "Missing or invalid MODULE_OPTIONS attribute in module '"
          << module_name << "'" << dendl;
      error_string = "Missing or invalid MODULE_OPTIONS attribute";
      return r;
    }

    load_notify_types();

    // We've imported the module and found a MgrModule subclass, at this
    // point the module is considered loaded.  It might still not be
    // runnable though, can_run populated later...
    loaded = true;

    r = load_subclass_of("MgrStandbyModule", &pStandbyClass);
    if (!r) {
      dout(4) << "Standby mode available in module '" << module_name
              << "'" << dendl;
      register_options(pStandbyClass);
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
        if (!PyBool_Check(pCanRun) || !PyUnicode_Check(can_run_str)) {
          derr << "Module " << get_name()
               << " returned wrong type in can_run" << dendl;
          error_string = "wrong type returned from can_run";
          can_run = false;
        } else {
          can_run = (pCanRun == Py_True);
          if (!can_run) {
            error_string = PyUnicode_AsUTF8(can_run_str);
            dout(4) << "Module " << get_name()
                    << " reported that it cannot run: "
                    << error_string << dendl;
          }
        }
      } else {
        derr << "Module " << get_name()
             << " returned wrong type in can_run" << dendl;
        error_string = "wrong type returned from can_run";
        can_run = false;
      }

      Py_DECREF(pCanRunTuple);
    } else {
      derr << "Exception calling can_run on " << get_name() << dendl;
      derr << handle_pyerror(true, get_name(), "PyModule::load") << dendl;
      can_run = false;
    }
  }
  return 0;
}

int PyModule::walk_dict_list(
    const std::string &attr_name,
    std::function<int(PyObject*)> fn)
{
  PyObject *command_list = PyObject_GetAttrString(pClass, attr_name.c_str());
  if (command_list == nullptr) {
    derr << "Module " << get_name() << " has missing " << attr_name
         << " member" << dendl;
    return -EINVAL;
  }
  if (!PyObject_TypeCheck(command_list, &PyList_Type)) {
    // Relatively easy mistake for human to make, e.g. defining COMMANDS
    // as a {} instead of a []
    derr << "Module " << get_name() << " has " << attr_name
         << " member of wrong type (should be a list)" << dendl;
    return -EINVAL;
  }

  // Invoke fn on each item in the list
  int r = 0;
  const size_t list_size = PyList_Size(command_list);
  for (size_t i = 0; i < list_size; ++i) {
    PyObject *command = PyList_GetItem(command_list, i);
    ceph_assert(command != nullptr);

    if (!PyDict_Check(command)) {
      derr << "Module " << get_name() << " has non-dict entry "
           << "in " << attr_name << " list" << dendl;
      return -EINVAL;
    }

    r = fn(command);
    if (r != 0) {
      break;
    }
  }
  Py_DECREF(command_list);

  return r;
}

int PyModule::register_options(PyObject *cls)
{
  PyObject *pRegCmd = PyObject_CallMethod(
    cls,
    const_cast<char*>("_register_options"), const_cast<char*>("(s)"),
      module_name.c_str());
  if (pRegCmd != nullptr) {
    Py_DECREF(pRegCmd);
  } else {
    derr << "Exception calling _register_options on " << get_name()
         << dendl;
    derr << handle_pyerror(true, module_name, "PyModule::register_options") << dendl;
  }
  return 0;
}

int PyModule::load_notify_types()
{
  PyObject *ls = PyObject_GetAttrString(pClass, "NOTIFY_TYPES");
  if (ls == nullptr) {
    dout(10) << "Module " << get_name() << " has no NOTIFY_TYPES member" << dendl;
    return 0;
  }
  if (!PyObject_TypeCheck(ls, &PyList_Type)) {
    // Relatively easy mistake for human to make, e.g. defining COMMANDS
    // as a {} instead of a []
    derr << "Module " << get_name() << " has NOTIFY_TYPES that is not a list" << dendl;
    return -EINVAL;
  }

  const size_t list_size = PyList_Size(ls);
  for (size_t i = 0; i < list_size; ++i) {
    PyObject *notify_type = PyList_GetItem(ls, i);
    ceph_assert(notify_type != nullptr);

    if (!PyObject_TypeCheck(notify_type, &PyUnicode_Type)) {
      derr << "Module " << get_name() << " has non-string entry in NOTIFY_TYPES list"
	   << dendl;
      return -EINVAL;
    }

    notify_types.insert(PyUnicode_AsUTF8(notify_type));
  }
  Py_DECREF(ls);
  dout(10) << "Module " << get_name() << " notify_types " << notify_types << dendl;

  return 0;
}

int PyModule::load_commands()
{
  PyObject *pRegCmd = PyObject_CallMethod(pClass,
      const_cast<char*>("_register_commands"), const_cast<char*>("(s)"),
      module_name.c_str());
  if (pRegCmd != nullptr) {
    Py_DECREF(pRegCmd);
  } else {
    derr << "Exception calling _register_commands on " << get_name()
         << dendl;
    derr << handle_pyerror(true, module_name, "PyModule::load_commands") << dendl;
  }

  int r = walk_dict_list("COMMANDS", [this](PyObject *pCommand) -> int {
    ModuleCommand command;

    PyObject *pCmd = PyDict_GetItemString(pCommand, "cmd");
    ceph_assert(pCmd != nullptr);
    command.cmdstring = PyUnicode_AsUTF8(pCmd);

    dout(20) << "loaded command " << command.cmdstring << dendl;

    PyObject *pDesc = PyDict_GetItemString(pCommand, "desc");
    ceph_assert(pDesc != nullptr);
    command.helpstring = PyUnicode_AsUTF8(pDesc);

    PyObject *pPerm = PyDict_GetItemString(pCommand, "perm");
    ceph_assert(pPerm != nullptr);
    command.perm = PyUnicode_AsUTF8(pPerm);

    command.polling = false;
    if (PyObject *pPoll = PyDict_GetItemString(pCommand, "poll");
	pPoll && PyObject_IsTrue(pPoll)) {
      command.polling = true;
    }

    command.module_name = module_name;

    commands.push_back(std::move(command));

    return 0;
  });

  dout(10) << "loaded " << commands.size() << " commands" << dendl;

  return r;
}

int PyModule::load_options()
{
  int r = walk_dict_list("MODULE_OPTIONS", [this](PyObject *pOption) -> int {
    MgrMap::ModuleOption option;
    PyObject *p;
    p = PyDict_GetItemString(pOption, "name");
    ceph_assert(p != nullptr);
    option.name = PyUnicode_AsUTF8(p);
    option.type = Option::TYPE_STR;
    p = PyDict_GetItemString(pOption, "type");
    if (p && PyObject_TypeCheck(p, &PyUnicode_Type)) {
      std::string s = PyUnicode_AsUTF8(p);
      int t = Option::str_to_type(s);
      if (t >= 0) {
	option.type = t;
      }
    }
    p = PyDict_GetItemString(pOption, "level");
    if (p && PyLong_Check(p)) {
      long v = PyLong_AsLong(p);
      option.level = static_cast<Option::level_t>(v);
    } else {
      option.level = Option::level_t::LEVEL_ADVANCED;
    }
    p = PyDict_GetItemString(pOption, "desc");
    if (p && PyObject_TypeCheck(p, &PyUnicode_Type)) {
      option.desc = PyUnicode_AsUTF8(p);
    }
    p = PyDict_GetItemString(pOption, "long_desc");
    if (p && PyObject_TypeCheck(p, &PyUnicode_Type)) {
      option.long_desc = PyUnicode_AsUTF8(p);
    }
    p = PyDict_GetItemString(pOption, "default");
    if (p) {
      auto q = PyObject_Str(p);
      option.default_value = PyUnicode_AsUTF8(q);
      Py_DECREF(q);
    }
    p = PyDict_GetItemString(pOption, "min");
    if (p) {
      auto q = PyObject_Str(p);
      option.min = PyUnicode_AsUTF8(q);
      Py_DECREF(q);
    }
    p = PyDict_GetItemString(pOption, "max");
    if (p) {
      auto q = PyObject_Str(p);
      option.max = PyUnicode_AsUTF8(q);
      Py_DECREF(q);
    }
    p = PyDict_GetItemString(pOption, "enum_allowed");
    if (p && PyObject_TypeCheck(p, &PyList_Type)) {
      for (Py_ssize_t i = 0; i < PyList_Size(p); ++i) {
	auto q = PyList_GetItem(p, i);
	if (q) {
	  auto r = PyObject_Str(q);
	  option.enum_allowed.insert(PyUnicode_AsUTF8(r));
	  Py_DECREF(r);
	}
      }
    }
    p = PyDict_GetItemString(pOption, "see_also");
    if (p && PyObject_TypeCheck(p, &PyList_Type)) {
      for (Py_ssize_t i = 0; i < PyList_Size(p); ++i) {
	auto q = PyList_GetItem(p, i);
	if (q && PyObject_TypeCheck(q, &PyUnicode_Type)) {
	  option.see_also.insert(PyUnicode_AsUTF8(q));
	}
      }
    }
    p = PyDict_GetItemString(pOption, "tags");
    if (p && PyObject_TypeCheck(p, &PyList_Type)) {
      for (Py_ssize_t i = 0; i < PyList_Size(p); ++i) {
	auto q = PyList_GetItem(p, i);
	if (q && PyObject_TypeCheck(q, &PyUnicode_Type)) {
	  option.tags.insert(PyUnicode_AsUTF8(q));
	}
      }
    }
    p = PyDict_GetItemString(pOption, "runtime");
    if (p && PyObject_TypeCheck(p, &PyBool_Type)) {
      if (p == Py_True) {
	option.flags |= Option::FLAG_RUNTIME;
      }
      if (p == Py_False) {
	option.flags &= ~Option::FLAG_RUNTIME;
      }
    }
    dout(20) << "loaded module option " << option.name << dendl;
    options[option.name] = std::move(option);
    return 0;
  });

  dout(10) << "loaded " << options.size() << " options" << dendl;

  return r;
}

bool PyModule::is_option(const std::string &option_name)
{
  std::lock_guard l(lock);
  return options.count(option_name) > 0;
}

PyObject *PyModule::get_typed_option_value(const std::string& name,
					   const std::string& value)
{
  // we don't need to hold a lock here because these MODULE_OPTIONS
  // are set up exactly once during startup.
  auto p = options.find(name);
  if (p != options.end()) {
    return get_python_typed_option_value((Option::type_t)p->second.type, value);
  }
  return PyUnicode_FromString(value.c_str());
}

int PyModule::load_subclass_of(const char* base_class, PyObject** py_class)
{
  // load the base class
  PyObject *mgr_module = PyImport_ImportModule("mgr_module");
  if (!mgr_module) {
    error_string = peek_pyerror();
    derr << "Module not found: 'mgr_module'" << dendl;
    derr << handle_pyerror(true, module_name, "PyModule::load_subclass_of") << dendl;
    return -EINVAL;
  }
  auto mgr_module_type = PyObject_GetAttrString(mgr_module, base_class);
  Py_DECREF(mgr_module);
  if (!mgr_module_type) {
    error_string = peek_pyerror();
    derr << "Unable to import MgrModule from mgr_module" << dendl;
    derr << handle_pyerror(true, module_name, "PyModule::load_subclass_of") << dendl;
    return -EINVAL;
  }

  // find the sub class
  PyObject *plugin_module = PyImport_ImportModule(module_name.c_str());
  if (!plugin_module) {
    error_string = peek_pyerror();
    derr << "Module not found: '" << module_name << "'" << dendl;
    derr << handle_pyerror(true, module_name, "PyModule::load_subclass_of") << dendl;
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
    auto class_name = PyUnicode_AsUTF8(key);
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

