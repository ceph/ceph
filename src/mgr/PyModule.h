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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <boost/optional.hpp>
#include "common/Mutex.h"
#include "Python.h"
#include "Gil.h"


class MonClient;

std::string handle_pyerror();

std::string peek_pyerror();

/**
 * A Ceph CLI command description provided from a Python module
 */
class ModuleCommand {
public:
  std::string cmdstring;
  std::string helpstring;
  std::string perm;
  bool polling;

  // Call the ActivePyModule of this name to handle the command
  std::string module_name;
};


/**
 * An option declared by the python module in its configuration schema
 */
class ModuleOption {
  public:
  std::string name;
};

class PyModule
{
  mutable Mutex lock{"PyModule::lock"};
private:
  const std::string module_name;
  const bool always_on;
  std::string get_site_packages();
  int load_subclass_of(const char* class_name, PyObject** py_class);

  // Did the MgrMap identify this module as one that should run?
  bool enabled = false;

  // Did we successfully import this python module and look up symbols?
  // (i.e. is it possible to instantiate a MgrModule subclass instance?)
  bool loaded = false;

  // Did the module identify itself as being able to run?
  // (i.e. should we expect instantiating and calling serve() to work?)
  bool can_run = false;

  // Did the module encounter an unexpected error while running?
  // (e.g. throwing an exception from serve())
  bool failed = false;

  // Populated if loaded, can_run or failed indicates a problem
  std::string error_string;

  // Helper for loading OPTIONS and COMMANDS members
  int walk_dict_list(
      const std::string &attr_name,
      std::function<int(PyObject*)> fn);

  int load_commands();
  std::vector<ModuleCommand> commands;

  int load_options();
  std::map<std::string, ModuleOption> options;

public:
  static std::string config_prefix;

  SafeThreadState pMyThreadState;
  PyObject *pClass = nullptr;
  PyObject *pStandbyClass = nullptr;

  explicit PyModule(const std::string &module_name_, bool always_on)
    : module_name(module_name_), always_on(always_on)
  {
  }

  ~PyModule();

  bool is_option(const std::string &option_name);

  int load(PyThreadState *pMainThreadState);
#if PY_MAJOR_VERSION >= 3
  static PyObject* init_ceph_logger();
  static PyObject* init_ceph_module();
#else
  static void init_ceph_logger();
  static void init_ceph_module();
#endif

  void set_enabled(const bool enabled_)
  {
    enabled = enabled_;
  }

  /**
   * Extend `out` with the contents of `this->commands`
   */
  void get_commands(std::vector<ModuleCommand> *out) const
  {
    Mutex::Locker l(lock);
    assert(out != nullptr);
    out->insert(out->end(), commands.begin(), commands.end());
  }


  /**
   * Mark the module as failed, recording the reason in the error
   * string.
   */
  void fail(const std::string &reason)
  {
    Mutex::Locker l(lock);
    failed = true;
    error_string = reason;
  }

  bool is_enabled() const {
    Mutex::Locker l(lock);
    return enabled || always_on;
  }

  bool is_failed() const { Mutex::Locker l(lock) ; return failed; }
  bool is_loaded() const { Mutex::Locker l(lock) ; return loaded; }
  bool is_always_on() const { Mutex::Locker l(lock) ; return always_on; }

  const std::string &get_name() const {
    Mutex::Locker l(lock) ; return module_name;
  }
  const std::string &get_error_string() const {
    Mutex::Locker l(lock) ; return error_string;
  }
  bool get_can_run() const {
    Mutex::Locker l(lock) ; return can_run;
  }
};

typedef std::shared_ptr<PyModule> PyModuleRef;

class PyModuleConfig {
public:
  mutable Mutex lock{"PyModuleConfig::lock"};
  std::map<std::string, std::string> config;

  PyModuleConfig();
  
  PyModuleConfig(PyModuleConfig &mconfig);
  
  ~PyModuleConfig();

  void set_config(
    MonClient *monc,
    const std::string &module_name,
    const std::string &key, const boost::optional<std::string>& val);

};
