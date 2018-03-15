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

#include "Python.h"
#include "Gil.h"

#include <string>
#include "common/Mutex.h"
#include <memory>

std::string handle_pyerror();

/**
 * A Ceph CLI command description provided from a Python module
 */
class ModuleCommand {
public:
  std::string cmdstring;
  std::string helpstring;
  std::string perm;

  // Call the ActivePyModule of this name to handle the command
  std::string module_name;
};

class PyModule
{
  mutable Mutex lock{"PyModule::lock"};
private:
  const std::string module_name;
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

  int load_commands();
  std::vector<ModuleCommand> commands;

public:
  SafeThreadState pMyThreadState;
  PyObject *pClass = nullptr;
  PyObject *pStandbyClass = nullptr;

  PyModule(const std::string &module_name_)
    : module_name(module_name_)
  {
  }

  ~PyModule();

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

  bool is_enabled() const { Mutex::Locker l(lock) ; return enabled; }
  bool is_failed() const { Mutex::Locker l(lock) ; return failed; }
  bool is_loaded() const { Mutex::Locker l(lock) ; return loaded; }

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

