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

public:
  SafeThreadState pMyThreadState;
  PyObject *pClass = nullptr;
  PyObject *pStandbyClass = nullptr;

  PyModule(const std::string &module_name_, bool const enabled_)
    : module_name(module_name_), enabled(enabled_)
  {
  }

  ~PyModule();

  int load(PyThreadState *pMainThreadState);

  bool is_enabled() const { Mutex::Locker l(lock) ; return enabled; }
  const std::string &get_name() const
    { Mutex::Locker l(lock) ; return module_name; }
};

typedef std::shared_ptr<PyModule> PyModuleRef;

