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


#pragma once

#include "common/Thread.h"
#include "mgr/Gil.h"

/**
 * Implement the pattern of calling serve() on a module in a thread,
 * until shutdown() is called.
 */
class PyModuleRunner
{
protected:
  const std::string module_name;

  // Passed in by whoever loaded our python module and looked up
  // the symbols in it.
  PyObject *pClass = nullptr;

  // Passed in by whoever created our subinterpreter for us
  SafeThreadState pMyThreadState = nullptr;

  // Populated when we construct our instance of pClass in load()
  PyObject *pClassInstance = nullptr;

  class PyModuleRunnerThread : public Thread
  {
    PyModuleRunner *mod;

  public:
    PyModuleRunnerThread(PyModuleRunner *mod_)
      : mod(mod_) {}

    void *entry() override;
  };

public:
  int serve();
  void shutdown();
  void log(int level, const std::string &record);

  PyModuleRunner(
      const std::string &module_name_,
      PyObject *pClass_,
      const SafeThreadState &pMyThreadState_)
    : 
      module_name(module_name_),
      pClass(pClass_), pMyThreadState(pMyThreadState_),
      thread(this)
  {
    assert(pClass != nullptr);
    assert(pMyThreadState.ts != nullptr);
    assert(!module_name.empty());
  }

  ~PyModuleRunner();

  PyModuleRunnerThread thread;

  std::string const &get_name() const { return module_name; }
};


