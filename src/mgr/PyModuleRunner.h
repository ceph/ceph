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
#include "common/LogClient.h"
#include "mgr/Gil.h"

#include "PyModule.h"

/**
 * Implement the pattern of calling serve() on a module in a thread,
 * until shutdown() is called.
 */
class PyModuleRunner
{
protected:
  // Info about the module we're going to run
  PyModuleRef py_module;

  // Populated by descendent class
  PyObject *pClassInstance = nullptr;

  LogChannelRef clog;

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
      PyModuleRef py_module_,
      LogChannelRef clog_)
    : 
      py_module(py_module_),
      clog(clog_),
      thread(this)
  {
    assert(py_module != nullptr);
  }

  ~PyModuleRunner();

  PyModuleRunnerThread thread;

  std::string const &get_name() const { return py_module->get_name(); }
};


