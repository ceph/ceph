// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "mgr/ThreadMonitor.h"
#include <future>

/**
 * Implement the pattern of calling serve() on a module in a thread,
 * until shutdown() is called.
 */
class PyModuleRunner
{
public:
  // Info about the module we're going to run
  PyModuleRef py_module;

protected:
  // Populated by descendent class
  PyObject *pClassInstance = nullptr;

  LogChannelRef clog;
  pid_t m_native_tid = 0;
  ThreadMonitor* m_thread_monitor = nullptr;
  class PyModuleRunnerThread : public Thread
  {
    PyModuleRunner *mod;
    std::atomic<pid_t> runner_tid{0};
  public:
    explicit PyModuleRunnerThread(PyModuleRunner *mod_)
      : mod(mod_) {}

    void *entry() override;
    pid_t get_tid() const { return runner_tid.load(); }
  };

  bool is_dead() const { return dead; }

  std::string thread_name;

public:
  int serve();
  void shutdown();
  void log(const std::string &record);

  const char *get_thread_name() const
  {
    return thread_name.c_str();
  }

  PyModuleRunner(
      const PyModuleRef &py_module_,
      LogChannelRef clog_,
      ThreadMonitor* monitor_ = nullptr)
    : 
      py_module(py_module_),
      clog(clog_),
      m_thread_monitor(monitor_),
      thread(this)
  {
    // Shortened name for use as thread name, because thread names
    // required to be <16 chars
    thread_name = py_module->get_name().substr(0, 15);

    ceph_assert(py_module != nullptr);
  }

  ~PyModuleRunner();

  PyModuleRunnerThread thread;

  std::string const &get_name() const { return py_module->get_name(); }
  void set_native_tid(pid_t tid) { m_native_tid = tid; }
  pid_t get_native_tid() const { return m_native_tid; }

private:
  bool dead = false;
};


