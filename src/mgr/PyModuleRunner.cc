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


// Python.h comes first because otherwise it clobbers ceph's assert
#include <Python.h>

#include "PyModule.h"

#include "common/config_proxy.h"
#include "common/debug.h"
#include "global/global_context.h"
#include "mgr/Gil.h"

#include "PyModuleRunner.h"

#include <chrono>
#include <thread>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr


PyModuleRunner::~PyModuleRunner()
{
  Gil gil(py_module->pMyThreadState, true);

  if (pClassInstance) {
    Py_XDECREF(pClassInstance);
    pClassInstance = nullptr;
  }
}

int PyModuleRunner::serve()
{
  ceph_assert(pClassInstance != nullptr);

  // This method is called from a separate OS thread (i.e. a thread not
  // created by Python), so tell Gil to wrap this in a new thread state.
  Gil gil(py_module->pMyThreadState, true);
  if (py_module->perfcounter) {
    py_module->perfcounter->set(py_module->l_pym_alive, 1);
  }
  auto pValue = PyObject_CallMethod(pClassInstance,
      const_cast<char*>("serve"), nullptr);

  int r = 0;
  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    // This is not a very informative log message because it's an
    // unknown/unexpected exception that we can't say much about.


    // Get short exception message for the cluster log, before
    // dumping the full backtrace to the local log.
    std::string exc_msg = peek_pyerror();
    
    clog->error() << "Unhandled exception from module '" << get_name()
                  << "' while running on mgr." << g_conf()->name.get_id()
                  << ": " << exc_msg;
    derr << get_name() << ".serve:" << dendl;
    derr << handle_pyerror(true, get_name(), "PyModuleRunner::serve") << dendl;

    py_module->fail(exc_msg);

    return -EINVAL;
  }

  if (py_module->perfcounter) {
    py_module->perfcounter->set(py_module->l_pym_alive, 0);
  }

  return r;
}

PyModuleRunner::ShutdownResult PyModuleRunner::shutdown()
{
  ceph_assert(pClassInstance != nullptr);

  auto timeout = std::chrono::seconds(
      g_conf().get_val<int64_t>("mgr_module_shutdown_timeout"));

  std::packaged_task<std::string()> task([this] {
    std::string exc_msg;
    {
      Gil gil(py_module->pMyThreadState, true);
      auto pValue = PyObject_CallMethod(pClassInstance,
          const_cast<char*>("shutdown"), nullptr);
      if (pValue != nullptr) {
        Py_DECREF(pValue);
      } else {
        exc_msg = peek_pyerror();
        derr << get_name() << ".shutdown:" << dendl;
        derr << handle_pyerror(true, get_name(), "PyModuleRunner::shutdown") << dendl;
      }
    }
    thread.join();
    return exc_msg;
  });
  auto fut = task.get_future();
  std::thread(std::move(task)).detach();

  if (fut.wait_for(timeout) == std::future_status::timeout) {
    derr << "shutdown() on " << get_name() << " timed out after "
         << timeout.count() << "s" << dendl;
    py_module->fail("shutdown() timed out after " +
                     std::to_string(timeout.count()) + "s");
    if (py_module->perfcounter) {
      py_module->perfcounter->set(py_module->l_pym_alive, 0);
    }
    dead = true;
    // Caller MUST NOT destruct `this` normally now -- the detached task
    // (call + join) is still running in the background, referencing
    // pClassInstance. Caller is responsible for leaking `this` instead.
    return ShutdownResult::TIMEOUT;
  }

  std::string exc_msg = fut.get();
  bool ok = exc_msg.empty();
  if (!ok) {
    py_module->fail(exc_msg);
  }
  if (py_module->perfcounter) {
    py_module->perfcounter->set(py_module->l_pym_alive, 0);
  }
  dead = true;
  return ok ? ShutdownResult::OK : ShutdownResult::EXCEPTION;
}

void PyModuleRunner::log(const std::string &record)
{
#undef dout_prefix
#define dout_prefix *_dout
  dout(0) << record << dendl;
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "
}

void* PyModuleRunner::PyModuleRunnerThread::entry()
{
  // No need to acquire the GIL here; the module does it.
  dout(4) << "Entering thread for " << mod->get_name() << dendl;
  runner_tid.store(ceph_gettid(), std::memory_order_release); 
  mod->serve();
  return nullptr;
}
