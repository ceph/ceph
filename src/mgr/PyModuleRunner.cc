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


// Python.h comes first because otherwise it clobbers ceph's assert
#include "PythonCompat.h"

#include "PyModule.h"

#include "common/debug.h"
#include "mgr/Gil.h"

#include "PyModuleRunner.h"

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
    derr << handle_pyerror() << dendl;

    py_module->fail(exc_msg);

    return -EINVAL;
  }

  return r;
}

void PyModuleRunner::shutdown()
{
  ceph_assert(pClassInstance != nullptr);

  Gil gil(py_module->pMyThreadState, true);

  auto pValue = PyObject_CallMethod(pClassInstance,
      const_cast<char*>("shutdown"), nullptr);

  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    derr << "Failed to invoke shutdown() on " << get_name() << dendl;
    derr << handle_pyerror() << dendl;
  }

  dead = true;
}

void PyModuleRunner::log(int level, const std::string &record)
{
#undef dout_prefix
#define dout_prefix *_dout << "mgr[" << get_name() << "] "
  dout(ceph::dout::need_dynamic(level)) << record << dendl;
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "
}

void* PyModuleRunner::PyModuleRunnerThread::entry()
{
  // No need to acquire the GIL here; the module does it.
  dout(4) << "Entering thread for " << mod->get_name() << dendl;
  mod->serve();
  return nullptr;
}
