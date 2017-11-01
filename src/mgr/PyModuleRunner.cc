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
#include "Python.h"

#include "common/debug.h"
#include "mgr/Gil.h"

#include "PyModuleRunner.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr


std::string handle_pyerror();

PyModuleRunner::~PyModuleRunner()
{
  Gil gil(pMyThreadState, true);

  if (pClassInstance) {
    Py_XDECREF(pClassInstance);
    pClassInstance = nullptr;
  }

  Py_DECREF(pClass);
  pClass = nullptr;
}

int PyModuleRunner::serve()
{
  assert(pClassInstance != nullptr);

  // This method is called from a separate OS thread (i.e. a thread not
  // created by Python), so tell Gil to wrap this in a new thread state.
  Gil gil(pMyThreadState, true);

  auto pValue = PyObject_CallMethod(pClassInstance,
      const_cast<char*>("serve"), nullptr);

  int r = 0;
  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    derr << module_name << ".serve:" << dendl;
    derr << handle_pyerror() << dendl;
    return -EINVAL;
  }

  return r;
}

void PyModuleRunner::shutdown()
{
  assert(pClassInstance != nullptr);

  Gil gil(pMyThreadState, true);

  auto pValue = PyObject_CallMethod(pClassInstance,
      const_cast<char*>("shutdown"), nullptr);

  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    derr << "Failed to invoke shutdown() on " << module_name << dendl;
    derr << handle_pyerror() << dendl;
  }
}

void PyModuleRunner::log(int level, const std::string &record)
{
#undef dout_prefix
#define dout_prefix *_dout << "mgr[" << module_name << "] "
  dout(level) << record << dendl;
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
