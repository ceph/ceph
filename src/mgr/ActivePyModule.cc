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

#include "PyFormatter.h"

#include "common/debug.h"

#include "ActivePyModule.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

int ActivePyModule::load(ActivePyModules *py_modules)
{
  assert(py_modules);
  Gil gil(py_module->pMyThreadState, true);

  // We tell the module how we name it, so that it can be consistent
  // with us in logging etc.
  auto pThisPtr = PyCapsule_New(this, nullptr, nullptr);
  auto pPyModules = PyCapsule_New(py_modules, nullptr, nullptr);
  auto pModuleName = PyString_FromString(get_name().c_str());
  auto pArgs = PyTuple_Pack(3, pModuleName, pPyModules, pThisPtr);

  pClassInstance = PyObject_CallObject(py_module->pClass, pArgs);
  Py_DECREF(pModuleName);
  Py_DECREF(pArgs);
  if (pClassInstance == nullptr) {
    derr << "Failed to construct class in '" << get_name() << "'" << dendl;
    derr << handle_pyerror() << dendl;
    return -EINVAL;
  } else {
    dout(1) << "Constructed class from module: " << get_name() << dendl;
  }

  return 0;
}

void ActivePyModule::notify(const std::string &notify_type, const std::string &notify_id)
{
  assert(pClassInstance != nullptr);

  Gil gil(py_module->pMyThreadState, true);

  // Execute
  auto pValue = PyObject_CallMethod(pClassInstance,
       const_cast<char*>("notify"), const_cast<char*>("(ss)"),
       notify_type.c_str(), notify_id.c_str());

  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    derr << get_name() << ".notify:" << dendl;
    derr << handle_pyerror() << dendl;
    // FIXME: callers can't be expected to handle a python module
    // that has spontaneously broken, but Mgr() should provide
    // a hook to unload misbehaving modules when they have an
    // error somewhere like this
  }
}

void ActivePyModule::notify_clog(const LogEntry &log_entry)
{
  assert(pClassInstance != nullptr);

  Gil gil(py_module->pMyThreadState, true);

  // Construct python-ized LogEntry
  PyFormatter f;
  log_entry.dump(&f);
  auto py_log_entry = f.get();

  // Execute
  auto pValue = PyObject_CallMethod(pClassInstance,
       const_cast<char*>("notify"), const_cast<char*>("(sN)"),
       "clog", py_log_entry);

  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    derr << get_name() << ".notify_clog:" << dendl;
    derr << handle_pyerror() << dendl;
    // FIXME: callers can't be expected to handle a python module
    // that has spontaneously broken, but Mgr() should provide
    // a hook to unload misbehaving modules when they have an
    // error somewhere like this
  }
}



int ActivePyModule::handle_command(
  const cmdmap_t &cmdmap,
  std::stringstream *ds,
  std::stringstream *ss)
{
  assert(ss != nullptr);
  assert(ds != nullptr);

  Gil gil(py_module->pMyThreadState, true);

  PyFormatter f;
  cmdmap_dump(cmdmap, &f);
  PyObject *py_cmd = f.get();

  auto pResult = PyObject_CallMethod(pClassInstance,
      const_cast<char*>("handle_command"), const_cast<char*>("(O)"), py_cmd);

  Py_DECREF(py_cmd);

  int r = 0;
  if (pResult != NULL) {
    if (PyTuple_Size(pResult) != 3) {
      r = -EINVAL;
    } else {
      r = PyInt_AsLong(PyTuple_GetItem(pResult, 0));
      *ds << PyString_AsString(PyTuple_GetItem(pResult, 1));
      *ss << PyString_AsString(PyTuple_GetItem(pResult, 2));
    }

    Py_DECREF(pResult);
  } else {
    *ds << "";
    *ss << handle_pyerror();
    r = -EINVAL;
  }

  return r;
}

void ActivePyModule::get_health_checks(health_check_map_t *checks)
{
  checks->merge(health_checks);
}

