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
#include "mon/MonCommand.h"

#include "ActivePyModule.h"
#include "MgrSession.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

using std::string;
using namespace std::literals;

int ActivePyModule::load(ActivePyModules *py_modules)
{
  ceph_assert(py_modules);
  Gil gil(py_module->pMyThreadState, true);

  // We tell the module how we name it, so that it can be consistent
  // with us in logging etc.
  auto pThisPtr = PyCapsule_New(this, nullptr, nullptr);
  auto pPyModules = PyCapsule_New(py_modules, nullptr, nullptr);
  auto pModuleName = PyUnicode_FromString(get_name().c_str());
  auto pArgs = PyTuple_Pack(3, pModuleName, pPyModules, pThisPtr);

  pClassInstance = PyObject_CallObject(py_module->pClass, pArgs);
  Py_DECREF(pModuleName);
  Py_DECREF(pArgs);
  if (pClassInstance == nullptr) {
    derr << "Failed to construct class in '" << get_name() << "'" << dendl;
    derr << handle_pyerror(true, get_name(), "ActivePyModule::load") << dendl;
    return -EINVAL;
  } else {
    dout(1) << "Constructed class from module: " << get_name() << dendl;
  }

  return 0;
}

void ActivePyModule::notify(const std::string &notify_type, const std::string &notify_id)
{
  if (is_dead()) {
    dout(5) << "cancelling notify " << notify_type << " " << notify_id << dendl;
    return;
  }

  ceph_assert(pClassInstance != nullptr);

  Gil gil(py_module->pMyThreadState, true);

  // Execute
  auto pValue = PyObject_CallMethod(pClassInstance,
       const_cast<char*>("notify"), const_cast<char*>("(ss)"),
       notify_type.c_str(), notify_id.c_str());

  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    derr << get_name() << ".notify:" << dendl;
    derr << handle_pyerror(true, get_name(), "ActivePyModule::notify") << dendl;
    // FIXME: callers can't be expected to handle a python module
    // that has spontaneously broken, but Mgr() should provide
    // a hook to unload misbehaving modules when they have an
    // error somewhere like this
  }
}

void ActivePyModule::notify_clog(const LogEntry &log_entry)
{
  if (is_dead()) {
    dout(5) << "cancelling notify_clog" << dendl;
    return;
  }

  ceph_assert(pClassInstance != nullptr);

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
    derr << handle_pyerror(true, get_name(), "ActivePyModule::notify_clog") << dendl;
    // FIXME: callers can't be expected to handle a python module
    // that has spontaneously broken, but Mgr() should provide
    // a hook to unload misbehaving modules when they have an
    // error somewhere like this
  }
}

bool ActivePyModule::method_exists(const std::string &method) const
{
  Gil gil(py_module->pMyThreadState, true);

  auto boundMethod = PyObject_GetAttrString(pClassInstance, method.c_str());
  if (boundMethod == nullptr) {
    return false;
  } else {
    Py_DECREF(boundMethod);
    return true;
  }
}

PyObject *ActivePyModule::dispatch_remote(
    const std::string &method,
    PyObject *args,
    PyObject *kwargs,
    std::string *err)
{
  ceph_assert(err != nullptr);

  // Rather than serializing arguments, pass the CPython objects.
  // Works because we happen to know that the subinterpreter
  // implementation shares a GIL, allocator, deallocator and GC state, so
  // it's okay to pass the objects between subinterpreters.
  // But in future this might involve serialization to support a CSP-aware
  // future Python interpreter a la PEP554

  Gil gil(py_module->pMyThreadState, true);

  // Fire the receiving method
  auto boundMethod = PyObject_GetAttrString(pClassInstance, method.c_str());

  // Caller should have done method_exists check first!
  ceph_assert(boundMethod != nullptr);

  dout(20) << "Calling " << py_module->get_name()
           << "." << method << "..." << dendl;

  auto remoteResult = PyObject_Call(boundMethod,
      args, kwargs);
  Py_DECREF(boundMethod);

  if (remoteResult == nullptr) {
    // Because the caller is in a different context, we can't let this
    // exception bubble up, need to re-raise it from the caller's
    // context later.
    std::string caller = "ActivePyModule::dispatch_remote "s + method;
    *err = handle_pyerror(true, get_name(), caller);
  } else {
    dout(20) << "Success calling '" << method << "'" << dendl;
  }

  return remoteResult;
}

void ActivePyModule::config_notify()
{
  if (is_dead()) {
    dout(5) << "cancelling config_notify" << dendl;
    return;
  }

  Gil gil(py_module->pMyThreadState, true);
  dout(20) << "Calling " << py_module->get_name() << "._config_notify..."
	   << dendl;
  auto remoteResult = PyObject_CallMethod(pClassInstance,
					  const_cast<char*>("_config_notify"),
					  (char*)NULL);
  if (remoteResult != nullptr) {
    Py_DECREF(remoteResult);
  }
}

int ActivePyModule::handle_command(
  const ModuleCommand& module_command,
  const MgrSession& session,
  const cmdmap_t &cmdmap,
  const bufferlist &inbuf,
  std::stringstream *ds,
  std::stringstream *ss)
{
  ceph_assert(ss != nullptr);
  ceph_assert(ds != nullptr);

  if (pClassInstance == nullptr) {
    // Not the friendliest error string, but we could only
    // hit this in quite niche cases, if at all.
    *ss << "Module not instantiated";
    return -EINVAL;
  }

  Gil gil(py_module->pMyThreadState, true);

  PyFormatter f;
  TOPNSPC::common::cmdmap_dump(cmdmap, &f);
  PyObject *py_cmd = f.get();
  string instr;
  inbuf.begin().copy(inbuf.length(), instr);

  ceph_assert(m_session == nullptr);
  m_command_perms = module_command.perm;
  m_session = &session;

  auto pResult = PyObject_CallMethod(pClassInstance,
      const_cast<char*>("_handle_command"), const_cast<char*>("s#O"),
      instr.c_str(), instr.length(), py_cmd);

  m_command_perms.clear();
  m_session = nullptr;
  Py_DECREF(py_cmd);

  int r = 0;
  if (pResult != NULL) {
    if (PyTuple_Size(pResult) != 3) {
      derr << "module '" << py_module->get_name() << "' command handler "
              "returned wrong type!" << dendl;
      r = -EINVAL;
    } else {
      r = PyLong_AsLong(PyTuple_GetItem(pResult, 0));
      *ds << PyUnicode_AsUTF8(PyTuple_GetItem(pResult, 1));
      *ss << PyUnicode_AsUTF8(PyTuple_GetItem(pResult, 2));
    }

    Py_DECREF(pResult);
  } else {
    derr << "module '" << py_module->get_name() << "' command handler "
            "threw exception: " << peek_pyerror() << dendl;
    *ds << "";
    *ss << handle_pyerror();
    r = -EINVAL;
  }

  return r;
}

void ActivePyModule::get_health_checks(health_check_map_t *checks)
{
  if (is_dead()) {
    dout(5) << "cancelling get_health_checks" << dendl;
    return;
  }
  checks->merge(health_checks);
}

bool ActivePyModule::is_authorized(
    const std::map<std::string, std::string>& arguments) const {
  if (m_session == nullptr) {
    return false;
  }

  // No need to pass command prefix here since that would have already been
  // tested before command invokation. Instead, only test for service/module
  // arguments as defined by the module itself.
  MonCommand mon_command {"", "", "", m_command_perms};
  return m_session->caps.is_capable(nullptr, m_session->entity_name, "py",
                                    py_module->get_name(), "", arguments,
                                    mon_command.requires_perm('r'),
                                    mon_command.requires_perm('w'),
                                    mon_command.requires_perm('x'),
                                    m_session->get_peer_addr());
}
