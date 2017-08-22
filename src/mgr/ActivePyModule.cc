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

#include "BaseMgrModule.h"

#include "PyFormatter.h"

#include "common/debug.h"

#include "ActivePyModule.h"

//XXX courtesy of http://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
#include <boost/python.hpp>
#include "include/assert.h"  // boost clobbers this


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

// decode a Python exception into a string
std::string handle_pyerror()
{
    using namespace boost::python;
    using namespace boost;

    PyObject *exc, *val, *tb;
    object formatted_list, formatted;
    PyErr_Fetch(&exc, &val, &tb);
    handle<> hexc(exc), hval(allow_null(val)), htb(allow_null(tb));
    object traceback(import("traceback"));
    if (!tb) {
        object format_exception_only(traceback.attr("format_exception_only"));
        formatted_list = format_exception_only(hexc, hval);
    } else {
        object format_exception(traceback.attr("format_exception"));
        formatted_list = format_exception(hexc,hval, htb);
    }
    formatted = str("").join(formatted_list);
    return extract<std::string>(formatted);
}


void *ServeThread::entry()
{
  // No need to acquire the GIL here; the module does it.
  dout(4) << "Entering thread for " << mod->get_name() << dendl;
  mod->serve();
  return nullptr;
}

ActivePyModule::ActivePyModule(const std::string &module_name_,
                               PyObject *pClass_,
                               PyThreadState *my_ts_)
  : module_name(module_name_),
    pClass(pClass_),
    pMyThreadState(my_ts_),
    thread(this)
{
}

ActivePyModule::~ActivePyModule()
{
  if (pMyThreadState.ts != nullptr) {
    Gil gil(pMyThreadState);

    Py_XDECREF(pClassInstance);

    //
    // Ideally, now, we'd be able to do this:
    //
    //    Py_EndInterpreter(pMyThreadState);
    //    PyThreadState_Swap(pMainThreadState);
    //
    // Unfortunately, if the module has any other *python* threads active
    // at this point, Py_EndInterpreter() will abort with:
    //
    //    Fatal Python error: Py_EndInterpreter: not the last thread
    //
    // This can happen when using CherryPy in a module, becuase CherryPy
    // runs an extra thread as a timeout monitor, which spends most of its
    // life inside a time.sleep(60).  Unless you are very, very lucky with
    // the timing calling this destructor, that thread will still be stuck
    // in a sleep, and Py_EndInterpreter() will abort.
    //
    // This could of course also happen with a poorly written module which
    // made no attempt to clean up any additional threads it created.
    //
    // The safest thing to do is just not call Py_EndInterpreter(), and
    // let Py_Finalize() kill everything after all modules are shut down.
    //
  }
}

int ActivePyModule::load(ActivePyModules *py_modules)
{
  Gil gil(pMyThreadState);

  // We tell the module how we name it, so that it can be consistent
  // with us in logging etc.
  auto pThisPtr = PyCapsule_New(this, nullptr, nullptr);
  auto pPyModules = PyCapsule_New(py_modules, nullptr, nullptr);
  auto pModuleName = PyString_FromString(module_name.c_str());
  auto pArgs = PyTuple_Pack(3, pModuleName, pPyModules, pThisPtr);

  pClassInstance = PyObject_CallObject(pClass, pArgs);
  Py_DECREF(pClass);
  Py_DECREF(pModuleName);
  Py_DECREF(pArgs);
  if (pClassInstance == nullptr) {
    derr << "Failed to construct class in '" << module_name << "'" << dendl;
    derr << handle_pyerror() << dendl;
    return -EINVAL;
  } else {
    dout(1) << "Constructed class from module: " << module_name << dendl;
  }

  return load_commands();
}

int ActivePyModule::serve()
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

void ActivePyModule::shutdown()
{
  assert(pClassInstance != nullptr);

  Gil gil(pMyThreadState);

  auto pValue = PyObject_CallMethod(pClassInstance,
      const_cast<char*>("shutdown"), nullptr);

  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    derr << "Failed to invoke shutdown() on " << module_name << dendl;
    derr << handle_pyerror() << dendl;
  }
}

void ActivePyModule::notify(const std::string &notify_type, const std::string &notify_id)
{
  assert(pClassInstance != nullptr);

  Gil gil(pMyThreadState);

  // Execute
  auto pValue = PyObject_CallMethod(pClassInstance,
       const_cast<char*>("notify"), const_cast<char*>("(ss)"),
       notify_type.c_str(), notify_id.c_str());

  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    derr << module_name << ".notify:" << dendl;
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

  Gil gil(pMyThreadState);

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
    derr << module_name << ".notify_clog:" << dendl;
    derr << handle_pyerror() << dendl;
    // FIXME: callers can't be expected to handle a python module
    // that has spontaneously broken, but Mgr() should provide
    // a hook to unload misbehaving modules when they have an
    // error somewhere like this
  }
}

int ActivePyModule::load_commands()
{
  // Don't need a Gil here -- this is called from ActivePyModule::load(),
  // which already has one.
  PyObject *command_list = PyObject_GetAttrString(pClassInstance, "COMMANDS");
  assert(command_list != nullptr);
  const size_t list_size = PyList_Size(command_list);
  for (size_t i = 0; i < list_size; ++i) {
    PyObject *command = PyList_GetItem(command_list, i);
    assert(command != nullptr);

    ModuleCommand item;

    PyObject *pCmd = PyDict_GetItemString(command, "cmd");
    assert(pCmd != nullptr);
    item.cmdstring = PyString_AsString(pCmd);

    dout(20) << "loaded command " << item.cmdstring << dendl;

    PyObject *pDesc = PyDict_GetItemString(command, "desc");
    assert(pDesc != nullptr);
    item.helpstring = PyString_AsString(pDesc);

    PyObject *pPerm = PyDict_GetItemString(command, "perm");
    assert(pPerm != nullptr);
    item.perm = PyString_AsString(pPerm);

    item.handler = this;

    commands.push_back(item);
  }
  Py_DECREF(command_list);

  dout(10) << "loaded " << commands.size() << " commands" << dendl;

  return 0;
}

int ActivePyModule::handle_command(
  const cmdmap_t &cmdmap,
  std::stringstream *ds,
  std::stringstream *ss)
{
  assert(ss != nullptr);
  assert(ds != nullptr);

  Gil gil(pMyThreadState);

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
