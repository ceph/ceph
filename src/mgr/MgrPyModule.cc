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

#include "MgrPyModule.h"

//XXX courtesy of http://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
#include <boost/python.hpp>
#include "include/assert.h"  // boost clobbers this

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

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

MgrPyModule::MgrPyModule(const std::string &module_name_)
  : module_name(module_name_),
    pClassInstance(nullptr)
{}

MgrPyModule::~MgrPyModule()
{
  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

  Py_XDECREF(pClassInstance);

  PyGILState_Release(gstate);
}

int MgrPyModule::load()
{
  // Load the module
  PyObject *pName = PyString_FromString(module_name.c_str());
  auto pModule = PyImport_Import(pName);
  Py_DECREF(pName);
  if (pModule == nullptr) {
    derr << "Module not found: '" << module_name << "'" << dendl;
    return -ENOENT;
  }

  // Find the class
  // TODO: let them call it what they want instead of just 'Module'
  auto pClass = PyObject_GetAttrString(pModule, (const char*)"Module");
  Py_DECREF(pModule);
  if (pClass == nullptr) {
    derr << "Class not found in module '" << module_name << "'" << dendl;
    return -EINVAL;
  }

  
  // Just using the module name as the handle, replace with a
  // uuidish thing if needed
  auto pyHandle = PyString_FromString(module_name.c_str());
  auto pArgs = PyTuple_Pack(1, pyHandle);
  pClassInstance = PyObject_CallObject(pClass, pArgs);
  Py_DECREF(pClass);
  Py_DECREF(pyHandle);
  Py_DECREF(pArgs);
  if (pClassInstance == nullptr) {
    derr << "Failed to construct class in '" << module_name << "'" << dendl;
    return -EINVAL;
  } else {
    dout(1) << "Constructed class from module: " << module_name << dendl;
  }

  return load_commands();
}

int MgrPyModule::serve()
{
  assert(pClassInstance != nullptr);

  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

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

  PyGILState_Release(gstate);

  return r;
}

// FIXME: DRY wrt serve
void MgrPyModule::shutdown()
{
  assert(pClassInstance != nullptr);

  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

  auto pValue = PyObject_CallMethod(pClassInstance,
      const_cast<char*>("shutdown"), nullptr);

  if (pValue != NULL) {
    Py_DECREF(pValue);
  } else {
    derr << "Failed to invoke shutdown() on " << module_name << dendl;
    derr << handle_pyerror() << dendl;
  }

  PyGILState_Release(gstate);
}

void MgrPyModule::notify(const std::string &notify_type, const std::string &notify_id)
{
  assert(pClassInstance != nullptr);

  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

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

  PyGILState_Release(gstate);
}

void MgrPyModule::notify_clog(const LogEntry &log_entry)
{
  assert(pClassInstance != nullptr);

  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

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

  PyGILState_Release(gstate);
}

int MgrPyModule::load_commands()
{
  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

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

  PyGILState_Release(gstate);

  dout(10) << "loaded " << commands.size() << " commands" << dendl;

  return 0;
}

int MgrPyModule::handle_command(
  const cmdmap_t &cmdmap,
  std::stringstream *ds,
  std::stringstream *ss)
{
  assert(ss != nullptr);
  assert(ds != nullptr);

  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();

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

  PyGILState_Release(gstate);

  return r;
}

