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

/**
 * The interface we present to python code that runs within
 * ceph-mgr.
 */

#include "Mgr.h"

#include "mon/MonClient.h"
#include "common/version.h"

#include "PyState.h"

#define dout_context g_ceph_context

PyModules *global_handle = NULL;


class MonCommandCompletion : public Context
{
  PyObject *python_completion;
  const std::string tag;

public:
  std::string outs;
  bufferlist outbl;

  MonCommandCompletion(PyObject* ev, const std::string &tag_)
    : python_completion(ev), tag(tag_)
  {
    assert(python_completion != nullptr);
    Py_INCREF(python_completion);
  }

  ~MonCommandCompletion() override
  {
    Py_DECREF(python_completion);
  }

  void finish(int r) override
  {
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    auto set_fn = PyObject_GetAttrString(python_completion, "complete");
    assert(set_fn != nullptr);

    auto pyR = PyInt_FromLong(r);
    auto pyOutBl = PyString_FromString(outbl.to_str().c_str());
    auto pyOutS = PyString_FromString(outs.c_str());
    auto args = PyTuple_Pack(3, pyR, pyOutBl, pyOutS);
    Py_DECREF(pyR);
    Py_DECREF(pyOutBl);
    Py_DECREF(pyOutS);

    auto rtn = PyObject_CallObject(set_fn, args);
    if (rtn != nullptr) {
      Py_DECREF(rtn);
    }
    Py_DECREF(args);

    PyGILState_Release(gstate);

    global_handle->notify_all("command", tag);
  }
};


static PyObject*
ceph_send_command(PyObject *self, PyObject *args)
{
  char *handle = nullptr;

  // Like mon, osd, mds
  char *type = nullptr;

  // Like "23" for an OSD or "myid" for an MDS
  char *name = nullptr;

  char *cmd_json = nullptr;
  char *tag = nullptr;
  PyObject *completion = nullptr;
  if (!PyArg_ParseTuple(args, "sOssss:ceph_send_command",
        &handle, &completion, &type, &name, &cmd_json, &tag)) {
    return nullptr;
  }

  auto set_fn = PyObject_GetAttrString(completion, "complete");
  if (set_fn == nullptr) {
    ceph_abort();  // TODO raise python exception instead
  } else {
    assert(PyCallable_Check(set_fn));
  }
  Py_DECREF(set_fn);

  auto c = new MonCommandCompletion(completion, tag);
  if (std::string(type) == "mon") {
    global_handle->get_monc().start_mon_command(
        {cmd_json},
        {},
        &c->outbl,
        &c->outs,
        c);
  } else if (std::string(type) == "osd") {
    std::string err;
    uint64_t osd_id = strict_strtoll(name, 10, &err);
    if (!err.empty()) {
      // TODO: raise exception
      return nullptr;
    }

    ceph_tid_t tid;
    global_handle->get_objecter().osd_command(
        osd_id,
        {cmd_json},
        {},
        &tid,
        &c->outbl,
        &c->outs,
        c);
  } else if (std::string(type) == "mds") {
    int r = global_handle->get_client().mds_command(
        name,
        {cmd_json},
        {},
        &c->outbl,
        &c->outs,
        c);
    if (r != 0) {
      // TODO: raise exception
      return nullptr;
    }
  } else if (std::string(type) == "pg") {
    // TODO: expose objecter::pg_command
    return nullptr;
  } else {
    // TODO: raise exception
    return nullptr;
  }

  Py_RETURN_NONE;
}


static PyObject*
ceph_state_get(PyObject *self, PyObject *args)
{
  char *handle = nullptr;
  char *what = NULL;
  if (!PyArg_ParseTuple(args, "ss:ceph_state_get", &handle, &what)) {
    return NULL;
  }

  return global_handle->get_python(what);
}


static PyObject*
ceph_get_server(PyObject *self, PyObject *args)
{
  char *handle = nullptr;
  char *hostname = NULL;
  if (!PyArg_ParseTuple(args, "sz:ceph_get_server", &handle, &hostname)) {
    return NULL;
  }

  if (hostname) {
    return global_handle->get_server_python(hostname);
  } else {
    return global_handle->list_servers_python();
  }
}

static PyObject*
ceph_config_get(PyObject *self, PyObject *args)
{
  char *handle = nullptr;
  char *what = nullptr;
  if (!PyArg_ParseTuple(args, "ss:ceph_config_get", &handle, &what)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  std::string value;
  bool found = global_handle->get_config(handle, what, &value);
  if (found) {
    derr << "Found" << dendl;
    return PyString_FromString(value.c_str());
  } else {
    derr << "Not found" << dendl;
    Py_RETURN_NONE;
  }
}

static PyObject*
ceph_config_set(PyObject *self, PyObject *args)
{
  char *handle = nullptr;
  char *key = nullptr;
  char *value = nullptr;
  if (!PyArg_ParseTuple(args, "sss:ceph_config_set", &handle, &key, &value)) {
    return nullptr;
  }

  global_handle->set_config(handle, key, value);

  Py_RETURN_NONE;
}

static entity_type_t svc_type_from_str(const std::string &type_str)
{
  if (type_str == std::string("mds")) {
    return CEPH_ENTITY_TYPE_MDS;
  } else if (type_str == std::string("osd")) {
    return CEPH_ENTITY_TYPE_OSD;
  } else if (type_str == std::string("mon")) {
    return CEPH_ENTITY_TYPE_MON;
  } else {
    return CEPH_ENTITY_TYPE_ANY;
  }
}

static PyObject*
get_metadata(PyObject *self, PyObject *args)
{
  char *handle = nullptr;
  char *type_str = NULL;
  char *svc_id = NULL;
  if (!PyArg_ParseTuple(args, "sss:get_metadata", &handle, &type_str, &svc_id)) {
    return nullptr;
  }

  entity_type_t svc_type = svc_type_from_str(type_str);
  if (svc_type == CEPH_ENTITY_TYPE_ANY) {
    // FIXME: form a proper exception
    return nullptr;
  }


  return global_handle->get_metadata_python(handle, svc_type, svc_id);
}

static PyObject*
ceph_log(PyObject *self, PyObject *args)
{
  int level = 0;
  char *record = nullptr;
  char *handle = nullptr;
  if (!PyArg_ParseTuple(args, "sis:log", &handle, &level, &record)) {
    return nullptr;
  }

  global_handle->log(handle, level, record);

  Py_RETURN_NONE;
}

static PyObject *
ceph_get_version(PyObject *self, PyObject *args)
{
  return PyString_FromString(pretty_version_to_str().c_str());
}

static PyObject *
ceph_get_context(PyObject *self, PyObject *args)
{
  return global_handle->get_context();
}

static PyObject*
get_counter(PyObject *self, PyObject *args)
{
  char *handle = nullptr;
  char *type_str = nullptr;
  char *svc_id = nullptr;
  char *counter_path = nullptr;
  if (!PyArg_ParseTuple(args, "ssss:get_counter", &handle, &type_str,
                                                  &svc_id, &counter_path)) {
    return nullptr;
  }

  entity_type_t svc_type = svc_type_from_str(type_str);
  if (svc_type == CEPH_ENTITY_TYPE_ANY) {
    // FIXME: form a proper exception
    return nullptr;
  }

  return global_handle->get_counter_python(
      handle, svc_type, svc_id, counter_path);
}

PyMethodDef CephStateMethods[] = {
    {"get", ceph_state_get, METH_VARARGS,
     "Get a cluster object"},
    {"get_server", ceph_get_server, METH_VARARGS,
     "Get a server object"},
    {"get_metadata", get_metadata, METH_VARARGS,
     "Get a service's metadata"},
    {"send_command", ceph_send_command, METH_VARARGS,
     "Send a mon command"},
    {"get_config", ceph_config_get, METH_VARARGS,
     "Get a configuration value"},
    {"set_config", ceph_config_set, METH_VARARGS,
     "Set a configuration value"},
    {"get_counter", get_counter, METH_VARARGS,
      "Get a performance counter"},
    {"log", ceph_log, METH_VARARGS,
     "Emit a (local) log message"},
    {"get_version", ceph_get_version, METH_VARARGS,
     "Get the ceph version of this process"},
    {"get_context", ceph_get_context, METH_NOARGS,
      "Get a CephContext* in a python capsule"},
    {NULL, NULL, 0, NULL}
};

