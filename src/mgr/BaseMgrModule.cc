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
 * ceph-mgr.  This is implemented as a Python class from which
 * all modules must inherit -- access to the Ceph state is then
 * available as methods on that object.
 */

#include "Python.h"

#include "Mgr.h"

#include "mon/MonClient.h"
#include "common/errno.h"
#include "common/version.h"

#include "BaseMgrModule.h"
#include "Gil.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

#define PLACEHOLDER ""


typedef struct {
  PyObject_HEAD
  ActivePyModules *py_modules;
  ActivePyModule *this_module;
} BaseMgrModule;

class MonCommandCompletion : public Context
{
  ActivePyModules *py_modules;
  PyObject *python_completion;
  const std::string tag;
  SafeThreadState pThreadState;

public:
  std::string outs;
  bufferlist outbl;

  MonCommandCompletion(
      ActivePyModules *py_modules_, PyObject* ev,
      const std::string &tag_, PyThreadState *ts_)
    : py_modules(py_modules_), python_completion(ev),
      tag(tag_), pThreadState(ts_)
  {
    assert(python_completion != nullptr);
    Py_INCREF(python_completion);
  }

  ~MonCommandCompletion() override
  {
    if (python_completion) {
      // Usually do this in finish(): this path is only for if we're
      // being destroyed without completing.
      Gil gil(pThreadState, true);
      Py_DECREF(python_completion);
      python_completion = nullptr;
    }
  }

  void finish(int r) override
  {
    assert(python_completion != nullptr);

    dout(10) << "MonCommandCompletion::finish()" << dendl;
    {
      // Scoped so the Gil is released before calling notify_all()
      // Create new thread state because this is called via the MonClient
      // Finisher, not the PyModules finisher.
      Gil gil(pThreadState, true);

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
      Py_DECREF(set_fn);

      Py_DECREF(python_completion);
      python_completion = nullptr;
    }
    py_modules->notify_all("command", tag);
  }
};


static PyObject*
ceph_send_command(BaseMgrModule *self, PyObject *args)
{
  // Like mon, osd, mds
  char *type = nullptr;

  // Like "23" for an OSD or "myid" for an MDS
  char *name = nullptr;

  char *cmd_json = nullptr;
  char *tag = nullptr;
  PyObject *completion = nullptr;
  if (!PyArg_ParseTuple(args, "Ossss:ceph_send_command",
        &completion, &type, &name, &cmd_json, &tag)) {
    return nullptr;
  }

  auto set_fn = PyObject_GetAttrString(completion, "complete");
  if (set_fn == nullptr) {
    ceph_abort();  // TODO raise python exception instead
  } else {
    assert(PyCallable_Check(set_fn));
  }
  Py_DECREF(set_fn);

  auto c = new MonCommandCompletion(self->py_modules,
      completion, tag, PyThreadState_Get());
  if (std::string(type) == "mon") {
    self->py_modules->get_monc().start_mon_command(
        {cmd_json},
        {},
        &c->outbl,
        &c->outs,
        c);
  } else if (std::string(type) == "osd") {
    std::string err;
    uint64_t osd_id = strict_strtoll(name, 10, &err);
    if (!err.empty()) {
      delete c;
      string msg("invalid osd_id: ");
      msg.append("\"").append(name).append("\"");
      PyErr_SetString(PyExc_ValueError, msg.c_str());
      return nullptr;
    }

    ceph_tid_t tid;
    self->py_modules->get_objecter().osd_command(
        osd_id,
        {cmd_json},
        {},
        &tid,
        &c->outbl,
        &c->outs,
        c);
  } else if (std::string(type) == "mds") {
    int r = self->py_modules->get_client().mds_command(
        name,
        {cmd_json},
        {},
        &c->outbl,
        &c->outs,
        c);
    if (r != 0) {
      string msg("failed to send command to mds: ");
      msg.append(cpp_strerror(r));
      PyErr_SetString(PyExc_RuntimeError, msg.c_str());
      return nullptr;
    }
  } else if (std::string(type) == "pg") {
    pg_t pgid;
    if (!pgid.parse(name)) {
      delete c;
      string msg("invalid pgid: ");
      msg.append("\"").append(name).append("\"");
      PyErr_SetString(PyExc_ValueError, msg.c_str());
      return nullptr;
    }

    ceph_tid_t tid;
    self->py_modules->get_objecter().pg_command(
        pgid,
        {cmd_json},
        {},
        &tid,
        &c->outbl,
        &c->outs,
        c);
    return nullptr;
  } else {
    delete c;
    string msg("unknown service type: ");
    msg.append(type);
    PyErr_SetString(PyExc_ValueError, msg.c_str());
    return nullptr;
  }

  Py_RETURN_NONE;
}

static PyObject*
ceph_set_health_checks(BaseMgrModule *self, PyObject *args)
{
  PyObject *checks = NULL;
  if (!PyArg_ParseTuple(args, "O:ceph_set_health_checks", &checks)) {
    return NULL;
  }
  if (!PyDict_Check(checks)) {
    derr << __func__ << " arg not a dict" << dendl;
    Py_RETURN_NONE;
  }
  PyObject *checksls = PyDict_Items(checks);
  health_check_map_t out_checks;
  for (int i = 0; i < PyList_Size(checksls); ++i) {
    PyObject *kv = PyList_GET_ITEM(checksls, i);
    char *check_name = nullptr;
    PyObject *check_info = nullptr;
    if (!PyArg_ParseTuple(kv, "sO:pair", &check_name, &check_info)) {
      derr << __func__ << " dict item " << i
	   << " not a size 2 tuple" << dendl;
      continue;
    }
    if (!PyDict_Check(check_info)) {
      derr << __func__ << " item " << i << " " << check_name
	   << " value not a dict" << dendl;
      continue;
    }
    health_status_t severity = HEALTH_OK;
    string summary;
    list<string> detail;
    PyObject *infols = PyDict_Items(check_info);
    for (int j = 0; j < PyList_Size(infols); ++j) {
      PyObject *pair = PyList_GET_ITEM(infols, j);
      if (!PyTuple_Check(pair)) {
	derr << __func__ << " item " << i << " pair " << j
	     << " not a tuple" << dendl;
	continue;
      }
      char *k = nullptr;
      PyObject *v = nullptr;
      if (!PyArg_ParseTuple(pair, "sO:pair", &k, &v)) {
	derr << __func__ << " item " << i << " pair " << j
	     << " not a size 2 tuple" << dendl;
	continue;
      }
      string ks(k);
      if (ks == "severity") {
	if (!PyString_Check(v)) {
	  derr << __func__ << " check " << check_name
	       << " severity value not string" << dendl;
	  continue;
	}
	string vs(PyString_AsString(v));
	if (vs == "warning") {
	  severity = HEALTH_WARN;
	} else if (vs == "error") {
	  severity = HEALTH_ERR;
	}
      } else if (ks == "summary") {
	if (!PyString_Check(v)) {
	  derr << __func__ << " check " << check_name
	       << " summary value not string" << dendl;
	  continue;
	}
	summary = PyString_AsString(v);
      } else if (ks == "detail") {
	if (!PyList_Check(v)) {
	  derr << __func__ << " check " << check_name
	       << " detail value not list" << dendl;
	  continue;
	}
	for (int k = 0; k < PyList_Size(v); ++k) {
	  PyObject *di = PyList_GET_ITEM(v, k);
	  if (!PyString_Check(di)) {
	    derr << __func__ << " check " << check_name
		 << " detail item " << k << " not a string" << dendl;
	    continue;
	  }
	  detail.push_back(PyString_AsString(di));
	}
      } else {
	derr << __func__ << " check " << check_name
	     << " unexpected key " << k << dendl;
      }
    }
    auto& d = out_checks.add(check_name, severity, summary);
    d.detail.swap(detail);
  }

  JSONFormatter jf(true);
  dout(10) << "module " << self->this_module->get_name()
          << " health checks:\n";
  out_checks.dump(&jf);
  jf.flush(*_dout);
  *_dout << dendl;

  PyThreadState *tstate = PyEval_SaveThread();
  self->py_modules->set_health_checks(self->this_module->get_name(),
                                      std::move(out_checks));
  PyEval_RestoreThread(tstate);
  
  Py_RETURN_NONE;
}


static PyObject*
ceph_state_get(BaseMgrModule *self, PyObject *args)
{
  char *what = NULL;
  if (!PyArg_ParseTuple(args, "s:ceph_state_get", &what)) {
    return NULL;
  }

  return self->py_modules->get_python(what);
}


static PyObject*
ceph_get_server(BaseMgrModule *self, PyObject *args)
{
  char *hostname = NULL;
  if (!PyArg_ParseTuple(args, "z:ceph_get_server", &hostname)) {
    return NULL;
  }

  if (hostname) {
    return self->py_modules->get_server_python(hostname);
  } else {
    return self->py_modules->list_servers_python();
  }
}

static PyObject*
ceph_get_mgr_id(BaseMgrModule *self, PyObject *args)
{
  return PyString_FromString(g_conf->name.get_id().c_str());
}

static PyObject*
ceph_config_get(BaseMgrModule *self, PyObject *args)
{
  char *what = nullptr;
  if (!PyArg_ParseTuple(args, "s:ceph_config_get", &what)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  std::string value;
  bool found = self->py_modules->get_config(self->this_module->get_name(),
      what, &value);
  if (found) {
    dout(10) << "ceph_config_get " << what << " found: " << value.c_str() << dendl;
    return PyString_FromString(value.c_str());
  } else {
    dout(4) << "ceph_config_get " << what << " not found " << dendl;
    Py_RETURN_NONE;
  }
}

static PyObject*
ceph_config_get_prefix(BaseMgrModule *self, PyObject *args)
{
  char *prefix = nullptr;
  if (!PyArg_ParseTuple(args, "s:ceph_config_get", &prefix)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  return self->py_modules->get_config_prefix(self->this_module->get_name(),
      prefix);
}

static PyObject*
ceph_config_set(BaseMgrModule *self, PyObject *args)
{
  char *key = nullptr;
  char *value = nullptr;
  if (!PyArg_ParseTuple(args, "sz:ceph_config_set", &key, &value)) {
    return nullptr;
  }
  boost::optional<string> val;
  if (value) {
    val = value;
  }
  self->py_modules->set_config(self->this_module->get_name(), key, val);

  Py_RETURN_NONE;
}

static PyObject*
get_metadata(BaseMgrModule *self, PyObject *args)
{
  char *svc_name = NULL;
  char *svc_id = NULL;
  if (!PyArg_ParseTuple(args, "ss:get_metadata", &svc_name, &svc_id)) {
    return nullptr;
  }
  return self->py_modules->get_metadata_python(svc_name, svc_id);
}

static PyObject*
get_daemon_status(BaseMgrModule *self, PyObject *args)
{
  char *svc_name = NULL;
  char *svc_id = NULL;
  if (!PyArg_ParseTuple(args, "ss:get_daemon_status", &svc_name,
			&svc_id)) {
    return nullptr;
  }
  return self->py_modules->get_daemon_status_python(svc_name, svc_id);
}

static PyObject*
ceph_log(BaseMgrModule *self, PyObject *args)
{

  int level = 0;
  char *record = nullptr;
  if (!PyArg_ParseTuple(args, "is:log", &level, &record)) {
    return nullptr;
  }

  assert(self->this_module);

  self->this_module->log(level, record);

  Py_RETURN_NONE;
}

static PyObject *
ceph_get_version(BaseMgrModule *self, PyObject *args)
{
  return PyString_FromString(pretty_version_to_str().c_str());
}

static PyObject *
ceph_get_context(BaseMgrModule *self, PyObject *args)
{
  return self->py_modules->get_context();
}

static PyObject*
get_counter(BaseMgrModule *self, PyObject *args)
{
  char *svc_name = nullptr;
  char *svc_id = nullptr;
  char *counter_path = nullptr;
  if (!PyArg_ParseTuple(args, "sss:get_counter", &svc_name,
                                                  &svc_id, &counter_path)) {
    return nullptr;
  }
  return self->py_modules->get_counter_python(
      svc_name, svc_id, counter_path);
}

static PyObject*
get_perf_schema(BaseMgrModule *self, PyObject *args)
{
  char *type_str = nullptr;
  char *svc_id = nullptr;
  if (!PyArg_ParseTuple(args, "ss:get_perf_schema", &type_str,
                                                    &svc_id)) {
    return nullptr;
  }

  return self->py_modules->get_perf_schema_python(type_str, svc_id);
}

static PyObject *
ceph_get_osdmap(BaseMgrModule *self, PyObject *args)
{
  return self->py_modules->get_osdmap();
}

static PyObject*
ceph_set_uri(BaseMgrModule *self, PyObject *args)
{
  char *svc_str = nullptr;
  if (!PyArg_ParseTuple(args, "s:ceph_advertize_service",
        &svc_str)) {
    return nullptr;
  }

  // We call down into PyModules even though we have a MgrPyModule
  // reference here, because MgrPyModule's fields are protected
  // by PyModules' lock.
  PyThreadState *tstate = PyEval_SaveThread();
  self->py_modules->set_uri(self->this_module->get_name(), svc_str);
  PyEval_RestoreThread(tstate);

  Py_RETURN_NONE;
}


PyMethodDef BaseMgrModule_methods[] = {
  {"_ceph_get", (PyCFunction)ceph_state_get, METH_VARARGS,
   "Get a cluster object"},

  {"_ceph_get_server", (PyCFunction)ceph_get_server, METH_VARARGS,
   "Get a server object"},

  {"_ceph_get_metadata", (PyCFunction)get_metadata, METH_VARARGS,
   "Get a service's metadata"},

  {"_ceph_get_daemon_status", (PyCFunction)get_daemon_status, METH_VARARGS,
   "Get a service's status"},

  {"_ceph_send_command", (PyCFunction)ceph_send_command, METH_VARARGS,
   "Send a mon command"},

  {"_ceph_set_health_checks", (PyCFunction)ceph_set_health_checks, METH_VARARGS,
   "Set health checks for this module"},

  {"_ceph_get_mgr_id", (PyCFunction)ceph_get_mgr_id, METH_NOARGS,
   "Get the name of the Mgr daemon where we are running"},

  {"_ceph_get_config", (PyCFunction)ceph_config_get, METH_VARARGS,
   "Get a configuration value"},

  {"_ceph_get_config_prefix", (PyCFunction)ceph_config_get_prefix, METH_VARARGS,
   "Get all configuration values with a given prefix"},

  {"_ceph_set_config", (PyCFunction)ceph_config_set, METH_VARARGS,
   "Set a configuration value"},

  {"_ceph_get_counter", (PyCFunction)get_counter, METH_VARARGS,
    "Get a performance counter"},

  {"_ceph_get_perf_schema", (PyCFunction)get_perf_schema, METH_VARARGS,
    "Get the performance counter schema"},

  {"_ceph_log", (PyCFunction)ceph_log, METH_VARARGS,
   "Emit a (local) log message"},

  {"_ceph_get_version", (PyCFunction)ceph_get_version, METH_VARARGS,
   "Get the ceph version of this process"},

  {"_ceph_get_context", (PyCFunction)ceph_get_context, METH_NOARGS,
    "Get a CephContext* in a python capsule"},

  {"_ceph_get_osdmap", (PyCFunction)ceph_get_osdmap, METH_NOARGS,
    "Get an OSDMap* in a python capsule"},

  {"_ceph_set_uri", (PyCFunction)ceph_set_uri, METH_VARARGS,
    "Advertize a service URI served by this module"},

  {NULL, NULL, 0, NULL}
};


static PyObject *
BaseMgrModule_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    BaseMgrModule *self;

    self = (BaseMgrModule *)type->tp_alloc(type, 0);

    return (PyObject *)self;
}

static int
BaseMgrModule_init(BaseMgrModule *self, PyObject *args, PyObject *kwds)
{
    PyObject *py_modules_capsule = nullptr;
    PyObject *this_module_capsule = nullptr;
    static const char *kwlist[] = {"py_modules", "this_module", NULL};

    if (! PyArg_ParseTupleAndKeywords(args, kwds, "OO",
                                      const_cast<char**>(kwlist),
                                      &py_modules_capsule,
                                      &this_module_capsule)) {
        return -1;
    }

    self->py_modules = (ActivePyModules*)PyCapsule_GetPointer(
        py_modules_capsule, nullptr);
    assert(self->py_modules);
    self->this_module = (ActivePyModule*)PyCapsule_GetPointer(
        this_module_capsule, nullptr);
    assert(self->this_module);

    return 0;
}

PyTypeObject BaseMgrModuleType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  "ceph_module.BaseMgrModule", /* tp_name */
  sizeof(BaseMgrModule),     /* tp_basicsize */
  0,                         /* tp_itemsize */
  0,                         /* tp_dealloc */
  0,                         /* tp_print */
  0,                         /* tp_getattr */
  0,                         /* tp_setattr */
  0,                         /* tp_compare */
  0,                         /* tp_repr */
  0,                         /* tp_as_number */
  0,                         /* tp_as_sequence */
  0,                         /* tp_as_mapping */
  0,                         /* tp_hash */
  0,                         /* tp_call */
  0,                         /* tp_str */
  0,                         /* tp_getattro */
  0,                         /* tp_setattro */
  0,                         /* tp_as_buffer */
  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,        /* tp_flags */
  "ceph-mgr Python Plugin", /* tp_doc */
  0,                         /* tp_traverse */
  0,                         /* tp_clear */
  0,                         /* tp_richcompare */
  0,                         /* tp_weaklistoffset */
  0,                         /* tp_iter */
  0,                         /* tp_iternext */
  BaseMgrModule_methods,     /* tp_methods */
  0,                         /* tp_members */
  0,                         /* tp_getset */
  0,                         /* tp_base */
  0,                         /* tp_dict */
  0,                         /* tp_descr_get */
  0,                         /* tp_descr_set */
  0,                         /* tp_dictoffset */
  (initproc)BaseMgrModule_init,                         /* tp_init */
  0,                         /* tp_alloc */
  BaseMgrModule_new,     /* tp_new */
};

