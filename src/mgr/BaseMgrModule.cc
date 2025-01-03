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
#include "mgr/Types.h"

#include "PyUtil.h"
#include "BaseMgrModule.h"
#include "Gil.h"

#include <algorithm>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

#define PLACEHOLDER ""

using std::list;
using std::string;

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
    ceph_assert(python_completion != nullptr);
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
    ceph_assert(python_completion != nullptr);

    dout(10) << "MonCommandCompletion::finish()" << dendl;
    {
      // Scoped so the Gil is released before calling notify_all()
      // Create new thread state because this is called via the MonClient
      // Finisher, not the PyModules finisher.
      Gil gil(pThreadState, true);

      auto set_fn = PyObject_GetAttrString(python_completion, "complete");
      ceph_assert(set_fn != nullptr);
      auto rtn = PyObject_CallFunction(set_fn, "(iss)", r, outbl.to_str().c_str(), outs.c_str());
      if (rtn == nullptr) {
        PyErr_Print();
      } else {
        Py_DECREF(rtn);
      }
      Py_DECREF(set_fn);
      Py_DECREF(python_completion);
      python_completion = nullptr;
    }
    py_modules->notify_all("command", tag);
  }
};


static PyObject*
ceph_send_command(BaseMgrModule *self, PyObject *args, PyObject *kwargs)
{
  // Like mon, osd, mds
  char *type = nullptr;

  // Like "23" for an OSD or "myid" for an MDS
  char *name = nullptr;

  char *cmd_json = nullptr;
  char *tag = nullptr;
  char *inbuf_ptr = nullptr;
  Py_ssize_t inbuf_len = 0;
  bufferlist inbuf = {};

  static const char * keywords[] {
    "result",
    "svc_type",
    "svc_id",
    "command",
    "tag",
    "inbuf",
    // --- kwargs star here
    "one_shot",   // whether to keep the command while we reestablish connection
    nullptr       // must be the last element
  };

  int one_shot = false;

  PyObject *completion = nullptr;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Ossssz#|$p:ceph_send_command", const_cast<char**>(keywords),
        &completion, &type, &name, &cmd_json, &tag, &inbuf_ptr, &inbuf_len, &one_shot)) {
    return nullptr;
  }

  if (inbuf_ptr) {
    inbuf.append(inbuf_ptr, (unsigned)inbuf_len);
  }

  auto set_fn = PyObject_GetAttrString(completion, "complete");
  if (set_fn == nullptr) {
    ceph_abort();  // TODO raise python exception instead
  } else {
    ceph_assert(PyCallable_Check(set_fn));
  }
  Py_DECREF(set_fn);

  MonCommandCompletion *command_c = new MonCommandCompletion(self->py_modules,
      completion, tag, PyThreadState_Get());

  PyThreadState *tstate = PyEval_SaveThread();

  if (std::string(type) == "mon") {

    // Wait for the latest OSDMap after each command we send to
    // the mons.  This is a heavy-handed hack to make life simpler
    // for python module authors, so that they know whenever they
    // run a command they've gt a fresh OSDMap afterwards.
    // TODO: enhance MCommand interface so that it returns
    // latest cluster map versions on completion, and callers
    // can wait for those.
    auto c = new LambdaContext([command_c, self](int command_r){
      self->py_modules->get_objecter().wait_for_latest_osdmap(
	[command_c, command_r](boost::system::error_code) {
	  command_c->complete(command_r);
	});
    });

    self->py_modules->get_monc().start_mon_command(
        name,
        {cmd_json},
        inbuf,
        &command_c->outbl,
        &command_c->outs,
        new C_OnFinisher(c, &self->py_modules->cmd_finisher));
  } else if (std::string(type) == "osd") {
    std::string err;
    uint64_t osd_id = strict_strtoll(name, 10, &err);
    if (!err.empty()) {
      delete command_c;
      string msg("invalid osd_id: ");
      msg.append("\"").append(name).append("\"");
      PyEval_RestoreThread(tstate);
      PyErr_SetString(PyExc_ValueError, msg.c_str());
      return nullptr;
    }

    ceph_tid_t tid;
    self->py_modules->get_objecter().osd_command(
        osd_id,
        {cmd_json},
        inbuf,
        &tid,
	[command_c, f = &self->py_modules->cmd_finisher]
	(boost::system::error_code ec, std::string s, ceph::buffer::list bl) {
	  command_c->outs = std::move(s);
	  command_c->outbl = std::move(bl);
	  f->queue(command_c);
	});
  } else if (std::string(type) == "mds") {
    int r = self->py_modules->get_client().mds_command(
        name,
        {cmd_json},
        inbuf,
        &command_c->outbl,
        &command_c->outs,
        new C_OnFinisher(command_c, &self->py_modules->cmd_finisher),
        one_shot);
    if (r != 0) {
      string msg("failed to send command to mds: ");
      msg.append(cpp_strerror(r));
      PyEval_RestoreThread(tstate);
      PyErr_SetString(PyExc_RuntimeError, msg.c_str());
      return nullptr;
    }
  } else if (std::string(type) == "pg") {
    pg_t pgid;
    if (!pgid.parse(name)) {
      delete command_c;
      string msg("invalid pgid: ");
      msg.append("\"").append(name).append("\"");
      PyEval_RestoreThread(tstate);
      PyErr_SetString(PyExc_ValueError, msg.c_str());
      return nullptr;
    }

    ceph_tid_t tid;
    self->py_modules->get_objecter().pg_command(
        pgid,
        {cmd_json},
        inbuf,
        &tid,
	[command_c, f = &self->py_modules->cmd_finisher]
	(boost::system::error_code ec, std::string s, ceph::buffer::list bl) {
	  command_c->outs = std::move(s);
	  command_c->outbl = std::move(bl);
	  f->queue(command_c);
	});
    PyEval_RestoreThread(tstate);
    return nullptr;
  } else {
    delete command_c;
    string msg("unknown service type: ");
    msg.append(type);
    PyEval_RestoreThread(tstate);
    PyErr_SetString(PyExc_ValueError, msg.c_str());
    return nullptr;
  }

  PyEval_RestoreThread(tstate);
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
    int64_t count = 0;
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
	if (!PyUnicode_Check(v)) {
	  derr << __func__ << " check " << check_name
	       << " severity value not string" << dendl;
	  continue;
	}
	if (const string vs = PyUnicode_AsUTF8(v); vs == "warning") {
	  severity = HEALTH_WARN;
	} else if (vs == "error") {
	  severity = HEALTH_ERR;
	}
      } else if (ks == "summary") {
	if (!PyUnicode_Check(v)) {
	  derr << __func__ << " check " << check_name
	       << " summary value not [unicode] string" << dendl;
	  continue;
	} else {
	  summary = PyUnicode_AsUTF8(v);
	}
      } else if (ks == "count") {
	if (PyLong_Check(v)) {
	  count = PyLong_AsLong(v);
	} else {
	  derr << __func__ << " check " << check_name
	       << " count value not int" << dendl;
	  continue;
	}
      } else if (ks == "detail") {
	if (!PyList_Check(v)) {
	  derr << __func__ << " check " << check_name
	       << " detail value not list" << dendl;
	  continue;
	}
	for (int k = 0; k < PyList_Size(v); ++k) {
	  PyObject *di = PyList_GET_ITEM(v, k);
	  if (!PyUnicode_Check(di)) {
	    derr << __func__ << " check " << check_name
		 << " detail item " << k << " not a [unicode] string" << dendl;
	    continue;
	  } else {
	    detail.push_back(PyUnicode_AsUTF8(di));
	  }
	}
      } else {
	derr << __func__ << " check " << check_name
	     << " unexpected key " << k << dendl;
      }
    }
    auto& d = out_checks.add(check_name, severity, summary, count);
    d.detail.swap(detail);
  }

  JSONFormatter jf(true);
  dout(10) << "module " << self->this_module->get_name()
          << " health checks:\n";
  out_checks.dump(&jf);
  jf.flush(*_dout);
  *_dout << dendl;
  without_gil([&] {
    self->py_modules->set_health_checks(self->this_module->get_name(),
					std::move(out_checks));
  });
  Py_RETURN_NONE;
}


static PyObject*
ceph_state_get(BaseMgrModule *self, PyObject *args)
{
  char *what = NULL;
  if (!PyArg_ParseTuple(args, "s:ceph_state_get", &what)) {
    return NULL;
  }

  return self->py_modules->cacheable_get_python(what);
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
  return PyUnicode_FromString(g_conf()->name.get_id().c_str());
}

static PyObject*
ceph_option_get(BaseMgrModule *self, PyObject *args)
{
  char *what = nullptr;
  if (!PyArg_ParseTuple(args, "s:ceph_option_get", &what)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  const Option *opt = g_conf().find_option(string(what));
  if (opt) {
    std::string value;
    switch (int r = g_conf().get_val(string(what), &value); r) {
    case -ENOMEM:
      PyErr_NoMemory();
      return nullptr;
    case -ENAMETOOLONG:
      PyErr_SetString(PyExc_ValueError, "value too long");
      return nullptr;
    default:
      ceph_assert(r == 0);
      break;
    }
    dout(10) << "ceph_option_get " << what << " found: " << value << dendl;
    return get_python_typed_option_value(opt->type, value);
  } else {
    dout(4) << "ceph_option_get " << what << " not found " << dendl;
    PyErr_Format(PyExc_KeyError, "option not found: %s", what);
    return nullptr;
  }
}

static PyObject*
ceph_foreign_option_get(BaseMgrModule *self, PyObject *args)
{
  char *who = nullptr;
  char *what = nullptr;
  if (!PyArg_ParseTuple(args, "ss:ceph_foreign_option_get", &who, &what)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }
  return self->py_modules->get_foreign_config(who, what);
}

static PyObject*
ceph_get_module_option(BaseMgrModule *self, PyObject *args)
{
  char *module = nullptr;
  char *key = nullptr;
  char *prefix = nullptr;
  if (!PyArg_ParseTuple(args, "ss|s:ceph_get_module_option", &module, &key,
			&prefix)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }
  std::string str_prefix;
  if (prefix) {
    str_prefix = prefix;
  }
  assert(self->this_module->py_module);
  auto pResult = self->py_modules->get_typed_config(module, key, str_prefix);
  return pResult;
}

static PyObject*
ceph_store_get_prefix(BaseMgrModule *self, PyObject *args)
{
  char *prefix = nullptr;
  if (!PyArg_ParseTuple(args, "s:ceph_store_get_prefix", &prefix)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  return self->py_modules->get_store_prefix(self->this_module->get_name(),
      prefix);
}

static PyObject*
ceph_set_module_option(BaseMgrModule *self, PyObject *args)
{
  char *module = nullptr;
  char *key = nullptr;
  char *value = nullptr;
  if (!PyArg_ParseTuple(args, "ssz:ceph_set_module_option",
        &module, &key, &value)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }
  std::optional<string> val;
  if (value) {
    val = value;
  }
  auto [ret, msg] = without_gil([&] {
    return self->py_modules->set_config(module, key, val);
  });
  if (ret) {
    PyErr_SetString(PyExc_ValueError, msg.c_str());
    return nullptr;
  }
  Py_RETURN_NONE;
}

static PyObject*
ceph_store_get(BaseMgrModule *self, PyObject *args)
{
  char *what = nullptr;
  if (!PyArg_ParseTuple(args, "s:ceph_store_get", &what)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  std::string value;
  bool found = self->py_modules->get_store(self->this_module->get_name(),
      what, &value);
  if (found) {
    dout(10) << "ceph_store_get " << what << " found: " << value.c_str() << dendl;
    return PyUnicode_FromString(value.c_str());
  } else {
    dout(4) << "ceph_store_get " << what << " not found " << dendl;
    Py_RETURN_NONE;
  }
}

static PyObject*
ceph_store_set(BaseMgrModule *self, PyObject *args)
{
  char *key = nullptr;
  char *value = nullptr;
  if (!PyArg_ParseTuple(args, "sz:ceph_store_set", &key, &value)) {
    return nullptr;
  }
  std::optional<string> val;
  if (value) {
    val = value;
  }
  without_gil([&] {
    self->py_modules->set_store(self->this_module->get_name(), key, val);
  });
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
  char *record = nullptr;
  if (!PyArg_ParseTuple(args, "s:log", &record)) {
    return nullptr;
  }

  ceph_assert(self->this_module);

  self->this_module->log(record);

  Py_RETURN_NONE;
}

static PyObject*
ceph_cluster_log(BaseMgrModule *self, PyObject *args)
{
  int prio = 0;
  char *channel = nullptr;
  char *message = nullptr;

  if (!PyArg_ParseTuple(args, "sis:ceph_cluster_log", &channel, &prio, &message)) {
    return nullptr;
  }
  without_gil([&] {
    self->py_modules->cluster_log(channel, (clog_type)prio, message);
  });
  Py_RETURN_NONE;
}

static PyObject *
ceph_get_version(BaseMgrModule *self, PyObject *args)
{
  return PyUnicode_FromString(pretty_version_to_str().c_str());
}

static PyObject *
ceph_get_ceph_conf_path(BaseMgrModule *self, PyObject *args)
{
  return PyUnicode_FromString(g_conf().get_conf_path().c_str());
}

static PyObject *
ceph_get_release_name(BaseMgrModule *self, PyObject *args)
{
  return PyUnicode_FromString(ceph_release_to_str());
}

static PyObject *
ceph_lookup_release_name(BaseMgrModule *self, PyObject *args)
{
  int major = 0;
  if (!PyArg_ParseTuple(args, "i:ceph_lookup_release_name", &major)) {
    return nullptr;
  }
  return PyUnicode_FromString(ceph_release_name(major));
}

static PyObject *
ceph_get_context(BaseMgrModule *self)
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
get_latest_counter(BaseMgrModule *self, PyObject *args)
{
  char *svc_name = nullptr;
  char *svc_id = nullptr;
  char *counter_path = nullptr;
  if (!PyArg_ParseTuple(args, "sss:get_counter", &svc_name,
                                                  &svc_id, &counter_path)) {
    return nullptr;
  }
  return self->py_modules->get_latest_counter_python(
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

static PyObject*
ceph_get_rocksdb_version(BaseMgrModule *self)
{
  return self->py_modules->get_rocksdb_version();
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
  without_gil([&] {
    self->py_modules->set_uri(self->this_module->get_name(), svc_str);
  });
  Py_RETURN_NONE;
}

static PyObject*
ceph_set_wear_level(BaseMgrModule *self, PyObject *args)
{
  char *devid = nullptr;
  float wear_level;
  if (!PyArg_ParseTuple(args, "sf:ceph_set_wear_level",
			&devid, &wear_level)) {
    return nullptr;
  }
  without_gil([&] {
    self->py_modules->set_device_wear_level(devid, wear_level);
  });
  Py_RETURN_NONE;
}

static PyObject*
ceph_have_mon_connection(BaseMgrModule *self, PyObject *args)
{
  if (self->py_modules->get_monc().is_connected()) {
    Py_RETURN_TRUE;
  } else {
    Py_RETURN_FALSE;
  }
}

static PyObject*
ceph_update_progress_event(BaseMgrModule *self, PyObject *args)
{
  char *evid = nullptr;
  char *desc = nullptr;
  float progress = 0.0;
  bool add_to_ceph_s = false;
  if (!PyArg_ParseTuple(args, "ssfb:ceph_update_progress_event",
			&evid, &desc, &progress, &add_to_ceph_s)) {
    return nullptr;
  }
  without_gil([&] {
    self->py_modules->update_progress_event(evid, desc, progress, add_to_ceph_s);
  });
  Py_RETURN_NONE;
}

static PyObject*
ceph_complete_progress_event(BaseMgrModule *self, PyObject *args)
{
  char *evid = nullptr;
  if (!PyArg_ParseTuple(args, "s:ceph_complete_progress_event",
			&evid)) {
    return nullptr;
  }
  without_gil([&] {
    self->py_modules->complete_progress_event(evid);
  });
  Py_RETURN_NONE;
}

static PyObject*
ceph_clear_all_progress_events(BaseMgrModule *self, PyObject *args)
{
  without_gil([&] {
    self->py_modules->clear_all_progress_events();
  });
  Py_RETURN_NONE;
}



static PyObject *
ceph_dispatch_remote(BaseMgrModule *self, PyObject *args)
{
  char *other_module = nullptr;
  char *method = nullptr;
  PyObject *remote_args = nullptr;
  PyObject *remote_kwargs = nullptr;
  if (!PyArg_ParseTuple(args, "ssOO:ceph_dispatch_remote",
        &other_module, &method, &remote_args, &remote_kwargs)) {
    return nullptr;
  }

  // Early error handling, because if the module doesn't exist then we
  // won't be able to use its thread state to set python error state
  // inside dispatch_remote().
  if (!self->py_modules->module_exists(other_module)) {
    derr << "no module '" << other_module << "'" << dendl;
    PyErr_SetString(PyExc_ImportError, "Module not found");
    return nullptr;
  }

  // Drop GIL from calling python thread state, it will be taken
  // both for checking for method existence and for executing method.
  PyThreadState *tstate = PyEval_SaveThread();

  if (!self->py_modules->method_exists(other_module, method)) {
    PyEval_RestoreThread(tstate);
    PyErr_SetString(PyExc_NameError, "Method not found");
    return nullptr;
  }

  std::string err;
  auto result = self->py_modules->dispatch_remote(other_module, method,
      remote_args, remote_kwargs, &err);

  PyEval_RestoreThread(tstate);

  if (result == nullptr) {
    std::stringstream ss;
    ss << "Remote method threw exception: " << err;
    PyErr_SetString(PyExc_RuntimeError, ss.str().c_str());
    derr << ss.str() << dendl;
  }

  return result;
}

static PyObject*
ceph_add_osd_perf_query(BaseMgrModule *self, PyObject *args)
{
  static const std::string NAME_KEY_DESCRIPTOR = "key_descriptor";
  static const std::string NAME_COUNTERS_DESCRIPTORS =
      "performance_counter_descriptors";
  static const std::string NAME_LIMIT = "limit";
  static const std::string NAME_SUB_KEY_TYPE = "type";
  static const std::string NAME_SUB_KEY_REGEX = "regex";
  static const std::string NAME_LIMIT_ORDER_BY = "order_by";
  static const std::string NAME_LIMIT_MAX_COUNT = "max_count";
  static const std::map<std::string, OSDPerfMetricSubKeyType> sub_key_types = {
    {"client_id", OSDPerfMetricSubKeyType::CLIENT_ID},
    {"client_address", OSDPerfMetricSubKeyType::CLIENT_ADDRESS},
    {"pool_id", OSDPerfMetricSubKeyType::POOL_ID},
    {"namespace", OSDPerfMetricSubKeyType::NAMESPACE},
    {"osd_id", OSDPerfMetricSubKeyType::OSD_ID},
    {"pg_id", OSDPerfMetricSubKeyType::PG_ID},
    {"object_name", OSDPerfMetricSubKeyType::OBJECT_NAME},
    {"snap_id", OSDPerfMetricSubKeyType::SNAP_ID},
  };
  static const std::map<std::string, PerformanceCounterType> counter_types = {
    {"ops", PerformanceCounterType::OPS},
    {"write_ops", PerformanceCounterType::WRITE_OPS},
    {"read_ops", PerformanceCounterType::READ_OPS},
    {"bytes", PerformanceCounterType::BYTES},
    {"write_bytes", PerformanceCounterType::WRITE_BYTES},
    {"read_bytes", PerformanceCounterType::READ_BYTES},
    {"latency", PerformanceCounterType::LATENCY},
    {"write_latency", PerformanceCounterType::WRITE_LATENCY},
    {"read_latency", PerformanceCounterType::READ_LATENCY},
  };

  PyObject *py_query = nullptr;
  if (!PyArg_ParseTuple(args, "O:ceph_add_osd_perf_query", &py_query)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }
  if (!PyDict_Check(py_query)) {
    derr << __func__ << " arg not a dict" << dendl;
    Py_RETURN_NONE;
  }

  PyObject *query_params = PyDict_Items(py_query);
  OSDPerfMetricQuery query;
  std::optional<OSDPerfMetricLimit> limit;

  // {
  //   'key_descriptor': [
  //     {'type': subkey_type, 'regex': regex_pattern},
  //     ...
  //   ],
  //   'performance_counter_descriptors': [
  //     list, of, descriptor, types
  //   ],
  //   'limit': {'order_by': performance_counter_type, 'max_count': n},
  // }

  for (int i = 0; i < PyList_Size(query_params); ++i) {
    PyObject *kv = PyList_GET_ITEM(query_params, i);
    char *query_param_name = nullptr;
    PyObject *query_param_val = nullptr;
    if (!PyArg_ParseTuple(kv, "sO:pair", &query_param_name, &query_param_val)) {
      derr << __func__ << " dict item " << i << " not a size 2 tuple" << dendl;
      Py_RETURN_NONE;
    }
    if (query_param_name == NAME_KEY_DESCRIPTOR) {
      if (!PyList_Check(query_param_val)) {
        derr << __func__ << " " << query_param_name << " not a list" << dendl;
        Py_RETURN_NONE;
      }
      for (int j = 0; j < PyList_Size(query_param_val); j++) {
        PyObject *sub_key = PyList_GET_ITEM(query_param_val, j);
        if (!PyDict_Check(sub_key)) {
          derr << __func__ << " query " << query_param_name << " item " << j
               << " not a dict" << dendl;
          Py_RETURN_NONE;
        }
        OSDPerfMetricSubKeyDescriptor d;
        PyObject *sub_key_params = PyDict_Items(sub_key);
        for (int k = 0; k < PyList_Size(sub_key_params); ++k) {
          PyObject *pair = PyList_GET_ITEM(sub_key_params, k);
          if (!PyTuple_Check(pair)) {
            derr << __func__ << " query " << query_param_name << " item " << j
                 << " pair " << k << " not a tuple" << dendl;
            Py_RETURN_NONE;
          }
          char *param_name = nullptr;
          PyObject *param_value = nullptr;
          if (!PyArg_ParseTuple(pair, "sO:pair", &param_name, &param_value)) {
            derr << __func__ << " query " << query_param_name << " item " << j
                 << " pair " << k << " not a size 2 tuple" << dendl;
            Py_RETURN_NONE;
          }
          if (param_name == NAME_SUB_KEY_TYPE) {
            if (!PyUnicode_Check(param_value)) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " contains invalid param " << param_name << dendl;
              Py_RETURN_NONE;
            }
            auto type = PyUnicode_AsUTF8(param_value);
            auto it = sub_key_types.find(type);
            if (it == sub_key_types.end()) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " contains invalid type " << dendl;
              Py_RETURN_NONE;
            }
            d.type = it->second;
          } else if (param_name == NAME_SUB_KEY_REGEX) {
            if (!PyUnicode_Check(param_value)) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " contains invalid param " << param_name << dendl;
              Py_RETURN_NONE;
            }
            d.regex_str = PyUnicode_AsUTF8(param_value);
            try {
              d.regex = d.regex_str.c_str();
            } catch (const std::regex_error& e) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " contains invalid regex " << d.regex_str << dendl;
              Py_RETURN_NONE;
            }
            if (d.regex.mark_count() == 0) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " regex " << d.regex_str << ": no capturing groups"
                   << dendl;
              Py_RETURN_NONE;
            }
          } else {
            derr << __func__ << " query " << query_param_name << " item " << j
                 << " contains invalid param " << param_name << dendl;
            Py_RETURN_NONE;
          }
        }
        if (d.type == static_cast<OSDPerfMetricSubKeyType>(-1) ||
            d.regex_str.empty()) {
          derr << __func__ << " query " << query_param_name << " item " << i
               << " invalid" << dendl;
          Py_RETURN_NONE;
        }
        query.key_descriptor.push_back(d);
      }
    } else if (query_param_name == NAME_COUNTERS_DESCRIPTORS) {
      if (!PyList_Check(query_param_val)) {
        derr << __func__ << " " << query_param_name << " not a list" << dendl;
        Py_RETURN_NONE;
      }
      for (int j = 0; j < PyList_Size(query_param_val); j++) {
        PyObject *py_type = PyList_GET_ITEM(query_param_val, j);
        if (!PyUnicode_Check(py_type)) {
          derr << __func__ << " query " << query_param_name << " item " << j
               << " not a string" << dendl;
          Py_RETURN_NONE;
        }
        auto type = PyUnicode_AsUTF8(py_type);
        auto it = counter_types.find(type);
        if (it == counter_types.end()) {
          derr << __func__ << " query " << query_param_name << " item " << type
               << " is not valid type" << dendl;
          Py_RETURN_NONE;
        }
        query.performance_counter_descriptors.push_back(it->second);
      }
    } else if (query_param_name == NAME_LIMIT) {
      if (!PyDict_Check(query_param_val)) {
        derr << __func__ << " query " << query_param_name << " not a dict"
             << dendl;
        Py_RETURN_NONE;
      }

      limit = OSDPerfMetricLimit();
      PyObject *limit_params = PyDict_Items(query_param_val);

      for (int j = 0; j < PyList_Size(limit_params); ++j) {
        PyObject *kv = PyList_GET_ITEM(limit_params, j);
        char *limit_param_name = nullptr;
        PyObject *limit_param_val = nullptr;
        if (!PyArg_ParseTuple(kv, "sO:pair", &limit_param_name,
                              &limit_param_val)) {
          derr << __func__ << " limit item " << j << " not a size 2 tuple"
               << dendl;
          Py_RETURN_NONE;
        }

        if (limit_param_name == NAME_LIMIT_ORDER_BY) {
          if (!PyUnicode_Check(limit_param_val)) {
            derr << __func__ << " " << limit_param_name << " not a string"
                 << dendl;
            Py_RETURN_NONE;
          }
          auto order_by = PyUnicode_AsUTF8(limit_param_val);
          auto it = counter_types.find(order_by);
          if (it == counter_types.end()) {
            derr << __func__ << " limit " << limit_param_name
                 << " not a valid counter type" << dendl;
            Py_RETURN_NONE;
          }
          limit->order_by = it->second;
        } else if (limit_param_name == NAME_LIMIT_MAX_COUNT) {
          if (!PyLong_Check(limit_param_val)) {
            derr << __func__ << " " << limit_param_name << " not an int"
                 << dendl;
            Py_RETURN_NONE;
          }
          limit->max_count = PyLong_AsLong(limit_param_val);
        } else {
          derr << __func__ << " unknown limit param: " << limit_param_name
               << dendl;
          Py_RETURN_NONE;
        }
      }
    } else {
      derr << __func__ << " unknown query param: " << query_param_name << dendl;
      Py_RETURN_NONE;
    }
  }

  if (query.key_descriptor.empty() ||
      query.performance_counter_descriptors.empty()) {
    derr << __func__ << " invalid query" << dendl;
    Py_RETURN_NONE;
  }

  if (limit) {
    auto &ds = query.performance_counter_descriptors;
    if (std::find(ds.begin(), ds.end(), limit->order_by) == ds.end()) {
      derr << __func__ << " limit order_by " << limit->order_by
           << " not in performance_counter_descriptors" << dendl;
      Py_RETURN_NONE;
    }
  }

  auto query_id = self->py_modules->add_osd_perf_query(query, limit);
  return PyLong_FromLong(query_id);
}

static PyObject*
ceph_remove_osd_perf_query(BaseMgrModule *self, PyObject *args)
{
  MetricQueryID query_id;
  if (!PyArg_ParseTuple(args, "i:ceph_remove_osd_perf_query", &query_id)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  self->py_modules->remove_osd_perf_query(query_id);
  Py_RETURN_NONE;
}

static PyObject*
ceph_get_osd_perf_counters(BaseMgrModule *self, PyObject *args)
{
  MetricQueryID query_id;
  if (!PyArg_ParseTuple(args, "i:ceph_get_osd_perf_counters", &query_id)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  return self->py_modules->get_osd_perf_counters(query_id);
}

// MDS perf query interface -- mostly follows ceph_add_osd_perf_query()
// style

static PyObject*
ceph_add_mds_perf_query(BaseMgrModule *self, PyObject *args)
{
  static const std::string NAME_KEY_DESCRIPTOR = "key_descriptor";
  static const std::string NAME_COUNTERS_DESCRIPTORS =
      "performance_counter_descriptors";
  static const std::string NAME_LIMIT = "limit";
  static const std::string NAME_SUB_KEY_TYPE = "type";
  static const std::string NAME_SUB_KEY_REGEX = "regex";
  static const std::string NAME_LIMIT_ORDER_BY = "order_by";
  static const std::string NAME_LIMIT_MAX_COUNT = "max_count";
  static const std::map<std::string, MDSPerfMetricSubKeyType> sub_key_types = {
    {"mds_rank", MDSPerfMetricSubKeyType::MDS_RANK},
    {"client_id", MDSPerfMetricSubKeyType::CLIENT_ID},
  };
  static const std::map<std::string, MDSPerformanceCounterType> counter_types = {
    {"cap_hit", MDSPerformanceCounterType::CAP_HIT_METRIC},
    {"read_latency", MDSPerformanceCounterType::READ_LATENCY_METRIC},
    {"write_latency", MDSPerformanceCounterType::WRITE_LATENCY_METRIC},
    {"metadata_latency", MDSPerformanceCounterType::METADATA_LATENCY_METRIC},
    {"dentry_lease", MDSPerformanceCounterType::DENTRY_LEASE_METRIC},
    {"opened_files", MDSPerformanceCounterType::OPENED_FILES_METRIC},
    {"pinned_icaps", MDSPerformanceCounterType::PINNED_ICAPS_METRIC},
    {"opened_inodes", MDSPerformanceCounterType::OPENED_INODES_METRIC},
    {"read_io_sizes", MDSPerformanceCounterType::READ_IO_SIZES_METRIC},
    {"write_io_sizes", MDSPerformanceCounterType::WRITE_IO_SIZES_METRIC},
    {"avg_read_latency", MDSPerformanceCounterType::AVG_READ_LATENCY_METRIC},
    {"stdev_read_latency", MDSPerformanceCounterType::STDEV_READ_LATENCY_METRIC},
    {"avg_write_latency", MDSPerformanceCounterType::AVG_WRITE_LATENCY_METRIC},
    {"stdev_write_latency", MDSPerformanceCounterType::STDEV_WRITE_LATENCY_METRIC},
    {"avg_metadata_latency", MDSPerformanceCounterType::AVG_METADATA_LATENCY_METRIC},
    {"stdev_metadata_latency", MDSPerformanceCounterType::STDEV_METADATA_LATENCY_METRIC},
  };

  PyObject *py_query = nullptr;
  if (!PyArg_ParseTuple(args, "O:ceph_add_mds_perf_query", &py_query)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }
  if (!PyDict_Check(py_query)) {
    derr << __func__ << " arg not a dict" << dendl;
    Py_RETURN_NONE;
  }

  PyObject *query_params = PyDict_Items(py_query);
  MDSPerfMetricQuery query;
  std::optional<MDSPerfMetricLimit> limit;

  // {
  //   'key_descriptor': [
  //     {'type': subkey_type, 'regex': regex_pattern},
  //     ...
  //   ],
  //   'performance_counter_descriptors': [
  //     list, of, descriptor, types
  //   ],
  //   'limit': {'order_by': performance_counter_type, 'max_count': n},
  // }

  for (int i = 0; i < PyList_Size(query_params); ++i) {
    PyObject *kv = PyList_GET_ITEM(query_params, i);
    char *query_param_name = nullptr;
    PyObject *query_param_val = nullptr;
    if (!PyArg_ParseTuple(kv, "sO:pair", &query_param_name, &query_param_val)) {
      derr << __func__ << " dict item " << i << " not a size 2 tuple" << dendl;
      Py_RETURN_NONE;
    }
    if (query_param_name == NAME_KEY_DESCRIPTOR) {
      if (!PyList_Check(query_param_val)) {
        derr << __func__ << " " << query_param_name << " not a list" << dendl;
        Py_RETURN_NONE;
      }
      for (int j = 0; j < PyList_Size(query_param_val); j++) {
        PyObject *sub_key = PyList_GET_ITEM(query_param_val, j);
        if (!PyDict_Check(sub_key)) {
          derr << __func__ << " query " << query_param_name << " item " << j
               << " not a dict" << dendl;
          Py_RETURN_NONE;
        }
        MDSPerfMetricSubKeyDescriptor d;
        PyObject *sub_key_params = PyDict_Items(sub_key);
        for (int k = 0; k < PyList_Size(sub_key_params); ++k) {
          PyObject *pair = PyList_GET_ITEM(sub_key_params, k);
          if (!PyTuple_Check(pair)) {
            derr << __func__ << " query " << query_param_name << " item " << j
                 << " pair " << k << " not a tuple" << dendl;
            Py_RETURN_NONE;
          }
          char *param_name = nullptr;
          PyObject *param_value = nullptr;
          if (!PyArg_ParseTuple(pair, "sO:pair", &param_name, &param_value)) {
            derr << __func__ << " query " << query_param_name << " item " << j
                 << " pair " << k << " not a size 2 tuple" << dendl;
            Py_RETURN_NONE;
          }
          if (param_name == NAME_SUB_KEY_TYPE) {
            if (!PyUnicode_Check(param_value)) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " contains invalid param " << param_name << dendl;
              Py_RETURN_NONE;
            }
            auto type = PyUnicode_AsUTF8(param_value);
            auto it = sub_key_types.find(type);
            if (it == sub_key_types.end()) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " contains invalid type " << dendl;
              Py_RETURN_NONE;
            }
            d.type = it->second;
          } else if (param_name == NAME_SUB_KEY_REGEX) {
            if (!PyUnicode_Check(param_value)) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " contains invalid param " << param_name << dendl;
              Py_RETURN_NONE;
            }
            d.regex_str = PyUnicode_AsUTF8(param_value);
            try {
              d.regex = d.regex_str.c_str();
            } catch (const std::regex_error& e) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " contains invalid regex " << d.regex_str << dendl;
              Py_RETURN_NONE;
            }
            if (d.regex.mark_count() == 0) {
              derr << __func__ << " query " << query_param_name << " item " << j
                   << " regex " << d.regex_str << ": no capturing groups"
                   << dendl;
              Py_RETURN_NONE;
            }
          } else {
            derr << __func__ << " query " << query_param_name << " item " << j
                 << " contains invalid param " << param_name << dendl;
            Py_RETURN_NONE;
          }
        }
        if (d.type == static_cast<MDSPerfMetricSubKeyType>(-1) ||
            d.regex_str.empty()) {
          derr << __func__ << " query " << query_param_name << " item " << i
               << " invalid" << dendl;
          Py_RETURN_NONE;
        }
        query.key_descriptor.push_back(d);
      }
    } else if (query_param_name == NAME_COUNTERS_DESCRIPTORS) {
      if (!PyList_Check(query_param_val)) {
        derr << __func__ << " " << query_param_name << " not a list" << dendl;
        Py_RETURN_NONE;
      }
      for (int j = 0; j < PyList_Size(query_param_val); j++) {
        PyObject *py_type = PyList_GET_ITEM(query_param_val, j);
        if (!PyUnicode_Check(py_type)) {
          derr << __func__ << " query " << query_param_name << " item " << j
               << " not a string" << dendl;
          Py_RETURN_NONE;
        }
        auto type = PyUnicode_AsUTF8(py_type);
        auto it = counter_types.find(type);
        if (it == counter_types.end()) {
          derr << __func__ << " query " << query_param_name << " item " << type
               << " is not valid type" << dendl;
          Py_RETURN_NONE;
        }
        query.performance_counter_descriptors.push_back(it->second);
      }
    } else if (query_param_name == NAME_LIMIT) {
      if (!PyDict_Check(query_param_val)) {
        derr << __func__ << " query " << query_param_name << " not a dict"
             << dendl;
        Py_RETURN_NONE;
      }

      limit = MDSPerfMetricLimit();
      PyObject *limit_params = PyDict_Items(query_param_val);

      for (int j = 0; j < PyList_Size(limit_params); ++j) {
        PyObject *kv = PyList_GET_ITEM(limit_params, j);
        char *limit_param_name = nullptr;
        PyObject *limit_param_val = nullptr;
        if (!PyArg_ParseTuple(kv, "sO:pair", &limit_param_name,
                              &limit_param_val)) {
          derr << __func__ << " limit item " << j << " not a size 2 tuple"
               << dendl;
          Py_RETURN_NONE;
        }

        if (limit_param_name == NAME_LIMIT_ORDER_BY) {
          if (!PyUnicode_Check(limit_param_val)) {
            derr << __func__ << " " << limit_param_name << " not a string"
                 << dendl;
            Py_RETURN_NONE;
          }
          auto order_by = PyUnicode_AsUTF8(limit_param_val);
          auto it = counter_types.find(order_by);
          if (it == counter_types.end()) {
            derr << __func__ << " limit " << limit_param_name
                 << " not a valid counter type" << dendl;
            Py_RETURN_NONE;
          }
          limit->order_by = it->second;
        } else if (limit_param_name == NAME_LIMIT_MAX_COUNT) {
          if (!PyLong_Check(limit_param_val)) {
            derr << __func__ << " " << limit_param_name << " not an int"
                 << dendl;
            Py_RETURN_NONE;
          }
          limit->max_count = PyLong_AsLong(limit_param_val);
        } else {
          derr << __func__ << " unknown limit param: " << limit_param_name
               << dendl;
          Py_RETURN_NONE;
        }
      }
    } else {
      derr << __func__ << " unknown query param: " << query_param_name << dendl;
      Py_RETURN_NONE;
    }
  }

  if (query.key_descriptor.empty()) {
    derr << __func__ << " invalid query" << dendl;
    Py_RETURN_NONE;
  }

  if (limit) {
    auto &ds = query.performance_counter_descriptors;
    if (std::find(ds.begin(), ds.end(), limit->order_by) == ds.end()) {
      derr << __func__ << " limit order_by " << limit->order_by
           << " not in performance_counter_descriptors" << dendl;
      Py_RETURN_NONE;
    }
  }

  auto query_id = self->py_modules->add_mds_perf_query(query, limit);
  return PyLong_FromLong(query_id);
}

static PyObject*
ceph_remove_mds_perf_query(BaseMgrModule *self, PyObject *args)
{
  MetricQueryID query_id;
  if (!PyArg_ParseTuple(args, "i:ceph_remove_mds_perf_query", &query_id)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  self->py_modules->remove_mds_perf_query(query_id);
  Py_RETURN_NONE;
}

static PyObject*
ceph_reregister_mds_perf_queries(BaseMgrModule *self, PyObject *args)
{
  self->py_modules->reregister_mds_perf_queries();
  Py_RETURN_NONE;
}

static PyObject*
ceph_get_mds_perf_counters(BaseMgrModule *self, PyObject *args)
{
  MetricQueryID query_id;
  if (!PyArg_ParseTuple(args, "i:ceph_get_mds_perf_counters", &query_id)) {
    derr << "Invalid args!" << dendl;
    return nullptr;
  }

  return self->py_modules->get_mds_perf_counters(query_id);
}

static PyObject*
ceph_is_authorized(BaseMgrModule *self, PyObject *args)
{
  PyObject *args_dict = NULL;
  if (!PyArg_ParseTuple(args, "O:ceph_is_authorized", &args_dict)) {
    return nullptr;
  }

  if (!PyDict_Check(args_dict)) {
    derr << __func__ << " arg not a dict" << dendl;
    Py_RETURN_FALSE;
  }

  std::map<std::string, std::string> arguments;

  PyObject *args_list = PyDict_Items(args_dict);
  for (int i = 0; i < PyList_Size(args_list); ++i) {
    PyObject *kv = PyList_GET_ITEM(args_list, i);

    char *arg_key = nullptr;
    char *arg_value = nullptr;
    if (!PyArg_ParseTuple(kv, "ss:pair", &arg_key, &arg_value)) {
      derr << __func__ << " dict item " << i << " not a size 2 tuple" << dendl;
      continue;
    }

    arguments[arg_key] = arg_value;
  }

  bool r = without_gil([&] {
    return self->this_module->is_authorized(arguments);
  });

  if (r) {
    Py_RETURN_TRUE;
  }
  Py_RETURN_FALSE;
}

static PyObject*
ceph_register_client(BaseMgrModule *self, PyObject *args)
{
  const char* _name = nullptr;
  char* addrs = nullptr;
  int replace = 0;
  if (!PyArg_ParseTuple(args, "zsp:ceph_register_client", &_name, &addrs, &replace)) {
    return nullptr;
  }
  auto name = _name ? std::string(_name) : std::string(self->this_module->get_name());
  without_gil([&] {
    self->py_modules->register_client(name, addrs, replace);
  });
  Py_RETURN_NONE;
}

static PyObject*
ceph_unregister_client(BaseMgrModule *self, PyObject *args)
{
  const char* _name = nullptr;
  char* addrs = nullptr;
  if (!PyArg_ParseTuple(args, "zs:ceph_unregister_client", &_name, &addrs)) {
    return nullptr;
  }
  auto name = _name ? std::string(_name) : std::string(self->this_module->get_name());
  without_gil([&] {
    self->py_modules->unregister_client(name, addrs);
  });
  Py_RETURN_NONE;
}

static PyObject*
ceph_get_daemon_health_metrics(BaseMgrModule *self, PyObject *args)
{
  return self->py_modules->get_daemon_health_metrics();
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

  {"_ceph_send_command", (PyCFunction)ceph_send_command, METH_VARARGS | METH_KEYWORDS,
   "Send a mon command"},

  {"_ceph_set_health_checks", (PyCFunction)ceph_set_health_checks, METH_VARARGS,
   "Set health checks for this module"},

  {"_ceph_get_mgr_id", (PyCFunction)ceph_get_mgr_id, METH_NOARGS,
   "Get the name of the Mgr daemon where we are running"},

  {"_ceph_get_ceph_conf_path", (PyCFunction)ceph_get_ceph_conf_path, METH_NOARGS,
   "Get path to ceph.conf"},

  {"_ceph_get_option", (PyCFunction)ceph_option_get, METH_VARARGS,
   "Get a native configuration option value"},

  {"_ceph_get_foreign_option", (PyCFunction)ceph_foreign_option_get, METH_VARARGS,
   "Get a native configuration option value for another entity"},

  {"_ceph_get_module_option", (PyCFunction)ceph_get_module_option, METH_VARARGS,
   "Get a module configuration option value"},

  {"_ceph_get_store_prefix", (PyCFunction)ceph_store_get_prefix, METH_VARARGS,
   "Get all KV store values with a given prefix"},

  {"_ceph_set_module_option", (PyCFunction)ceph_set_module_option, METH_VARARGS,
   "Set a module configuration option value"},

  {"_ceph_get_store", (PyCFunction)ceph_store_get, METH_VARARGS,
   "Get a stored field"},

  {"_ceph_set_store", (PyCFunction)ceph_store_set, METH_VARARGS,
   "Set a stored field"},

  {"_ceph_get_counter", (PyCFunction)get_counter, METH_VARARGS,
    "Get a performance counter"},

  {"_ceph_get_latest_counter", (PyCFunction)get_latest_counter, METH_VARARGS,
    "Get the latest performance counter"},

  {"_ceph_get_perf_schema", (PyCFunction)get_perf_schema, METH_VARARGS,
    "Get the performance counter schema"},

  {"_ceph_get_rocksdb_version", (PyCFunction)ceph_get_rocksdb_version, METH_NOARGS,
    "Get the current RocksDB version number"},

  {"_ceph_log", (PyCFunction)ceph_log, METH_VARARGS,
   "Emit a (local) log message"},

  {"_ceph_cluster_log", (PyCFunction)ceph_cluster_log, METH_VARARGS,
   "Emit a cluster log message"},

  {"_ceph_get_version", (PyCFunction)ceph_get_version, METH_NOARGS,
   "Get the ceph version of this process"},

  {"_ceph_get_release_name", (PyCFunction)ceph_get_release_name, METH_NOARGS,
   "Get the ceph release name of this process"},

  {"_ceph_lookup_release_name", (PyCFunction)ceph_lookup_release_name, METH_VARARGS,
   "Get the ceph release name for a given major number"},

  {"_ceph_get_context", (PyCFunction)ceph_get_context, METH_NOARGS,
    "Get a CephContext* in a python capsule"},

  {"_ceph_get_osdmap", (PyCFunction)ceph_get_osdmap, METH_NOARGS,
    "Get an OSDMap* in a python capsule"},

  {"_ceph_set_uri", (PyCFunction)ceph_set_uri, METH_VARARGS,
    "Advertize a service URI served by this module"},

  {"_ceph_set_device_wear_level", (PyCFunction)ceph_set_wear_level, METH_VARARGS,
   "Set device wear_level value"},

  {"_ceph_have_mon_connection", (PyCFunction)ceph_have_mon_connection,
    METH_NOARGS, "Find out whether this mgr daemon currently has "
                 "a connection to a monitor"},

  {"_ceph_update_progress_event", (PyCFunction)ceph_update_progress_event,
   METH_VARARGS, "Update status of a progress event"},
  {"_ceph_complete_progress_event", (PyCFunction)ceph_complete_progress_event,
   METH_VARARGS, "Complete a progress event"},
  {"_ceph_clear_all_progress_events", (PyCFunction)ceph_clear_all_progress_events,
   METH_NOARGS, "Clear all progress events"},

  {"_ceph_dispatch_remote", (PyCFunction)ceph_dispatch_remote,
    METH_VARARGS, "Dispatch a call to another module"},

  {"_ceph_add_osd_perf_query", (PyCFunction)ceph_add_osd_perf_query,
    METH_VARARGS, "Add an osd perf query"},

  {"_ceph_remove_osd_perf_query", (PyCFunction)ceph_remove_osd_perf_query,
    METH_VARARGS, "Remove an osd perf query"},

  {"_ceph_get_osd_perf_counters", (PyCFunction)ceph_get_osd_perf_counters,
    METH_VARARGS, "Get osd perf counters"},

  {"_ceph_add_mds_perf_query", (PyCFunction)ceph_add_mds_perf_query,
    METH_VARARGS, "Add an mds perf query"},

  {"_ceph_remove_mds_perf_query", (PyCFunction)ceph_remove_mds_perf_query,
    METH_VARARGS, "Remove an mds perf query"},

  {"_ceph_reregister_mds_perf_queries", (PyCFunction)ceph_reregister_mds_perf_queries,
    METH_NOARGS, "Re-register mds perf queries"},

  {"_ceph_get_mds_perf_counters", (PyCFunction)ceph_get_mds_perf_counters,
    METH_VARARGS, "Get mds perf counters"},

  {"_ceph_is_authorized", (PyCFunction)ceph_is_authorized,
    METH_VARARGS, "Verify the current session caps are valid"},

  {"_ceph_register_client", (PyCFunction)ceph_register_client,
    METH_VARARGS, "Register RADOS instance for potential blocklisting"},

  {"_ceph_unregister_client", (PyCFunction)ceph_unregister_client,
    METH_VARARGS, "Unregister RADOS instance for potential blocklisting"},

  {"_ceph_get_daemon_health_metrics", (PyCFunction)ceph_get_daemon_health_metrics,
    METH_VARARGS, "Get health metrics for all daemons"},

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

    self->py_modules = static_cast<ActivePyModules*>(PyCapsule_GetPointer(
        py_modules_capsule, nullptr));
    ceph_assert(self->py_modules);
    self->this_module = static_cast<ActivePyModule*>(PyCapsule_GetPointer(
        this_module_capsule, nullptr));
    ceph_assert(self->this_module);

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
