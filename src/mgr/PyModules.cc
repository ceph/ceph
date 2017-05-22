// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

// Include this first to get python headers earlier
#include "PyState.h"

#include <boost/tokenizer.hpp>
#include "common/errno.h"
#include "include/stringify.h"

#include "PyFormatter.h"

#include "osd/OSDMap.h"
#include "mon/MonMap.h"

#include "mgr/MgrContext.h"

#include "PyModules.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr

#undef dout_prefix
#define dout_prefix *_dout << "mgr[py] "

namespace {
  PyObject* log_write(PyObject*, PyObject* args) {
    char* m = nullptr;
    if (PyArg_ParseTuple(args, "s", &m)) {
      auto len = strlen(m);
      if (len && m[len-1] == '\n') {
	m[len-1] = '\0';
      }
      dout(4) << m << dendl;
    }
    Py_RETURN_NONE;
  }

  PyObject* log_flush(PyObject*, PyObject*){
    Py_RETURN_NONE;
  }

  static PyMethodDef log_methods[] = {
    {"write", log_write, METH_VARARGS, "write stdout and stderr"},
    {"flush", log_flush, METH_VARARGS, "flush"},
    {nullptr, nullptr, 0, nullptr}
  };
}

#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

PyModules::PyModules(DaemonStateIndex &ds, ClusterState &cs, MonClient &mc,
                     Objecter &objecter_, Client &client_,
		     Finisher &f)
  : daemon_state(ds), cluster_state(cs), monc(mc),
    objecter(objecter_), client(client_),
    finisher(f)
{}

// we can not have the default destructor in header, because ServeThread is
// still an "incomplete" type. so we need to define the destructor here.
PyModules::~PyModules() = default;

void PyModules::dump_server(const std::string &hostname,
                      const DaemonStateCollection &dmc,
                      Formatter *f)
{
  f->dump_string("hostname", hostname);
  f->open_array_section("services");
  std::string ceph_version;

  for (const auto &i : dmc) {
    const auto &key = i.first;
    const std::string str_type = ceph_entity_type_name(key.first);
    const std::string &svc_name = key.second;

    // TODO: pick the highest version, and make sure that
    // somewhere else (during health reporting?) we are
    // indicating to the user if we see mixed versions
    auto ver_iter = i.second->metadata.find("ceph_version");
    if (ver_iter != i.second->metadata.end()) {
      ceph_version = i.second->metadata.at("ceph_version");
    }

    f->open_object_section("service");
    f->dump_string("type", str_type);
    f->dump_string("id", svc_name);
    f->close_section();
  }
  f->close_section();

  f->dump_string("ceph_version", ceph_version);
}



PyObject *PyModules::get_server_python(const std::string &hostname)
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);
  dout(10) << " (" << hostname << ")" << dendl;

  auto dmc = daemon_state.get_by_server(hostname);

  PyFormatter f;
  dump_server(hostname, dmc, &f);
  return f.get();
}


PyObject *PyModules::list_servers_python()
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);
  dout(10) << " >" << dendl;

  PyFormatter f(false, true);
  const auto &all = daemon_state.get_all_servers();
  for (const auto &i : all) {
    const auto &hostname = i.first;

    f.open_object_section("server");
    dump_server(hostname, i.second, &f);
    f.close_section();
  }

  return f.get();
}

PyObject *PyModules::get_metadata_python(std::string const &handle,
    entity_type_t svc_type, const std::string &svc_id)
{
  auto metadata = daemon_state.get(DaemonKey(svc_type, svc_id));
  PyFormatter f;
  f.dump_string("hostname", metadata->hostname);
  for (const auto &i : metadata->metadata) {
    f.dump_string(i.first.c_str(), i.second);
  }

  return f.get();
}


PyObject *PyModules::get_python(const std::string &what)
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  if (what == "fs_map") {
    PyFormatter f;
    cluster_state.with_fsmap([&f](const FSMap &fsmap) {
      fsmap.dump(&f);
    });
    return f.get();
  } else if (what == "osdmap_crush_map_text") {
    bufferlist rdata;
    cluster_state.with_osdmap([&rdata](const OSDMap &osd_map){
	osd_map.crush->encode(rdata, CEPH_FEATURES_SUPPORTED_DEFAULT);
    });
    std::string crush_text = rdata.to_str();
    return PyString_FromString(crush_text.c_str());
  } else if (what.substr(0, 7) == "osd_map") {
    PyFormatter f;
    cluster_state.with_osdmap([&f, &what](const OSDMap &osd_map){
      if (what == "osd_map") {
        osd_map.dump(&f);
      } else if (what == "osd_map_tree") {
        osd_map.print_tree(&f, nullptr);
      } else if (what == "osd_map_crush") {
        osd_map.crush->dump(&f);
      }
    });
    return f.get();
  } else if (what == "config") {
    PyFormatter f;
    g_conf->show_config(&f);
    return f.get();
  } else if (what == "mon_map") {
    PyFormatter f;
    cluster_state.with_monmap(
      [&f](const MonMap &monmap) {
        monmap.dump(&f);
      }
    );
    return f.get();
  } else if (what == "osd_metadata") {
    PyFormatter f;
    auto dmc = daemon_state.get_by_type(CEPH_ENTITY_TYPE_OSD);
    for (const auto &i : dmc) {
      f.open_object_section(i.first.second.c_str());
      f.dump_string("hostname", i.second->hostname);
      for (const auto &j : i.second->metadata) {
        f.dump_string(j.first.c_str(), j.second);
      }
      f.close_section();
    }
    return f.get();
  } else if (what == "pg_summary") {
    PyFormatter f;
    cluster_state.with_pgmap(
        [&f](const PGMap &pg_map) {
          std::map<std::string, std::map<std::string, uint32_t> > osds;
          std::map<std::string, std::map<std::string, uint32_t> > pools;
          std::map<std::string, uint32_t> all;
          for (const auto &i : pg_map.pg_stat) {
            const auto pool = i.first.m_pool;
            const std::string state = pg_state_string(i.second.state);
            // Insert to per-pool map
            pools[stringify(pool)][state]++;
            for (const auto &osd_id : i.second.acting) {
              osds[stringify(osd_id)][state]++;
            }
            all[state]++;
          }
          f.open_object_section("by_osd");
          for (const auto &i : osds) {
            f.open_object_section(i.first.c_str());
            for (const auto &j : i.second) {
              f.dump_int(j.first.c_str(), j.second);
            }
            f.close_section();
          }
          f.close_section();
          f.open_object_section("by_pool");
          for (const auto &i : pools) {
            f.open_object_section(i.first.c_str());
            for (const auto &j : i.second) {
              f.dump_int(j.first.c_str(), j.second);
            }
            f.close_section();
          }
          f.close_section();
          f.open_object_section("all");
          for (const auto &i : all) {
            f.dump_int(i.first.c_str(), i.second);
          }
          f.close_section();
        }
    );
    return f.get();

  } else if (what == "df") {
    PyFormatter f;

    cluster_state.with_osdmap([this, &f](const OSDMap &osd_map){
      cluster_state.with_pgmap(
          [&osd_map, &f](const PGMap &pg_map) {
        pg_map.dump_fs_stats(nullptr, &f, true);
        pg_map.dump_pool_stats(osd_map, nullptr, &f, true);
      });
    });
    return f.get();
  } else if (what == "osd_stats") {
    PyFormatter f;
    cluster_state.with_pgmap(
        [&f](const PGMap &pg_map) {
      pg_map.dump_osd_stats(&f);
    });
    return f.get();
  } else if (what == "health" || what == "mon_status") {
    PyFormatter f;
    bufferlist json;
    if (what == "health") {
      json = cluster_state.get_health();
    } else if (what == "mon_status") {
      json = cluster_state.get_mon_status();
    } else {
      assert(false);
    }
    f.dump_string("json", json.to_str());
    return f.get();
  } else {
    derr << "Python module requested unknown data '" << what << "'" << dendl;
    Py_RETURN_NONE;
  }
}

std::string PyModules::get_site_packages()
{
  std::stringstream site_packages;

  // CPython doesn't auto-add site-packages dirs to sys.path for us,
  // but it does provide a module that we can ask for them.
  auto site_module = PyImport_ImportModule("site");
  assert(site_module);

  auto site_packages_fn = PyObject_GetAttrString(site_module, "getsitepackages");
  if (site_packages_fn != nullptr) {
    auto site_packages_list = PyObject_CallObject(site_packages_fn, nullptr);
    assert(site_packages_list);

    auto n = PyList_Size(site_packages_list);
    for (Py_ssize_t i = 0; i < n; ++i) {
      if (i != 0) {
        site_packages << ":";
      }
      site_packages << PyString_AsString(PyList_GetItem(site_packages_list, i));
    }

    Py_DECREF(site_packages_list);
    Py_DECREF(site_packages_fn);
  } else {
    // Fall back to generating our own site-packages paths by imitating
    // what the standard site.py does.  This is annoying but it lets us
    // run inside virtualenvs :-/

    auto site_packages_fn = PyObject_GetAttrString(site_module, "addsitepackages");
    assert(site_packages_fn);

    auto known_paths = PySet_New(nullptr);
    auto pArgs = PyTuple_Pack(1, known_paths);
    PyObject_CallObject(site_packages_fn, pArgs);
    Py_DECREF(pArgs);
    Py_DECREF(known_paths);
    Py_DECREF(site_packages_fn);

    auto sys_module = PyImport_ImportModule("sys");
    assert(sys_module);
    auto sys_path = PyObject_GetAttrString(sys_module, "path");
    assert(sys_path);

    dout(1) << "sys.path:" << dendl;
    auto n = PyList_Size(sys_path);
    bool first = true;
    for (Py_ssize_t i = 0; i < n; ++i) {
      dout(1) << "  " << PyString_AsString(PyList_GetItem(sys_path, i)) << dendl;
      if (first) {
        first = false;
      } else {
        site_packages << ":";
      }
      site_packages << PyString_AsString(PyList_GetItem(sys_path, i));
    }

    Py_DECREF(sys_path);
    Py_DECREF(sys_module);
  }

  Py_DECREF(site_module);

  return site_packages.str();
}


int PyModules::init()
{
  Mutex::Locker locker(lock);

  global_handle = this;

  // Set up global python interpreter
  Py_SetProgramName(const_cast<char*>(PYTHON_EXECUTABLE));
  Py_InitializeEx(0);

  // Some python modules do not cope with an unpopulated argv, so lets
  // fake one.  This step also picks up site-packages into sys.path.
  const char *argv[] = {"ceph-mgr"};
  PySys_SetArgv(1, (char**)argv);

  if (g_conf->daemonize) {
    auto py_logger = Py_InitModule("ceph_logger", log_methods);
#if PY_MAJOR_VERSION >= 3
    PySys_SetObject("stderr", py_logger);
    PySys_SetObject("stdout", py_logger);
#else
    PySys_SetObject(const_cast<char*>("stderr"), py_logger);
    PySys_SetObject(const_cast<char*>("stdout"), py_logger);
#endif
  }
  // Populate python namespace with callable hooks
  Py_InitModule("ceph_state", CephStateMethods);

  // Configure sys.path to include mgr_module_path
  std::string sys_path = std::string(Py_GetPath()) + ":" + get_site_packages()
                         + ":" + g_conf->mgr_module_path;
  dout(10) << "Computed sys.path '" << sys_path << "'" << dendl;
  PySys_SetPath((char*)(sys_path.c_str()));

  // Let CPython know that we will be calling it back from other
  // threads in future.
  if (! PyEval_ThreadsInitialized()) {
    PyEval_InitThreads();
  }

  // Load python code
  boost::tokenizer<> tok(g_conf->mgr_modules);
  for(const auto& module_name : tok) {
    dout(1) << "Loading python module '" << module_name << "'" << dendl;
    auto mod = std::unique_ptr<MgrPyModule>(new MgrPyModule(module_name));
    int r = mod->load();
    if (r != 0) {
      derr << "Error loading module '" << module_name << "': "
        << cpp_strerror(r) << dendl;
      derr << handle_pyerror() << dendl;
      // Don't drop out here, load the other modules
    } else {
      // Success!
      modules[module_name] = std::move(mod);
    }
  } 

  // Drop the GIL
  PyEval_SaveThread();
  
  return 0;
}

class ServeThread : public Thread
{
  MgrPyModule *mod;

public:
  ServeThread(MgrPyModule *mod_)
    : mod(mod_) {}

  void *entry() override
  {
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();

    dout(4) << "Entering thread for " << mod->get_name() << dendl;
    mod->serve();

    PyGILState_Release(gstate);

    return nullptr;
  }
};

void PyModules::start()
{
  Mutex::Locker l(lock);

  dout(1) << "Creating threads for " << modules.size() << " modules" << dendl;
  for (auto& i : modules) {
    auto thread = new ServeThread(i.second.get());
    serve_threads[i.first].reset(thread);
  }

  for (auto &i : serve_threads) {
    std::ostringstream thread_name;
    thread_name << "mgr." << i.first;
    dout(4) << "Starting thread for " << i.first << dendl;
    i.second->create(thread_name.str().c_str());
  }
}

void PyModules::shutdown()
{
  Mutex::Locker locker(lock);
  assert(global_handle);

  // Signal modules to drop out of serve() and/or tear down resources
  for (auto &i : modules) {
    auto module = i.second.get();
    const auto& name = i.first;
    dout(10) << "waiting for module " << name << " to shutdown" << dendl;
    module->shutdown();
    dout(10) << "module " << name << " shutdown" << dendl;
  }

  // For modules implementing serve(), finish the threads where we
  // were running that.
  for (auto &i : serve_threads) {
    lock.Unlock();
    i.second->join();
    lock.Lock();
  }
  serve_threads.clear();

  modules.clear();

  PyGILState_Ensure();
  Py_Finalize();

  // nobody needs me anymore.
  global_handle = nullptr;
}

void PyModules::notify_all(const std::string &notify_type,
                     const std::string &notify_id)
{
  Mutex::Locker l(lock);

  dout(10) << __func__ << ": notify_all " << notify_type << dendl;
  for (auto& i : modules) {
    auto module = i.second.get();
    // Send all python calls down a Finisher to avoid blocking
    // C++ code, and avoid any potential lock cycles.
    finisher.queue(new FunctionContext([module, notify_type, notify_id](int r){
      module->notify(notify_type, notify_id);
    }));
  }
}

void PyModules::notify_all(const LogEntry &log_entry)
{
  Mutex::Locker l(lock);

  dout(10) << __func__ << ": notify_all (clog)" << dendl;
  for (auto& i : modules) {
    auto module = i.second.get();
    // Send all python calls down a Finisher to avoid blocking
    // C++ code, and avoid any potential lock cycles.
    //
    // Note intentional use of non-reference lambda binding on
    // log_entry: we take a copy because caller's instance is
    // probably ephemeral.
    finisher.queue(new FunctionContext([module, log_entry](int r){
      module->notify_clog(log_entry);
    }));
  }
}

bool PyModules::get_config(const std::string &handle,
    const std::string &key, std::string *val) const
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  const std::string global_key = config_prefix + handle + "." + key;

  if (config_cache.count(global_key)) {
    *val = config_cache.at(global_key);
    return true;
  } else {
    return false;
  }
}

void PyModules::set_config(const std::string &handle,
    const std::string &key, const std::string &val)
{
  const std::string global_key = config_prefix + handle + "." + key;

  Command set_cmd;
  {
    PyThreadState *tstate = PyEval_SaveThread();
    Mutex::Locker l(lock);
    PyEval_RestoreThread(tstate);
    config_cache[global_key] = val;

    std::ostringstream cmd_json;

    JSONFormatter jf;
    jf.open_object_section("cmd");
    jf.dump_string("prefix", "config-key put");
    jf.dump_string("key", global_key);
    jf.dump_string("val", val);
    jf.close_section();
    jf.flush(cmd_json);

    set_cmd.run(&monc, cmd_json.str());
  }
  set_cmd.wait();

  if (set_cmd.r != 0) {
    // config-key put will fail if mgr's auth key has insufficient
    // permission to set config keys
    // FIXME: should this somehow raise an exception back into Python land?
    dout(0) << "`config-key put " << global_key << " " << val << "` failed: "
      << cpp_strerror(set_cmd.r) << dendl;
    dout(0) << "mon returned " << set_cmd.r << ": " << set_cmd.outs << dendl;
  }
}

std::vector<ModuleCommand> PyModules::get_commands()
{
  Mutex::Locker l(lock);

  std::vector<ModuleCommand> result;
  for (auto& i : modules) {
    auto module = i.second.get();
    auto mod_commands = module->get_commands();
    for (auto j : mod_commands) {
      result.push_back(j);
    }
  }

  return result;
}

void PyModules::insert_config(const std::map<std::string,
                              std::string> &new_config)
{
  Mutex::Locker l(lock);

  dout(4) << "Loaded " << new_config.size() << " config settings" << dendl;
  config_cache = new_config;
}

void PyModules::log(const std::string &handle,
    int level, const std::string &record)
{
#undef dout_prefix
#define dout_prefix *_dout << "mgr[" << handle << "] "
  dout(level) << record << dendl;
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "
}

PyObject* PyModules::get_counter_python(
    const std::string &handle,
    entity_type_t svc_type,
    const std::string &svc_id,
    const std::string &path)
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  PyFormatter f;
  f.open_array_section(path.c_str());

  auto metadata = daemon_state.get(DaemonKey(svc_type, svc_id));

  // FIXME: this is unsafe, I need to either be inside DaemonStateIndex's
  // lock or put a lock on individual DaemonStates
  if (metadata) {
    if (metadata->perf_counters.instances.count(path)) {
      auto counter_instance = metadata->perf_counters.instances.at(path);
      const auto &data = counter_instance.get_data();
      for (const auto &datapoint : data) {
        f.open_array_section("datapoint");
        f.dump_unsigned("t", datapoint.t.sec());
        f.dump_unsigned("v", datapoint.v);
        f.close_section();

      }
    } else {
      dout(4) << "Missing counter: '" << path << "' ("
              << ceph_entity_type_name(svc_type) << "."
              << svc_id << ")" << dendl;
      dout(20) << "Paths are:" << dendl;
      for (const auto &i : metadata->perf_counters.instances) {
        dout(20) << i.first << dendl;
      }
    }
  } else {
    dout(4) << "No daemon state for "
              << ceph_entity_type_name(svc_type) << "."
              << svc_id << ")" << dendl;
  }
  f.close_section();
  return f.get();
}

PyObject *PyModules::get_context()
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  // Construct a capsule containing ceph context.
  // Not incrementing/decrementing ref count on the context because
  // it's the global one and it has process lifetime.
  auto capsule = PyCapsule_New(g_ceph_context, nullptr, nullptr);
  return capsule;
}

