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


#include <boost/tokenizer.hpp>
#include "common/errno.h"

#include "PyState.h"
#include "PyFormatter.h"

#include "osd/OSDMap.h"
#include "mon/MonMap.h"

#include "mgr/MgrContext.h"

#include "PyModules.h"

#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

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
    ceph_version = i.second->metadata.at("ceph_version");

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
      osd_map.crush->encode(rdata);
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
  } else if (what == "fs_map") {
    PyFormatter f;
    cluster_state.with_fsmap(
      [&f](const FSMap &fsmap) {
        fsmap.dump(&f);
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
  } else if (what == "pg_summary" || what == "health" || what == "mon_status") {
    PyFormatter f;
    bufferlist json;
    if (what == "pg_summary") {
      json = cluster_state.get_pg_summary();
    } else if (what == "health") {
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

//XXX courtesy of http://stackoverflow.com/questions/1418015/how-to-get-python-exception-text
#include <boost/python.hpp>
// decode a Python exception into a string
std::string handle_pyerror()
{
    using namespace boost::python;
    using namespace boost;

    PyObject *exc,*val,*tb;
    object formatted_list, formatted;
    PyErr_Fetch(&exc,&val,&tb);
    handle<> hexc(exc),hval(allow_null(val)),htb(allow_null(tb)); 
    object traceback(import("traceback"));
    if (!tb) {
        object format_exception_only(traceback.attr("format_exception_only"));
        formatted_list = format_exception_only(hexc,hval);
    } else {
        object format_exception(traceback.attr("format_exception"));
        formatted_list = format_exception(hexc,hval,htb);
    }
    formatted = str("\n").join(formatted_list);
    return extract<std::string>(formatted);
}


int PyModules::init()
{
  Mutex::Locker locker(lock);

  global_handle = this;

  // Set up global python interpreter
  Py_Initialize();

  // Some python modules do not cope with an unpopulated argv, so lets
  // fake one.  This step also picks up site-packages into sys.path.
  const char *argv[] = {"ceph-mgr"};
  PySys_SetArgv(1, (char**)argv);
  
  // Populate python namespace with callable hooks
  Py_InitModule("ceph_state", CephStateMethods);

  // Configure sys.path to include mgr_module_path
  const std::string module_path = g_conf->mgr_module_path;
  dout(4) << "Loading modules from '" << module_path << "'" << dendl;
  std::string sys_path = Py_GetPath();

  // We need site-packages for flask et al, unless we choose to
  // embed them in the ceph package.  site-packages is an interpreter-specific
  // thing, so as an embedded interpreter we're responsible for picking
  // this.  FIXME: don't hardcode this.
  std::string site_packages = "/usr/lib/python2.7/site-packages:/usr/lib64/python2.7/site-packages:/usr/lib64/python2.7";
  sys_path += ":";
  sys_path += site_packages;

  sys_path += ":";
  sys_path += module_path;
  dout(10) << "Computed sys.path '" << sys_path << "'" << dendl;
  PySys_SetPath((char*)(sys_path.c_str()));

  // Let CPython know that we will be calling it back from other
  // threads in future.
  if (! PyEval_ThreadsInitialized()) {
    PyEval_InitThreads();
  }

  // Load python code
  boost::tokenizer<> tok(g_conf->mgr_modules);
  for(boost::tokenizer<>::iterator module_name=tok.begin();
      module_name != tok.end();++module_name){
    dout(1) << "Loading python module '" << *module_name << "'" << dendl;
    auto mod = new MgrPyModule(*module_name);
    int r = mod->load();
    if (r != 0) {
      derr << "Error loading module '" << *module_name << "': "
        << cpp_strerror(r) << dendl;
      derr << handle_pyerror() << dendl;

      return r;
    } else {
      // Success!
      modules[*module_name] = mod;
    }
  } 

  // Drop the GIL
#if 1
  PyThreadState *tstate = PyEval_SaveThread();
#else
  PyGILState_STATE gstate;
  gstate = PyGILState_Ensure();
  PyGILState_Release(gstate);
#endif
  
  return 0;
}

class ServeThread : public Thread
{
  MgrPyModule *mod;

public:
  ServeThread(MgrPyModule *mod_)
    : mod(mod_) {}

  void *entry()
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
  for (auto &i : modules) {
    auto thread = new ServeThread(i.second);
    serve_threads[i.first] = thread;
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

  // Signal modules to drop out of serve()
  for (auto i : modules) {
    auto module = i.second;
    finisher.queue(new C_StdFunction([module](){
      module->shutdown();
    }));
  }

  for (auto &i : serve_threads) {
    lock.Unlock();
    i.second->join();
    lock.Lock();
    delete i.second;
  }
  serve_threads.clear();

  // Tear down modules
  for (auto i : modules) {
    delete i.second;
  }
  modules.clear();

  Py_Finalize();
}

void PyModules::notify_all(const std::string &notify_type,
                     const std::string &notify_id)
{
  Mutex::Locker l(lock);

  dout(10) << __func__ << ": notify_all " << notify_type << dendl;
  for (auto i : modules) {
    auto module = i.second;
    // Send all python calls down a Finisher to avoid blocking
    // C++ code, and avoid any potential lock cycles.
    finisher.queue(new C_StdFunction([module, notify_type, notify_id](){
      module->notify(notify_type, notify_id);
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

  // FIXME: is config-key put ever allowed to fail?
  assert(set_cmd.r == 0);
}

std::vector<ModuleCommand> PyModules::get_commands()
{
  Mutex::Locker l(lock);

  std::vector<ModuleCommand> result;
  for (auto i : modules) {
    auto module = i.second;
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
