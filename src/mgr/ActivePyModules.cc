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
#include "Gil.h"

#include "common/errno.h"
#include "include/stringify.h"

#include "PyFormatter.h"

#include "osd/OSDMap.h"
#include "mon/MonMap.h"

#include "mgr/MgrContext.h"

// For ::config_prefix
#include "PyModule.h"

#include "ActivePyModules.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "    

ActivePyModules::ActivePyModules(PyModuleConfig &module_config_,
          std::map<std::string, std::string> store_data,
          DaemonStateIndex &ds, ClusterState &cs,
	  MonClient &mc, LogChannelRef clog_, Objecter &objecter_,
          Client &client_, Finisher &f)
  : module_config(module_config_), daemon_state(ds), cluster_state(cs),
    monc(mc), clog(clog_), objecter(objecter_), client(client_), finisher(f),
    lock("ActivePyModules")
{
  store_cache = std::move(store_data);
}

ActivePyModules::~ActivePyModules() = default;

void ActivePyModules::dump_server(const std::string &hostname,
                      const DaemonStateCollection &dmc,
                      Formatter *f)
{
  f->dump_string("hostname", hostname);
  f->open_array_section("services");
  std::string ceph_version;

  for (const auto &i : dmc) {
    Mutex::Locker l(i.second->lock);
    const auto &key = i.first;
    const std::string &str_type = key.first;
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



PyObject *ActivePyModules::get_server_python(const std::string &hostname)
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


PyObject *ActivePyModules::list_servers_python()
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);
  dout(10) << " >" << dendl;

  PyFormatter f(false, true);
  daemon_state.with_daemons_by_server([this, &f]
      (const std::map<std::string, DaemonStateCollection> &all) {
    for (const auto &i : all) {
      const auto &hostname = i.first;

      f.open_object_section("server");
      dump_server(hostname, i.second, &f);
      f.close_section();
    }
  });

  return f.get();
}

PyObject *ActivePyModules::get_metadata_python(
  const std::string &svc_type,
  const std::string &svc_id)
{
  auto metadata = daemon_state.get(DaemonKey(svc_type, svc_id));
  if (metadata == nullptr) {
    derr << "Requested missing service " << svc_type << "." << svc_id << dendl;
    Py_RETURN_NONE;
  }

  Mutex::Locker l(metadata->lock);
  PyFormatter f;
  f.dump_string("hostname", metadata->hostname);
  for (const auto &i : metadata->metadata) {
    f.dump_string(i.first.c_str(), i.second);
  }

  return f.get();
}

PyObject *ActivePyModules::get_daemon_status_python(
  const std::string &svc_type,
  const std::string &svc_id)
{
  auto metadata = daemon_state.get(DaemonKey(svc_type, svc_id));
  if (metadata == nullptr) {
    derr << "Requested missing service " << svc_type << "." << svc_id << dendl;
    Py_RETURN_NONE;
  }

  Mutex::Locker l(metadata->lock);
  PyFormatter f;
  for (const auto &i : metadata->service_status) {
    f.dump_string(i.first.c_str(), i.second);
  }
  return f.get();
}

PyObject *ActivePyModules::get_python(const std::string &what)
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
  } else if (what.substr(0, 6) == "config") {
    PyFormatter f;
    if (what == "config_options") {
      g_conf->config_options(&f);  
    } else if (what == "config") {
      g_conf->show_config(&f);
    }
    return f.get();
  } else if (what == "mon_map") {
    PyFormatter f;
    cluster_state.with_monmap(
      [&f](const MonMap &monmap) {
        monmap.dump(&f);
      }
    );
    return f.get();
  } else if (what == "service_map") {
    PyFormatter f;
    cluster_state.with_servicemap(
      [&f](const ServiceMap &service_map) {
        service_map.dump(&f);
      }
    );
    return f.get();
  } else if (what == "osd_metadata") {
    PyFormatter f;
    auto dmc = daemon_state.get_by_service("osd");
    for (const auto &i : dmc) {
      Mutex::Locker l(i.second->lock);
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
          f.open_object_section("pg_stats_sum");
          pg_map.pg_sum.dump(&f);
          f.close_section();
        }
    );
    return f.get();
  } else if (what == "pg_status") {
    PyFormatter f;
    cluster_state.with_pgmap(
        [&f](const PGMap &pg_map) {
	  pg_map.print_summary(&f, nullptr);
        }
    );
    return f.get();
  } else if (what == "pg_dump") {
    PyFormatter f;
    cluster_state.with_pgmap(
      [&f](const PGMap &pg_map) {
	pg_map.dump(&f);
      }
    );
    return f.get();
  } else if (what == "devices") {
    PyFormatter f;
    f.open_array_section("devices");
    daemon_state.with_devices([&f] (const DeviceState& dev) {
	f.dump_object("device", dev);
      });
    f.close_section();
    return f.get();
  } else if (what.size() > 7 &&
	     what.substr(0, 7) == "device ") {
    string devid = what.substr(7);
    PyFormatter f;
    daemon_state.with_device(devid, [&f] (const DeviceState& dev) {
	f.dump_object("device", dev);
      });
    return f.get();
  } else if (what == "io_rate") {
    PyFormatter f;
    cluster_state.with_pgmap(
      [&f](const PGMap &pg_map) {
        pg_map.dump_delta(&f);
      }
    );
    return f.get();
  } else if (what == "df") {
    PyFormatter f;

    cluster_state.with_osdmap([this, &f](const OSDMap &osd_map){
      cluster_state.with_pgmap(
          [&osd_map, &f](const PGMap &pg_map) {
        pg_map.dump_fs_stats(nullptr, &f, true);
        pg_map.dump_pool_stats_full(osd_map, nullptr, &f, true);
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
  } else if (what == "osd_pool_stats") {
    int64_t poolid = -ENOENT;
    string pool_name;
    PyFormatter f;
    cluster_state.with_pgmap([&](const PGMap& pg_map) {
      return cluster_state.with_osdmap([&](const OSDMap& osdmap) {
        f.open_array_section("pool_stats");
        for (auto &p : osdmap.get_pools()) {
          poolid = p.first;
          pg_map.dump_pool_stats_and_io_rate(poolid, osdmap, &f, nullptr);
        }
        f.close_section();
      });
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
      ceph_abort();
    }
    f.dump_string("json", json.to_str());
    return f.get();
  } else if (what == "mgr_map") {
    PyFormatter f;
    cluster_state.with_mgrmap([&f](const MgrMap &mgr_map) {
      mgr_map.dump(&f);
    });
    return f.get();
  } else {
    derr << "Python module requested unknown data '" << what << "'" << dendl;
    Py_RETURN_NONE;
  }
}

int ActivePyModules::start_one(PyModuleRef py_module)
{
  Mutex::Locker l(lock);

  assert(modules.count(py_module->get_name()) == 0);

  modules[py_module->get_name()].reset(new ActivePyModule(py_module, clog));

  int r = modules[py_module->get_name()]->load(this);
  if (r != 0) {
    return r;
  } else {
    dout(4) << "Starting thread for " << py_module->get_name() << dendl;
    // Giving Thread the module's module_name member as its
    // char* thread name: thread must not outlive module class lifetime.
    modules[py_module->get_name()]->thread.create(
        py_module->get_name().c_str());

    return 0;
  }
}

void ActivePyModules::shutdown()
{
  Mutex::Locker locker(lock);

  // Signal modules to drop out of serve() and/or tear down resources
  for (auto &i : modules) {
    auto module = i.second.get();
    const auto& name = i.first;

    lock.Unlock();
    dout(10) << "calling module " << name << " shutdown()" << dendl;
    module->shutdown();
    dout(10) << "module " << name << " shutdown() returned" << dendl;
    lock.Lock();
  }

  // For modules implementing serve(), finish the threads where we
  // were running that.
  for (auto &i : modules) {
    lock.Unlock();
    dout(10) << "joining module " << i.first << dendl;
    i.second->thread.join();
    dout(10) << "joined module " << i.first << dendl;
    lock.Lock();
  }

  modules.clear();
}

void ActivePyModules::notify_all(const std::string &notify_type,
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

void ActivePyModules::notify_all(const LogEntry &log_entry)
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

bool ActivePyModules::get_store(const std::string &module_name,
    const std::string &key, std::string *val) const
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  const std::string global_key = PyModule::config_prefix
    + module_name + "/" + key;

  dout(4) << __func__ << " key: " << global_key << dendl;

  auto i = store_cache.find(global_key);
  if (i != store_cache.end()) {
    *val = i->second;
    return true;
  } else {
    return false;
  }
}

bool ActivePyModules::get_config(const std::string &module_name,
    const std::string &key, std::string *val) const
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  const std::string global_key = PyModule::config_prefix
    + module_name + "/" + key;

  dout(4) << __func__ << " key: " << global_key << dendl;

  Mutex::Locker lock(module_config.lock);
  
  auto i = module_config.config.find(global_key);
  if (i != module_config.config.end()) {
    *val = i->second;
    return true;
  } else {
    return false;
  }
}

PyObject *ActivePyModules::get_store_prefix(const std::string &module_name,
    const std::string &prefix) const
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  const std::string base_prefix = PyModule::config_prefix
                                    + module_name + "/";
  const std::string global_prefix = base_prefix + prefix;
  dout(4) << __func__ << " prefix: " << global_prefix << dendl;

  PyFormatter f;
  
  Mutex::Locker lock(module_config.lock);
  
  for (auto p = store_cache.lower_bound(global_prefix);
       p != store_cache.end() && p->first.find(global_prefix) == 0;
       ++p) {
    f.dump_string(p->first.c_str() + base_prefix.size(), p->second);
  }
  return f.get();
}

void ActivePyModules::set_store(const std::string &module_name,
    const std::string &key, const boost::optional<std::string>& val)
{
  const std::string global_key = PyModule::config_prefix
                                   + module_name + "/" + key;
  
  Command set_cmd;
  {
    PyThreadState *tstate = PyEval_SaveThread();
    Mutex::Locker l(lock);
    PyEval_RestoreThread(tstate);

    if (val) {
      store_cache[global_key] = *val;
    } else {
      store_cache.erase(global_key);
    }

    std::ostringstream cmd_json;
    JSONFormatter jf;
    jf.open_object_section("cmd");
    if (val) {
      jf.dump_string("prefix", "config-key set");
      jf.dump_string("key", global_key);
      jf.dump_string("val", *val);
    } else {
      jf.dump_string("prefix", "config-key del");
      jf.dump_string("key", global_key);
    }
    jf.close_section();
    jf.flush(cmd_json);
    set_cmd.run(&monc, cmd_json.str());
  }
  set_cmd.wait();

  if (set_cmd.r != 0) {
    // config-key set will fail if mgr's auth key has insufficient
    // permission to set config keys
    // FIXME: should this somehow raise an exception back into Python land?
    dout(0) << "`config-key set " << global_key << " " << val << "` failed: "
      << cpp_strerror(set_cmd.r) << dendl;
    dout(0) << "mon returned " << set_cmd.r << ": " << set_cmd.outs << dendl;
  }
}

void ActivePyModules::set_config(const std::string &module_name,
    const std::string &key, const boost::optional<std::string>& val)
{
  module_config.set_config(&monc, module_name, key, val);
}

std::map<std::string, std::string> ActivePyModules::get_services() const
{
  std::map<std::string, std::string> result;
  Mutex::Locker l(lock);
  for (const auto& i : modules) {
    const auto &module = i.second.get();
    std::string svc_str = module->get_uri();
    if (!svc_str.empty()) {
      result[module->get_name()] = svc_str;
    }
  }

  return result;
}

PyObject* ActivePyModules::get_counter_python(
    const std::string &svc_name,
    const std::string &svc_id,
    const std::string &path)
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  PyFormatter f;
  f.open_array_section(path.c_str());

  auto metadata = daemon_state.get(DaemonKey(svc_name, svc_id));
  if (metadata) {
    Mutex::Locker l2(metadata->lock);
    if (metadata->perf_counters.instances.count(path)) {
      auto counter_instance = metadata->perf_counters.instances.at(path);
      auto counter_type = metadata->perf_counters.types.at(path);
      if (counter_type.type & PERFCOUNTER_LONGRUNAVG) {
        const auto &avg_data = counter_instance.get_data_avg();
        for (const auto &datapoint : avg_data) {
          f.open_array_section("datapoint");
          f.dump_unsigned("t", datapoint.t.sec());
          f.dump_unsigned("s", datapoint.s);
          f.dump_unsigned("c", datapoint.c);
          f.close_section();
        }
      } else {
        const auto &data = counter_instance.get_data();
        for (const auto &datapoint : data) {
          f.open_array_section("datapoint");
          f.dump_unsigned("t", datapoint.t.sec());
          f.dump_unsigned("v", datapoint.v);
          f.close_section();
        }
      }
    } else {
      dout(4) << "Missing counter: '" << path << "' ("
              << svc_name << "." << svc_id << ")" << dendl;
      dout(20) << "Paths are:" << dendl;
      for (const auto &i : metadata->perf_counters.instances) {
        dout(20) << i.first << dendl;
      }
    }
  } else {
    dout(4) << "No daemon state for "
              << svc_name << "." << svc_id << ")" << dendl;
  }
  f.close_section();
  return f.get();
}

PyObject* ActivePyModules::get_perf_schema_python(
    const std::string &svc_type,
    const std::string &svc_id)
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  DaemonStateCollection daemons;

  if (svc_type == "") {
    daemons = daemon_state.get_all();
  } else if (svc_id.empty()) {
    daemons = daemon_state.get_by_service(svc_type);
  } else {
    auto key = DaemonKey(svc_type, svc_id);
    // so that the below can be a loop in all cases
    auto got = daemon_state.get(key);
    if (got != nullptr) {
      daemons[key] = got;
    }
  }

  PyFormatter f;
  if (!daemons.empty()) {
    for (auto statepair : daemons) {
      auto key = statepair.first;
      auto state = statepair.second;

      std::ostringstream daemon_name;
      daemon_name << key.first << "." << key.second;
      f.open_object_section(daemon_name.str().c_str());

      Mutex::Locker l(state->lock);
      for (auto ctr_inst_iter : state->perf_counters.instances) {
        const auto &counter_name = ctr_inst_iter.first;
	f.open_object_section(counter_name.c_str());
	auto type = state->perf_counters.types[counter_name];
	f.dump_string("description", type.description);
	if (!type.nick.empty()) {
	  f.dump_string("nick", type.nick);
	}
	f.dump_unsigned("type", type.type);
	f.dump_unsigned("priority", type.priority);
	f.dump_unsigned("units", type.unit);
	f.close_section();
      }
      f.close_section();
    }
  } else {
    dout(4) << __func__ << ": No daemon state found for "
              << svc_type << "." << svc_id << ")" << dendl;
  }
  return f.get();
}

PyObject *ActivePyModules::get_context()
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

/**
 * Helper for our wrapped types that take a capsule in their constructor.
 */
PyObject *construct_with_capsule(
    const std::string &module_name,
    const std::string &clsname,
    void *wrapped)
{
  // Look up the OSDMap type which we will construct
  PyObject *module = PyImport_ImportModule(module_name.c_str());
  if (!module) {
    derr << "Failed to import python module:" << dendl;
    derr << handle_pyerror() << dendl;
  }
  assert(module);

  PyObject *wrapper_type = PyObject_GetAttrString(
      module, (const char*)clsname.c_str());
  if (!wrapper_type) {
    derr << "Failed to get python type:" << dendl;
    derr << handle_pyerror() << dendl;
  }
  assert(wrapper_type);

  // Construct a capsule containing an OSDMap.
  auto wrapped_capsule = PyCapsule_New(wrapped, nullptr, nullptr);
  assert(wrapped_capsule);

  // Construct the python OSDMap
  auto pArgs = PyTuple_Pack(1, wrapped_capsule);
  auto wrapper_instance = PyObject_CallObject(wrapper_type, pArgs);
  if (wrapper_instance == nullptr) {
    derr << "Failed to construct python OSDMap:" << dendl;
    derr << handle_pyerror() << dendl;
  }
  assert(wrapper_instance != nullptr);
  Py_DECREF(pArgs);
  Py_DECREF(wrapped_capsule);

  Py_DECREF(wrapper_type);
  Py_DECREF(module);

  return wrapper_instance;
}

PyObject *ActivePyModules::get_osdmap()
{
  PyThreadState *tstate = PyEval_SaveThread();
  Mutex::Locker l(lock);
  PyEval_RestoreThread(tstate);

  OSDMap *newmap = new OSDMap;

  cluster_state.with_osdmap([&](const OSDMap& o) {
      newmap->deepish_copy_from(o);
    });

  return construct_with_capsule("mgr_module", "OSDMap", (void*)newmap);
}

void ActivePyModules::set_health_checks(const std::string& module_name,
				  health_check_map_t&& checks)
{
  Mutex::Locker l(lock);
  auto p = modules.find(module_name);
  if (p != modules.end()) {
    p->second->set_health_checks(std::move(checks));
  }
}

int ActivePyModules::handle_command(
  std::string const &module_name,
  const cmdmap_t &cmdmap,
  std::stringstream *ds,
  std::stringstream *ss)
{
  lock.Lock();
  auto mod_iter = modules.find(module_name);
  if (mod_iter == modules.end()) {
    *ss << "Module '" << module_name << "' is not available";
    return -ENOENT;
  }

  lock.Unlock();
  return mod_iter->second->handle_command(cmdmap, ds, ss);
}

void ActivePyModules::get_health_checks(health_check_map_t *checks)
{
  Mutex::Locker l(lock);
  for (auto& p : modules) {
    p.second->get_health_checks(checks);
  }
}

void ActivePyModules::set_uri(const std::string& module_name,
                        const std::string &uri)
{
  Mutex::Locker l(lock);

  dout(4) << " module " << module_name << " set URI '" << uri << "'" << dendl;

  modules[module_name]->set_uri(uri);
}

