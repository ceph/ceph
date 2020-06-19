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
#include "PyModuleRegistry.h"

#include "ActivePyModules.h"
#include "DaemonKey.h"
#include "DaemonServer.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

ActivePyModules::ActivePyModules(PyModuleConfig &module_config_,
          std::map<std::string, std::string> store_data,
          DaemonStateIndex &ds, ClusterState &cs,
          MonClient &mc, LogChannelRef clog_,
          LogChannelRef audit_clog_, Objecter &objecter_,
          Client &client_, Finisher &f, DaemonServer &server,
          PyModuleRegistry &pmr)
  : module_config(module_config_), daemon_state(ds), cluster_state(cs),
    monc(mc), clog(clog_), audit_clog(audit_clog_), objecter(objecter_),
    client(client_), finisher(f),
    cmd_finisher(g_ceph_context, "cmd_finisher", "cmdfin"),
    server(server), py_module_registry(pmr)
{
  store_cache = std::move(store_data);
  cmd_finisher.start();
}

ActivePyModules::~ActivePyModules() = default;

void ActivePyModules::dump_server(const std::string &hostname,
                      const DaemonStateCollection &dmc,
                      Formatter *f)
{
  f->dump_string("hostname", hostname);
  f->open_array_section("services");
  std::string ceph_version;

  for (const auto &[key, state] : dmc) {
    std::lock_guard l(state->lock);
    // TODO: pick the highest version, and make sure that
    // somewhere else (during health reporting?) we are
    // indicating to the user if we see mixed versions
    auto ver_iter = state->metadata.find("ceph_version");
    if (ver_iter != state->metadata.end()) {
      ceph_version = state->metadata.at("ceph_version");
    }

    f->open_object_section("service");
    f->dump_string("type", key.type);
    f->dump_string("id", key.name);
    f->close_section();
  }
  f->close_section();

  f->dump_string("ceph_version", ceph_version);
}



PyObject *ActivePyModules::get_server_python(const std::string &hostname)
{
  PyThreadState *tstate = PyEval_SaveThread();
  std::lock_guard l(lock);
  PyEval_RestoreThread(tstate);
  dout(10) << " (" << hostname << ")" << dendl;

  auto dmc = daemon_state.get_by_server(hostname);

  PyFormatter f;
  dump_server(hostname, dmc, &f);
  return f.get();
}


PyObject *ActivePyModules::list_servers_python()
{
  PyFormatter f(false, true);
  PyThreadState *tstate = PyEval_SaveThread();
  dout(10) << " >" << dendl;

  daemon_state.with_daemons_by_server([this, &f, &tstate]
      (const std::map<std::string, DaemonStateCollection> &all) {
    PyEval_RestoreThread(tstate);

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
  auto metadata = daemon_state.get(DaemonKey{svc_type, svc_id});
  if (metadata == nullptr) {
    derr << "Requested missing service " << svc_type << "." << svc_id << dendl;
    Py_RETURN_NONE;
  }

  std::lock_guard l(metadata->lock);
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
  auto metadata = daemon_state.get(DaemonKey{svc_type, svc_id});
  if (metadata == nullptr) {
    derr << "Requested missing service " << svc_type << "." << svc_id << dendl;
    Py_RETURN_NONE;
  }

  std::lock_guard l(metadata->lock);
  PyFormatter f;
  for (const auto &i : metadata->service_status) {
    f.dump_string(i.first.c_str(), i.second);
  }
  return f.get();
}

PyObject *ActivePyModules::get_python(const std::string &what)
{
  PyFormatter f;

  // Drop the GIL, as most of the following blocks will block on
  // a mutex -- they are all responsible for re-taking the GIL before
  // touching the PyFormatter instance or returning from the function.
  PyThreadState *tstate = PyEval_SaveThread();

  if (what == "fs_map") {
    cluster_state.with_fsmap([&f, &tstate](const FSMap &fsmap) {
      PyEval_RestoreThread(tstate);
      fsmap.dump(&f);
    });
    return f.get();
  } else if (what == "osdmap_crush_map_text") {
    bufferlist rdata;
    cluster_state.with_osdmap([&rdata, &tstate](const OSDMap &osd_map){
      PyEval_RestoreThread(tstate);
      osd_map.crush->encode(rdata, CEPH_FEATURES_SUPPORTED_DEFAULT);
    });
    std::string crush_text = rdata.to_str();
    return PyUnicode_FromString(crush_text.c_str());
  } else if (what.substr(0, 7) == "osd_map") {
    cluster_state.with_osdmap([&f, &what, &tstate](const OSDMap &osd_map){
      PyEval_RestoreThread(tstate);
      if (what == "osd_map") {
        osd_map.dump(&f);
      } else if (what == "osd_map_tree") {
        osd_map.print_tree(&f, nullptr);
      } else if (what == "osd_map_crush") {
        osd_map.crush->dump(&f);
      }
    });
    return f.get();
  } else if (what == "modified_config_options") {
    PyEval_RestoreThread(tstate);
    auto all_daemons = daemon_state.get_all();
    set<string> names;
    for (auto& [key, daemon] : all_daemons) {
      std::lock_guard l(daemon->lock);
      for (auto& [name, valmap] : daemon->config) {
	names.insert(name);
      }
    }
    f.open_array_section("options");
    for (auto& name : names) {
      f.dump_string("name", name);
    }
    f.close_section();
    return f.get();
  } else if (what.substr(0, 6) == "config") {
    PyEval_RestoreThread(tstate);
    if (what == "config_options") {
      g_conf().config_options(&f);
    } else if (what == "config") {
      g_conf().show_config(&f);
    }
    return f.get();
  } else if (what == "mon_map") {
    cluster_state.with_monmap(
      [&f, &tstate](const MonMap &monmap) {
        PyEval_RestoreThread(tstate);
        monmap.dump(&f);
      }
    );
    return f.get();
  } else if (what == "service_map") {
    cluster_state.with_servicemap(
      [&f, &tstate](const ServiceMap &service_map) {
        PyEval_RestoreThread(tstate);
        service_map.dump(&f);
      }
    );
    return f.get();
  } else if (what == "osd_metadata") {
    auto dmc = daemon_state.get_by_service("osd");
    PyEval_RestoreThread(tstate);

    for (const auto &[key, state] : dmc) {
      std::lock_guard l(state->lock);
      f.open_object_section(key.name.c_str());
      f.dump_string("hostname", state->hostname);
      for (const auto &[name, val] : state->metadata) {
        f.dump_string(name.c_str(), val);
      }
      f.close_section();
    }
    return f.get();
  } else if (what == "mds_metadata") {
    auto dmc = daemon_state.get_by_service("mds");
    PyEval_RestoreThread(tstate);

    for (const auto &[key, state] : dmc) {
      std::lock_guard l(state->lock);
      f.open_object_section(key.name.c_str());
      f.dump_string("hostname", state->hostname);
      for (const auto &[name, val] : state->metadata) {
        f.dump_string(name.c_str(), val);
      }
      f.close_section();
    }
    return f.get();
  } else if (what == "pg_summary") {
    cluster_state.with_pgmap(
        [&f, &tstate](const PGMap &pg_map) {
          PyEval_RestoreThread(tstate);

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
    cluster_state.with_pgmap(
        [&f, &tstate](const PGMap &pg_map) {
          PyEval_RestoreThread(tstate);
	  pg_map.print_summary(&f, nullptr);
        }
    );
    return f.get();
  } else if (what == "pg_dump") {
    cluster_state.with_pgmap(
      [&f, &tstate](const PGMap &pg_map) {
        PyEval_RestoreThread(tstate);
	pg_map.dump(&f, false);
      }
    );
    return f.get();
  } else if (what == "devices") {
    daemon_state.with_devices2(
      [&tstate, &f]() {
	PyEval_RestoreThread(tstate);
	f.open_array_section("devices");
      },
      [&f] (const DeviceState& dev) {
	f.dump_object("device", dev);
      });
    f.close_section();
    return f.get();
  } else if (what.size() > 7 &&
	     what.substr(0, 7) == "device ") {
    string devid = what.substr(7);
    if (!daemon_state.with_device(
	  devid,
	  [&f, &tstate] (const DeviceState& dev) {
	    PyEval_RestoreThread(tstate);
	    f.dump_object("device", dev);
	  })) {
      // device not found
      PyEval_RestoreThread(tstate);
    }
    return f.get();
  } else if (what == "io_rate") {
    cluster_state.with_pgmap(
      [&f, &tstate](const PGMap &pg_map) {
        PyEval_RestoreThread(tstate);
        pg_map.dump_delta(&f);
      }
    );
    return f.get();
  } else if (what == "df") {
    cluster_state.with_osdmap_and_pgmap(
      [&f, &tstate](
	const OSDMap& osd_map,
	const PGMap &pg_map) {
	PyEval_RestoreThread(tstate);
        pg_map.dump_cluster_stats(nullptr, &f, true);
        pg_map.dump_pool_stats_full(osd_map, nullptr, &f, true);
      });
    return f.get();
  } else if (what == "pg_stats") {
    cluster_state.with_pgmap(
        [&f, &tstate](const PGMap &pg_map) {
      PyEval_RestoreThread(tstate);
      pg_map.dump_pg_stats(&f, false);
    });
    return f.get();
  } else if (what == "pool_stats") {
    cluster_state.with_pgmap(
        [&f, &tstate](const PGMap &pg_map) {
      PyEval_RestoreThread(tstate);
      pg_map.dump_pool_stats(&f);
    });
    return f.get();
  } else if (what == "pg_ready") {
    PyEval_RestoreThread(tstate);
    server.dump_pg_ready(&f);
    return f.get();
  } else if (what == "osd_stats") {
    cluster_state.with_pgmap(
        [&f, &tstate](const PGMap &pg_map) {
      PyEval_RestoreThread(tstate);
      pg_map.dump_osd_stats(&f, false);
    });
    return f.get();
  } else if (what == "osd_ping_times") {
    cluster_state.with_pgmap(
        [&f, &tstate](const PGMap &pg_map) {
      PyEval_RestoreThread(tstate);
      pg_map.dump_osd_ping_times(&f);
    });
    return f.get();
  } else if (what == "osd_pool_stats") {
    int64_t poolid = -ENOENT;
    cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap,
					    const PGMap& pg_map) {
        PyEval_RestoreThread(tstate);
        f.open_array_section("pool_stats");
        for (auto &p : osdmap.get_pools()) {
          poolid = p.first;
          pg_map.dump_pool_stats_and_io_rate(poolid, osdmap, &f, nullptr);
        }
        f.close_section();
    });
    return f.get();
  } else if (what == "health") {
    cluster_state.with_health(
        [&f, &tstate](const ceph::bufferlist &health_json) {
      PyEval_RestoreThread(tstate);
      f.dump_string("json", health_json.to_str());
    });
    return f.get();
  } else if (what == "mon_status") {
    cluster_state.with_mon_status(
        [&f, &tstate](const ceph::bufferlist &mon_status_json) {
      PyEval_RestoreThread(tstate);
      f.dump_string("json", mon_status_json.to_str());
    });
    return f.get();
  } else if (what == "mgr_map") {
    cluster_state.with_mgrmap([&f, &tstate](const MgrMap &mgr_map) {
      PyEval_RestoreThread(tstate);
      mgr_map.dump(&f);
    });
    return f.get();
  } else {
    derr << "Python module requested unknown data '" << what << "'" << dendl;
    PyEval_RestoreThread(tstate);
    Py_RETURN_NONE;
  }
}

void ActivePyModules::start_one(PyModuleRef py_module)
{
  std::lock_guard l(lock);

  const auto name = py_module->get_name();
  auto active_module = std::make_shared<ActivePyModule>(py_module, clog);

  // Send all python calls down a Finisher to avoid blocking
  // C++ code, and avoid any potential lock cycles.
  finisher.queue(new LambdaContext([this, active_module, name](int) {
    int r = active_module->load(this);
    if (r != 0) {
      derr << "Failed to run module in active mode ('" << name << "')"
           << dendl;
    } else {
      std::lock_guard l(lock);
      auto em = modules.emplace(name, active_module);
      ceph_assert(em.second); // actually inserted

      dout(4) << "Starting thread for " << name << dendl;
      active_module->thread.create(active_module->get_thread_name());
    }
  }));
}

void ActivePyModules::shutdown()
{
  std::lock_guard locker(lock);

  // Signal modules to drop out of serve() and/or tear down resources
  for (auto& [name, module] : modules) {
    lock.unlock();
    dout(10) << "calling module " << name << " shutdown()" << dendl;
    module->shutdown();
    dout(10) << "module " << name << " shutdown() returned" << dendl;
    lock.lock();
  }

  // For modules implementing serve(), finish the threads where we
  // were running that.
  for (auto& [name, module] : modules) {
    lock.unlock();
    dout(10) << "joining module " << name << dendl;
    module->thread.join();
    dout(10) << "joined module " << name << dendl;
    lock.lock();
  }

  cmd_finisher.wait_for_empty();
  cmd_finisher.stop();

  modules.clear();
}

void ActivePyModules::notify_all(const std::string &notify_type,
                     const std::string &notify_id)
{
  std::lock_guard l(lock);

  dout(10) << __func__ << ": notify_all " << notify_type << dendl;
  for (auto& [name, module] : modules) {
    // Send all python calls down a Finisher to avoid blocking
    // C++ code, and avoid any potential lock cycles.
    dout(15) << "queuing notify to " << name << dendl;
    // workaround for https://bugs.llvm.org/show_bug.cgi?id=35984
    finisher.queue(new LambdaContext([module=module, notify_type, notify_id]
      (int r){ 
        module->notify(notify_type, notify_id); 
    }));
  }
}

void ActivePyModules::notify_all(const LogEntry &log_entry)
{
  std::lock_guard l(lock);

  dout(10) << __func__ << ": notify_all (clog)" << dendl;
  for (auto& [name, module] : modules) {
    // Send all python calls down a Finisher to avoid blocking
    // C++ code, and avoid any potential lock cycles.
    //
    // Note intentional use of non-reference lambda binding on
    // log_entry: we take a copy because caller's instance is
    // probably ephemeral.
    dout(15) << "queuing notify (clog) to " << name << dendl;
    // workaround for https://bugs.llvm.org/show_bug.cgi?id=35984
    finisher.queue(new LambdaContext([module=module, log_entry](int r){
      module->notify_clog(log_entry);
    }));
  }
}

bool ActivePyModules::get_store(const std::string &module_name,
    const std::string &key, std::string *val) const
{
  PyThreadState *tstate = PyEval_SaveThread();
  std::lock_guard l(lock);
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

PyObject *ActivePyModules::dispatch_remote(
    const std::string &other_module,
    const std::string &method,
    PyObject *args,
    PyObject *kwargs,
    std::string *err)
{
  auto mod_iter = modules.find(other_module);
  ceph_assert(mod_iter != modules.end());

  return mod_iter->second->dispatch_remote(method, args, kwargs, err);
}

bool ActivePyModules::get_config(const std::string &module_name,
    const std::string &key, std::string *val) const
{
  const std::string global_key = PyModule::config_prefix
    + module_name + "/" + key;

  dout(20) << " key: " << global_key << dendl;

  std::lock_guard lock(module_config.lock);

  auto i = module_config.config.find(global_key);
  if (i != module_config.config.end()) {
    *val = i->second;
    return true;
  } else {
    return false;
  }
}

PyObject *ActivePyModules::get_typed_config(
  const std::string &module_name,
  const std::string &key,
  const std::string &prefix) const
{
  PyThreadState *tstate = PyEval_SaveThread();
  std::string value;
  std::string final_key;
  bool found = false;
  if (prefix.size()) {
    final_key = prefix + "/" + key;
    found = get_config(module_name, final_key, &value);
  }
  if (!found) {
    final_key = key;
    found = get_config(module_name, final_key, &value);
  }
  if (found) {
    PyModuleRef module = py_module_registry.get_module(module_name);
    PyEval_RestoreThread(tstate);
    if (!module) {
        derr << "Module '" << module_name << "' is not available" << dendl;
        Py_RETURN_NONE;
    }
    dout(10) << __func__ << " " << final_key << " found: " << value << dendl;
    return module->get_typed_option_value(key, value);
  }
  PyEval_RestoreThread(tstate);
  if (prefix.size()) {
    dout(10) << " [" << prefix << "/]" << key << " not found "
	    << dendl;
  } else {
    dout(10) << " " << key << " not found " << dendl;
  }
  Py_RETURN_NONE;
}

PyObject *ActivePyModules::get_store_prefix(const std::string &module_name,
    const std::string &prefix) const
{
  PyThreadState *tstate = PyEval_SaveThread();
  std::lock_guard l(lock);
  std::lock_guard lock(module_config.lock);
  PyEval_RestoreThread(tstate);

  const std::string base_prefix = PyModule::config_prefix
                                    + module_name + "/";
  const std::string global_prefix = base_prefix + prefix;
  dout(4) << __func__ << " prefix: " << global_prefix << dendl;

  PyFormatter f;

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
    std::lock_guard l(lock);
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
  std::lock_guard l(lock);
  for (const auto& [name, module] : modules) {
    std::string svc_str = module->get_uri();
    if (!svc_str.empty()) {
      result[name] = svc_str;
    }
  }

  return result;
}

PyObject* ActivePyModules::with_perf_counters(
    std::function<void(PerfCounterInstance& counter_instance, PerfCounterType& counter_type, PyFormatter& f)> fct,
    const std::string &svc_name,
    const std::string &svc_id,
    const std::string &path) const
{
  PyThreadState *tstate = PyEval_SaveThread();
  std::lock_guard l(lock);
  PyEval_RestoreThread(tstate);

  PyFormatter f;
  f.open_array_section(path.c_str());

  auto metadata = daemon_state.get(DaemonKey{svc_name, svc_id});
  if (metadata) {
    std::lock_guard l2(metadata->lock);
    if (metadata->perf_counters.instances.count(path)) {
      auto counter_instance = metadata->perf_counters.instances.at(path);
      auto counter_type = metadata->perf_counters.types.at(path);
      fct(counter_instance, counter_type, f);
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

PyObject* ActivePyModules::get_counter_python(
    const std::string &svc_name,
    const std::string &svc_id,
    const std::string &path)
{
  auto extract_counters = [](
      PerfCounterInstance& counter_instance,
      PerfCounterType& counter_type,
      PyFormatter& f)
  {
    if (counter_type.type & PERFCOUNTER_LONGRUNAVG) {
      const auto &avg_data = counter_instance.get_data_avg();
      for (const auto &datapoint : avg_data) {
        f.open_array_section("datapoint");
        f.dump_float("t", datapoint.t);
        f.dump_unsigned("s", datapoint.s);
        f.dump_unsigned("c", datapoint.c);
        f.close_section();
      }
    } else {
      const auto &data = counter_instance.get_data();
      for (const auto &datapoint : data) {
        f.open_array_section("datapoint");
        f.dump_float("t", datapoint.t);
        f.dump_unsigned("v", datapoint.v);
        f.close_section();
      }
    }
  };
  return with_perf_counters(extract_counters, svc_name, svc_id, path);
}

PyObject* ActivePyModules::get_latest_counter_python(
    const std::string &svc_name,
    const std::string &svc_id,
    const std::string &path)
{
  auto extract_latest_counters = [](
      PerfCounterInstance& counter_instance,
      PerfCounterType& counter_type,
      PyFormatter& f)
  {
    if (counter_type.type & PERFCOUNTER_LONGRUNAVG) {
      const auto &datapoint = counter_instance.get_latest_data_avg();
      f.dump_float("t", datapoint.t);
      f.dump_unsigned("s", datapoint.s);
      f.dump_unsigned("c", datapoint.c);
    } else {
      const auto &datapoint = counter_instance.get_latest_data();
      f.dump_float("t", datapoint.t);
      f.dump_unsigned("v", datapoint.v);
    }
  };
  return with_perf_counters(extract_latest_counters, svc_name, svc_id, path);
}

PyObject* ActivePyModules::get_perf_schema_python(
    const std::string &svc_type,
    const std::string &svc_id)
{
  PyThreadState *tstate = PyEval_SaveThread();
  std::lock_guard l(lock);
  PyEval_RestoreThread(tstate);

  DaemonStateCollection daemons;

  if (svc_type == "") {
    daemons = daemon_state.get_all();
  } else if (svc_id.empty()) {
    daemons = daemon_state.get_by_service(svc_type);
  } else {
    auto key = DaemonKey{svc_type, svc_id};
    // so that the below can be a loop in all cases
    auto got = daemon_state.get(key);
    if (got != nullptr) {
      daemons[key] = got;
    }
  }

  PyFormatter f;
  if (!daemons.empty()) {
    for (auto& [key, state] : daemons) {
      f.open_object_section(ceph::to_string(key).c_str());

      std::lock_guard l(state->lock);
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
  std::lock_guard l(lock);
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
  ceph_assert(module);

  PyObject *wrapper_type = PyObject_GetAttrString(
      module, (const char*)clsname.c_str());
  if (!wrapper_type) {
    derr << "Failed to get python type:" << dendl;
    derr << handle_pyerror() << dendl;
  }
  ceph_assert(wrapper_type);

  // Construct a capsule containing an OSDMap.
  auto wrapped_capsule = PyCapsule_New(wrapped, nullptr, nullptr);
  ceph_assert(wrapped_capsule);

  // Construct the python OSDMap
  auto pArgs = PyTuple_Pack(1, wrapped_capsule);
  auto wrapper_instance = PyObject_CallObject(wrapper_type, pArgs);
  if (wrapper_instance == nullptr) {
    derr << "Failed to construct python OSDMap:" << dendl;
    derr << handle_pyerror() << dendl;
  }
  ceph_assert(wrapper_instance != nullptr);
  Py_DECREF(pArgs);
  Py_DECREF(wrapped_capsule);

  Py_DECREF(wrapper_type);
  Py_DECREF(module);

  return wrapper_instance;
}

PyObject *ActivePyModules::get_osdmap()
{
  OSDMap *newmap = new OSDMap;

  PyThreadState *tstate = PyEval_SaveThread();
  {
    std::lock_guard l(lock);
    cluster_state.with_osdmap([&](const OSDMap& o) {
        newmap->deepish_copy_from(o);
      });
  }
  PyEval_RestoreThread(tstate);

  return construct_with_capsule("mgr_module", "OSDMap", (void*)newmap);
}

void ActivePyModules::set_health_checks(const std::string& module_name,
				  health_check_map_t&& checks)
{
  bool changed = false;

  lock.lock();
  auto p = modules.find(module_name);
  if (p != modules.end()) {
    changed = p->second->set_health_checks(std::move(checks));
  }
  lock.unlock();

  // immediately schedule a report to be sent to the monitors with the new
  // health checks that have changed. This is done asynchronusly to avoid
  // blocking python land. ActivePyModules::lock needs to be dropped to make
  // lockdep happy:
  //
  //   send_report callers: DaemonServer::lock -> PyModuleRegistery::lock
  //   active_start: PyModuleRegistry::lock -> ActivePyModules::lock
  //
  // if we don't release this->lock before calling schedule_tick a cycle is
  // formed with the addition of ActivePyModules::lock -> DaemonServer::lock.
  // This is still correct as send_report is run asynchronously under
  // DaemonServer::lock.
  if (changed)
    server.schedule_tick(0);
}

int ActivePyModules::handle_command(
  const ModuleCommand& module_command,
  const MgrSession& session,
  const cmdmap_t &cmdmap,
  const bufferlist &inbuf,
  std::stringstream *ds,
  std::stringstream *ss)
{
  lock.lock();
  auto mod_iter = modules.find(module_command.module_name);
  if (mod_iter == modules.end()) {
    *ss << "Module '" << module_command.module_name << "' is not available";
    lock.unlock();
    return -ENOENT;
  }

  lock.unlock();
  return mod_iter->second->handle_command(module_command, session, cmdmap,
                                          inbuf, ds, ss);
}

void ActivePyModules::get_health_checks(health_check_map_t *checks)
{
  std::lock_guard l(lock);
  for (auto& [name, module] : modules) {
    dout(15) << "getting health checks for " << name << dendl;
    module->get_health_checks(checks);
  }
}

void ActivePyModules::update_progress_event(
  const std::string& evid,
  const std::string& desc,
  float progress)
{
  std::lock_guard l(lock);
  auto& pe = progress_events[evid];
  pe.message = desc;
  pe.progress = progress;
}

void ActivePyModules::complete_progress_event(const std::string& evid)
{
  std::lock_guard l(lock);
  progress_events.erase(evid);
}

void ActivePyModules::clear_all_progress_events()
{
  std::lock_guard l(lock);
  progress_events.clear();
}

void ActivePyModules::get_progress_events(std::map<std::string,ProgressEvent> *events)
{
  std::lock_guard l(lock);
  *events = progress_events;
}

void ActivePyModules::config_notify()
{
  std::lock_guard l(lock);
  for (auto& [name, module] : modules) {
    // Send all python calls down a Finisher to avoid blocking
    // C++ code, and avoid any potential lock cycles.
    dout(15) << "notify (config) " << name << dendl;
    // workaround for https://bugs.llvm.org/show_bug.cgi?id=35984
    finisher.queue(new LambdaContext([module=module](int r){ 
      module->config_notify();
    }));
  }
}

void ActivePyModules::set_uri(const std::string& module_name,
                        const std::string &uri)
{
  std::lock_guard l(lock);

  dout(4) << " module " << module_name << " set URI '" << uri << "'" << dendl;

  modules.at(module_name)->set_uri(uri);
}

MetricQueryID ActivePyModules::add_osd_perf_query(
    const OSDPerfMetricQuery &query,
    const std::optional<OSDPerfMetricLimit> &limit)
{
  return server.add_osd_perf_query(query, limit);
}

void ActivePyModules::remove_osd_perf_query(MetricQueryID query_id)
{
  int r = server.remove_osd_perf_query(query_id);
  if (r < 0) {
    dout(0) << "remove_osd_perf_query for query_id=" << query_id << " failed: "
            << cpp_strerror(r) << dendl;
  }
}

PyObject *ActivePyModules::get_osd_perf_counters(MetricQueryID query_id)
{
  std::map<OSDPerfMetricKey, PerformanceCounters> counters;

  int r = server.get_osd_perf_counters(query_id, &counters);
  if (r < 0) {
    dout(0) << "get_osd_perf_counters for query_id=" << query_id << " failed: "
            << cpp_strerror(r) << dendl;
    Py_RETURN_NONE;
  }

  PyFormatter f;

  f.open_array_section("counters");
  for (auto &it : counters) {
    auto &key = it.first;
    auto  &instance_counters = it.second;
    f.open_object_section("i");
    f.open_array_section("k");
    for (auto &sub_key : key) {
      f.open_array_section("s");
      for (size_t i = 0; i < sub_key.size(); i++) {
        f.dump_string(stringify(i).c_str(), sub_key[i]);
      }
      f.close_section(); // s
    }
    f.close_section(); // k
    f.open_array_section("c");
    for (auto &c : instance_counters) {
      f.open_array_section("p");
      f.dump_unsigned("0", c.first);
      f.dump_unsigned("1", c.second);
      f.close_section(); // p
    }
    f.close_section(); // c
    f.close_section(); // i
  }
  f.close_section(); // counters

  return f.get();
}

void ActivePyModules::cluster_log(const std::string &channel, clog_type prio,
  const std::string &message)
{
  std::lock_guard l(lock);

  auto cl = monc.get_log_client()->create_channel(channel);
  map<string,string> log_to_monitors;
  map<string,string> log_to_syslog;
  map<string,string> log_channel;
  map<string,string> log_prio;
  map<string,string> log_to_graylog;
  map<string,string> log_to_graylog_host;
  map<string,string> log_to_graylog_port;
  uuid_d fsid;
  string host;
  if (parse_log_client_options(g_ceph_context, log_to_monitors, log_to_syslog,
			       log_channel, log_prio, log_to_graylog,
			       log_to_graylog_host, log_to_graylog_port,
			       fsid, host) == 0)
    cl->update_config(log_to_monitors, log_to_syslog,
		      log_channel, log_prio, log_to_graylog,
		      log_to_graylog_host, log_to_graylog_port,
		      fsid, host);
  cl->do_log(prio, message);
}

void ActivePyModules::register_client(std::string_view name, std::string addrs)
{
  std::lock_guard l(lock);

  entity_addrvec_t addrv;
  addrv.parse(addrs.data());

  dout(7) << "registering msgr client handle " << addrv << dendl;
  py_module_registry.register_client(name, std::move(addrv));
}

void ActivePyModules::unregister_client(std::string_view name, std::string addrs)
{
  std::lock_guard l(lock);

  entity_addrvec_t addrv;
  addrv.parse(addrs.data());

  dout(7) << "unregistering msgr client handle " << addrv << dendl;
  py_module_registry.unregister_client(name, addrv);
}
