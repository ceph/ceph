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

#include "ActivePyModules.h"

#include <rocksdb/version.h>

#include "common/errno.h"
#include "include/stringify.h"

#include "mon/MonMap.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "mgr/MgrContext.h"
#include "mgr/TTLCache.h"
#include "mgr/mgr_perf_counters.h"

#include "DaemonKey.h"
#include "DaemonServer.h"
#include "mgr/MgrContext.h"
#include "PyFormatter.h"
// For ::mgr_store_prefix
#include "PyModule.h"
#include "PyModuleRegistry.h"
#include "PyUtil.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

using std::pair;
using std::string;
using namespace std::literals;

ActivePyModules::ActivePyModules(
  PyModuleConfig &module_config_,
  std::map<std::string, std::string> store_data,
  bool mon_provides_kv_sub,
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
  // we can only trust our ConfigMap if the mon cluster has provided
  // kv sub since our startup.
  have_local_config_map = mon_provides_kv_sub;
  _refresh_config_map();
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
    std::string id;
    without_gil([&ceph_version, &id, state=state] {
      std::lock_guard l(state->lock);
      // TODO: pick the highest version, and make sure that
      // somewhere else (during health reporting?) we are
      // indicating to the user if we see mixed versions
      auto ver_iter = state->metadata.find("ceph_version");
      if (ver_iter != state->metadata.end()) {
        ceph_version = state->metadata.at("ceph_version");
      }
      if (state->metadata.find("id") != state->metadata.end()) {
        id = state->metadata.at("id");
      }
    });
    f->open_object_section("service");
    f->dump_string("type", key.type);
    f->dump_string("id", key.name);
    f->dump_string("ceph_version", ceph_version);
    if (!id.empty()) {
      f->dump_string("name", id);
    }
    f->close_section();
  }
  f->close_section();

  f->dump_string("ceph_version", ceph_version);
}

PyObject *ActivePyModules::get_server_python(const std::string &hostname)
{
  const auto dmc = without_gil([&]{
    std::lock_guard l(lock);
    dout(10) << " (" << hostname << ")" << dendl;
    return daemon_state.get_by_server(hostname);
  });
  PyFormatter f;
  dump_server(hostname, dmc, &f);
  return f.get();
}


PyObject *ActivePyModules::list_servers_python()
{
  dout(10) << " >" << dendl;

  without_gil_t no_gil;
  return daemon_state.with_daemons_by_server([this, &no_gil]
      (const std::map<std::string, DaemonStateCollection> &all) {
    no_gil.acquire_gil();
    PyFormatter f(false, true);
    for (const auto &[hostname, daemon_state] : all) {
      f.open_object_section("server");
      dump_server(hostname, daemon_state, &f);
      f.close_section();
    }
    return f.get();
  });
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
  auto l = without_gil([&] {
    return std::lock_guard(lock);
  });
  PyFormatter f;
  f.dump_string("hostname", metadata->hostname);
  for (const auto &[key, val] : metadata->metadata) {
    f.dump_string(key, val);
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
  auto l = without_gil([&] {
    return std::lock_guard(lock);
  });
  PyFormatter f;
  for (const auto &[daemon, status] : metadata->service_status) {
    f.dump_string(daemon, status);
  }
  return f.get();
}

void ActivePyModules::update_cache_metrics() {
    auto hit_miss_ratio = ttl_cache.get_hit_miss_ratio();
    perfcounter->set(l_mgr_cache_hit, hit_miss_ratio.first);
    perfcounter->set(l_mgr_cache_miss, hit_miss_ratio.second);
}

PyObject *ActivePyModules::cacheable_get_python(const std::string &what)
{
  uint64_t ttl_seconds = g_conf().get_val<uint64_t>("mgr_ttl_cache_expire_seconds");
  if(ttl_seconds > 0) {
    ttl_cache.set_ttl(ttl_seconds);
    try{
      PyObject* cached = ttl_cache.get(what);
      update_cache_metrics();
      return cached;
    } catch (std::out_of_range& e) {}
  }

  PyObject *obj = get_python(what);
  if(ttl_seconds && ttl_cache.is_cacheable(what)) {
    ttl_cache.insert(what, obj);
    Py_INCREF(obj);
  }
  update_cache_metrics();
  return obj;
}

PyObject *ActivePyModules::get_python(const std::string &what)
{
  uint64_t ttl_seconds = g_conf().get_val<uint64_t>("mgr_ttl_cache_expire_seconds");

  PyFormatter pf;
  PyJSONFormatter jf;
  // Use PyJSONFormatter if TTL cache is enabled.
  Formatter &f = ttl_seconds ? (Formatter&)jf : (Formatter&)pf;

  if (what == "fs_map") {
    without_gil_t no_gil;
    cluster_state.with_fsmap([&](const FSMap &fsmap) {
      no_gil.acquire_gil();
      fsmap.dump(&f);
    });
  } else if (what == "osdmap_crush_map_text") {
    without_gil_t no_gil;
    bufferlist rdata;
    cluster_state.with_osdmap([&](const OSDMap &osd_map){
      osd_map.crush->encode(rdata, CEPH_FEATURES_SUPPORTED_DEFAULT);
    });
    std::string crush_text = rdata.to_str();
    no_gil.acquire_gil();
    return PyUnicode_FromString(crush_text.c_str());
  } else if (what.substr(0, 7) == "osd_map") {
    without_gil_t no_gil;
    cluster_state.with_osdmap([&](const OSDMap &osd_map){
      no_gil.acquire_gil();
      if (what == "osd_map") {
        osd_map.dump(&f, g_ceph_context);
      } else if (what == "osd_map_tree") {
        osd_map.print_tree(&f, nullptr);
      } else if (what == "osd_map_crush") {
        osd_map.crush->dump(&f);
      }
    });
  } else if (what == "modified_config_options") {
    without_gil_t no_gil;
    auto all_daemons = daemon_state.get_all();
    set<string> names;
    for (auto& [key, daemon] : all_daemons) {
      std::lock_guard l(daemon->lock);
      for (auto& [name, valmap] : daemon->config) {
	names.insert(name);
      }
    }
    no_gil.acquire_gil();
    f.open_array_section("options");
    for (auto& name : names) {
      f.dump_string("name", name);
    }
    f.close_section();
  } else if (what.substr(0, 6) == "config") {
    // We make a copy of the global config to avoid printing
    // to py formater (which may drop-take GIL) while holding
    // the global config lock, which might deadlock with other
    // thread that is holding the GIL and acquiring the global
    // config lock.
    ConfigProxy config{g_conf()};
    if (what == "config_options") {
      config.config_options(&f);
    } else if (what == "config") {
      config.show_config(&f);
    }
  } else if (what == "mon_map") {
    without_gil_t no_gil;
    cluster_state.with_monmap([&](const MonMap &monmap) {
      no_gil.acquire_gil();
      monmap.dump(&f);
    });
  } else if (what == "service_map") {
    without_gil_t no_gil;
    cluster_state.with_servicemap([&](const ServiceMap &service_map) {
      no_gil.acquire_gil();
      service_map.dump(&f);
    });
  } else if (what == "osd_metadata") {
    without_gil_t no_gil;
    auto dmc = daemon_state.get_by_service("osd");
    for (const auto &[key, state] : dmc) {
      std::lock_guard l(state->lock);
      with_gil(no_gil, [&f, &name=key.name, state=state] {
        f.open_object_section(name.c_str());
        f.dump_string("hostname", state->hostname);
        for (const auto &[name, val] : state->metadata) {
          f.dump_string(name.c_str(), val);
        }
        f.close_section();
      });
    }
  } else if (what == "mds_metadata") {
    without_gil_t no_gil;
    auto dmc = daemon_state.get_by_service("mds");
    for (const auto &[key, state] : dmc) {
      std::lock_guard l(state->lock);
      with_gil(no_gil, [&f, &name=key.name, state=state] {
        f.open_object_section(name.c_str());
        f.dump_string("hostname", state->hostname);
        for (const auto &[name, val] : state->metadata) {
          f.dump_string(name.c_str(), val);
        }
        f.close_section();
      });
    }
  } else if (what == "pg_summary") {
    without_gil_t no_gil;
    cluster_state.with_pgmap(
        [&f, &no_gil](const PGMap &pg_map) {
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
          no_gil.acquire_gil();
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
  } else if (what == "pg_status") {
    without_gil_t no_gil;
    cluster_state.with_pgmap(
        [&](const PGMap &pg_map) {
	  no_gil.acquire_gil();
	  pg_map.print_summary(&f, nullptr);
        }
    );
  } else if (what == "pg_dump") {
    without_gil_t no_gil;
    cluster_state.with_pgmap(
      [&](const PGMap &pg_map) {
	no_gil.acquire_gil();
	pg_map.dump(&f, false);
      }
    );
  } else if (what == "devices") {
    without_gil_t no_gil;
    daemon_state.with_devices2(
      [&] {
        with_gil(no_gil, [&] { f.open_array_section("devices"); });
      },
      [&](const DeviceState &dev) {
        with_gil(no_gil, [&] { f.dump_object("device", dev); });
      });
    with_gil(no_gil, [&] {
      f.close_section();
    });
  } else if (what.size() > 7 &&
	     what.substr(0, 7) == "device ") {
    without_gil_t no_gil;
    string devid = what.substr(7);
    if (!daemon_state.with_device(devid,
      [&] (const DeviceState& dev) {
        with_gil_t with_gil{no_gil};
        f.dump_object("device", dev);
      })) {
      // device not found
    }
  } else if (what == "io_rate") {
    without_gil_t no_gil;
    cluster_state.with_pgmap(
      [&](const PGMap &pg_map) {
        no_gil.acquire_gil();
        pg_map.dump_delta(&f);
      }
    );
  } else if (what == "df") {
    without_gil_t no_gil;
    cluster_state.with_osdmap_and_pgmap(
      [&](
	const OSDMap& osd_map,
	const PGMap &pg_map) {
        no_gil.acquire_gil();
        pg_map.dump_cluster_stats(nullptr, &f, true);
        pg_map.dump_pool_stats_full(osd_map, nullptr, &f, true);
      });
  } else if (what == "pg_stats") {
    without_gil_t no_gil;
    cluster_state.with_pgmap([&](const PGMap &pg_map) {
      no_gil.acquire_gil();
      pg_map.dump_pg_stats(&f, false);
    });
  } else if (what == "pool_stats") {
    without_gil_t no_gil;
    cluster_state.with_pgmap([&](const PGMap &pg_map) {
      no_gil.acquire_gil();
      pg_map.dump_pool_stats(&f);
    });
  } else if (what == "pg_ready") {
    server.dump_pg_ready(&f);
  } else if (what == "pg_progress") {
    without_gil_t no_gil;
    cluster_state.with_pgmap([&](const PGMap &pg_map) {
      no_gil.acquire_gil();
      pg_map.dump_pg_progress(&f);
      server.dump_pg_ready(&f);
    });
  } else if (what == "osd_stats") {
    without_gil_t no_gil;
    cluster_state.with_pgmap([&](const PGMap &pg_map) {
      no_gil.acquire_gil();
      pg_map.dump_osd_stats(&f, false);
    });
  } else if (what == "osd_ping_times") {
    without_gil_t no_gil;
    cluster_state.with_pgmap([&](const PGMap &pg_map) {
      no_gil.acquire_gil();
      pg_map.dump_osd_ping_times(&f);
    });
  } else if (what == "osd_pool_stats") {
    without_gil_t no_gil;
    int64_t poolid = -ENOENT;
    cluster_state.with_osdmap_and_pgmap([&](const OSDMap& osdmap,
					    const PGMap& pg_map) {
      no_gil.acquire_gil();
      f.open_array_section("pool_stats");
      for (auto &p : osdmap.get_pools()) {
        poolid = p.first;
        pg_map.dump_pool_stats_and_io_rate(poolid, osdmap, &f, nullptr);
      }
      f.close_section();
    });
  } else if (what == "health") {
    without_gil_t no_gil;
    cluster_state.with_health([&](const ceph::bufferlist &health_json) {
      no_gil.acquire_gil();
      f.dump_string("json", health_json.to_str());
    });
  } else if (what == "mon_status") {
    without_gil_t no_gil;
    cluster_state.with_mon_status(
        [&](const ceph::bufferlist &mon_status_json) {
      no_gil.acquire_gil();
      f.dump_string("json", mon_status_json.to_str());
    });
  } else if (what == "mgr_map") {
    without_gil_t no_gil;
    cluster_state.with_mgrmap([&](const MgrMap &mgr_map) {
      no_gil.acquire_gil();
      mgr_map.dump(&f);
    });
  } else if (what == "mgr_ips") {
    entity_addrvec_t myaddrs = server.get_myaddrs();
    f.open_array_section("ips");
    std::set<std::string> did;
    for (auto& i : myaddrs.v) {
      std::string ip = i.ip_only_to_str();
      if (auto [where, inserted] = did.insert(ip); inserted) {
	f.dump_string("ip", ip);
      }
    }
    f.close_section();
  } else if (what == "have_local_config_map") {
    f.dump_bool("have_local_config_map", have_local_config_map);
  } else if (what == "active_clean_pgs"){
    without_gil_t no_gil;
    cluster_state.with_pgmap(
        [&](const PGMap &pg_map) {
      no_gil.acquire_gil();
      f.open_array_section("pg_stats");
      for (auto &i : pg_map.pg_stat) {
        const auto state = i.second.state;
	const auto pgid_raw = i.first;
	const auto pgid = stringify(pgid_raw.m_pool) + "." + stringify(pgid_raw.m_seed);
	const auto reported_epoch = i.second.reported_epoch;
	if (state & PG_STATE_ACTIVE && state & PG_STATE_CLEAN) {
	  f.open_object_section("pg_stat");
	  f.dump_string("pgid", pgid);
	  f.dump_string("state", pg_state_string(state));
	  f.dump_unsigned("reported_epoch", reported_epoch);
	  f.close_section();
	}
      }
      f.close_section();
      const auto num_pg = pg_map.num_pg;
      f.dump_unsigned("total_num_pgs", num_pg);
    });
  } else {
    derr << "Python module requested unknown data '" << what << "'" << dendl;
    Py_RETURN_NONE;
  }
  if(ttl_seconds) {
    return jf.get();
  } else {
    return pf.get();
  }
}

void ActivePyModules::start_one(PyModuleRef py_module)
{
  std::lock_guard l(lock);

  const auto name = py_module->get_name();
  auto active_module = std::make_shared<ActivePyModule>(py_module, clog);

  pending_modules.insert(name);
  // Send all python calls down a Finisher to avoid blocking
  // C++ code, and avoid any potential lock cycles.
  finisher.queue(new LambdaContext([this, active_module, name](int) {
    int r = active_module->load(this);
    std::lock_guard l(lock);
    pending_modules.erase(name);
    if (r != 0) {
      derr << "Failed to run module in active mode ('" << name << "')"
           << dendl;
    } else {
      auto em = modules.emplace(name, active_module);
      ceph_assert(em.second); // actually inserted

      dout(4) << "Starting thread for " << name << dendl;
      active_module->thread.create(active_module->get_thread_name());
      dout(4) << "Starting active module " << name <<" finisher thread "
        << active_module->get_fin_thread_name() << dendl;
      active_module->finisher.start();
    }
  }));
}

void ActivePyModules::notify_all(const std::string &notify_type,
                     const std::string &notify_id)
{
  std::lock_guard l(lock);

  dout(10) << __func__ << ": notify_all " << notify_type << dendl;
  for (auto& [name, module] : modules) {
    if (!py_module_registry.should_notify(name, notify_type)) {
      continue;
    }
    // Send all python calls down a Finisher to avoid blocking
    // C++ code, and avoid any potential lock cycles.
    dout(15) << "queuing notify (" << notify_type << ") to " << name << dendl;
    Finisher& mod_finisher = py_module_registry.get_active_module_finisher(name);
    // workaround for https://bugs.llvm.org/show_bug.cgi?id=35984
    mod_finisher.queue(new LambdaContext([module=module, notify_type, notify_id]
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
    if (!py_module_registry.should_notify(name, "clog")) {
      continue;
    }
    // Send all python calls down a Finisher to avoid blocking
    // C++ code, and avoid any potential lock cycles.
    //
    // Note intentional use of non-reference lambda binding on
    // log_entry: we take a copy because caller's instance is
    // probably ephemeral.
    dout(15) << "queuing notify (clog) to " << name << dendl;
    Finisher& mod_finisher = py_module_registry.get_active_module_finisher(name);
    // workaround for https://bugs.llvm.org/show_bug.cgi?id=35984
    mod_finisher.queue(new LambdaContext([module=module, log_entry](int r){
      module->notify_clog(log_entry);
    }));
  }
}

bool ActivePyModules::get_store(const std::string &module_name,
    const std::string &key, std::string *val) const
{
  without_gil_t no_gil;
  std::lock_guard l(lock);

  const std::string global_key = PyModule::mgr_store_prefix
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
  const std::string global_key = "mgr/" + module_name + "/" + key;

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
  without_gil_t no_gil;
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
    no_gil.acquire_gil();
    if (!module) {
        derr << "Module '" << module_name << "' is not available" << dendl;
        Py_RETURN_NONE;
    }
    // removing value to hide sensitive data going into mgr logs
    // leaving this for debugging purposes
    // dout(10) << __func__ << " " << final_key << " found: " << value << dendl;
    dout(10) << __func__ << " " << final_key << " found" << dendl;
    return module->get_typed_option_value(key, value);
  }
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
  without_gil_t no_gil;
  std::lock_guard l(lock);
  std::lock_guard lock(module_config.lock);
  no_gil.acquire_gil();

  const std::string base_prefix = PyModule::mgr_store_prefix
                                    + module_name + "/";
  const std::string global_prefix = base_prefix + prefix;
  dout(4) << __func__ << " prefix: " << global_prefix << dendl;

  PyFormatter f;
  for (auto p = store_cache.lower_bound(global_prefix);
       p != store_cache.end() && p->first.find(global_prefix) == 0; ++p) {
    f.dump_string(p->first.c_str() + base_prefix.size(), p->second);
  }
  return f.get();
}

void ActivePyModules::set_store(const std::string &module_name,
    const std::string &key, const std::optional<std::string>& val)
{
  const std::string global_key = PyModule::mgr_store_prefix
                                   + module_name + "/" + key;

  Command set_cmd;
  {
    std::lock_guard l(lock);

    // NOTE: this isn't strictly necessary since we'll also get an MKVData
    // update from the mon due to our subscription *before* our command is acked.
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

std::pair<int, std::string> ActivePyModules::set_config(
  const std::string &module_name,
  const std::string &key,
  const std::optional<std::string>& val)
{
  return module_config.set_config(&monc, module_name, key, val);
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

void ActivePyModules::update_kv_data(
  const std::string prefix,
  bool incremental,
  const map<std::string, std::optional<bufferlist>, std::less<>>& data)
{
  std::lock_guard l(lock);
  bool do_config = false;
  if (!incremental) {
    dout(10) << "full update on " << prefix << dendl;
    auto p = store_cache.lower_bound(prefix);
    while (p != store_cache.end() && p->first.find(prefix) == 0) {
      dout(20) << " rm prior " << p->first << dendl;
      p = store_cache.erase(p);
    }
  } else {
    dout(10) << "incremental update on " << prefix << dendl;
  }
  for (auto& i : data) {
    if (i.second) {
      dout(20) << " set " << i.first << " = " << i.second->to_str() << dendl;
      store_cache[i.first] = i.second->to_str();
    } else {
      dout(20) << " rm " << i.first << dendl;
      store_cache.erase(i.first);
    }
    if (i.first.find("config/") == 0) {
      do_config = true;
    }
  }
  if (do_config) {
    _refresh_config_map();
  }
}

void ActivePyModules::_refresh_config_map()
{
  dout(10) << dendl;
  config_map.clear();
  for (auto p = store_cache.lower_bound("config/");
       p != store_cache.end() && p->first.find("config/") == 0;
       ++p) {
    string key = p->first.substr(7);
    if (key.find("mgr/") == 0) {
      // NOTE: for now, we ignore module options.  see also ceph_foreign_option_get().
      continue;
    }
    string value = p->second;
    string name;
    string who;
    config_map.parse_key(key, &name, &who);

    config_map.add_option(
      g_ceph_context, name, who, value,
      [&](const std::string& name) {
	return  g_conf().find_option(name);
      });
  }
}

PyObject* ActivePyModules::with_perf_counters(
    std::function<void(PerfCounterInstance& counter_instance, PerfCounterType& counter_type, PyFormatter& f)> fct,
    const std::string &svc_name,
    const std::string &svc_id,
    const std::string &path) const
{
  PyFormatter f;
  f.open_array_section(path);
  {
    without_gil_t no_gil;
    std::lock_guard l(lock);
    auto metadata = daemon_state.get(DaemonKey{svc_name, svc_id});
    if (metadata) {
      std::lock_guard l2(metadata->lock);
      if (metadata->perf_counters.instances.count(path)) {
        auto counter_instance = metadata->perf_counters.instances.at(path);
        auto counter_type = metadata->perf_counters.types.at(path);
        with_gil(no_gil, [&] {
          fct(counter_instance, counter_type, f);
        });
      } else {
        dout(4) << "Missing counter: '" << path << "' ("
		<< svc_name << "." << svc_id << ")" << dendl;
        dout(20) << "Paths are:" << dendl;
        for (const auto &i : metadata->perf_counters.instances) {
          dout(20) << i.first << dendl;
        }
      }
    } else {
      dout(4) << "No daemon state for " << svc_name << "." << svc_id << ")"
              << dendl;
    }
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
  without_gil_t no_gil;
  std::lock_guard l(lock);

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

  auto f = with_gil(no_gil, [&] {
    return PyFormatter();
  });
  if (!daemons.empty()) {
    for (auto& [key, state] : daemons) {
      std::lock_guard l(state->lock);
      with_gil(no_gil, [&, key=ceph::to_string(key), state=state] {
        f.open_object_section(key.c_str());
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
      });
    }
  } else {
    dout(4) << __func__ << ": No daemon state found for "
              << svc_type << "." << svc_id << ")" << dendl;
  }
  return f.get();
}

PyObject* ActivePyModules::get_rocksdb_version()
{
  std::string version = std::to_string(ROCKSDB_MAJOR) + "." +
                        std::to_string(ROCKSDB_MINOR) + "." +
                        std::to_string(ROCKSDB_PATCH);

  return PyUnicode_FromString(version.c_str());
}

PyObject *ActivePyModules::get_context()
{
  auto l = without_gil([&] {
    return std::lock_guard(lock);
  });
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
    derr << handle_pyerror(true, module_name,
			   "construct_with_capsule "s + module_name + " " + clsname) << dendl;
  }
  ceph_assert(module);

  PyObject *wrapper_type = PyObject_GetAttrString(
      module, (const char*)clsname.c_str());
  if (!wrapper_type) {
    derr << "Failed to get python type:" << dendl;
    derr << handle_pyerror(true, module_name,
			   "construct_with_capsule "s + module_name + " " + clsname) << dendl;
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
    derr << handle_pyerror(true, module_name,
			   "construct_with_capsule "s + module_name + " " + clsname) << dendl;
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
  auto newmap = without_gil([&] {
    OSDMap *newmap = new OSDMap;
    cluster_state.with_osdmap([&](const OSDMap& o) {
      newmap->deepish_copy_from(o);
    });
    return newmap;
  });
  return construct_with_capsule("mgr_module", "OSDMap", (void*)newmap);
}

PyObject *ActivePyModules::get_foreign_config(
  const std::string& who,
  const std::string& name)
{
  dout(10) << "ceph_foreign_option_get " << who << " " << name << dendl;

  // NOTE: for now this will only work with build-in options, not module options.
  const Option *opt = g_conf().find_option(name);
  if (!opt) {
    dout(4) << "ceph_foreign_option_get " << name << " not found " << dendl;
    PyErr_Format(PyExc_KeyError, "option not found: %s", name.c_str());
    return nullptr;
  }

  // If the monitors are not yet running pacific, we cannot rely on our local
  // ConfigMap
  if (!have_local_config_map) {
    dout(20) << "mon cluster wasn't pacific when we started: falling back to 'config get'"
	     << dendl;
    without_gil_t no_gil;
    Command cmd;
    {
      std::lock_guard l(lock);
      cmd.run(
	&monc,
	"{\"prefix\": \"config get\","s +
	"\"who\": \""s + who + "\","s +
	"\"key\": \""s + name + "\"}");
    }
    cmd.wait();
    dout(10) << "ceph_foreign_option_get (mon command) " << who << " " << name << " = "
	     << cmd.outbl.to_str() << dendl;
    no_gil.acquire_gil();
    return get_python_typed_option_value(opt->type, cmd.outbl.to_str());
  }

  // mimic the behavor of mon/ConfigMonitor's 'config get' command
  EntityName entity;
  if (!entity.from_str(who) &&
      !entity.from_str(who + ".")) {
    dout(5) << "unrecognized entity '" << who << "'" << dendl;
    PyErr_Format(PyExc_KeyError, "invalid entity: %s", who.c_str());
    return nullptr;
  }

  without_gil_t no_gil;
  lock.lock();

  // FIXME: this is super inefficient, since we generate the entire daemon
  // config just to extract one value from it!

  std::map<std::string,std::string,std::less<>> config;
  cluster_state.with_osdmap([&](const OSDMap &osdmap) {
      map<string,string> crush_location;
      string device_class;
      if (entity.is_osd()) {
	osdmap.crush->get_full_location(who, &crush_location);
	int id = atoi(entity.get_id().c_str());
	const char *c = osdmap.crush->get_item_class(id);
	if (c) {
	  device_class = c;
	}
	dout(10) << __func__ << " crush_location " << crush_location
		 << " class " << device_class << dendl;
      }

      config = config_map.generate_entity_map(
	entity,
	crush_location,
	osdmap.crush.get(),
	device_class);
    });

  // get a single value
  string value;
  auto p = config.find(name);
  if (p != config.end()) {
    value = p->second;
  } else {
    if (!entity.is_client() &&
	opt->daemon_value != Option::value_t{}) {
      value = Option::to_str(opt->daemon_value);
    } else {
      value = Option::to_str(opt->value);
    }
  }

  dout(10) << "ceph_foreign_option_get (configmap) " << who << " " << name << " = "
	   << value << dendl;
  lock.unlock();
  no_gil.acquire_gil();
  return get_python_typed_option_value(opt->type, value);
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
  float progress,
  bool add_to_ceph_s)
{
  std::lock_guard l(lock);
  auto& pe = progress_events[evid];
  pe.message = desc;
  pe.progress = progress;
  pe.add_to_ceph_s = add_to_ceph_s;
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
    Finisher& mod_finisher = py_module_registry.get_active_module_finisher(name);
    // workaround for https://bugs.llvm.org/show_bug.cgi?id=35984
    mod_finisher.queue(new LambdaContext([module=module](int r){
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

void ActivePyModules::set_device_wear_level(const std::string& devid,
					    float wear_level)
{
  // update mgr state
  map<string,string> meta;
  daemon_state.with_device(
    devid,
    [wear_level, &meta] (DeviceState& dev) {
      dev.set_wear_level(wear_level);
      meta = dev.metadata;
    });

  // tell mon
  json_spirit::Object json_object;
  for (auto& i : meta) {
    json_spirit::Config::add(json_object, i.first, i.second);
  }
  bufferlist json;
  json.append(json_spirit::write(json_object));
  const string cmd =
    "{"
    "\"prefix\": \"config-key set\", "
    "\"key\": \"device/" + devid + "\""
    "}";

  Command set_cmd;
  set_cmd.run(&monc, cmd, json);
  set_cmd.wait();
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
  OSDPerfCollector collector(query_id);
  int r = server.get_osd_perf_counters(&collector);
  if (r < 0) {
    dout(0) << "get_osd_perf_counters for query_id=" << query_id << " failed: "
            << cpp_strerror(r) << dendl;
    Py_RETURN_NONE;
  }

  PyFormatter f;
  const std::map<OSDPerfMetricKey, PerformanceCounters> &counters = collector.counters;

  f.open_array_section("counters");
  for (auto &[key, instance_counters] : counters) {
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

MetricQueryID ActivePyModules::add_mds_perf_query(
    const MDSPerfMetricQuery &query,
    const std::optional<MDSPerfMetricLimit> &limit)
{
  return server.add_mds_perf_query(query, limit);
}

void ActivePyModules::remove_mds_perf_query(MetricQueryID query_id)
{
  int r = server.remove_mds_perf_query(query_id);
  if (r < 0) {
    dout(0) << "remove_mds_perf_query for query_id=" << query_id << " failed: "
            << cpp_strerror(r) << dendl;
  }
}

void ActivePyModules::reregister_mds_perf_queries()
{
  server.reregister_mds_perf_queries();
}

PyObject *ActivePyModules::get_mds_perf_counters(MetricQueryID query_id)
{
  MDSPerfCollector collector(query_id);
  int r = server.get_mds_perf_counters(&collector);
  if (r < 0) {
    dout(0) << "get_mds_perf_counters for query_id=" << query_id << " failed: "
            << cpp_strerror(r) << dendl;
    Py_RETURN_NONE;
  }

  PyFormatter f;
  const std::map<MDSPerfMetricKey, PerformanceCounters> &counters = collector.counters;

  f.open_array_section("metrics");

  f.open_array_section("delayed_ranks");
  f.dump_string("ranks", stringify(collector.delayed_ranks).c_str());
  f.close_section(); // delayed_ranks

  f.open_array_section("counters");
  for (auto &[key, instance_counters] : counters) {
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

  f.open_array_section("last_updated");
  f.dump_float("last_updated_mono", collector.last_updated_mono);
  f.close_section(); // last_updated

  f.close_section(); // metrics

  return f.get();
}

void ActivePyModules::cluster_log(const std::string &channel, clog_type prio,
  const std::string &message)
{
  std::lock_guard l(lock);

  auto cl = monc.get_log_client()->create_channel(channel);
  cl->parse_client_options(g_ceph_context);
  cl->do_log(prio, message);
}

void ActivePyModules::register_client(std::string_view name, std::string addrs, bool replace)
{
  entity_addrvec_t addrv;
  addrv.parse(addrs.data());

  dout(7) << "registering msgr client handle " << addrv << " (replace=" << replace << ")" << dendl;
  py_module_registry.register_client(name, std::move(addrv), replace);
}

void ActivePyModules::unregister_client(std::string_view name, std::string addrs)
{
  entity_addrvec_t addrv;
  addrv.parse(addrs.data());

  dout(7) << "unregistering msgr client handle " << addrv << dendl;
  py_module_registry.unregister_client(name, addrv);
}

PyObject* ActivePyModules::get_daemon_health_metrics()
{
  without_gil_t no_gil;
  return daemon_state.with_daemons_by_server([&no_gil]
      (const std::map<std::string, DaemonStateCollection> &all) {
      no_gil.acquire_gil();
      PyFormatter f;
      for (const auto &[hostname, daemon_state] : all) {
        for (const auto &[key, state] : daemon_state) {
          f.open_array_section(ceph::to_string(key));
          for (const auto &metric : state->daemon_health_metrics) {
            f.open_object_section(metric.get_type_name());
            f.dump_int("value", metric.get_n1());
            f.dump_string("type", metric.get_type_name());
            f.close_section();
          }
          f.close_section();
        }
      }
      return f.get();
  });
}
