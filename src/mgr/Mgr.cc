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

#include <Python.h>

#include "osdc/Objecter.h"
#include "client/Client.h"
#include "common/errno.h"
#include "mon/MonClient.h"
#include "include/stringify.h"
#include "global/global_context.h"
#include "global/signal_handler.h"

#ifdef WITH_LIBCEPHSQLITE
#  include <sqlite3.h>
#  include "include/libcephsqlite.h"
#endif

#include "mgr/MgrContext.h"

#include "DaemonServer.h"
#include "messages/MMgrDigest.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MLog.h"
#include "messages/MServiceMap.h"
#include "messages/MKVData.h"
#include "PyModule.h"
#include "Mgr.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

using namespace std::literals;

using std::map;
using std::ostringstream;
using std::string;

Mgr::Mgr(MonClient *monc_, const MgrMap& mgrmap,
         PyModuleRegistry *py_module_registry_,
	 Messenger *clientm_, Objecter *objecter_,
	 Client* client_, LogChannelRef clog_, LogChannelRef audit_clog_) :
  monc(monc_),
  objecter(objecter_),
  client(client_),
  client_messenger(clientm_),
  finisher(g_ceph_context, "Mgr", "mgr-fin"),
  digest_received(false),
  py_module_registry(py_module_registry_),
  cluster_state(monc, nullptr, mgrmap),
  server(monc, finisher, daemon_state, cluster_state, *py_module_registry,
         clog_, audit_clog_),
  clog(clog_),
  audit_clog(audit_clog_),
  initialized(false),
  initializing(false)
{
  cluster_state.set_objecter(objecter);
}


Mgr::~Mgr()
{
}

void MetadataUpdate::finish(int r)
{
  daemon_state.clear_updating(key);
  if (r == 0) {
    if (key.type == "mds" || key.type == "osd" ||
        key.type == "mgr" || key.type == "mon") {
      json_spirit::mValue json_result;
      bool read_ok = json_spirit::read(
          outbl.to_str(), json_result);
      if (!read_ok) {
        dout(1) << "mon returned invalid JSON for " << key << dendl;
        return;
      }
      if (json_result.type() != json_spirit::obj_type) {
        dout(1) << "mon returned valid JSON " << key
		<< " but not an object: '" << outbl.to_str() << "'" << dendl;
        return;
      }
      dout(4) << "mon returned valid metadata JSON for " << key << dendl;

      json_spirit::mObject daemon_meta = json_result.get_obj();

      // Skip daemon who doesn't have hostname yet
      if (daemon_meta.count("hostname") == 0) {
        dout(1) << "Skipping incomplete metadata entry for " << key << dendl;
        return;
      }

      // Apply any defaults
      for (const auto &i : defaults) {
        if (daemon_meta.find(i.first) == daemon_meta.end()) {
          daemon_meta[i.first] = i.second;
        }
      }

      if (daemon_state.exists(key)) {
        DaemonStatePtr state = daemon_state.get(key);
	map<string,string> m;
	{
	  std::lock_guard l(state->lock);
	  state->hostname = daemon_meta.at("hostname").get_str();

	  if (key.type == "mds" || key.type == "mgr" || key.type == "mon") {
	    daemon_meta.erase("name");
	  } else if (key.type == "osd") {
	    daemon_meta.erase("id");
	  }
	  daemon_meta.erase("hostname");
	  for (const auto &[key, val] : daemon_meta) {
	    m.emplace(key, val.get_str());
	  }
	}
	daemon_state.update_metadata(state, m);
      } else {
        auto state = std::make_shared<DaemonState>(daemon_state.types);
        state->key = key;
        state->hostname = daemon_meta.at("hostname").get_str();

        if (key.type == "mds" || key.type == "mgr" || key.type == "mon") {
          daemon_meta.erase("name");
        } else if (key.type == "osd") {
          daemon_meta.erase("id");
        }
        daemon_meta.erase("hostname");

	map<string,string> m;
        for (const auto &[key, val] : daemon_meta) {
          m.emplace(key, val.get_str());
        }
	state->set_metadata(m);

        daemon_state.insert(state);
      }
    } else {
      ceph_abort();
    }
  } else {
    dout(1) << "mon failed to return metadata for " << key
	    << ": " << cpp_strerror(r) << dendl;
  }
}

void Mgr::background_init(Context *completion)
{
  std::lock_guard l(lock);
  ceph_assert(!initializing);
  ceph_assert(!initialized);
  initializing = true;

  finisher.start();

  finisher.queue(new LambdaContext([this, completion](int r){
    init();
    completion->complete(0);
  }));
}

std::map<std::string, std::string> Mgr::load_store()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  dout(10) << "listing keys" << dendl;
  JSONCommand cmd;
  cmd.run(monc, "{\"prefix\": \"config-key ls\"}");
  lock.unlock();
  cmd.wait();
  lock.lock();
  ceph_assert(cmd.r == 0);

  std::map<std::string, std::string> loaded;
  
  for (auto &key_str : cmd.json_result.get_array()) {
    std::string const key = key_str.get_str();
    
    dout(20) << "saw key '" << key << "'" << dendl;

    const std::string store_prefix = PyModule::mgr_store_prefix;
    const std::string device_prefix = "device/";

    if (key.substr(0, device_prefix.size()) == device_prefix ||
	key.substr(0, store_prefix.size()) == store_prefix) {
      dout(20) << "fetching '" << key << "'" << dendl;
      Command get_cmd;
      std::ostringstream cmd_json;
      cmd_json << "{\"prefix\": \"config-key get\", \"key\": \"" << key << "\"}";
      get_cmd.run(monc, cmd_json.str());
      lock.unlock();
      get_cmd.wait();
      lock.lock();
      if (get_cmd.r == 0) { // tolerate racing config-key change
	loaded[key] = get_cmd.outbl.to_str();
      }
    }
  }

  return loaded;
}

static void handle_mgr_signal(int signum)
{
  derr << " *** Got signal " << sig_str(signum) << " ***" << dendl;

  // The python modules don't reliably shut down, so don't even
  // try. The mon will blocklist us (and all of our rados/cephfs
  // clients) anyway. Just exit!

  _exit(0);  // exit with 0 result code, as if we had done an orderly shutdown
}

void Mgr::init()
{
  std::unique_lock l(lock);
  ceph_assert(initializing);
  ceph_assert(!initialized);

  // Enable signal handlers
  register_async_signal_handler_oneshot(SIGINT, handle_mgr_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_mgr_signal);

  // Only pacific+ monitors support subscribe to kv updates
  bool mon_allows_kv_sub = false;
  monc->with_monmap(
    [&](const MonMap &monmap) {
      if (monmap.get_required_features().contains_all(
	    ceph::features::mon::FEATURE_PACIFIC)) {
	mon_allows_kv_sub = true;
      }
    });
  if (!mon_allows_kv_sub) {
    // mons are still pre-pacific.  wait long enough to ensure our
    // next beacon is processed so that our module options are
    // propagated.  See https://tracker.ceph.com/issues/49778
    lock.unlock();
    dout(10) << "waiting a bit for the pre-pacific mon to process our beacon" << dendl;
    sleep(g_conf().get_val<std::chrono::seconds>("mgr_tick_period").count() * 3);
    lock.lock();
  }

  // subscribe to all the maps
  monc->sub_want("log-info", 0, 0);
  monc->sub_want("mgrdigest", 0, 0);
  monc->sub_want("fsmap", 0, 0);
  monc->sub_want("servicemap", 0, 0);
  if (mon_allows_kv_sub) {
    monc->sub_want("kv:config/", 0, 0);
    monc->sub_want("kv:mgr/", 0, 0);
    monc->sub_want("kv:device/", 0, 0);
  }

  dout(4) << "waiting for OSDMap..." << dendl;
  // Subscribe to OSDMap update to pass on to ClusterState
  objecter->maybe_request_map();

  // reset the mon session.  we get these maps through subscriptions which
  // are stateful with the connection, so even if *we* don't have them a
  // previous incarnation sharing the same MonClient may have.
  monc->reopen_session();

  // Start Objecter and wait for OSD map
  lock.unlock();  // Drop lock because OSDMap dispatch calls into my ms_dispatch
  epoch_t e;
  cluster_state.with_mgrmap([&e](const MgrMap& m) {
    e = m.last_failure_osd_epoch;
  });
  /* wait for any blocklists to be applied to previous mgr instance */
  dout(4) << "Waiting for new OSDMap (e=" << e
          << ") that may blocklist prior active." << dendl;
  objecter->wait_for_osd_map(e);
  lock.lock();

  // Start communicating with daemons to learn statistics etc
  int r = server.init(monc->get_global_id(), client_messenger->get_myaddrs());
  if (r < 0) {
    derr << "Initialize server fail: " << cpp_strerror(r) << dendl;
    // This is typically due to a bind() failure, so let's let
    // systemd restart us.
    exit(1);
  }
  dout(4) << "Initialized server at " << server.get_myaddrs() << dendl;

  // Preload all daemon metadata (will subsequently keep this
  // up to date by watching maps, so do the initial load before
  // we subscribe to any maps)
  dout(4) << "Loading daemon metadata..." << dendl;
  load_all_metadata();

  // Populate PGs in ClusterState
  cluster_state.with_osdmap_and_pgmap([this](const OSDMap &osd_map,
					     const PGMap& pg_map) {
    cluster_state.notify_osdmap(osd_map);
  });

  // Wait for FSMap
  dout(4) << "waiting for FSMap..." << dendl;
  fs_map_cond.wait(l, [this] { return cluster_state.have_fsmap();});

  // Wait for MgrDigest...
  dout(4) << "waiting for MgrDigest..." << dendl;
  digest_cond.wait(l, [this] { return digest_received; });

  if (!mon_allows_kv_sub) {
    dout(4) << "loading config-key data from pre-pacific mon cluster..." << dendl;
    pre_init_store = load_store();
  }

  dout(4) << "initializing device state..." << dendl;
  // Note: we only have to do this during startup because once we are
  // active the only changes to this state will originate from one of our
  // own modules.
  for (auto p = pre_init_store.lower_bound("device/");
       p != pre_init_store.end() && p->first.find("device/") == 0;
       ++p) {
    string devid = p->first.substr(7);
    dout(10) << "  updating " << devid << dendl;
    map<string,string> meta;
    ostringstream ss;
    int r = get_json_str_map(p->second, ss, &meta, false);
    if (r < 0) {
      derr << __func__ << " failed to parse " << p->second << ": " << ss.str()
	   << dendl;
    } else {
      daemon_state.with_device_create(
	devid, [&meta] (DeviceState& dev) {
		 dev.set_metadata(std::move(meta));
	       });
    }
  }
  
  // assume finisher already initialized in background_init
  dout(4) << "starting python modules..." << dendl;
  py_module_registry->active_start(
    daemon_state, cluster_state,
    pre_init_store, mon_allows_kv_sub,
    *monc, clog, audit_clog, *objecter, *client,
    finisher, server);

  cluster_state.final_init();

  AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
  r = admin_socket->register_command(
    "mgr_status", this,
    "Dump mgr status");
  ceph_assert(r == 0);

#ifdef WITH_LIBCEPHSQLITE
  dout(4) << "Using sqlite3 version: " << sqlite3_libversion() << dendl;
  /* See libcephsqlite.h for rationale of this code. */
  sqlite3_auto_extension((void (*)())sqlite3_cephsqlite_init);
  {
    sqlite3* db = nullptr;
    if (int rc = sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE, nullptr); rc == SQLITE_OK) {
      sqlite3_close(db);
    } else {
      derr << "could not open sqlite3: " << rc << dendl;
      ceph_abort();
    }
  }
  {
    char *ident = nullptr;
    if (int rc = cephsqlite_setcct(g_ceph_context, &ident); rc < 0) {
      derr << "could not set libcephsqlite cct: " << rc << dendl;
      ceph_abort();
    }
    entity_addrvec_t addrv;
    addrv.parse(ident);
    ident = (char*)realloc(ident, 0);
    py_module_registry->register_client("libcephsqlite", addrv, true);
  }
#endif

  dout(4) << "Complete." << dendl;
  initializing = false;
  initialized = true;
}

void Mgr::load_all_metadata()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  JSONCommand mds_cmd;
  mds_cmd.run(monc, "{\"prefix\": \"mds metadata\"}");
  JSONCommand osd_cmd;
  osd_cmd.run(monc, "{\"prefix\": \"osd metadata\"}");
  JSONCommand mon_cmd;
  mon_cmd.run(monc, "{\"prefix\": \"mon metadata\"}");

  lock.unlock();
  mds_cmd.wait();
  osd_cmd.wait();
  mon_cmd.wait();
  lock.lock();

  ceph_assert(mds_cmd.r == 0);
  ceph_assert(mon_cmd.r == 0);
  ceph_assert(osd_cmd.r == 0);

  for (auto &metadata_val : mds_cmd.json_result.get_array()) {
    json_spirit::mObject daemon_meta = metadata_val.get_obj();
    if (daemon_meta.count("hostname") == 0) {
      dout(1) << "Skipping incomplete metadata entry" << dendl;
      continue;
    }

    DaemonStatePtr dm = std::make_shared<DaemonState>(daemon_state.types);
    dm->key = DaemonKey{"mds",
                        daemon_meta.at("name").get_str()};
    dm->hostname = daemon_meta.at("hostname").get_str();

    daemon_meta.erase("name");
    daemon_meta.erase("hostname");

    for (const auto &[key, val] : daemon_meta) {
      dm->metadata.emplace(key, val.get_str());
    }

    daemon_state.insert(dm);
  }

  for (auto &metadata_val : mon_cmd.json_result.get_array()) {
    json_spirit::mObject daemon_meta = metadata_val.get_obj();
    if (daemon_meta.count("hostname") == 0) {
      dout(1) << "Skipping incomplete metadata entry" << dendl;
      continue;
    }

    DaemonStatePtr dm = std::make_shared<DaemonState>(daemon_state.types);
    dm->key = DaemonKey{"mon",
                        daemon_meta.at("name").get_str()};
    dm->hostname = daemon_meta.at("hostname").get_str();

    daemon_meta.erase("name");
    daemon_meta.erase("hostname");

    map<string,string> m;
    for (const auto &[key, val] : daemon_meta) {
      m.emplace(key, val.get_str());
    }
    dm->set_metadata(m);

    daemon_state.insert(dm);
  }

  for (auto &osd_metadata_val : osd_cmd.json_result.get_array()) {
    json_spirit::mObject osd_metadata = osd_metadata_val.get_obj();
    if (osd_metadata.count("hostname") == 0) {
      dout(1) << "Skipping incomplete metadata entry" << dendl;
      continue;
    }
    dout(4) << osd_metadata.at("hostname").get_str() << dendl;

    DaemonStatePtr dm = std::make_shared<DaemonState>(daemon_state.types);
    dm->key = DaemonKey{"osd",
                        stringify(osd_metadata.at("id").get_int())};
    dm->hostname = osd_metadata.at("hostname").get_str();

    osd_metadata.erase("id");
    osd_metadata.erase("hostname");

    map<string,string> m;
    for (const auto &i : osd_metadata) {
      m[i.first] = i.second.get_str();
    }
    dm->set_metadata(m);

    daemon_state.insert(dm);
  }
}

void Mgr::handle_osd_map()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  std::set<std::string> names_exist;

  /**
   * When we see a new OSD map, inspect the entity addrs to
   * see if they have changed (service restart), and if so
   * reload the metadata.
   */
  cluster_state.with_osdmap_and_pgmap([this, &names_exist](const OSDMap &osd_map,
							   const PGMap &pg_map) {
    for (int osd_id = 0; osd_id < osd_map.get_max_osd(); ++osd_id) {
      if (!osd_map.exists(osd_id) || (osd_map.is_out(osd_id) && osd_map.is_down(osd_id))) {
        continue;
      }

      // Remember which OSDs exist so that we can cull any that don't
      names_exist.insert(stringify(osd_id));

      // Consider whether to update the daemon metadata (new/restarted daemon)
      const auto k = DaemonKey{"osd", std::to_string(osd_id)};
      if (daemon_state.is_updating(k)) {
        continue;
      }

      bool update_meta = false;
      if (daemon_state.exists(k)) {
        if (osd_map.get_up_from(osd_id) == osd_map.get_epoch()) {
          dout(4) << "Mgr::handle_osd_map: osd." << osd_id
		  << " joined cluster at " << "e" << osd_map.get_epoch()
		  << dendl;
          update_meta = true;
        }
      } else {
        update_meta = true;
      }
      if (update_meta) {
        auto c = new MetadataUpdate(daemon_state, k);
        std::ostringstream cmd;
        cmd << "{\"prefix\": \"osd metadata\", \"id\": "
            << osd_id << "}";
        monc->start_mon_command(
            {cmd.str()},
            {}, &c->outbl, &c->outs, c);
      }
    }

    cluster_state.notify_osdmap(osd_map);
  });

  // TODO: same culling for MonMap
  daemon_state.cull("osd", names_exist);
}

void Mgr::handle_log(ref_t<MLog> m)
{
  for (const auto &e : m->entries) {
    py_module_registry->notify_all(e);
  }
}

void Mgr::handle_service_map(ref_t<MServiceMap> m)
{
  dout(10) << "e" << m->service_map.epoch << dendl;
  monc->sub_got("servicemap", m->service_map.epoch);
  cluster_state.set_service_map(m->service_map);
  server.got_service_map();
}

void Mgr::handle_mon_map()
{
  dout(20) << __func__ << dendl;
  assert(ceph_mutex_is_locked_by_me(lock));
  std::set<std::string> names_exist;
  cluster_state.with_monmap([&] (auto &monmap) {
    for (unsigned int i = 0; i < monmap.size(); i++) {
      names_exist.insert(monmap.get_name(i));
    }
  });
  for (const auto& name : names_exist) {
    const auto k = DaemonKey{"mon", name};
    if (daemon_state.is_updating(k)) {
      continue;
    }
    auto c = new MetadataUpdate(daemon_state, k);
    constexpr std::string_view cmd = R"({{"prefix": "mon metadata", "id": "{}"}})";
    monc->start_mon_command({fmt::format(cmd, name)}, {},
			    &c->outbl, &c->outs, c);
  }
  daemon_state.cull("mon", names_exist);
}

bool Mgr::ms_dispatch2(const ref_t<Message>& m)
{
  dout(10) << *m << dendl;
  std::lock_guard l(lock);

  switch (m->get_type()) {
    case MSG_MGR_DIGEST:
      handle_mgr_digest(ref_cast<MMgrDigest>(m));
      break;
    case CEPH_MSG_MON_MAP:
      py_module_registry->notify_all("mon_map", "");
      handle_mon_map();
      break;
    case CEPH_MSG_FS_MAP:
      py_module_registry->notify_all("fs_map", "");
      handle_fs_map(ref_cast<MFSMap>(m));
      return false; // I shall let this pass through for Client
    case CEPH_MSG_OSD_MAP:
      handle_osd_map();

      py_module_registry->notify_all("osd_map", "");

      // Continuous subscribe, so that we can generate notifications
      // for our MgrPyModules
      objecter->maybe_request_map();
      break;
    case MSG_SERVICE_MAP:
      handle_service_map(ref_cast<MServiceMap>(m));
      //no users: py_module_registry->notify_all("service_map", "");
      break;
    case MSG_LOG:
      handle_log(ref_cast<MLog>(m));
      break;
    case MSG_KV_DATA:
      {
	auto msg = ref_cast<MKVData>(m);
	monc->sub_got("kv:"s + msg->prefix, msg->version);
	if (!msg->data.empty()) {
	  if (initialized) {
	    py_module_registry->update_kv_data(
	      msg->prefix,
	      msg->incremental,
	      msg->data
	      );
	  } else {
	    // before we have created the ActivePyModules, we need to
	    // track the store regions we're monitoring
	    if (!msg->incremental) {
	      dout(10) << "full update on " << msg->prefix << dendl;
	      auto p = pre_init_store.lower_bound(msg->prefix);
	      while (p != pre_init_store.end() && p->first.find(msg->prefix) == 0) {
		dout(20) << " rm prior " << p->first << dendl;
		p = pre_init_store.erase(p);
	      }
	    } else {
	      dout(10) << "incremental update on " << msg->prefix << dendl;
	    }
	    for (auto& i : msg->data) {
	      if (i.second) {
		dout(20) << " set " << i.first << " = " << i.second->to_str() << dendl;
		pre_init_store[i.first] = i.second->to_str();
	      } else {
		dout(20) << " rm " << i.first << dendl;
		pre_init_store.erase(i.first);
	      }
	    }
	  }
	}
      }
      break;

    default:
      return false;
  }
  return true;
}


void Mgr::handle_fs_map(ref_t<MFSMap> m)
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));

  std::set<std::string> names_exist;
  const FSMap &new_fsmap = m->get_fsmap();

  monc->sub_got("fsmap", m->epoch);

  fs_map_cond.notify_all();

  // TODO: callers (e.g. from python land) are potentially going to see
  // the new fsmap before we've bothered populating all the resulting
  // daemon_state.  Maybe we should block python land while we're making
  // this kind of update?
  
  cluster_state.set_fsmap(new_fsmap);

  auto mds_info = new_fsmap.get_mds_info();
  for (const auto &i : mds_info) {
    const auto &info = i.second;

    if (!new_fsmap.gid_exists(i.first)){
      continue;
    }

    // Remember which MDS exists so that we can cull any that don't
    names_exist.insert(info.name);

    const auto k = DaemonKey{"mds", info.name};
    if (daemon_state.is_updating(k)) {
      continue;
    }

    bool update = false;
    if (daemon_state.exists(k)) {
      auto metadata = daemon_state.get(k);
      std::lock_guard l(metadata->lock);
      if (metadata->metadata.empty() ||
	  metadata->metadata.count("addr") == 0) {
        update = true;
      } else {
        auto metadata_addrs = metadata->metadata.at("addr");
        const auto map_addrs = info.addrs;
        update = metadata_addrs != stringify(map_addrs);
        if (update) {
          dout(4) << "MDS[" << info.name << "] addr change " << metadata_addrs
                  << " != " << stringify(map_addrs) << dendl;
        }
      }
    } else {
      update = true;
    }

    if (update) {
      auto c = new MetadataUpdate(daemon_state, k);

      // Older MDS daemons don't have addr in the metadata, so
      // fake it if the returned metadata doesn't have the field.
      c->set_default("addr", stringify(info.addrs));

      std::ostringstream cmd;
      cmd << "{\"prefix\": \"mds metadata\", \"who\": \""
          << info.name << "\"}";
      monc->start_mon_command(
          {cmd.str()},
          {}, &c->outbl, &c->outs, c);
    }
  }
  daemon_state.cull("mds", names_exist);
}

bool Mgr::got_mgr_map(const MgrMap& m)
{
  std::lock_guard l(lock);
  dout(10) << m << dendl;

  set<string> old_modules;
  cluster_state.with_mgrmap([&](const MgrMap& m) {
      old_modules = m.modules;
    });
  if (m.modules != old_modules) {
    derr << "mgrmap module list changed to (" << m.modules << "), respawn"
	 << dendl;
    return true;
  }

  cluster_state.set_mgr_map(m);
  server.got_mgr_map();

  return false;
}

void Mgr::handle_mgr_digest(ref_t<MMgrDigest> m)
{
  dout(10) << m->mon_status_json.length() << dendl;
  dout(10) << m->health_json.length() << dendl;
  cluster_state.load_digest(m.get());
  //no users: py_module_registry->notify_all("mon_status", "");
  py_module_registry->notify_all("health", "");

  // Hack: use this as a tick/opportunity to prompt python-land that
  // the pgmap might have changed since last time we were here.
  py_module_registry->notify_all("pg_summary", "");
  dout(10) << "done." << dendl;
  m.reset();

  if (!digest_received) {
    digest_received = true;
    digest_cond.notify_all();
  }
}

std::map<std::string, std::string> Mgr::get_services() const
{
  std::lock_guard l(lock);

  return py_module_registry->get_services();
}

int Mgr::call(
  std::string_view admin_command,
  const cmdmap_t& cmdmap,
  const bufferlist&,
  Formatter *f,
  std::ostream& errss,
  bufferlist& out)
{
  try {
    if (admin_command == "mgr_status") {
      f->open_object_section("mgr_status");
      cluster_state.with_mgrmap(
	[f](const MgrMap& mm) {
	  f->dump_unsigned("mgrmap_epoch", mm.get_epoch());
	});
      f->dump_bool("initialized", initialized);
      f->close_section();
      return 0;
    } else {
      return -ENOSYS;
    }
  } catch (const TOPNSPC::common::bad_cmd_get& e) {
    errss << e.what();
    return -EINVAL;
  }
  return 0;
}
