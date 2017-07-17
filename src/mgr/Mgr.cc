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

#include "mgr/MgrContext.h"

#include "MgrPyModule.h"
#include "DaemonServer.h"
#include "messages/MMgrBeacon.h"
#include "messages/MMgrDigest.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MLog.h"
#include "messages/MServiceMap.h"

#include "Mgr.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "


Mgr::Mgr(MonClient *monc_, const MgrMap& mgrmap,
	 Messenger *clientm_, Objecter *objecter_,
	 Client* client_, LogChannelRef clog_, LogChannelRef audit_clog_) :
  monc(monc_),
  objecter(objecter_),
  client(client_),
  client_messenger(clientm_),
  lock("Mgr::lock"),
  timer(g_ceph_context, lock),
  finisher(g_ceph_context, "Mgr", "mgr-fin"),
  digest_received(false),
  py_modules(daemon_state, cluster_state, *monc, clog_, *objecter, *client,
             finisher),
  cluster_state(monc, nullptr, mgrmap),
  server(monc, finisher, daemon_state, cluster_state, py_modules,
         clog_, audit_clog_),
  initialized(false),
  initializing(false)
{
  cluster_state.set_objecter(objecter);
}


Mgr::~Mgr()
{
}


/**
 * Context for completion of metadata mon commands: take
 * the result and stash it in DaemonStateIndex
 */
class MetadataUpdate : public Context
{
  DaemonStateIndex &daemon_state;
  DaemonKey key;

  std::map<std::string, std::string> defaults;

public:
  bufferlist outbl;
  std::string outs;

  MetadataUpdate(DaemonStateIndex &daemon_state_, const DaemonKey &key_)
    : daemon_state(daemon_state_), key(key_) {}

  void set_default(const std::string &k, const std::string &v)
  {
    defaults[k] = v;
  }

  void finish(int r) override
  {
    daemon_state.clear_updating(key);
    if (r == 0) {
      if (key.first == "mds") {
        json_spirit::mValue json_result;
        bool read_ok = json_spirit::read(
            outbl.to_str(), json_result);
        if (!read_ok) {
          dout(1) << "mon returned invalid JSON for "
                  << key.first << "." << key.second << dendl;
          return;
        }

        json_spirit::mObject daemon_meta = json_result.get_obj();

        // Apply any defaults
        for (const auto &i : defaults) {
          if (daemon_meta.find(i.first) == daemon_meta.end()) {
            daemon_meta[i.first] = i.second;
          }
        }

        DaemonStatePtr state;
        if (daemon_state.exists(key)) {
          state = daemon_state.get(key);
          // TODO lock state
          daemon_meta.erase("name");
          daemon_meta.erase("hostname");
          state->metadata.clear();
          for (const auto &i : daemon_meta) {
            state->metadata[i.first] = i.second.get_str();
          }
        } else {
          state = std::make_shared<DaemonState>(daemon_state.types);
          state->key = key;
          state->hostname = daemon_meta.at("hostname").get_str();

          for (const auto &i : daemon_meta) {
            state->metadata[i.first] = i.second.get_str();
          }

          daemon_state.insert(state);
        }
      } else if (key.first == "osd") {
      } else {
        ceph_abort();
      }
    } else {
      dout(1) << "mon failed to return metadata for "
              << key.first << "." << key.second << ": "
	      << cpp_strerror(r) << dendl;
    }
  }
};


void Mgr::background_init(Context *completion)
{
  Mutex::Locker l(lock);
  assert(!initializing);
  assert(!initialized);
  initializing = true;

  finisher.start();

  finisher.queue(new FunctionContext([this, completion](int r){
    init();
    completion->complete(0);
  }));
}

void Mgr::init()
{
  Mutex::Locker l(lock);
  assert(initializing);
  assert(!initialized);

  // Start communicating with daemons to learn statistics etc
  int r = server.init(monc->get_global_id(), client_messenger->get_myaddr());
  if (r < 0) {
    derr << "Initialize server fail"<< dendl;
    return;
  }
  dout(4) << "Initialized server at " << server.get_myaddr() << dendl;

  // Preload all daemon metadata (will subsequently keep this
  // up to date by watching maps, so do the initial load before
  // we subscribe to any maps)
  dout(4) << "Loading daemon metadata..." << dendl;
  load_all_metadata();

  // subscribe to all the maps
  monc->sub_want("log-info", 0, 0);
  monc->sub_want("mgrdigest", 0, 0);
  monc->sub_want("fsmap", 0, 0);
  monc->sub_want("servicemap", 0, 0);

  dout(4) << "waiting for OSDMap..." << dendl;
  // Subscribe to OSDMap update to pass on to ClusterState
  objecter->maybe_request_map();

  // reset the mon session.  we get these maps through subscriptions which
  // are stateful with the connection, so even if *we* don't have them a
  // previous incarnation sharing the same MonClient may have.
  monc->reopen_session();

  // Start Objecter and wait for OSD map
  lock.Unlock();  // Drop lock because OSDMap dispatch calls into my ms_dispatch
  objecter->wait_for_osd_map();
  lock.Lock();

  // Populate PGs in ClusterState
  objecter->with_osdmap([this](const OSDMap &osd_map) {
    cluster_state.notify_osdmap(osd_map);
  });

  // Wait for FSMap
  dout(4) << "waiting for FSMap..." << dendl;
  while (!cluster_state.have_fsmap()) {
    fs_map_cond.Wait(lock);
  }

  dout(4) << "waiting for config-keys..." << dendl;

  // Preload config keys (`get` for plugins is to be a fast local
  // operation, we we don't have to synchronize these later because
  // all sets will come via mgr)
  load_config();

  // Wait for MgrDigest...
  dout(4) << "waiting for MgrDigest..." << dendl;
  while (!digest_received) {
    digest_cond.Wait(lock);
  }

  // assume finisher already initialized in background_init
  dout(4) << "starting PyModules..." << dendl;
  py_modules.init();
  py_modules.start();

  dout(4) << "Complete." << dendl;
  initializing = false;
  initialized = true;
}

void Mgr::load_all_metadata()
{
  assert(lock.is_locked_by_me());

  JSONCommand mds_cmd;
  mds_cmd.run(monc, "{\"prefix\": \"mds metadata\"}");
  JSONCommand osd_cmd;
  osd_cmd.run(monc, "{\"prefix\": \"osd metadata\"}");
  JSONCommand mon_cmd;
  mon_cmd.run(monc, "{\"prefix\": \"mon metadata\"}");

  lock.Unlock();
  mds_cmd.wait();
  osd_cmd.wait();
  mon_cmd.wait();
  lock.Lock();

  assert(mds_cmd.r == 0);
  assert(mon_cmd.r == 0);
  assert(osd_cmd.r == 0);

  for (auto &metadata_val : mds_cmd.json_result.get_array()) {
    json_spirit::mObject daemon_meta = metadata_val.get_obj();
    if (daemon_meta.count("hostname") == 0) {
      dout(1) << "Skipping incomplete metadata entry" << dendl;
      continue;
    }

    DaemonStatePtr dm = std::make_shared<DaemonState>(daemon_state.types);
    dm->key = DaemonKey("mds",
                        daemon_meta.at("name").get_str());
    dm->hostname = daemon_meta.at("hostname").get_str();

    daemon_meta.erase("name");
    daemon_meta.erase("hostname");

    for (const auto &i : daemon_meta) {
      dm->metadata[i.first] = i.second.get_str();
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
    dm->key = DaemonKey("mon",
                        daemon_meta.at("name").get_str());
    dm->hostname = daemon_meta.at("hostname").get_str();

    daemon_meta.erase("name");
    daemon_meta.erase("hostname");

    for (const auto &i : daemon_meta) {
      dm->metadata[i.first] = i.second.get_str();
    }

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
    dm->key = DaemonKey("osd",
                        stringify(osd_metadata.at("id").get_int()));
    dm->hostname = osd_metadata.at("hostname").get_str();

    osd_metadata.erase("id");
    osd_metadata.erase("hostname");

    for (const auto &i : osd_metadata) {
      dm->metadata[i.first] = i.second.get_str();
    }

    daemon_state.insert(dm);
  }
}

void Mgr::load_config()
{
  assert(lock.is_locked_by_me());

  dout(10) << "listing keys" << dendl;
  JSONCommand cmd;
  cmd.run(monc, "{\"prefix\": \"config-key list\"}");
  lock.Unlock();
  cmd.wait();
  lock.Lock();
  assert(cmd.r == 0);

  std::map<std::string, std::string> loaded;
  
  for (auto &key_str : cmd.json_result.get_array()) {
    std::string const key = key_str.get_str();
    dout(20) << "saw key '" << key << "'" << dendl;

    const std::string config_prefix = PyModules::config_prefix;

    if (key.substr(0, config_prefix.size()) == config_prefix) {
      dout(20) << "fetching '" << key << "'" << dendl;
      Command get_cmd;
      std::ostringstream cmd_json;
      cmd_json << "{\"prefix\": \"config-key get\", \"key\": \"" << key << "\"}";
      get_cmd.run(monc, cmd_json.str());
      lock.Unlock();
      get_cmd.wait();
      lock.Lock();
      assert(get_cmd.r == 0);
      loaded[key] = get_cmd.outbl.to_str();
    }
  }

  py_modules.insert_config(loaded);
}

void Mgr::shutdown()
{
  finisher.queue(new FunctionContext([&](int) {
    {
      Mutex::Locker l(lock);
      monc->sub_unwant("log-info");
      monc->sub_unwant("mgrdigest");
      monc->sub_unwant("fsmap");
      // First stop the server so that we're not taking any more incoming
      // requests
      server.shutdown();
    }
    // after the messenger is stopped, signal modules to shutdown via finisher
    py_modules.shutdown();
  }));

  // Then stop the finisher to ensure its enqueued contexts aren't going
  // to touch references to the things we're about to tear down
  finisher.wait_for_empty();
  finisher.stop();
}

void Mgr::handle_osd_map()
{
  assert(lock.is_locked_by_me());

  std::set<std::string> names_exist;

  /**
   * When we see a new OSD map, inspect the entity addrs to
   * see if they have changed (service restart), and if so
   * reload the metadata.
   */
  objecter->with_osdmap([this, &names_exist](const OSDMap &osd_map) {
    for (unsigned int osd_id = 0; osd_id < osd_map.get_num_osds(); ++osd_id) {
      if (!osd_map.exists(osd_id)) {
        continue;
      }

      // Remember which OSDs exist so that we can cull any that don't
      names_exist.insert(stringify(osd_id));

      // Consider whether to update the daemon metadata (new/restarted daemon)
      bool update_meta = false;
      const auto k = DaemonKey("osd", stringify(osd_id));
      if (daemon_state.is_updating(k)) {
        continue;
      }

      if (daemon_state.exists(k)) {
        auto metadata = daemon_state.get(k);
        auto addr_iter = metadata->metadata.find("front_addr");
        if (addr_iter != metadata->metadata.end()) {
          const std::string &metadata_addr = addr_iter->second;
          const auto &map_addr = osd_map.get_addr(osd_id);

          if (metadata_addr != stringify(map_addr)) {
            dout(4) << "OSD[" << osd_id << "] addr change " << metadata_addr
                    << " != " << stringify(map_addr) << dendl;
            update_meta = true;
          } else {
            dout(20) << "OSD[" << osd_id << "] addr unchanged: "
                     << metadata_addr << dendl;
          }
        } else {
          // Awkward case where daemon went into DaemonState because it
          // sent us a report but its metadata didn't get loaded yet
          update_meta = true;
        }
      } else {
        update_meta = true;
      }

      if (update_meta) {
        daemon_state.notify_updating(k);
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

void Mgr::handle_log(MLog *m)
{
  for (const auto &e : m->entries) {
    py_modules.notify_all(e);
  }

  m->put();
}

void Mgr::handle_service_map(MServiceMap *m)
{
  dout(10) << "e" << m->service_map.epoch << dendl;
  cluster_state.set_service_map(m->service_map);
  server.got_service_map();
}

bool Mgr::ms_dispatch(Message *m)
{
  dout(4) << *m << dendl;
  Mutex::Locker l(lock);

  switch (m->get_type()) {
    case MSG_MGR_DIGEST:
      handle_mgr_digest(static_cast<MMgrDigest*>(m));
      break;
    case CEPH_MSG_MON_MAP:
      py_modules.notify_all("mon_map", "");
      m->put();
      break;
    case CEPH_MSG_FS_MAP:
      py_modules.notify_all("fs_map", "");
      handle_fs_map((MFSMap*)m);
      return false; // I shall let this pass through for Client
      break;
    case CEPH_MSG_OSD_MAP:
      handle_osd_map();

      py_modules.notify_all("osd_map", "");

      // Continuous subscribe, so that we can generate notifications
      // for our MgrPyModules
      objecter->maybe_request_map();
      m->put();
      break;
    case MSG_SERVICE_MAP:
      handle_service_map((MServiceMap*)m);
      py_modules.notify_all("service_map", "");
      m->put();
      break;
    case MSG_LOG:
      handle_log(static_cast<MLog *>(m));
      break;

    default:
      return false;
  }
  return true;
}


void Mgr::handle_fs_map(MFSMap* m)
{
  assert(lock.is_locked_by_me());

  std::set<std::string> names_exist;
  
  const FSMap &new_fsmap = m->get_fsmap();

  fs_map_cond.Signal();

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

    const auto k = DaemonKey("mds", info.name);
    if (daemon_state.is_updating(k)) {
      continue;
    }

    bool update = false;
    if (daemon_state.exists(k)) {
      auto metadata = daemon_state.get(k);
      if (metadata->metadata.empty() ||
	  metadata->metadata.count("addr") == 0) {
        update = true;
      } else {
        auto metadata_addr = metadata->metadata.at("addr");
        const auto map_addr = info.addr;
        update = metadata_addr != stringify(map_addr);
        if (update) {
          dout(4) << "MDS[" << info.name << "] addr change " << metadata_addr
                  << " != " << stringify(map_addr) << dendl;
        }
      }
    } else {
      update = true;
    }

    if (update) {
      daemon_state.notify_updating(k);
      auto c = new MetadataUpdate(daemon_state, k);

      // Older MDS daemons don't have addr in the metadata, so
      // fake it if the returned metadata doesn't have the field.
      c->set_default("addr", stringify(info.addr));

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
  Mutex::Locker l(lock);
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

  return false;
}

void Mgr::handle_mgr_digest(MMgrDigest* m)
{
  dout(10) << m->mon_status_json.length() << dendl;
  dout(10) << m->health_json.length() << dendl;
  cluster_state.load_digest(m);
  py_modules.notify_all("mon_status", "");
  py_modules.notify_all("health", "");

  // Hack: use this as a tick/opportunity to prompt python-land that
  // the pgmap might have changed since last time we were here.
  py_modules.notify_all("pg_summary", "");
  dout(10) << "done." << dendl;

  m->put();

  if (!digest_received) {
    digest_received = true;
    digest_cond.Signal();
  }
}

void Mgr::tick()
{
  dout(10) << dendl;
  server.send_report();
}
