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

#include "osdc/Objecter.h"
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

#include "Mgr.h"

#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "


Mgr::Mgr(MonClient *monc_, Messenger *clientm_, Objecter *objecter_) :
  monc(monc_),
  objecter(objecter_),
  client_messenger(clientm_),
  lock("Mgr::lock"),
  timer(g_ceph_context, lock),
  finisher(g_ceph_context, "Mgr", "mgr-fin"),
  waiting_for_fs_map(NULL),
  py_modules(daemon_state, cluster_state, *monc, finisher),
  cluster_state(monc, nullptr),
  server(monc, daemon_state, py_modules),
  initialized(false),
  initializing(false)
{
  cluster_state.set_objecter(objecter);
}


Mgr::~Mgr()
{
  assert(waiting_for_fs_map == nullptr);
}


/**
 * Context for completion of metadata mon commands: take
 * the result and stash it in DaemonStateIndex
 */
class MetadataUpdate : public Context
{
  DaemonStateIndex &daemon_state;
  DaemonKey key;

public:
  bufferlist outbl;
  std::string outs;

  MetadataUpdate(DaemonStateIndex &daemon_state_, const DaemonKey &key_)
    : daemon_state(daemon_state_), key(key_) {}

  void finish(int r)
  {
    daemon_state.clear_updating(key);
    if (r == 0) {
      if (key.first == CEPH_ENTITY_TYPE_MDS) {
        json_spirit::mValue json_result;
        bool read_ok = json_spirit::read(
            outbl.to_str(), json_result);
        if (!read_ok) {
          dout(1) << "mon returned invalid JSON for "
                  << ceph_entity_type_name(key.first)
                  << "." << key.second << dendl;
          return;
        }

        json_spirit::mObject daemon_meta = json_result.get_obj();

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
      } else if (key.first == CEPH_ENTITY_TYPE_OSD) {
      } else {
        assert(0);
      }
    } else {
      dout(1) << "mon failed to return metadata for "
              << ceph_entity_type_name(key.first)
              << "." << key.second << ": " << cpp_strerror(r) << dendl;
    }
  }
};


void Mgr::background_init()
{
  Mutex::Locker l(lock);
  assert(!initializing);
  assert(!initialized);
  initializing = true;

  finisher.start();

  finisher.queue(new C_StdFunction([this](){
    init();
  }));
}

void Mgr::init()
{
  Mutex::Locker l(lock);
  assert(initializing);
  assert(!initialized);

  // Start communicating with daemons to learn statistics etc
  server.init(monc->get_global_id(), client_messenger->get_myaddr());
  dout(4) << "Initialized server at " << server.get_myaddr() << dendl;

  // Preload all daemon metadata (will subsequently keep this
  // up to date by watching maps, so do the initial load before
  // we subscribe to any maps)
  dout(4) << "Loading daemon metadata..." << dendl;
  load_all_metadata();

  // Preload config keys (`get` for plugins is to be a fast local
  // operation, we we don't have to synchronize these later because
  // all sets will come via mgr)
  load_config();

  // Start Objecter and wait for OSD map
  lock.Unlock();  // Drop lock because OSDMap dispatch calls into my ms_dispatch
  objecter->wait_for_osd_map();
  lock.Lock();

  monc->sub_want("mgrdigest", 0, 0);

  // Prepare to receive FSMap and request it
  dout(4) << "requesting FSMap..." << dendl;
  C_SaferCond cond;
  waiting_for_fs_map = &cond;
  monc->sub_want("fsmap", 0, 0);
  monc->renew_subs();

  // Wait for FSMap
  dout(4) << "waiting for FSMap..." << dendl;
  lock.Unlock();
  cond.wait();
  lock.Lock();
  waiting_for_fs_map = nullptr;
  dout(4) << "Got FSMap." << dendl;

  // Wait for MgrDigest...?
  // TODO

  // assume finisher already initialized in background_init

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

  mds_cmd.wait();
  osd_cmd.wait();
  mon_cmd.wait();

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
    dm->key = DaemonKey(CEPH_ENTITY_TYPE_MDS,
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
    dm->key = DaemonKey(CEPH_ENTITY_TYPE_MON,
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
    dm->key = DaemonKey(CEPH_ENTITY_TYPE_OSD,
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

  cmd.wait();
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
      get_cmd.wait();
      assert(get_cmd.r == 0);

      loaded[key] = get_cmd.outbl.to_str();
    }
  }

  py_modules.insert_config(loaded);
}

void Mgr::shutdown()
{
  // FIXME: pre-empt init() if it is currently running, so that it will
  // give up the lock for us.
  Mutex::Locker l(lock);

  // First stop the server so that we're not taking any more incoming requests
  server.shutdown();

  // Then stop the finisher to ensure its enqueued contexts aren't going
  // to touch references to the things we're about to tear down
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
      const auto k = DaemonKey(CEPH_ENTITY_TYPE_OSD, stringify(osd_id));
      if (daemon_state.is_updating(k)) {
        continue;
      }

      if (daemon_state.exists(k)) {
        auto metadata = daemon_state.get(k);
        auto metadata_addr = metadata->metadata.at("front_addr");
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
        update_meta = true;
      }

      if (update_meta) {
        daemon_state.notify_updating(k);
        auto c = new MetadataUpdate(daemon_state, k);
        std::ostringstream cmd;
        cmd << "{\"prefix\": \"osd metadata\", \"id\": "
            << osd_id << "}";
        int r = monc->start_mon_command(
            {cmd.str()},
            {}, &c->outbl, &c->outs, c);
        assert(r == 0);  // start_mon_command defined to not fail
      }
    }
  });

  // TODO: same culling for MonMap and FSMap
  daemon_state.cull(CEPH_ENTITY_TYPE_OSD, names_exist);
}

bool Mgr::ms_dispatch(Message *m)
{
  derr << *m << dendl;
  Mutex::Locker l(lock);

  switch (m->get_type()) {
    case MSG_MGR_DIGEST:
      handle_mgr_digest(static_cast<MMgrDigest*>(m));
      break;
    case CEPH_MSG_MON_MAP:
      // FIXME: we probably never get called here because MonClient
      // has consumed the message.  For consuming OSDMap we need
      // to be the tail dispatcher, but to see MonMap we would
      // need to be at the head.
      // Result is that ClusterState has access to monmap (it reads
      // from monclient anyway), but we don't see notifications.  Hook
      // into MonClient to get notifications instead of messing
      // with message delivery to achieve it?
      assert(0);

      py_modules.notify_all("mon_map", "");
      break;
    case CEPH_MSG_FS_MAP:
      py_modules.notify_all("fs_map", "");
      handle_fs_map((MFSMap*)m);
      m->put();
      break;
    case CEPH_MSG_OSD_MAP:

      handle_osd_map();

      py_modules.notify_all("osd_map", "");

      // Continuous subscribe, so that we can generate notifications
      // for our MgrPyModules
      objecter->maybe_request_map();
      m->put();
      break;

    default:
      return false;
  }
  return true;
}


void Mgr::handle_fs_map(MFSMap* m)
{
  assert(lock.is_locked_by_me());

  const FSMap &new_fsmap = m->get_fsmap();

  if (waiting_for_fs_map) {
    waiting_for_fs_map->complete(0);
    waiting_for_fs_map = NULL;
  }

  // TODO: callers (e.g. from python land) are potentially going to see
  // the new fsmap before we've bothered populating all the resulting
  // daemon_state.  Maybe we should block python land while we're making
  // this kind of update?
  
  cluster_state.set_fsmap(new_fsmap);

  auto mds_info = new_fsmap.get_mds_info();
  for (const auto &i : mds_info) {
    const auto &info = i.second;

    const auto k = DaemonKey(CEPH_ENTITY_TYPE_MDS, info.name);
    if (daemon_state.is_updating(k)) {
      continue;
    }

    bool update = false;
    if (daemon_state.exists(k)) {
      auto metadata = daemon_state.get(k);
      // FIXME: nothing stopping old daemons being here, they won't have
      // addr: need to handle case of pre-ceph-mgr daemons that don't have
      // the fields we expect
      if (metadata->metadata.empty()) {
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
      std::ostringstream cmd;
      cmd << "{\"prefix\": \"mds metadata\", \"who\": \""
          << info.name << "\"}";
      int r = monc->start_mon_command(
          {cmd.str()},
          {}, &c->outbl, &c->outs, c);
      assert(r == 0);  // start_mon_command defined to not fail
    }
  }
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
  
  m->put();
}

