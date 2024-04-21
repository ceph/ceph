// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023,2024 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <boost/algorithm/string/replace.hpp>

#include "common/errno.h"
#include "common/signal.h"
#include "common/ceph_argparse.h"
#include "include/compat.h"

#include "include/stringify.h"
#include "global/global_context.h"
#include "global/signal_handler.h"


#include "messages/MNVMeofGwBeacon.h"
#include "messages/MNVMeofGwMap.h"
#include "NVMeofGwMonitorClient.h"
#include "NVMeofGwClient.h"
#include "NVMeofGwMonitorGroupClient.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout << "nvmeofgw " << __PRETTY_FUNCTION__ << " "

NVMeofGwMonitorClient::NVMeofGwMonitorClient(int argc, const char **argv) :
  Dispatcher(g_ceph_context),
  osdmap_epoch(0),
  gwmap_epoch(0),
  last_map_time(std::chrono::steady_clock::now()),
  monc{g_ceph_context, poolctx},
  client_messenger(Messenger::create(g_ceph_context, "async", entity_name_t::CLIENT(-1), "client", getpid())),
  objecter{g_ceph_context, client_messenger.get(), &monc, poolctx},
  client{client_messenger.get(), &monc, &objecter},
  timer(g_ceph_context, lock),
  orig_argc(argc),
  orig_argv(argv)
{
}

NVMeofGwMonitorClient::~NVMeofGwMonitorClient() = default;

const char** NVMeofGwMonitorClient::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    NULL
  };
  return KEYS;
}

int NVMeofGwMonitorClient::init()
{
  dout(0) << dendl;
  std::string val;
  auto args = argv_to_vec(orig_argc, orig_argv);

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--gateway-name", (char*)NULL)) {
      name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--gateway-pool", (char*)NULL)) {
      pool = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--gateway-group", (char*)NULL)) {
      group = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--gateway-address", (char*)NULL)) {
      gateway_address = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--monitor-group-address", (char*)NULL)) {
      monitor_address = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--server-key", (char*)NULL)) {
      server_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--server-cert", (char*)NULL)) {
      server_cert = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--client-cert", (char*)NULL)) {
      client_cert = val;
    } else {
      ++i;
    }
  }

  dout(0) << "gateway name: " << name <<
    " pool:" << pool <<
    " group:" << group <<
    " address: " << gateway_address << dendl;
  ceph_assert(name != "" && pool != "" && gateway_address != "" && monitor_address != "");

  // todo
  ceph_assert(server_key == "" && server_cert == "" && client_cert == "");

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);

  std::lock_guard l(lock);

  // Initialize Messenger
  client_messenger->add_dispatcher_tail(this);
  client_messenger->add_dispatcher_head(&objecter);
  client_messenger->add_dispatcher_tail(&client);
  client_messenger->start();

  poolctx.start(2);

  // Initialize MonClient
  if (monc.build_initial_monmap() < 0) {
    client_messenger->shutdown();
    client_messenger->wait();
    return -1;
  }

  monc.sub_want("NVMeofGw", 0, 0);
  monc.set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD
      |CEPH_ENTITY_TYPE_MDS|CEPH_ENTITY_TYPE_MGR);
  monc.set_messenger(client_messenger.get());

  // We must register our config callback before calling init(), so
  // that we see the initial configuration message
  monc.register_config_callback([this](const std::string &k, const std::string &v){
      // leaving this for debugging purposes
      dout(10) << "nvmeof config_callback: " << k << " : " << v << dendl;
      
      return false;
    });
  monc.register_config_notify_callback([this]() {
      dout(4) << "nvmeof monc config notify callback" << dendl;
    });
  dout(4) << "nvmeof Registered monc callback" << dendl;

  int r = monc.init();
  if (r < 0) {
    monc.shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }
  dout(0) << "nvmeof Registered monc callback" << dendl;

  r = monc.authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify an ID with a valid keyring?" << dendl;
    monc.shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }
  dout(0) << "monc.authentication done" << dendl;
  monc.set_passthrough_monmap();

  client_t whoami = monc.get_global_id();
  client_messenger->set_myname(entity_name_t::MGR(whoami.v));
  objecter.set_client_incarnation(0);
  objecter.init();
  objecter.enable_blocklist_events();
  objecter.start();
  client.init();
  timer.init();

  tick();

  dout(0) << "Complete." << dendl;
  return 0;
}

static bool get_gw_state(const char* desc, const std::map<NvmeGroupKey, NvmeGwMap>& m, const NvmeGroupKey& group_key, const NvmeGwId& gw_id, NvmeGwState& out)
{
  auto gw_group = m.find(group_key);
  if (gw_group == m.end()) {
    dout(0) << "can not find group (" << group_key.first << "," << group_key.second << ") "  << desc << " map: " << m << dendl;
    return false;
  }
  auto gw_state = gw_group->second.find(gw_id);
  if (gw_state == gw_group->second.end()) {
    dout(0) << "can not find gw id: " << gw_id << " in " << desc << "group: " << gw_group->second  << dendl;
    return false;
  }
  out = gw_state->second;
  return true;
}

void NVMeofGwMonitorClient::send_beacon()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));
  GW_AVAILABILITY_E gw_availability = GW_AVAILABILITY_E::GW_CREATED;
  BeaconSubsystems subs;
  NVMeofGwClient gw_client(
     grpc::CreateChannel(gateway_address, grpc::InsecureChannelCredentials()));
  subsystems_info gw_subsystems;
  bool ok = gw_client.get_subsystems(gw_subsystems);
  if (ok) {
    for (int i = 0; i < gw_subsystems.subsystems_size(); i++) {
      const subsystem& sub = gw_subsystems.subsystems(i);
      BeaconSubsystem bsub;
      bsub.nqn = sub.nqn();
      for (int j = 0; j < sub.namespaces_size(); j++) {
        const auto& ns = sub.namespaces(j);
        BeaconNamespace bns = {ns.anagrpid(), ns.nonce()};
        bsub.namespaces.push_back(bns);
      }
      for (int k = 0; k < sub.listen_addresses_size(); k++) {
        const auto& ls = sub.listen_addresses(k);
        BeaconListener bls = { ls.adrfam(), ls.traddr(), ls.trsvcid() };
        bsub.listeners.push_back(bls);
      }
      subs.push_back(bsub);
    }
  }

  auto group_key = std::make_pair(pool, group);
  NvmeGwState old_gw_state;
  // if already got gateway state in the map
  if (get_gw_state("old map", map, group_key, name, old_gw_state))
    gw_availability = ok ? GW_AVAILABILITY_E::GW_AVAILABLE : GW_AVAILABILITY_E::GW_UNAVAILABLE;
  dout(0) << "sending beacon as gid " << monc.get_global_id() << " availability " << (int)gw_availability <<
    " osdmap_epoch " << osdmap_epoch << " gwmap_epoch " << gwmap_epoch << dendl;
  auto m = ceph::make_message<MNVMeofGwBeacon>(
      name,
      pool,
      group,
      subs,
      gw_availability,
      osdmap_epoch,
      gwmap_epoch);
  monc.send_mon_message(std::move(m));
}

void NVMeofGwMonitorClient::disconnect_panic()
{
  auto disconnect_panic_duration = g_conf().get_val<std::chrono::seconds>("mon_nvmeofgw_beacon_grace").count();
  auto now = std::chrono::steady_clock::now();
  auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - last_map_time).count();
  if (elapsed_seconds > disconnect_panic_duration) {
    dout(4) << "Triggering a panic upon disconnection from the monitor, elapsed " << elapsed_seconds << ", configured disconnect panic duration " << disconnect_panic_duration << dendl;
    throw std::runtime_error("Lost connection to the monitor (mon).");
  }
}

void NVMeofGwMonitorClient::tick()
{
  dout(0) << dendl;

  disconnect_panic();
  send_beacon();

  timer.add_event_after(
      g_conf().get_val<std::chrono::seconds>("nvmeof_mon_client_tick_period").count(),
      new LambdaContext([this](int r){
          tick();
      }
  ));
}

void NVMeofGwMonitorClient::shutdown()
{
  std::lock_guard l(lock);

  dout(4) << "nvmeof Shutting down" << dendl;


  // stop sending beacon first, I use monc to talk with monitors
  timer.shutdown();
  // client uses monc and objecter
  client.shutdown();
  // Stop asio threads, so leftover events won't call into shut down
  // monclient/objecter.
  poolctx.finish();
  // stop monc
  monc.shutdown();

  // objecter is used by monc
  objecter.shutdown();
  // client_messenger is used by all of them, so stop it in the end
  client_messenger->shutdown();
}

void NVMeofGwMonitorClient::handle_nvmeof_gw_map(ceph::ref_t<MNVMeofGwMap> nmap)
{
  last_map_time = std::chrono::steady_clock::now(); // record time of last monitor message

  auto &new_map = nmap->get_map();
  gwmap_epoch = nmap->get_gwmap_epoch();
  auto group_key = std::make_pair(pool, group);
  dout(0) << "handle nvmeof gw map: " << new_map << dendl;

  NvmeGwState old_gw_state;
  auto got_old_gw_state = get_gw_state("old map", map, group_key, name, old_gw_state); 
  NvmeGwState new_gw_state;
  auto got_new_gw_state = get_gw_state("new map", new_map, group_key, name, new_gw_state); 

  if (!got_old_gw_state) {
    if (!got_new_gw_state) {
      dout(0) << "Can not find new gw state" << dendl;
      return;
    }
    bool set_group_id = false;
    while (!set_group_id) {
      NVMeofGwMonitorGroupClient monitor_group_client(
          grpc::CreateChannel(monitor_address, grpc::InsecureChannelCredentials()));
      dout(0) << "GRPC set_group_id: " <<  new_gw_state.group_id << dendl;
      set_group_id = monitor_group_client.set_group_id( new_gw_state.group_id);
      if (!set_group_id) {
	      dout(0) << "GRPC set_group_id failed" << dendl;
	      usleep(1000); // TODO: conf options
      }
    }
  }

  // Make sure we do not get out of order state changes from the monitor
  if (got_old_gw_state && got_new_gw_state) {
    dout(0) << "got_old_gw_state: " << old_gw_state << "got_new_gw_state: " << new_gw_state << dendl;
    ceph_assert(new_gw_state.gw_map_epoch >= old_gw_state.gw_map_epoch);
  }

  // Gather all state changes
  ana_info ai;
  epoch_t max_blocklist_epoch = 0;
  for (const auto& nqn_state_pair: new_gw_state.subsystems) {
    auto& sub = nqn_state_pair.second;
    const auto& nqn = nqn_state_pair.first;
    nqn_ana_states nas;
    nas.set_nqn(nqn);
    for (NvmeAnaGrpId  ana_grp_index = 0; ana_grp_index < sub.ana_state.size(); ana_grp_index++) {
      const auto& old_nqn_state_pair = old_gw_state.subsystems.find(nqn);
      auto found_old_nqn_state = (old_nqn_state_pair != old_gw_state.subsystems.end());
      auto new_group_state = sub.ana_state[ana_grp_index];

      // if no state change detected for this nqn, group id
      if (got_old_gw_state && found_old_nqn_state &&
           new_group_state == old_nqn_state_pair->second.ana_state[ana_grp_index]) {
        continue;
      }
      ana_group_state gs;
      gs.set_grp_id(ana_grp_index + 1); // offset by 1, index 0 is ANAGRP1
      const auto& new_agroup_state = new_group_state.first;
      const epoch_t& blocklist_epoch = new_group_state.second;

      if (new_agroup_state == GW_EXPORTED_STATES_PER_AGROUP_E::GW_EXPORTED_OPTIMIZED_STATE &&
          blocklist_epoch != 0) {
        if (blocklist_epoch > max_blocklist_epoch) max_blocklist_epoch = blocklist_epoch;
      }
      gs.set_state(new_agroup_state == GW_EXPORTED_STATES_PER_AGROUP_E::GW_EXPORTED_OPTIMIZED_STATE ? OPTIMIZED : INACCESSIBLE); // Set the ANA state
      nas.mutable_states()->Add(std::move(gs));
      dout(0) << " grpid " << (ana_grp_index + 1) << " state: " << new_gw_state << dendl;
    }
    if (nas.states_size()) ai.mutable_states()->Add(std::move(nas));
  }

  // if there is state change, notify the gateway
  if (ai.states_size()) {
    bool set_ana_state = false;
    while (!set_ana_state) {
      NVMeofGwClient gw_client(
          grpc::CreateChannel(gateway_address, grpc::InsecureChannelCredentials()));
      set_ana_state = gw_client.set_ana_state(ai);
      if (!set_ana_state) {
	dout(0) << "GRPC set_ana_state failed" << dendl;
	usleep(1000); // TODO conf option
      }
    }
    // Update latest accepted osdmap epoch, for beacons
    if (max_blocklist_epoch > osdmap_epoch) {
      osdmap_epoch = max_blocklist_epoch;
      dout(0) << "Ready for blocklist osd map epoch: " << osdmap_epoch << dendl;
    }
  }
  map = new_map;
}

bool NVMeofGwMonitorClient::ms_dispatch2(const ref_t<Message>& m)
{
  std::lock_guard l(lock);
  dout(0) << "got map type " << m->get_type() << dendl;

  if (m->get_type() == MSG_MNVMEOF_GW_MAP) {
    handle_nvmeof_gw_map(ref_cast<MNVMeofGwMap>(m));
  }
  bool handled = false;
  return handled;
}

int NVMeofGwMonitorClient::main(std::vector<const char *> args)
{
  client_messenger->wait();

  // Disable signal handlers
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  shutdown_async_signal_handler();

  return 0;
}
