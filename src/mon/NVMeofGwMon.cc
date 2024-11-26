// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <boost/tokenizer.hpp>
#include "include/stringify.h"
#include "NVMeofGwMon.h"
#include "messages/MNVMeofGwBeacon.h"
#include "messages/MNVMeofGwMap.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout << "nvmeofgw " << __PRETTY_FUNCTION__ << " "

using std::string;

void NVMeofGwMon::init()
{
  dout(10) <<  "called " << dendl;
}

void NVMeofGwMon::on_restart()
{
  dout(10) <<  "called " << dendl;
  last_beacon.clear();
  last_tick = ceph::coarse_mono_clock::now();
  synchronize_last_beacon();
}


void NVMeofGwMon::synchronize_last_beacon()
{
  dout(10) << "called, is leader : " << mon.is_leader()
	   << " active " << is_active()  << dendl;
  // Initialize last_beacon to identify transitions of available
  // GWs to unavailable state
  for (const auto& created_map_pair: map.created_gws) {
    const auto& group_key = created_map_pair.first;
    const NvmeGwMonStates& gw_created_map = created_map_pair.second;
    for (const auto& gw_created_pair: gw_created_map) {
      const auto& gw_id = gw_created_pair.first;
      if (gw_created_pair.second.availability ==
	  gw_availability_t::GW_AVAILABLE) {
	dout(10) << "synchronize last_beacon for  GW :" << gw_id << dendl;
	LastBeacon lb = {gw_id, group_key};
	last_beacon[lb] = last_tick;
      }
    }
  }
}

void NVMeofGwMon::on_shutdown()
{
  dout(10) <<  "called " << dendl;
}

void NVMeofGwMon::tick()
{
  if (!is_active() || !mon.is_leader()) {
    dout(10) << "NVMeofGwMon leader : " << mon.is_leader()
	     << "active : " << is_active()  << dendl;
    return;
  }
  bool _propose_pending = false;
  
  const auto now = ceph::coarse_mono_clock::now();
  const auto nvmegw_beacon_grace =
    g_conf().get_val<std::chrono::seconds>("mon_nvmeofgw_beacon_grace");
  dout(15) <<  "NVMeofGwMon leader got a tick, pending epoch "
	   << pending_map.epoch << dendl;

  const auto client_tick_period =
    g_conf().get_val<std::chrono::seconds>("nvmeof_mon_client_tick_period");
  // handle exception of tick overdued in order to avoid false detection of
  // overdued beacons, like it done in  MgrMonitor::tick
  if (last_tick != ceph::coarse_mono_clock::zero() &&
      (now - last_tick > (nvmegw_beacon_grace - client_tick_period))) {
    // This case handles either local slowness (calls being delayed
    // for whatever reason) or cluster election slowness (a long gap
    // between calls while an election happened)
    dout(10) << ": resetting beacon timeouts due to mon delay "
      "(slow election?) of " << now - last_tick << " seconds" << dendl;
    for (auto &i : last_beacon) {
      i.second = now;
    }
  }

  last_tick = now;
  bool propose = false;

  // Periodic: check active FSM timers
  pending_map.update_active_timers(propose);
  _propose_pending |= propose;

  const auto cutoff = now - nvmegw_beacon_grace;

  // Pass over all the stored beacons
  NvmeGroupKey old_group_key;
  for (auto &itr : last_beacon) {
    auto& lb = itr.first;
    auto last_beacon_time = itr.second;
    if (last_beacon_time < cutoff) {
      dout(10) << "beacon timeout for GW " << lb.gw_id << dendl;
      pending_map.process_gw_map_gw_down(lb.gw_id, lb.group_key, propose);
      _propose_pending |= propose;
      last_beacon.erase(lb);
    } else {
      dout(20) << "beacon live for GW key: " << lb.gw_id << dendl;
    }
  }
  BeaconSubsystems empty_subsystems;
  for (auto &[group_key, gws_states]: pending_map.created_gws) {
    BeaconSubsystems *subsystems = &empty_subsystems;
    for (auto& gw_state : gws_states) { // loop for GWs inside nqn group
      subsystems = &gw_state.second.subsystems;
      if (subsystems->size()) { // Set subsystems to the valid value
        break;
      }
    }
    pending_map.track_deleting_gws(group_key, *subsystems, propose);
    _propose_pending |= propose;
  }
  // Periodic: take care of not handled ANA groups
  pending_map.handle_abandoned_ana_groups(propose);
  _propose_pending |= propose;

  if (_propose_pending) {
    dout(10) << "propose pending " <<dendl;
    propose_pending();
  }
}

const char **NVMeofGwMon::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    NULL
  };
  return KEYS;
}

version_t NVMeofGwMon::get_trim_to() const
{
  // we don't actually need *any* old states, but keep a few.
  int64_t max = g_conf().get_val<int64_t>("mon_max_nvmeof_epochs");
  if (map.epoch > max) {
    return map.epoch - max;
  }
  return 0;
}

void NVMeofGwMon::create_pending()
{
  pending_map = map;// deep copy of the object
  pending_map.epoch++;
  dout(10) << " pending " << pending_map  << dendl;
}

void NVMeofGwMon::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << dendl;
  ceph_assert(get_last_committed() + 1 == pending_map.epoch);
  bufferlist bl;
  uint64_t features = mon.get_quorum_con_features();
  pending_map.encode(bl, features);
  dout(10) << " has NVMEOFHA: "
	   << HAVE_FEATURE(mon.get_quorum_con_features(), NVMEOFHA) << dendl;
  put_version(t, pending_map.epoch, bl);
  put_last_committed(t, pending_map.epoch);
}

void NVMeofGwMon::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();

  if (version != map.epoch) {
    dout(10) << " NVMeGW loading version " << version
	     << " " << map.epoch << dendl;
    bufferlist bl;
    int err = get_version(version, bl);
    ceph_assert(err == 0);

    auto p = bl.cbegin();
    map.decode(p);
    if (!mon.is_leader()) {
      dout(10) << "leader map: " << map <<  dendl;
    }
    check_subs(true);
  }
}

void NVMeofGwMon::check_sub(Subscription *sub)
{
  dout(10) << "sub->next , map-epoch " << sub->next
	   << " " << map.epoch << dendl;
  if (sub->next <= map.epoch)
  {
    dout(10) << "Sending map to subscriber " << sub->session->con
	     << " " << sub->session->con->get_peer_addr() << dendl;
    sub->session->con->send_message2(make_message<MNVMeofGwMap>(map));

    if (sub->onetime) {
      mon.session_map.remove_sub(sub);
    } else {
      sub->next = map.epoch + 1;
    }
  }
}

void NVMeofGwMon::check_subs(bool t)
{
  const std::string type = "NVMeofGw";
  dout(10) <<  "count " << mon.session_map.subs.count(type) << dendl;

  if (mon.session_map.subs.count(type) == 0) {
    return;
  }
  for (auto sub : *(mon.session_map.subs[type])) {
    check_sub(sub);
  }
}

bool NVMeofGwMon::preprocess_query(MonOpRequestRef op)
{
  dout(20) << dendl;

  auto m = op->get_req<PaxosServiceMessage>();
  switch (m->get_type()) {
  case MSG_MNVMEOF_GW_BEACON:
    return preprocess_beacon(op);

  case MSG_MON_COMMAND:
    try {
      return preprocess_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }

  default:
    mon.no_reply(op);
    derr << "Unhandled message type " << m->get_type() << dendl;
    return true;
  }
  return false;
}

bool NVMeofGwMon::prepare_update(MonOpRequestRef op)
{
  auto m = op->get_req<PaxosServiceMessage>();
  switch (m->get_type()) {
  case MSG_MNVMEOF_GW_BEACON:
    return prepare_beacon(op);

  case MSG_MON_COMMAND:
    try {
      return prepare_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return false; /* nothing to propose! */
    }

  default:
    mon.no_reply(op);
    dout(1) << "Unhandled message type " << m->get_type() << dendl;
    return false; /* nothing to propose! */
  }
}

bool NVMeofGwMon::preprocess_command(MonOpRequestRef op)
{
  dout(10) << dendl;
  auto m = op->get_req<MMonCommand>();
  std::stringstream sstrm;
  bufferlist rdata;
  string rs;
  int err = 0;
  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, sstrm))
  {
    string rs = sstrm.str();
    dout(4) << "Error : Invalid command "  << m->cmd
	    << "Error " << rs << dendl;
    mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);
  dout(10) << "MonCommand : "<< prefix <<  dendl;
  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));
  if (prefix == "nvme-gw show") {
    std::string  pool, group;
    if (!f) {
      f.reset(Formatter::create(format, "json-pretty", "json-pretty"));
    }
    cmd_getval(cmdmap, "pool", pool);
    cmd_getval(cmdmap, "group", group);
    auto group_key = std::make_pair(pool, group);
    dout(10) << "nvme-gw show  pool " << pool << " group " << group << dendl;

    f->open_object_section("common");
    f->dump_unsigned("epoch", map.epoch);
    f->dump_string("pool", pool);
    f->dump_string("group", group);
    if (HAVE_FEATURE(mon.get_quorum_con_features(), NVMEOFHA)) {
      f->dump_string("features", "LB");
    }
    f->dump_unsigned("num gws", map.created_gws[group_key].size());
    if (map.created_gws[group_key].size() == 0) {
      f->close_section();
      f->flush(rdata);
      sstrm.str("");
    } else {
      sstrm << "[ ";
      NvmeGwId gw_id;
      BeaconSubsystems   *subsystems = NULL;
      for (auto& gw_created_pair: map.created_gws[group_key]) {
        gw_id = gw_created_pair.first;
        auto& st = gw_created_pair.second;
        if (st.availability != gw_availability_t::GW_DELETING) {
          // not show ana group of deleting gw in the list -
          // it is information for the GW used in rebalancing process
          sstrm << st.ana_grp_id+1 << " ";
        }
        if (st.availability == gw_availability_t::GW_AVAILABLE) {
          subsystems = &st.subsystems;
        }
      }
      sstrm << "]";
      f->dump_string("Anagrp list", sstrm.str());
      std::map<NvmeAnaGrpId, uint16_t> num_ns;
      uint16_t total_ns = 0;
      if (subsystems && subsystems->size()) {
        for (auto & subs_it:*subsystems) {
          for (auto & ns :subs_it.namespaces) {
            if (num_ns.find(ns.anagrpid) == num_ns.end()) num_ns[ns.anagrpid] = 0;
              num_ns[ns.anagrpid] +=1;
              total_ns += 1;
          }
        }
      }
      f->dump_unsigned("num-namespaces", total_ns);
      f->open_array_section("Created Gateways:");
      uint32_t i = 0;
      for (auto& gw_created_pair: map.created_gws[group_key]) {
	auto& gw_id = gw_created_pair.first;
	auto& state = gw_created_pair.second;
	i = 0;
	f->open_object_section("stat");
	f->dump_string("gw-id", gw_id);
	f->dump_unsigned("anagrp-id",state.ana_grp_id+1);
	f->dump_unsigned("num-namespaces", num_ns[state.ana_grp_id+1]);
	f->dump_unsigned("performed-full-startup", state.performed_full_startup);
	std::stringstream  sstrm1;
	sstrm1 << state.availability;
	f->dump_string("Availability", sstrm1.str());
	uint32_t num_listeners = 0;
	if (state.availability == gw_availability_t::GW_AVAILABLE) {
	  for (auto &subs: state.subsystems) {
	    num_listeners += subs.listeners.size();
	  }
	  f->dump_unsigned("num-listeners", num_listeners);
	}
	sstrm1.str("");
	for (auto &state_itr: map.created_gws[group_key][gw_id].sm_state) {
	  sstrm1 << " " << state_itr.first + 1 << ": "
		 << state.sm_state[state_itr.first];
		 if (++i < map.created_gws[group_key][gw_id].sm_state.size())
		  sstrm1<<  ", ";
	}
	f->dump_string("ana states", sstrm1.str());
	f->close_section();
      }
      f->close_section();
      f->close_section();
      f->flush(rdata);
      sstrm.str("");
    }
    getline(sstrm, rs);
    mon.reply_command(op, err, rs, rdata, get_last_committed());
    return true;
  }
  return false;
}

bool NVMeofGwMon::prepare_command(MonOpRequestRef op)
{
  dout(10)  << dendl;
  auto m = op->get_req<MMonCommand>();
  int rc;
  std::stringstream sstrm;
  bufferlist rdata;
  string rs;
  int err = 0;
  cmdmap_t cmdmap;
  bool response = false;

  if (!cmdmap_from_json(m->cmd, &cmdmap, sstrm))
  {
    string rs = sstrm.str();
    mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  const auto prefix = cmd_getval_or<string>(cmdmap, "prefix", string{});

  dout(10) << "MonCommand : "<< prefix <<  dendl;
  if (prefix == "nvme-gw create" || prefix == "nvme-gw delete") {
    std::string id, pool, group;

    cmd_getval(cmdmap, "id", id);
    cmd_getval(cmdmap, "pool", pool);
    cmd_getval(cmdmap, "group", group);
    auto group_key = std::make_pair(pool, group);
    dout(10) << " id "<< id <<" pool "<< pool << " group "<< group << dendl;
    if (prefix == "nvme-gw create") {
      rc = pending_map.cfg_add_gw(id, group_key);
      if (rc == -EINVAL) {
	err = rc;
	dout (4) << "Error: GW cannot be created " << id
		 << " " << pool << " " << group << "  rc " << rc << dendl;
	sstrm.str("");
      }
    } else {
      rc = pending_map.cfg_delete_gw(id, group_key);
      if (rc == 0) {
        bool propose = false;
        // Simulate  immediate Failover of this GW
        process_gw_down(id, group_key, propose,
           gw_availability_t::GW_UNAVAILABLE);
      } else if (rc == -EINVAL) {
	dout (4) << "Error: GW not found in the database " << id << " "
		 << pool << " " << group << "  rc " << rc << dendl;
	err = 0;
	sstrm.str("");
      }
    }
    // propose pending would be generated by the PaxosService
    if ((rc != -EEXIST) && (rc != -EINVAL)) {
      response = true;
    }
  }

  getline(sstrm, rs);
  if (response == false) {
    if (err < 0 && rs.length() == 0) {
      rs = cpp_strerror(err);
      dout(10) << "Error command  err : "<< err  << " rs-len: "
	       << rs.length() <<  dendl;
    }
    mon.reply_command(op, err, rs, rdata, get_last_committed());
  } else {
    wait_for_commit(op, new Monitor::C_Command(mon, op, 0, rs,
					       get_last_committed() + 1));
  }
  return response;
}

void NVMeofGwMon::process_gw_down(const NvmeGwId &gw_id,
   const NvmeGroupKey& group_key, bool &propose_pending,
   gw_availability_t avail)
{
  LastBeacon lb = {gw_id, group_key};
  auto it = last_beacon.find(lb);
  if (it != last_beacon.end()) {
    last_beacon.erase(it);
    if (avail == gw_availability_t::GW_UNAVAILABLE) {
      pending_map.process_gw_map_gw_down(gw_id, group_key, propose_pending);
    } else {
      pending_map.process_gw_map_gw_no_subsys_no_listeners(gw_id, group_key, propose_pending);
    }

  }
}

bool NVMeofGwMon::preprocess_beacon(MonOpRequestRef op)
{
  auto m = op->get_req<MNVMeofGwBeacon>();
  const BeaconSubsystems& sub = m->get_subsystems();
  dout(15) << "beacon from " << m->get_type()
	   << " GW : " << m->get_gw_id()
	   << " num subsystems " << sub.size() <<  dendl;

  // allways  return false to call leader's prepare beacon
  return false;
}


bool NVMeofGwMon::prepare_beacon(MonOpRequestRef op)
{
  auto m = op->get_req<MNVMeofGwBeacon>();

  dout(20) << "availability " <<  m->get_availability()
	   << " GW : " << m->get_gw_id()
	   << " osdmap_epoch " << m->get_last_osd_epoch()
	   << " subsystems " << m->get_subsystems() << dendl;

  NvmeGwId gw_id = m->get_gw_id();
  NvmeGroupKey group_key = std::make_pair(m->get_gw_pool(),  m->get_gw_group());
  gw_availability_t  avail = m->get_availability();
  bool propose = false;
  bool nonce_propose = false;
  bool timer_propose = false;
  bool gw_created = true;
  NVMeofGwMap ack_map;
  auto& group_gws = map.created_gws[group_key];
  auto gw = group_gws.find(gw_id);
  const BeaconSubsystems& sub = m->get_subsystems();
  auto now = ceph::coarse_mono_clock::now();

  if (avail == gw_availability_t::GW_CREATED) {
    if (gw == group_gws.end()) {
      gw_created = false;
      dout(10) << "Warning: GW " << gw_id << " group_key " << group_key
	       << " was not found in the  map.Created_gws "
	       << map.created_gws << dendl;
      goto set_propose;
    } else {
      dout(10) << "GW  prepares the full startup " << gw_id
	       << " GW availability: "
	       << pending_map.created_gws[group_key][gw_id].availability
	       << dendl;
      if (pending_map.created_gws[group_key][gw_id].availability ==
	  gw_availability_t::GW_AVAILABLE) {
	dout(4) << " Warning :GW marked as Available in the NVmeofGwMon "
		<< "database, performed full startup - Apply GW!"
		<< gw_id << dendl;
	 pending_map.handle_gw_performing_fast_reboot(gw_id, group_key, propose);
	 LastBeacon lb = {gw_id, group_key};
	 last_beacon[lb] = now; //Update last beacon
      } else if (
	pending_map.created_gws[group_key][gw_id].performed_full_startup ==
	false) {
	pending_map.created_gws[group_key][gw_id].performed_full_startup = true;
	propose = true;
      }
      goto set_propose;
    }
  // gw already created
  } else {
    // if GW reports Available but in monitor's database it is Unavailable
    if (gw != group_gws.end()) {
      // it means it did not perform "exit" after failover was set by
      // NVMeofGWMon
      if ((pending_map.created_gws[group_key][gw_id].availability ==
	   gw_availability_t::GW_UNAVAILABLE) &&
	  (pending_map.created_gws[group_key][gw_id].performed_full_startup ==
	   false) &&
	  avail == gw_availability_t::GW_AVAILABLE) {
	ack_map.created_gws[group_key][gw_id] =
	  pending_map.created_gws[group_key][gw_id];
	ack_map.epoch = map.epoch;
	dout(4) << " Force gw to exit: Sending ack_map to GW: "
		<< gw_id << dendl;
	auto msg = make_message<MNVMeofGwMap>(ack_map);
	mon.send_reply(op, msg.detach());
	goto false_return;
      }
    }
  }

  // At this stage the gw has to be in the Created_gws
  if (gw == group_gws.end()) {
    dout(4) << "GW that does not appear in the map sends beacon, ignore "
       << gw_id << dendl;
    mon.no_reply(op);
    goto false_return; // not sending ack to this beacon
  }
  if (pending_map.created_gws[group_key][gw_id].availability ==
    gw_availability_t::GW_DELETING) {
    dout(4) << "GW sends beacon in DELETING state, ignore "
       << gw_id << dendl;
    mon.no_reply(op);
    goto false_return; // not sending ack to this beacon
  }
  // deep copy the whole nonce map of this GW
  if (m->get_nonce_map().size()) {
    if (pending_map.created_gws[group_key][gw_id].nonce_map !=
	m->get_nonce_map()) {
      dout(10) << "nonce map of GW  changed , propose pending "
	       << gw_id << dendl;
      pending_map.created_gws[group_key][gw_id].nonce_map = m->get_nonce_map();
      dout(10) << "nonce map of GW " << gw_id << " "
	       << pending_map.created_gws[group_key][gw_id].nonce_map  << dendl;
      nonce_propose = true;
    }
  } else {
    dout(10) << "Warning: received empty nonce map in the beacon of GW "
	     << gw_id << " " << dendl;
  }

  if (sub.size() == 0) {
    avail = gw_availability_t::GW_CREATED;
    dout(20) << "No-subsystems condition detected for GW " << gw_id <<dendl;
  } else {
    bool listener_found = true;
    for (auto &subs: sub) {
      if (subs.listeners.size() == 0) {
        listener_found = false;
        dout(10) << "No-listeners condition detected for GW " << gw_id << " for nqn " << subs.nqn << dendl;
        break;
      }
    }
    if (!listener_found) {
     avail = gw_availability_t::GW_CREATED;
    }
  }// for HA no-subsystems and no-listeners are same usecases
  if (pending_map.created_gws[group_key][gw_id].subsystems != sub) {
    dout(10) << "subsystems of GW changed, propose pending " << gw_id << dendl;
    pending_map.created_gws[group_key][gw_id].subsystems =  sub;
    dout(20) << "subsystems of GW " << gw_id << " "
	     << pending_map.created_gws[group_key][gw_id].subsystems << dendl;
    nonce_propose = true;
  }
  pending_map.created_gws[group_key][gw_id].last_gw_map_epoch_valid =
    (map.epoch == m->get_last_gwmap_epoch());
  if (pending_map.created_gws[group_key][gw_id].last_gw_map_epoch_valid ==
      false) {
    dout(20) <<  "map epoch of gw is not up-to-date " << gw_id
	     << " epoch " << map.epoch
	     << " beacon_epoch " << m->get_last_gwmap_epoch() <<  dendl;
  }
  if (avail == gw_availability_t::GW_AVAILABLE) {
    // check pending_map.epoch vs m->get_version() -
    // if different - drop the beacon

    LastBeacon lb = {gw_id, group_key};
    last_beacon[lb] = now;
    epoch_t last_osd_epoch = m->get_last_osd_epoch();
    pending_map.process_gw_map_ka(gw_id, group_key, last_osd_epoch, propose);
  // state set by GW client application
  } else if (avail == gw_availability_t::GW_UNAVAILABLE ||
      avail == gw_availability_t::GW_CREATED) {
      process_gw_down(gw_id, group_key, propose, avail);
  }
  // Periodic: check active FSM timers
  pending_map.update_active_timers(timer_propose);
  propose |= timer_propose;
  propose |= nonce_propose;

set_propose:
  if (!propose) {
    if (gw_created) {
      // respond with a map slice correspondent to the same GW
      ack_map.created_gws[group_key][gw_id] = map.created_gws[group_key][gw_id];
    }
    ack_map.epoch = map.epoch;
    dout(20) << "ack_map " << ack_map <<dendl;
    auto msg = make_message<MNVMeofGwMap>(ack_map);
    mon.send_reply(op, msg.detach());
  } else {
    mon.no_reply(op);
  }
false_return:
  if (propose) {
    dout(10) << "decision in prepare_beacon" <<dendl;
    return true;
  } else {
    return false; // if no changes are need in the map
  }
}
