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
#include "NVMeofGwMap.h"
#include "OSDMonitor.h"
#include "mon/health_check.h"

using std::list;
using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::string;

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout << "nvmeofgw " << __PRETTY_FUNCTION__ << " "

void NVMeofGwMap::to_gmap(
  std::map<NvmeGroupKey, NvmeGwMonClientStates>& Gmap) const
{
  Gmap.clear();
  for (const auto& created_map_pair: created_gws) {
    const auto& group_key = created_map_pair.first;
    const NvmeGwMonStates& gw_created_map = created_map_pair.second;
    for (const auto& gw_created_pair: gw_created_map) {
      const auto& gw_id = gw_created_pair.first;
      const auto& gw_created  = gw_created_pair.second;
      gw_availability_t availability = gw_created.availability;
      // Gateways expect to see UNAVAILABLE, not DELETING
      // for entries in DELETING state
      if (gw_created.availability == gw_availability_t::GW_DELETING) {
         availability = gw_availability_t::GW_UNAVAILABLE;
      }

      auto gw_state = NvmeGwClientState(
	gw_created.ana_grp_id, epoch, availability);
      for (const auto& sub: gw_created.subsystems) {
	gw_state.subsystems.insert({
	    sub.nqn,
	    NqnState(sub.nqn, gw_created.sm_state, gw_created)
	  });
      }
      Gmap[group_key][gw_id] = gw_state;
      dout (20) << gw_id << " Gw-Client: " << gw_state << dendl;
    }
  }
}

void NVMeofGwMap::add_grp_id(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key, const NvmeAnaGrpId grpid)
{
  Tmdata tm_data;
  Blocklist_data blklist_data;
  created_gws[group_key][gw_id].sm_state[grpid] =
    gw_states_per_group_t::GW_STANDBY_STATE;
  fsm_timers[group_key][gw_id].data[grpid] = tm_data;
  created_gws[group_key][gw_id].blocklist_data[grpid] = blklist_data;
}

void NVMeofGwMap::remove_grp_id(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key, const NvmeAnaGrpId grpid)
{
  created_gws[group_key][gw_id].sm_state.erase(grpid);
  created_gws[group_key][gw_id].blocklist_data.erase(grpid);
  fsm_timers[group_key][gw_id].data.erase(grpid);
}

int NVMeofGwMap::cfg_add_gw(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key)
{
  std::set<NvmeAnaGrpId> allocated;
  if (HAVE_FEATURE(mon->get_quorum_con_features(), NVMEOFHAMAP)) {
    auto gw_epoch_it = gw_epoch.find(group_key);
    if (gw_epoch_it == gw_epoch.end()) {
      gw_epoch[group_key] = epoch;
      dout(10) << "Allocated first gw_epoch : group_key "
          << group_key << " epoch " << gw_epoch[group_key] << dendl;
    }
  }
  for (auto& itr: created_gws[group_key]) {
    allocated.insert(itr.second.ana_grp_id);
    if (itr.first == gw_id) {
      if (itr.second.availability  != gw_availability_t::GW_DELETING) {
        dout(1) << __func__ << " ERROR create GW: already exists in map "
	          << gw_id << dendl;
        return -EEXIST;
      } else {
        //this GW exists in the map in "Deleting" state
        // but user again creates it - need just set attribute values
        created_gws[group_key][gw_id].performed_full_startup = true;
        created_gws[group_key][gw_id].availability
          = gw_availability_t::GW_CREATED;
        dout(4) << "GW in Deleting state " << gw_id
             << " was created again" << dendl;
        return 0;
      }
    }
  }
  for (auto& itr: created_gws[group_key]) {
    if (itr.second.availability == gw_availability_t::GW_DELETING) {
      //Was found some GW in "Deleting" state. Just to inherit its ANA group
      NvmeGwMonState & gw_created = created_gws[group_key][itr.first];
      created_gws[group_key][gw_id] = gw_created;
      // Deep copy of all data of "Deleting" GW
      created_gws[group_key][gw_id].performed_full_startup = true;
      created_gws[group_key][gw_id].availability
         =  gw_availability_t::GW_CREATED;
      dout(4) << "Created GW inherits ANA group of deleting GW-id :"
          << itr.first << " group " << itr.second.ana_grp_id  <<  dendl;
      do_erase_gw_id(itr.first, group_key);
      dout(4) << "Created GWS after create/delete:  "
         << created_gws << dendl;
      return 0;
   }
  }
  if (allocated.size() == MAX_SUPPORTED_ANA_GROUPS) {
    dout(4) << "Warning:  cannot add GW " << gw_id
         << " since number GWs in the group is "
         <<  MAX_SUPPORTED_ANA_GROUPS << dendl;
    return -EINVAL;
  }
  // Allocate the new group id
  NvmeAnaGrpId i = 0;
  bool was_allocated = false;

  // "allocated" is a sorted set (!),so if found any gap between numbers,
  // it should be filled
  for (NvmeAnaGrpId elem: allocated) {
    if (i != elem) {
      allocated.insert(i);
      was_allocated = true;
      break;
    }
    i++;
  }
  if (!was_allocated) allocated.insert(i);

  dout(10) << "allocated ANA groupId " << i << " for GW " << gw_id << dendl;
  // add new allocated grp_id to maps of created gateways
  for (auto& itr: created_gws[group_key]) {
    add_grp_id(itr.first, group_key, i);
  }
  NvmeGwMonState gw_created(i);
  created_gws[group_key][gw_id] = gw_created;
  created_gws[group_key][gw_id].performed_full_startup = true;
  for (NvmeAnaGrpId elem: allocated) {
    // add all existed grp_ids to newly created gateway
    add_grp_id(gw_id, group_key, elem);
    dout(4) << "adding group " << elem << " to gw " << gw_id << dendl;
  }
  dout(10) << __func__ << " Created GWS:  " << created_gws  <<  dendl;
  return 0;
}

int NVMeofGwMap::cfg_delete_gw(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key)
{
  if (HAVE_FEATURE(mon->get_quorum_con_features(), NVMEOFHA)) {
    dout(10) << " has NVMEOFHA: 1" << dendl;
    for (auto& gws_states: created_gws[group_key]) {
      if (gws_states.first == gw_id) {
        auto& state = gws_states.second;
        state.availability = gw_availability_t::GW_DELETING;
        dout(4) << " Deleting  GW :"<< gw_id  << " in state  "
            << state.availability <<  " Resulting GW availability: "
            << state.availability  << dendl;
        state.subsystems.clear();//ignore subsystems of this GW
        utime_t now = ceph_clock_now();
        mon->nvmegwmon()->gws_deleting_time[group_key][gw_id] = now;
        return 0;
      }
    }
  } else {
    return do_delete_gw(gw_id, group_key);
  }
  return -EINVAL;
}

int  NVMeofGwMap::do_erase_gw_id(const NvmeGwId &gw_id,
      const NvmeGroupKey& group_key) {

  fsm_timers[group_key].erase(gw_id);
  if (fsm_timers[group_key].size() == 0)
    fsm_timers.erase(group_key);

  created_gws[group_key].erase(gw_id);
  if (created_gws[group_key].size() == 0) {
    created_gws.erase(group_key);
    gw_epoch.erase(group_key);
  }
  return 0;
}

int NVMeofGwMap::do_delete_gw(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key)
{
  for (auto& gws_states: created_gws[group_key]) {

    if (gws_states.first == gw_id) {
      auto& state = gws_states.second;
      for (auto& state_itr: created_gws[group_key][gw_id].sm_state) {
	bool modified;
	fsm_handle_gw_delete(
	  gw_id, group_key,state_itr.second , state_itr.first, modified);
      }
      dout(10) << " Delete GW :"<< gw_id  << " ANA grpid: "
        << state.ana_grp_id  << dendl;
      for (auto& itr: created_gws[group_key]) {
        // Update state map and other maps
	remove_grp_id(itr.first, group_key, state.ana_grp_id);
	// of all created gateways. Removed key = anagrp
      }
      return do_erase_gw_id(gw_id, group_key);
    }
  }

  return -EINVAL;
}

void  NVMeofGwMap::gw_performed_startup(const NvmeGwId &gw_id,
      const NvmeGroupKey& group_key, bool &propose_pending)
{
  dout(4) << "GW  performed the full startup " << gw_id << dendl;
  propose_pending = true;
  increment_gw_epoch( group_key);
}

void NVMeofGwMap::increment_gw_epoch(const NvmeGroupKey& group_key)
{
  if (HAVE_FEATURE(mon->get_quorum_con_features(), NVMEOFHAMAP)) {
    gw_epoch[group_key] ++;
    dout(4) << "incremented epoch of " << group_key
         << " " << gw_epoch[group_key] << dendl;
  }
}

int NVMeofGwMap::get_num_namespaces(const NvmeGwId &gw_id,
    const NvmeGroupKey& group_key,  const BeaconSubsystems&  subs)
{
  auto grpid = created_gws[group_key][gw_id].ana_grp_id;
  int num_ns = 0;
  if (subs.size() == 0) {
    dout(20) << "Empty subsystems  for GW " << gw_id  << dendl;
  }
  for (auto & subsystem:subs) {
    for (auto & ns :subsystem.namespaces) {
      if (ns.anagrpid == (grpid+1)) {
         num_ns++;
      }
    }
  }
  return num_ns;
}

void NVMeofGwMap::track_deleting_gws(const NvmeGroupKey& group_key,
    const BeaconSubsystems& subs,  bool &propose_pending)
{
  propose_pending = false;
  for (auto& itr: created_gws[group_key]) {
    auto &gw_id = itr.first;
    if (itr.second.availability == gw_availability_t::GW_DELETING) {
      int num_ns = 0;
      if ( (num_ns = get_num_namespaces(gw_id, group_key, subs)) == 0) {
        do_delete_gw(gw_id, group_key);
        propose_pending =  true;
      }
      dout(4) << " to delete ? " << gw_id  << " num_ns " << num_ns
          << " subsystems size "<< subs.size() << dendl;
      break; // handle just one GW in "Deleting" state in time.
    }
  }
}

int NVMeofGwMap::process_gw_map_gw_no_subsys_no_listeners(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key, bool &propose_pending)
{
  int rc = 0;
  auto& gws_states = created_gws[group_key];
  auto  gw_state = gws_states.find(gw_id);
  if (gw_state != gws_states.end()) {
    dout(10) << "GW- no subsystems configured " << gw_id << dendl;
    auto& st = gw_state->second;
    st.availability = gw_availability_t::GW_CREATED;
    for (auto& state_itr: created_gws[group_key][gw_id].sm_state) {
      fsm_handle_gw_no_subsystems(
    gw_id, group_key, state_itr.second,state_itr.first, propose_pending);
    }
    propose_pending = true; // map should reflect that gw becames Created
    if (propose_pending) {
      validate_gw_map(group_key);
      increment_gw_epoch(group_key);
    }
  } else {
    dout(1)  << __FUNCTION__ << "ERROR GW-id was not found in the map "
         << gw_id << dendl;
    rc = -EINVAL;
  }
  return rc;
}

int NVMeofGwMap::process_gw_map_gw_down(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key, bool &propose_pending)
{
  int rc = 0;
  auto& gws_states = created_gws[group_key];
  auto  gw_state = gws_states.find(gw_id);
  if (gw_state != gws_states.end()) {
    dout(10) << "GW down " << gw_id << dendl;
    auto& st = gw_state->second;
    st.set_unavailable_state();
    for (auto& state_itr: created_gws[group_key][gw_id].sm_state) {
      fsm_handle_gw_down(
	gw_id, group_key, state_itr.second,
	state_itr.first, propose_pending);
      state_itr.second = gw_states_per_group_t::GW_STANDBY_STATE;
    }
    propose_pending = true; // map should reflect that gw becames Unavailable
    if (propose_pending) {
      validate_gw_map(group_key);
      increment_gw_epoch(group_key);
    }
  } else {
    dout(1)  << __FUNCTION__ << "ERROR GW-id was not found in the map "
	     << gw_id << dendl;
    rc = -EINVAL;
  }
  return rc;
}

void NVMeofGwMap::process_gw_map_ka(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
  epoch_t& last_osd_epoch, bool &propose_pending)
{
  auto& gws_states = created_gws[group_key];
  auto  gw_state = gws_states.find(gw_id);
  auto& st = gw_state->second;
  dout(20)  << "KA beacon from the GW " << gw_id
	    << " in state " << (int)st.availability << dendl;

  if (st.availability == gw_availability_t::GW_CREATED ||
      st.availability == gw_availability_t::GW_UNAVAILABLE) {
    st.availability = gw_availability_t::GW_AVAILABLE;
    if (st.ana_grp_id == REDUNDANT_GW_ANA_GROUP_ID) {
      for (auto& state_itr: created_gws[group_key][gw_id].sm_state) {
	state_itr.second = gw_states_per_group_t::GW_STANDBY_STATE;
      }
      propose_pending = true;
    } else {
      //========= prepare to Failback to this GW =========
      // find the GW that took over on the group st.ana_grp_id
      find_failback_gw(gw_id, group_key, propose_pending);
    }
  } else if (st.availability == gw_availability_t::GW_AVAILABLE) {
    for (auto& state_itr: created_gws[group_key][gw_id].sm_state) {
      fsm_handle_gw_alive(
	gw_id, group_key, gw_state->second, state_itr.second,
	state_itr.first, last_osd_epoch, propose_pending);
    }
  }
  if (propose_pending) {
    validate_gw_map(group_key);
    increment_gw_epoch(group_key);
  }
}

void NVMeofGwMap::handle_abandoned_ana_groups(bool& propose)
{
  propose = false;
  for (auto& group_state: created_gws) {
    auto& group_key = group_state.first;
    auto& gws_states = group_state.second;

    for (auto& gw_state : gws_states) { // loop for GWs inside nqn group
      auto& gw_id = gw_state.first;
      NvmeGwMonState& state = gw_state.second;

      // 1. Failover missed : is there is a GW in unavailable state?
      // if yes, is its ANA group handled by some other GW?
      if ((state.availability == gw_availability_t::GW_UNAVAILABLE ||
          state.availability == gw_availability_t::GW_DELETING ||
          state.availability == gw_availability_t::GW_CREATED) &&
	  state.ana_grp_id != REDUNDANT_GW_ANA_GROUP_ID) {
	auto found_gw_for_ana_group = false;
	for (auto& gw_state2 : gws_states) {
	  NvmeGwMonState& state2 = gw_state2.second;
	  if (state2.availability == gw_availability_t::GW_AVAILABLE &&
	      (state2.sm_state[state.ana_grp_id] ==
	       gw_states_per_group_t::GW_ACTIVE_STATE)) {
	    found_gw_for_ana_group = true;
	    break;
	  }
	}
        // choose the GW for handle ana group
	if (found_gw_for_ana_group == false) {
	  dout(20) << "Was not found the GW " << " that handles ANA grp "
		   << (int)state.ana_grp_id << " find candidate "<< dendl;
	  for (auto& state_itr: created_gws[group_key][gw_id].sm_state) {
	    find_failover_candidate(gw_id, group_key, state_itr.first, propose);
	  }
	}
      } else if (state.availability == gw_availability_t::GW_AVAILABLE &&
		 state.ana_grp_id != REDUNDANT_GW_ANA_GROUP_ID &&
		 (state.sm_state[state.ana_grp_id] ==
		  gw_states_per_group_t::GW_STANDBY_STATE)) {
	// 2. Failback missed: Check this GW is Available and Standby and
	// no other GW is doing Failback to it
	find_failback_gw(gw_id, group_key, propose);
      }
    }
    if (propose) {
      validate_gw_map(group_key);
      increment_gw_epoch(group_key);
    }
  }
}

void NVMeofGwMap::set_failover_gw_for_ANA_group(
  const NvmeGwId &failed_gw_id, const NvmeGroupKey& group_key,
  const NvmeGwId &gw_id, NvmeAnaGrpId ANA_groupid)
{
  NvmeGwMonState& gw_state = created_gws[group_key][gw_id];
  NvmeGwMonState& failed_gw_state = created_gws[group_key][failed_gw_id];
  epoch_t epoch;
  dout(10) << "Found failover GW " << gw_id
	   << " for ANA group " << (int)ANA_groupid << dendl;
  if (failed_gw_state.availability == gw_availability_t::GW_CREATED) {
    dout(10) << "Failover GW " << gw_id <<
       " takes over the group of GW in Created state " <<
       failed_gw_id  << dendl;
    // just take over on the group of created GW
    gw_state.active_state(ANA_groupid);
    return;
  }
  int rc = blocklist_gw(failed_gw_id, group_key, ANA_groupid, epoch, true);
  if (rc) {
    //start failover even when nonces are empty !
    gw_state.active_state(ANA_groupid);
  } else {
    gw_state.sm_state[ANA_groupid] =
      gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL;
    gw_state.blocklist_data[ANA_groupid].osd_epoch = epoch;
    gw_state.blocklist_data[ANA_groupid].is_failover = true;
    // start Failover preparation timer
    start_timer(gw_id, group_key, ANA_groupid, 30);
  }
}

void NVMeofGwMap::find_failback_gw(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key, bool &propose)
{
  auto& gws_states = created_gws[group_key];
  auto& gw_state = created_gws[group_key][gw_id];
  bool do_failback = false;
  dout(10) << "Find failback GW for GW " << gw_id << dendl;
  for (auto& gw_state_it: gws_states) {
    auto& st = gw_state_it.second;
    // some other gw owns or owned the desired ana-group
    if (st.sm_state[gw_state.ana_grp_id] !=
	gw_states_per_group_t::GW_STANDBY_STATE) {
      // if candidate is in state ACTIVE for the desired ana-group,
      // then failback starts immediately, otherwise need to wait
      do_failback = true;
      dout(10) << "Found some gw " << gw_state_it.first
	       << " in state " << st.sm_state[gw_state.ana_grp_id]  << dendl;
      break;
    }
  }

  if (do_failback == false) {
    // No other gw currently performs some activity with desired ana
    // group of coming-up GW - so it just takes over on the group
    dout(10) << "Failback GW candidate was not found, "
	     << "just set Optimized to group " << gw_state.ana_grp_id
	     << " to GW " << gw_id << dendl;
    gw_state.active_state(gw_state.ana_grp_id);
    propose = true;
    return;
  }

  // try to do_failback
  for (auto& gw_state_it: gws_states) {
    auto& failback_gw_id = gw_state_it.first;
    auto& st = gw_state_it.second;
    if (st.sm_state[gw_state.ana_grp_id] ==
	gw_states_per_group_t::GW_ACTIVE_STATE) {
      dout(10)  << "Found Failback GW " << failback_gw_id
		<< " that previously took over the ANAGRP "
		<< gw_state.ana_grp_id << " of the available GW "
		<< gw_id << dendl;
      st.sm_state[gw_state.ana_grp_id] =
	gw_states_per_group_t::GW_WAIT_FAILBACK_PREPARED;

      // Add timestamp of start Failback preparation
      start_timer(failback_gw_id, group_key, gw_state.ana_grp_id, 3);
      gw_state.sm_state[gw_state.ana_grp_id] =
	gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED;
      propose = true;
      break;
    }
  }
}

void  NVMeofGwMap::find_failover_candidate(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
  NvmeAnaGrpId grpid, bool &propose_pending)
{
  dout(10) << __func__<< " " << gw_id << dendl;
#define ILLEGAL_GW_ID " "
#define MIN_NUM_ANA_GROUPS 0xFFF
  int min_num_ana_groups_in_gw = 0;
  int current_ana_groups_in_gw = 0;
  NvmeGwId min_loaded_gw_id = ILLEGAL_GW_ID;
  auto& gws_states = created_gws[group_key];
  auto gw_state = gws_states.find(gw_id);

  // this GW may handle several ANA groups and  for each
  // of them need to found the candidate GW
  if ((gw_state->second.sm_state[grpid] ==
       gw_states_per_group_t::GW_ACTIVE_STATE) ||
      gw_state->second.ana_grp_id == grpid) {

    // for all the gateways of the subsystem
    for (auto& found_gw_state: gws_states) {
      auto st = found_gw_state.second;
      // some GW already started failover/failback on this group
      if (st.sm_state[grpid] ==  gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL) {
	dout(4) << "Warning : Failover" << st.blocklist_data[grpid].is_failover
		<<  " already started for the group " << grpid
		<< " by GW " << found_gw_state.first << dendl;
	gw_state->second.standby_state(grpid);
	return ;
      }
    }
    // Find a GW that takes over the ANA group(s)
    min_num_ana_groups_in_gw = MIN_NUM_ANA_GROUPS;
    min_loaded_gw_id = ILLEGAL_GW_ID;

    // for all the gateways of the subsystem
    for (auto& found_gw_state: gws_states) {
      auto st = found_gw_state.second;
      if (st.availability == gw_availability_t::GW_AVAILABLE) {
	current_ana_groups_in_gw = 0;
	for (auto& state_itr: created_gws[group_key][gw_id].sm_state) {
	  NvmeAnaGrpId anagrp = state_itr.first;
	  if ((st.sm_state[anagrp] ==
	       gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED) ||
	      (st.sm_state[anagrp] ==
	       gw_states_per_group_t::GW_WAIT_FAILBACK_PREPARED) ||
	      (st.sm_state[anagrp] ==
	       gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL)) {
	    current_ana_groups_in_gw = 0xFFFF;
	    break; // dont take into account   GWs in the transitive state
	  } else if (st.sm_state[anagrp] ==
		     gw_states_per_group_t::GW_ACTIVE_STATE) {
            // how many ANA groups are handled by this GW
	    current_ana_groups_in_gw++;
	  }
	}
	if (min_num_ana_groups_in_gw > current_ana_groups_in_gw) {
	  min_num_ana_groups_in_gw = current_ana_groups_in_gw;
	  min_loaded_gw_id = found_gw_state.first;
	  dout(10) << "choose: gw-id  min_ana_groups " << min_loaded_gw_id
		   << current_ana_groups_in_gw << " min "
		   << min_num_ana_groups_in_gw << dendl;
	}
      }
    }
    if (min_loaded_gw_id != ILLEGAL_GW_ID) {
      propose_pending = true;
      set_failover_gw_for_ANA_group(gw_id, group_key, min_loaded_gw_id, grpid);
    } else {
      // not found candidate but map changed.
      if (gw_state->second.sm_state[grpid] ==
	  gw_states_per_group_t::GW_ACTIVE_STATE) {
	propose_pending = true;
	dout(10) << "gw down:  no candidate found " << dendl;
      }
    }
    gw_state->second.standby_state(grpid);
  }
}

void NVMeofGwMap::fsm_handle_gw_alive(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
  NvmeGwMonState & gw_state, gw_states_per_group_t state,
  NvmeAnaGrpId grpid, epoch_t& last_osd_epoch, bool &map_modified)
{
  switch (state) {
  case gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL:
  {
    int timer_val = get_timer(gw_id, group_key, grpid);
    NvmeGwMonState& gw_map = created_gws[group_key][gw_id];
    if (gw_map.blocklist_data[grpid].osd_epoch <= last_osd_epoch) {
      dout(10) << "is-failover: " << gw_map.blocklist_data[grpid].is_failover
	       << " osd epoch changed from "
	       << gw_map.blocklist_data[grpid].osd_epoch
	       << " to "<< last_osd_epoch
	       << " Ana-grp: " << grpid
	       << " timer:" << timer_val << dendl;
      // Failover Gw still alive and guaranteed that
      gw_state.active_state(grpid);
      // ana group wouldnt be taken back  during blocklist wait period
      cancel_timer(gw_id, group_key, grpid);
      map_modified = true;
    } else {
      dout(20) << "osd epoch not changed from "
	       <<  gw_map.blocklist_data[grpid].osd_epoch
	       << " to "<< last_osd_epoch
	       << " Ana-grp: " << grpid
	       << " timer:" << timer_val << dendl;
    }
  }
  break;

  default:
    break;
  }
}

void NVMeofGwMap::fsm_handle_gw_no_subsystems(
    const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
    gw_states_per_group_t state, NvmeAnaGrpId grpid,  bool &map_modified)
{
  switch (state) {
  case gw_states_per_group_t::GW_STANDBY_STATE:
  case gw_states_per_group_t::GW_IDLE_STATE:
    // nothing to do
    break;

  case gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL:
  {
    cancel_timer(gw_id, group_key, grpid);
    auto& gw_st = created_gws[group_key][gw_id];
    gw_st.standby_state(grpid);
    map_modified = true;
  }
  break;

  case gw_states_per_group_t::GW_WAIT_FAILBACK_PREPARED:
  {
    auto& gw_id_st = created_gws[group_key][gw_id];
    cancel_timer(gw_id, group_key,  grpid);
    map_modified = true;
    for (auto& gw_st: created_gws[group_key]) {
      auto& st = gw_st.second;
      // found GW   that was intended for  Failback for this ana grp
      if (st.sm_state[grpid] ==
      gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED) {
    dout(4) << "Warning: Outgoing Failback when GW is without subsystems"
        <<" Owner GW set to standby state " << gw_st.first << "for ANA Group "
        << grpid << dendl;
    st.standby_state(grpid);
    break;
      }
    }
    dout(4) << "Warning: Outgoing Failback when GW is without subsystems"
       <<" Failback GW set to standby state " << gw_id << "for ANA Group "
       << grpid << dendl;
    gw_id_st.standby_state(grpid);
  }
  break;

  case gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED:
  case gw_states_per_group_t::GW_ACTIVE_STATE:
  {
    dout(4) << "Set state to Standby for GW " << gw_id << " group "
        << grpid << dendl;
    auto& gw_st = created_gws[group_key][gw_id];
    gw_st.standby_state(grpid);
  }
  break;

  default:
  {
    dout(4) << "Error : Invalid state " << state << "for GW " << gw_id  << dendl;
  }
  }
}

void NVMeofGwMap::fsm_handle_gw_down(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
  gw_states_per_group_t state, NvmeAnaGrpId grpid,  bool &map_modified)
{
  switch (state) {
  case gw_states_per_group_t::GW_STANDBY_STATE:
  case gw_states_per_group_t::GW_IDLE_STATE:
    // nothing to do
    break;

  case gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL:
  {
    cancel_timer(gw_id, group_key, grpid);
    map_modified = true;
  }
  break;

  case gw_states_per_group_t::GW_WAIT_FAILBACK_PREPARED:
  {
    auto& gw_id_st = created_gws[group_key][gw_id];
    cancel_timer(gw_id, group_key,  grpid);
    map_modified = true;
    for (auto& gw_st: created_gws[group_key]) {
      auto& st = gw_st.second;
      // found GW   that was intended for  Failback for this ana grp
      if (st.sm_state[grpid] ==
	  gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED) {
	dout(4) << "Warning: Outgoing Failback when GW is down back"
		<<"Owner GW set to standby state " << gw_id << "for ANA Group "
		<< grpid << dendl;
	st.standby_state(grpid);
	break;
      }
    }
    dout(4) << "Warning: Outgoing Failback when GW is down back"
       <<" Failback GW set to standby state " << gw_id << "for ANA Group "
       << grpid << dendl;
    gw_id_st.standby_state(grpid);
  }
  break;

  case gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED:
    // nothing to do - let failback timer expire
    break;

  case gw_states_per_group_t::GW_ACTIVE_STATE:
  {
    find_failover_candidate(gw_id, group_key, grpid, map_modified);
  }
  break;

  default:
  {
    dout(4) << "Error : Invalid state " << state << "for GW " << gw_id  << dendl;
  }
  }
}

void NVMeofGwMap::fsm_handle_gw_delete(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
  gw_states_per_group_t state , NvmeAnaGrpId grpid, bool &map_modified) {
  //This function is called when GW already passed Failover and its native
  //Ana group has no volumes, so some states are not relevant
  switch (state) {
  case gw_states_per_group_t::GW_STANDBY_STATE:
  case gw_states_per_group_t::GW_IDLE_STATE:
  case gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED:
  {
    NvmeGwMonState& gw_state = created_gws[group_key][gw_id];

    // Try to find GW that temporary owns gw-id group that is about to disappear!
    // - if found, this GW should pass to standby for this group
    if (grpid == gw_state.ana_grp_id) {
      auto& gateway_states = created_gws[group_key];
      for (auto& gs: gateway_states) {
	if ((gs.second.sm_state[grpid] ==
	     gw_states_per_group_t::GW_ACTIVE_STATE) ||
	    (gs.second.sm_state[grpid] ==
	     gw_states_per_group_t::GW_WAIT_FAILBACK_PREPARED)) {
	  gs.second.standby_state(grpid);
	  map_modified = true;
	  if (gs.second.sm_state[grpid] ==
	      gw_states_per_group_t::GW_WAIT_FAILBACK_PREPARED) {
	    cancel_timer(gs.first, group_key, grpid);
	  }
	  break;
	}
      }
    }
  }
  break;

  default: {
    dout(4) << "Error : Invalid state " << state
	    << "for GW " << gw_id  << dendl;
  }
  }
  if (map_modified) {
    validate_gw_map(group_key);
    increment_gw_epoch(group_key);
  }
}

void NVMeofGwMap::fsm_handle_to_expired(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
  NvmeAnaGrpId grpid,  bool &map_modified)
{
  // GW in Fail-back preparation state fbp
  auto& fbp_gw_state = created_gws[group_key][gw_id];
  bool grp_owner_found = false;
  if (fbp_gw_state.sm_state[grpid] ==
      gw_states_per_group_t::GW_WAIT_FAILBACK_PREPARED) {
    for (auto& gw_state: created_gws[group_key]) {
      auto& st = gw_state.second;
      // group owner
      if (st.ana_grp_id == grpid) {
	grp_owner_found = true;
	if (st.availability == gw_availability_t::GW_AVAILABLE) {
	  if (!(fbp_gw_state.last_gw_map_epoch_valid &&
		st.last_gw_map_epoch_valid)) {
	    // Timer is not cancelled so it would expire over
	    // and over as long as both gws are not updated
	    dout(10) << "gw " << gw_id  <<" or gw "
		     << gw_state.first  << "map epochs are not updated "<< dendl;
	    return;
	  }
	}
	cancel_timer(gw_id, group_key, grpid);
	map_modified = true;
	if ((st.sm_state[grpid] ==
	     gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED) &&
	    (st.availability == gw_availability_t::GW_AVAILABLE)) {
          // Previous failover GW  set to standby
	  fbp_gw_state.standby_state(grpid);
	  st.active_state(grpid);
	  dout(10)  << "Expired Failback-preparation timer from GW "
		    << gw_id << " ANA groupId "<< grpid << dendl;
	  map_modified = true;
	  break;
	} else if ((st.sm_state[grpid] ==
		    gw_states_per_group_t::GW_STANDBY_STATE) &&
		   (st.availability == gw_availability_t::GW_AVAILABLE)) {
          // GW failed during the persistency interval
	  st.standby_state(grpid);
	  dout(10)  << "Failback unsuccessfull. GW: " << gw_state.first
		    << " becomes Standby for the ANA groupId " << grpid << dendl;
	}
	fbp_gw_state.standby_state(grpid);
	dout(10) << "Failback unsuccessfull GW: " << gw_id
		 << " becomes Standby for the ANA groupId " << grpid  << dendl;
	map_modified = true;
	break;
      }
    }
    if (grp_owner_found == false) {
      // when  GW group owner is deleted the fbk gw is put to standby
      dout(4) << "group owner not found " << grpid << " GW: " << gw_id << dendl;
    }
  } else if (fbp_gw_state.sm_state[grpid] ==
	     gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL) {
    dout(4) << "Warning: Expired GW_WAIT_BLOCKLIST_CMPL timer "
	    << "from GW, Force exit the GW " << gw_id
	    << " ANA groupId: "<< grpid << dendl;
    //another Trigger for GW down (failover)
    process_gw_map_gw_down(gw_id, group_key, map_modified);
  }
  if (map_modified) {
    validate_gw_map(group_key);
    increment_gw_epoch(group_key);
  }
}

struct CMonRequestProposal : public Context {
  NVMeofGwMap *m;
  entity_addrvec_t addr_vect;
  utime_t expires;
  CMonRequestProposal(
    NVMeofGwMap *mon , entity_addrvec_t addr_vector, utime_t until)
    : m(mon), addr_vect(addr_vector), expires (until)  {}
  void finish(int r) {
    dout(10) << "osdmon is  writable? "
	     << m->mon->osdmon()->is_writeable() << dendl;
    if (m->mon->osdmon()->is_writeable()) {
      epoch_t epoch = m->mon->osdmon()->blocklist(addr_vect, expires);
      dout(10) << "epoch " << epoch << dendl;
      m->mon->nvmegwmon()->request_proposal(m->mon->osdmon());
    } else {
      m->mon->osdmon()->wait_for_writeable_ctx(
	new CMonRequestProposal(m, addr_vect, expires)
      );
    }
  }
};

void NVMeofGwMap::get_health_checks(health_check_map_t *checks) 
{
  list<string> singleGatewayDetail;
  list<string> gatewayDownDetail;
  list<string> gatewayInDeletingDetail;
  int deleting_gateways = 0;
  for (const auto& created_map_pair: created_gws) {
    const auto& group_key = created_map_pair.first;
    auto& group = group_key.second;
    const NvmeGwMonStates& gw_created_map = created_map_pair.second;
    if ( gw_created_map.size() == 1) {
      ostringstream ss;
      ss << "NVMeoF Gateway Group '" << group << "' has 1 gateway." ;
      singleGatewayDetail.push_back(ss.str());
    }
    for (const auto& gw_created_pair: gw_created_map) {
      const auto& gw_id = gw_created_pair.first;
      const auto& gw_created  = gw_created_pair.second;
      if (gw_created.availability == gw_availability_t::GW_UNAVAILABLE) {
        ostringstream ss;
        ss << "NVMeoF Gateway '" << gw_id << "' is unavailable." ;
        gatewayDownDetail.push_back(ss.str());
      } else if (gw_created.availability == gw_availability_t::GW_DELETING) {
        deleting_gateways++;
        utime_t now = ceph_clock_now();
        bool found_deleting_time = false;
        auto gws_deleting_time = mon->nvmegwmon()->gws_deleting_time;
        auto group_it = gws_deleting_time.find(group_key);
        if (group_it != gws_deleting_time.end()) {
          auto& gw_map = group_it->second;
          auto gw_it = gw_map.find(gw_id);
          if (gw_it != gw_map.end()) {
            found_deleting_time = true;
            utime_t delete_time = gw_it->second;
            if ((now - delete_time) > g_conf().get_val<std::chrono::seconds>("mon_nvmeofgw_delete_grace").count()) {
              ostringstream ss;
              ss << "NVMeoF Gateway '" << gw_id << "' is in deleting state.";
              gatewayInDeletingDetail.push_back(ss.str());
            }
          }
        }
        if (!found_deleting_time) {
          // DELETING gateway not found in gws_deleting_time, set timeout now 
          mon->nvmegwmon()->gws_deleting_time[group_key][gw_id] = now; 
        }
      }
    }
  }
  if (deleting_gateways == 0) {
    // no gateway in GW_DELETING state currently, flush old gws_deleting_time
    mon->nvmegwmon()->gws_deleting_time.clear();
  }

  if (!singleGatewayDetail.empty()) {
    ostringstream ss;
    ss << singleGatewayDetail.size() << " group(s) have only 1 nvmeof gateway"
      << "; HA is not possible with single gateway.";
    auto& d = checks->add("NVMEOF_SINGLE_GATEWAY", HEALTH_WARN,
        ss.str(), singleGatewayDetail.size());
    d.detail.swap(singleGatewayDetail);
  }
  if (!gatewayDownDetail.empty()) {
    ostringstream ss;
    ss << gatewayDownDetail.size() << " gateway(s) are in unavailable state"
      << "; gateway might be down, try to redeploy.";
    auto& d = checks->add("NVMEOF_GATEWAY_DOWN", HEALTH_WARN,
        ss.str(), gatewayDownDetail.size());
    d.detail.swap(gatewayDownDetail);
  }
  if (!gatewayInDeletingDetail.empty()) {
    ostringstream ss;
    ss << gatewayInDeletingDetail.size() << " gateway(s) are in deleting state"
      << "; namespaces are automatically balanced across remaining gateways, "
      << "this should take a few minutes.";
    auto& d = checks->add("NVMEOF_GATEWAY_DELETING", HEALTH_WARN,
        ss.str(), gatewayInDeletingDetail.size());
    d.detail.swap(gatewayInDeletingDetail);
  }
}

int NVMeofGwMap::blocklist_gw(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
  NvmeAnaGrpId grpid, epoch_t &epoch, bool failover)
{
  // find_already_created_gw(gw_id, group_key);
  NvmeGwMonState& gw_map = created_gws[group_key][gw_id];
  NvmeNonceVector nonces;
  for (auto& state_itr: gw_map.sm_state) {
    // to make blocklist on all clusters of the failing GW
    nonces.insert(nonces.end(), gw_map.nonce_map[state_itr.first].begin(),
        gw_map.nonce_map[state_itr.first].end());
  }

  if (nonces.size() > 0) {
    NvmeNonceVector &nonce_vector = gw_map.nonce_map[grpid];;
    std::string str = "[";
    entity_addrvec_t addr_vect;

    double d = g_conf().get_val<double>("mon_osd_blocklist_default_expire");
    utime_t expires = ceph_clock_now();
    expires += d;
    dout(10) << " blocklist timestamp " << expires << dendl;
    for (auto &it: nonce_vector ) {
      if (str != "[") str += ",";
      str += it;
    }
    str += "]";
    bool success = addr_vect.parse(&str[0]);
    dout(10) << str << " parse success " << success <<  " network vector: " << addr_vect
	     << " " << addr_vect.size() << dendl;
    if (!success) {
      return 1;
    }

    if (!mon->osdmon()->is_writeable()) {
      dout(10) << "osdmon is not writable, waiting, epoch = " << epoch << dendl;
      mon->osdmon()->wait_for_writeable_ctx(
	new CMonRequestProposal(this, addr_vect, expires)
      );
      // return false;
    } else {
      epoch = mon->osdmon()->blocklist(addr_vect, expires);
      if (!mon->osdmon()->is_writeable()) {
	dout(10) << "osdmon is not writable after blocklist is "
		 << "done, waiting, epoch = " << epoch << dendl;
	mon->osdmon()->wait_for_writeable_ctx(
	  new CMonRequestProposal(this, addr_vect, expires)
	);
        // return false;
      } else {
	mon->nvmegwmon()->request_proposal(mon->osdmon());
      }
    }
    dout(10) << str << " mon->osdmon()->blocklist: epoch : " << epoch
	     << " address vector: " << addr_vect << " "
	     << addr_vect.size() << dendl;
  } else {
    dout(4) << "Error: No nonces context present for gw: "
	    << gw_id  << " ANA group: " << grpid << dendl;
    return 1;
  }
  return 0;
}

void  NVMeofGwMap::validate_gw_map(const NvmeGroupKey& group_key)
{
  for (auto& gw_created: created_gws[group_key]) {
    auto gw_id = gw_created.first;
    for (auto& state_itr: created_gws[group_key][gw_id].sm_state) {
      NvmeAnaGrpId ana_group = state_itr.first;
      int count = 0;
      for (auto& gw_created_pair: created_gws[group_key]) {
	auto& st = gw_created_pair.second;
	if (st.sm_state[ana_group] == gw_states_per_group_t::GW_ACTIVE_STATE) {
	  count ++;
	  if (count == 2) {
	    dout(1) << "Critical Error : number active states per ana-group "
		    << ana_group << "more than 1 in pool-group " << group_key
		    << dendl;
	    dout(4) << created_gws[group_key] << dendl;
	  }
	}
      }
    }
    break;
  }
}

void NVMeofGwMap::update_active_timers(bool &propose_pending)
{
  const auto now = std::chrono::system_clock::now();
  for (auto& group_to: fsm_timers) {
    auto& group_key = group_to.first;
    auto& pool = group_key.first;
    auto& group = group_key.second;
    for (auto& gw_to: group_to.second) {
      auto& gw_id = gw_to.first;
      auto& to = gw_to.second;
      for (auto &to_itr:to.data) {
	if (to.data[to_itr.first].timer_started == 0) continue;
	dout(20) << "Checking timer for GW " << gw_id << " ANA GRP "
		 << to_itr.first<< " value(seconds): "
		 << (int)to.data[to_itr.first].timer_value << dendl;
	if (now >= to.data[to_itr.first].end_time) {
	  fsm_handle_to_expired(
	    gw_id,
	    std::make_pair(pool, group), to_itr.first, propose_pending);
	}
      }
    }
  }
}

void NVMeofGwMap::start_timer(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key,
  NvmeAnaGrpId anagrpid, uint8_t value_sec)
{
  fsm_timers[group_key][gw_id].data[anagrpid].timer_started = 1;
  fsm_timers[group_key][gw_id].data[anagrpid].timer_value = value_sec;
  dout(10) << "start timer for ana " << anagrpid << " gw "
	   << gw_id << "value sec " << (int)value_sec << dendl;
  const auto now = std::chrono::system_clock::now();
  fsm_timers[group_key][gw_id].data[anagrpid].end_time =
    now + std::chrono::seconds(value_sec);
}

int NVMeofGwMap::get_timer(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId anagrpid)
{
  auto timer = fsm_timers[group_key][gw_id].data[anagrpid].timer_value;
  return timer;
}

void NVMeofGwMap::cancel_timer(
  const NvmeGwId &gw_id, const NvmeGroupKey& group_key, NvmeAnaGrpId anagrpid)
{
  fsm_timers[group_key][gw_id].data[anagrpid].timer_started = 0;
}
