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

#ifndef MON_NVMEOFGWTYPES_H_
#define MON_NVMEOFGWTYPES_H_
#include <string>
#include <iomanip>
#include <map>
#include <iostream>

using NvmeGwId = std::string;
using NvmeGroupKey = std::pair<std::string, std::string>;
using NvmeNqnId = std::string;
using NvmeAnaGrpId = uint32_t;


enum class gw_states_per_group_t {
  GW_IDLE_STATE = 0, //invalid state
  GW_STANDBY_STATE,
  GW_ACTIVE_STATE,
  GW_OWNER_WAIT_FAILBACK_PREPARED,
  GW_WAIT_FAILBACK_PREPARED,
  GW_WAIT_BLOCKLIST_CMPL
};

enum class gw_exported_states_per_group_t {
  GW_EXPORTED_OPTIMIZED_STATE = 0,
  GW_EXPORTED_INACCESSIBLE_STATE
};

enum class gw_availability_t {
  GW_CREATED = 0,
  GW_AVAILABLE,
  GW_UNAVAILABLE,
  GW_DELETING,
  GW_DELETED
};

#define REDUNDANT_GW_ANA_GROUP_ID 0xFF
using SmState = std::map < NvmeAnaGrpId, gw_states_per_group_t>;

using ana_state_t =
  std::vector<std::pair<gw_exported_states_per_group_t, epoch_t>>;

struct BeaconNamespace {
  NvmeAnaGrpId anagrpid;
  std::string  nonce;

  // Define the equality operator
  bool operator==(const BeaconNamespace& other) const {
    return anagrpid == other.anagrpid &&
      nonce == other.nonce;
  }
};

// Beacon Listener represents an NVME Subsystem listener,
// which generally does not have to use TCP/IP.
// It is derived from the SPDK listener JSON RPC representation.
// For more details, see
// https://spdk.io/doc/jsonrpc.html#rpc_nvmf_listen_address.
struct BeaconListener {
  std::string address_family; // IPv4 or IPv6
  std::string address;        //
  std::string svcid;          // port

  // Define the equality operator
  bool operator==(const BeaconListener& other) const {
    return address_family == other.address_family &&
      address == other.address &&
      svcid == other.svcid;
  }
};

struct BeaconSubsystem {
  NvmeNqnId nqn;
  std::list<BeaconListener>  listeners;
  std::list<BeaconNamespace> namespaces;

  // Define the equality operator
  bool operator==(const BeaconSubsystem& other) const {
    return nqn == other.nqn &&
      listeners == other.listeners &&
      namespaces == other.namespaces;
  }
};

using BeaconSubsystems = std::list<BeaconSubsystem>;

using NvmeNonceVector = std::vector<std::string>;
using NvmeAnaNonceMap = std::map <NvmeAnaGrpId, NvmeNonceVector>;

struct Blocklist_data{
  epoch_t osd_epoch;
  bool is_failover;
  Blocklist_data() {
    osd_epoch = 0;
    is_failover = true;
  };
  Blocklist_data(epoch_t epoch, bool failover)
    : osd_epoch(epoch), is_failover(failover) {};
};

using BlocklistData = std::map<NvmeAnaGrpId, Blocklist_data>;

struct NvmeGwMonState {
  // ana-group-id allocated for this GW, GW owns this group-id
  NvmeAnaGrpId ana_grp_id;
  // in absence of  beacon  heartbeat messages it becomes inavailable
  gw_availability_t availability;
  // "true" if the last epoch seen by the gw-client is up-to-date
  bool last_gw_map_epoch_valid;
  // in order to identify gws that did not exit upon failover
  bool performed_full_startup;
  // gateway susbsystem and their state machine states
  BeaconSubsystems subsystems;
  NvmeAnaNonceMap nonce_map;

  // state machine states per ANA group
  SmState sm_state;
  BlocklistData blocklist_data;
  //ceph entity address allocated for the GW-client that represents this GW-id
  entity_addrvec_t addr_vect;
  uint16_t beacon_index = 0;
  /**
   * during redeploy action and maybe other emergency use-cases gw performs scenario
   * that we call fast-reboot. It quickly reboots(due to redeploy f.e) and sends the
   * first beacon to monitor in "Created" state while according to monitor FSM it
   * still appears "Available".
   * This lost of synchronizarion with GW is detected by monitor. After fast reboot, the monitor
   * still considers this GW as not eligible for owning any ANA group until it becomes
   * in Available state (sends the next beacon that includes the subsystems information).
   * In this specific fast-reboot case, we prefer to avoid failing over the ANA groups
   * that were owned by this GW for a short time frame, assuming that this GW will be
   * in Available State in few seconds. Doing too many failovers and failbacks
   * in a very short times frame, on many GWs, is causing a lot of pain to the
   * initiators, up to the point that they might stuck.
   *
   * So was decided to set to GW new timeout varible "allow_failovers_ts" - no failovers
   * are performed to GW's pool-group during 12 seconds from the time it is set.
   * this variable is not persistent - not serialized to peers, so need to prevent
   * it from being overriden by new epochs in monitor's function create_pending -
   * function restore_pending_map_info is called for this purpose
  */
  std::chrono::system_clock::time_point allow_failovers_ts =
             std::chrono::system_clock::now();
  std::chrono::system_clock::time_point last_gw_down_ts =
             std::chrono::system_clock::now() - std::chrono::seconds(30);
  NvmeGwMonState(): ana_grp_id(REDUNDANT_GW_ANA_GROUP_ID) {}

  NvmeGwMonState(NvmeAnaGrpId id)
    : ana_grp_id(id), availability(gw_availability_t::GW_CREATED),
      last_gw_map_epoch_valid(false), performed_full_startup(false) {}
  void set_unavailable_state() {
    if (availability != gw_availability_t::GW_DELETING) {
      //for not to override Deleting
      availability = gw_availability_t::GW_UNAVAILABLE;
    }
     // after setting this state, the next time monitor sees GW,
     // it expects it performed the full startup
    performed_full_startup = false;
  }
  void standby_state(NvmeAnaGrpId grpid) {
    sm_state[grpid]       = gw_states_per_group_t::GW_STANDBY_STATE;
  }
  void active_state(NvmeAnaGrpId grpid) {
    sm_state[grpid]       = gw_states_per_group_t::GW_ACTIVE_STATE;
    blocklist_data[grpid].osd_epoch = 0;
  }
  void set_last_gw_down_ts(){
    last_gw_down_ts = std::chrono::system_clock::now();
  }
};

struct NqnState {
  std::string nqn;          // subsystem NQN
  ana_state_t ana_state;    // subsystem's ANA state

  // constructors
  NqnState(const std::string& _nqn, const ana_state_t& _ana_state)
    : nqn(_nqn), ana_state(_ana_state)  {}
  NqnState(
    const std::string& _nqn, const SmState& sm_state,
    const NvmeGwMonState & gw_created)
    : nqn(_nqn)  {
    uint32_t i = 0;
    for (auto& state_itr: sm_state) {
      if (state_itr.first > i) {
	uint32_t num_to_add = state_itr.first - i;
        // add fake elements to the ana_state in order to
	// preserve vector index == correct ana_group_id
	for (uint32_t j = 0; j < num_to_add; j++) {
	  std::pair<gw_exported_states_per_group_t, epoch_t> state_pair;
	  state_pair.first =
	    gw_exported_states_per_group_t::GW_EXPORTED_INACCESSIBLE_STATE;
	  state_pair.second = 0;
	  ana_state.push_back(state_pair);
	}
	i += num_to_add;
      }
      std::pair<gw_exported_states_per_group_t, epoch_t> state_pair;
      state_pair.first = (
	(sm_state.at(state_itr.first) ==
	 gw_states_per_group_t::GW_ACTIVE_STATE) ||
	(sm_state.at(state_itr.first) ==
	 gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL))
	? gw_exported_states_per_group_t::GW_EXPORTED_OPTIMIZED_STATE
	: gw_exported_states_per_group_t::GW_EXPORTED_INACCESSIBLE_STATE;
      state_pair.second =
	gw_created.blocklist_data.at(state_itr.first).osd_epoch;
      ana_state.push_back(state_pair);
      i++;
    }
  }
};

typedef std::map<NvmeNqnId, NqnState> GwSubsystems;

struct NvmeGwClientState {
  NvmeAnaGrpId group_id;
  epoch_t gw_map_epoch;
  GwSubsystems subsystems;
  gw_availability_t availability;
  NvmeGwClientState(NvmeAnaGrpId id, epoch_t epoch, gw_availability_t available)
    : group_id(id), gw_map_epoch(epoch), availability(available) {}

  NvmeGwClientState()
    : NvmeGwClientState(
      REDUNDANT_GW_ANA_GROUP_ID, 0, gw_availability_t::GW_UNAVAILABLE) {}
};

struct Tmdata {
  uint32_t timer_started; // statemachine timer(timestamp) set in some state
  uint8_t timer_value;
  std::chrono::system_clock::time_point end_time;
  Tmdata() {
    timer_started = 0;
    timer_value = 0;
  }
};

using TmData = std::map<NvmeAnaGrpId, Tmdata>;

struct NvmeGwTimerState {
  TmData data;
  NvmeGwTimerState() {};
};

using NvmeGwMonClientStates = std::map<NvmeGwId, NvmeGwClientState>;
using NvmeGwTimers = std::map<NvmeGwId, NvmeGwTimerState>;
using NvmeGwMonStates = std::map<NvmeGwId, NvmeGwMonState>;

#endif /* SRC_MON_NVMEOFGWTYPES_H_ */
