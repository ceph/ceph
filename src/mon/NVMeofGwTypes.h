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

using NvmeGwId      = std::string;
using NvmeGroupKey  = std::pair<std::string, std::string>;
using NvmeNqnId     = std::string;
using NvmeAnaGrpId  = uint32_t;


enum class GW_STATES_PER_AGROUP_E {
    GW_IDLE_STATE = 0, //invalid state
    GW_STANDBY_STATE,
    GW_ACTIVE_STATE,
    GW_OWNER_WAIT_FAILBACK_PREPARED,
    GW_WAIT_FAILBACK_PREPARED,
    GW_WAIT_BLOCKLIST_CMPL
};

enum class GW_EXPORTED_STATES_PER_AGROUP_E {
    GW_EXPORTED_OPTIMIZED_STATE = 0,
    GW_EXPORTED_INACCESSIBLE_STATE
};

enum class GW_AVAILABILITY_E {
    GW_CREATED = 0,
    GW_AVAILABLE,
    GW_UNAVAILABLE,
    GW_DELETED
};

#define MAX_SUPPORTED_ANA_GROUPS 16
#define REDUNDANT_GW_ANA_GROUP_ID 0xFF

typedef GW_STATES_PER_AGROUP_E          SM_STATE         [MAX_SUPPORTED_ANA_GROUPS];

using ANA_STATE = std::vector<std::pair<GW_EXPORTED_STATES_PER_AGROUP_E, epoch_t>>;

struct BeaconNamespace {
    NvmeAnaGrpId anagrpid;
    std::string  nonce;

    // Define the equality operator
    bool operator==(const BeaconNamespace& other) const {
        return anagrpid == other.anagrpid &&
               nonce == other.nonce;
    }
};

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

using NvmeNonceVector    = std::vector<std::string>;
using NvmeAnaNonceMap  = std::map <NvmeAnaGrpId, NvmeNonceVector>;

struct NvmeGwCreated {
    NvmeAnaGrpId       ana_grp_id;                    // ana-group-id allocated for this GW, GW owns this group-id
    GW_AVAILABILITY_E  availability;                  // in absence of  beacon  heartbeat messages it becomes inavailable
    bool               last_gw_map_epoch_valid;       // "true" if the last epoch seen by the gw-client is up-to-date
    BeaconSubsystems   subsystems;                    // gateway susbsystem and their state machine states
    NvmeAnaNonceMap    nonce_map;
    SM_STATE           sm_state;                      // state machine states per ANA group
    struct{
       epoch_t     osd_epoch;
       bool        is_failover;
    }blocklist_data[MAX_SUPPORTED_ANA_GROUPS];

    NvmeGwCreated(): ana_grp_id(REDUNDANT_GW_ANA_GROUP_ID) {};

    NvmeGwCreated(NvmeAnaGrpId id): ana_grp_id(id), availability(GW_AVAILABILITY_E::GW_CREATED), last_gw_map_epoch_valid(false)
    {
        for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++){
            sm_state[i] = GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE;
            blocklist_data[i].osd_epoch = 0;
            blocklist_data[i].is_failover = true;
        }
    };

    void standby_state(NvmeAnaGrpId grpid) {
           sm_state[grpid]       = GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE;
    };
    void active_state(NvmeAnaGrpId grpid) {
           sm_state[grpid]       = GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE;
           blocklist_data[grpid].osd_epoch = 0;
    };
};

struct NqnState {
    std::string   nqn;          // subsystem NQN
    ANA_STATE     ana_state;    // subsystem's ANA state

    // constructors
    NqnState(const std::string& _nqn, const ANA_STATE& _ana_state):
        nqn(_nqn), ana_state(_ana_state)  {}
    NqnState(const std::string& _nqn, const SM_STATE& sm_state, const NvmeGwCreated & gw_created) : nqn(_nqn)  {
        for (int i=0; i < MAX_SUPPORTED_ANA_GROUPS; i++){
            std::pair<GW_EXPORTED_STATES_PER_AGROUP_E, epoch_t> state_pair;
            state_pair.first = (  sm_state[i] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE
			       || sm_state[i] == GW_STATES_PER_AGROUP_E::GW_WAIT_BLOCKLIST_CMPL)
                           ? GW_EXPORTED_STATES_PER_AGROUP_E::GW_EXPORTED_OPTIMIZED_STATE
                           : GW_EXPORTED_STATES_PER_AGROUP_E::GW_EXPORTED_INACCESSIBLE_STATE;
            state_pair.second = gw_created.blocklist_data[i].osd_epoch;
            ana_state.push_back(state_pair);
        }
    }
};

typedef std::map<NvmeNqnId, NqnState> GwSubsystems;

struct NvmeGwState {
    NvmeAnaGrpId              group_id;
    epoch_t                   gw_map_epoch;
    GwSubsystems              subsystems;
    GW_AVAILABILITY_E         availability;
    NvmeGwState(NvmeAnaGrpId id, epoch_t epoch, GW_AVAILABILITY_E available):
        group_id(id),
        gw_map_epoch(epoch),
        availability(available)
    {};

    NvmeGwState() : NvmeGwState(REDUNDANT_GW_ANA_GROUP_ID, 0, GW_AVAILABILITY_E::GW_UNAVAILABLE) {};
};

struct NvmeGwMetaData {
   struct{
      uint32_t     timer_started; // statemachine timer(timestamp) set in some state
      uint8_t      timer_value;
      std::chrono::system_clock::time_point end_time;
   } data[MAX_SUPPORTED_ANA_GROUPS];

    NvmeGwMetaData() {
        for (int i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++){
            data[i].timer_started = 0;
            data[i].timer_value = 0;
        }
    };
};

using NvmeGwMap             = std::map<NvmeGwId, NvmeGwState>;
using NvmeGwMetaDataMap     = std::map<NvmeGwId, NvmeGwMetaData>;
using NvmeGwCreatedMap      = std::map<NvmeGwId, NvmeGwCreated>;

#endif /* SRC_MON_NVMEOFGWTYPES_H_ */
