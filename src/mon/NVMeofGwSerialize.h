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
#ifndef MON_NVMEOFGWSERIALIZE_H_
#define MON_NVMEOFGWSERIALIZE_H_
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define MODULE_PREFFIX "nvmeofgw "
#define dout_prefix *_dout << MODULE_PREFFIX << __PRETTY_FUNCTION__ << " "
#define MAX_SUPPORTED_ANA_GROUPS 16

inline std::ostream& operator<<(
  std::ostream& os, const gw_exported_states_per_group_t value) {
  switch (value) {
  case gw_exported_states_per_group_t::GW_EXPORTED_OPTIMIZED_STATE:
    os << "OPTIMIZED ";
    break;
  case gw_exported_states_per_group_t::GW_EXPORTED_INACCESSIBLE_STATE:
    os << "INACCESSIBLE ";
    break;
  default:
    os << "Invalid " << (int)value << " ";
  }
  return os;
}

inline std::ostream& operator<<(
  std::ostream& os, const gw_states_per_group_t value) {
  switch (value) {
  case gw_states_per_group_t::GW_IDLE_STATE:
    os << "IDLE ";
    break;
  case gw_states_per_group_t::GW_STANDBY_STATE:
    os << "STANDBY ";
    break;
  case gw_states_per_group_t::GW_ACTIVE_STATE:
    os << "ACTIVE ";
    break;
  case gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED:
    os << "OWNER_FAILBACK_PREPARED ";
    break;
  case gw_states_per_group_t::GW_WAIT_FAILBACK_PREPARED:
    os << "WAIT_FAILBACK_PREPARED ";
    break;
  case gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL:
    os <<   "WAIT_BLOCKLIST_CMPL ";
    break;
  default:
    os << "Invalid " << (int)value << " ";
  }
  return os;
}

inline std::ostream& operator<<(
  std::ostream& os, const gw_availability_t value) {
  switch (value) {

  case gw_availability_t::GW_CREATED:
    os << "CREATED";
    break;
  case gw_availability_t::GW_AVAILABLE:
    os << "AVAILABLE";
    break;
  case gw_availability_t::GW_UNAVAILABLE:
    os << "UNAVAILABLE";
    break;
  case gw_availability_t::GW_DELETING:
    os << "DELETING"; break;

  default:
    os << "Invalid " << (int)value << " ";
  }
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const SmState value) {
  os << "SM_STATE [ ";
  for (auto& state_itr: value ) {
    os << value.at(state_itr.first);
  }
  os << "]";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const BeaconNamespace value) {
  os << "BeaconNamespace( anagrpid:" << value.anagrpid
     << ", nonce:" << value.nonce <<" )";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const BeaconListener value) {
  os << "BeaconListener( addrfam:" << value.address_family
     << ", addr:" << value.address
     << ", svcid:" << value.svcid << " )";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const BeaconSubsystem value) {
  os << "BeaconSubsystem( nqn:" << value.nqn << ", listeners [ ";
  for (const auto& list: value.listeners) os << list << " ";
  os << "] namespaces [ ";
  for (const auto& ns: value.namespaces) os << ns << " ";
  os << "] )";
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const NqnState value) {
  os << "NqnState( nqn: " << value.nqn << ", " << value.ana_state << " )";
  return os;
}

inline std::ostream& operator<<(
  std::ostream& os, const NvmeGwClientState value) {
  os <<  "NvmeGwState { group id: " << value.group_id
     << " gw_map_epoch " <<  value.gw_map_epoch
     << " availablilty "<< value.availability
     << " GwSubsystems: [ ";
  for (const auto& sub: value.subsystems) {
    os << sub.second << " ";
  }
  os << " ] }";

  return os;
};

inline std::ostream& operator<<(std::ostream& os, const NvmeGroupKey value) {
  os << "NvmeGroupKey {" << value.first << "," << value.second << "}";
  return os;
};

inline std::ostream& operator<<(
  std::ostream& os, const NvmeGwMonClientStates value) {
  os << "NvmeGwMap ";
  for (auto& gw_state: value) {
    os << "\n" << MODULE_PREFFIX <<" { == gw_id: " << gw_state.first
       << " -> " <<  gw_state.second << "}";
  }
  os << "}";

  return os;
};

inline std::ostream& operator<<(std::ostream& os, const NvmeNonceVector value) {
  for (auto & nonces : value) {
    os <<  nonces << " ";
  }
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const NvmeAnaNonceMap value) {
  if (value.size()) {
    os << "\n" << MODULE_PREFFIX;
  }
  for (auto &nonce_map : value) {
    os  << "  ana_grp: " << nonce_map.first
	<< " [ " << nonce_map.second << "]\n"<< MODULE_PREFFIX;
  }
  return os;
}

inline std::ostream& print_gw_created_t(
  std::ostream& os, const NvmeGwMonState value, size_t num_ana_groups) {
  os << "==Internal map ==NvmeGwCreated { ana_group_id "
     << value.ana_grp_id << " osd_epochs: ";
  for (auto& blklst_itr: value.blocklist_data) {
    os << " " << blklst_itr.first <<": " << blklst_itr.second.osd_epoch
       << ":" <<blklst_itr.second.is_failover ;
  }
  os << "\n" << MODULE_PREFFIX << "nonces: " << value.nonce_map << " }";

  for (auto& state_itr: value.sm_state )
  {
    os << " " << state_itr.first <<": " << state_itr.second << ",";
  }
  os << "]\n"<< MODULE_PREFFIX << " entity-addr : " << value.addr_vect
     << " availability " << value.availability
     << " full-startup " << value.performed_full_startup  << " ]";

  return os;
}

inline std::ostream& operator<<(std::ostream& os, const NvmeGwMonState value) {
  os << "==Internal map ==G W_CREATED_T { ana_group_id " << value.ana_grp_id
     << " osd_epochs: ";
  for (auto &blklst_itr: value.blocklist_data) {
    os << " " << blklst_itr.second.osd_epoch;
  }
  os << "\n" << MODULE_PREFFIX << "nonces: " << value.nonce_map << " }";

  for (auto& state_itr: value.sm_state ) {
    os << value.sm_state.at(state_itr.first) << ",";
  }

  os <<  "]\n"<< MODULE_PREFFIX << " beacon-subsystems ";
  for (const auto& sub: value.subsystems) {
    os << sub << ",";
  }

  os << "]\n"<< MODULE_PREFFIX << "availability " << value.availability << "]";

  return os;
}

inline std::ostream& operator<<(std::ostream& os, const NvmeGwMonStates value) {
  if(value.size()) os << "\n" << MODULE_PREFFIX;;

  for (auto &gw_created_map : value) {
    os  <<  "gw_id: " << gw_created_map.first  << " [ " ;
      //<< gw_created_map.second << "] \n"<< MODULE_PREFFIX;
    print_gw_created_t(os, gw_created_map.second,  value.size());
    os << "] \n"<< MODULE_PREFFIX;
  }
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const NVMeofGwMap value) {
  os <<  "\n" <<  MODULE_PREFFIX << "== NVMeofGwMap [ Created_gws: epoch "
     << value.epoch;
  for (auto& group_gws: value.gw_epoch) {
    os <<  "\n" <<  MODULE_PREFFIX  << "{ " << group_gws.first
       << " } -> GW epoch: " << group_gws.second << " }";
  }
  for (auto& group_gws: value.created_gws) {
   os <<  "\n" <<  MODULE_PREFFIX  << "{ " << group_gws.first
      << " } -> { " << group_gws.second << " }";
  }
  return os;
}

inline void encode(const ana_state_t& st,  ceph::bufferlist &bl) {
  ENCODE_START(1, 1, bl);
  encode((uint32_t)st.size(), bl);
  for (const auto& gr: st) {
    encode((uint32_t)gr.first, bl);
    encode((uint32_t)gr.second, bl);
  }
  ENCODE_FINISH(bl);
}

inline void decode(ana_state_t& st, ceph::buffer::list::const_iterator &bl) {
  uint32_t n;
  DECODE_START(1, bl);
  decode(n, bl);
  st.resize(n);
  for (uint32_t i = 0; i < n; i++) {
    uint32_t a, b;
    decode(a, bl);
    decode(b, bl);
    st[i].first  = (gw_exported_states_per_group_t)a;
    st[i].second = (epoch_t)b;
  }
  DECODE_FINISH(bl);
}

inline void encode(
  const GwSubsystems& subsystems,  ceph::bufferlist &bl, uint64_t features) {
  uint8_t version = 1;
  if (HAVE_FEATURE(features, NVMEOFHA)) {
    version = 2;
  }
  ENCODE_START(version, version, bl);
  encode((uint32_t)subsystems.size(), bl);
  for (const auto& sub: subsystems) {
    encode(sub.second.nqn, bl);
    if (version == 1) {
      dout(20) << "encode ana_state vector version1 = " << version << dendl;
      /* Version 1 requires exactly 16 entries */
      ana_state_t filled(sub.second.ana_state);
      filled.resize(
	MAX_SUPPORTED_ANA_GROUPS,
	std::make_pair(
	  gw_exported_states_per_group_t::GW_EXPORTED_INACCESSIBLE_STATE,
	  0));
      encode(filled, bl);
    } else {
      dout(20) << "encode ana_state vector version2 = " << version << dendl;
      encode(sub.second.ana_state, bl);
    }
  }
  ENCODE_FINISH(bl);
}

inline  void decode(
  GwSubsystems& subsystems, ceph::bufferlist::const_iterator& bl) {
  uint32_t num_subsystems;
  DECODE_START(2, bl);
  decode(num_subsystems, bl);
  subsystems.clear();
  for (uint32_t i=0; i<num_subsystems; i++) {
    std::string  nqn;
    decode(nqn, bl);
    ana_state_t st;
    decode(st, bl);
    subsystems.insert({nqn, NqnState(nqn, st)});
  }
  DECODE_FINISH(bl);
}

inline void encode(const NvmeGwClientState& state,  ceph::bufferlist &bl, uint64_t features) {
  ENCODE_START(1, 1, bl);
  encode(state.group_id, bl);
  encode(state.gw_map_epoch, bl);
  encode (state.subsystems, bl, features);
  encode((uint32_t)state.availability, bl);
  ENCODE_FINISH(bl);
}

inline  void decode(
  NvmeGwClientState& state,  ceph::bufferlist::const_iterator& bl) {
  DECODE_START(1, bl);
  decode(state.group_id, bl);
  decode(state.gw_map_epoch, bl);
  decode(state.subsystems, bl);
  uint32_t avail;
  decode(avail, bl);
  state.availability = (gw_availability_t)avail;
  DECODE_FINISH(bl);
}

inline  void encode(const NvmeGwTimerState& state,  ceph::bufferlist &bl,
  uint64_t features) {
  uint8_t version = 1;
  if (HAVE_FEATURE(features, NVMEOFHA)) {
    version = 2;
  }
  ENCODE_START(version, version, bl);

  if (version >= 2) {
    encode((uint32_t)state.data.size(), bl);
    for (auto &tm_itr:state.data) {
      encode((uint32_t)tm_itr.first, bl);// encode key
      uint32_t tick = tm_itr.second.timer_started;
      uint8_t  val  = tm_itr.second.timer_value;
      encode(tick, bl);
      encode(val,  bl);
      auto endtime  = tm_itr.second.end_time;
      // Convert the time point to milliseconds since the epoch
      uint64_t  millisecondsSinceEpoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
      endtime.time_since_epoch()).count();
      encode(millisecondsSinceEpoch , bl);
    }
  } else {
    encode((uint32_t)MAX_SUPPORTED_ANA_GROUPS, bl);
    Tmdata empty;
    for (uint32_t i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
      auto tmiter = state.data.find(i);
      const Tmdata *to_encode = &empty;
      if (tmiter != state.data.end()) {
	to_encode = &(tmiter->second);
      }
      encode(to_encode->timer_started, bl);
      encode(to_encode->timer_value,  bl);
      auto endtime  = to_encode->end_time;
      // Convert the time point to milliseconds since the epoch
      uint64_t  millisecondsSinceEpoch =
         std::chrono::duration_cast<std::chrono::milliseconds>(
      endtime.time_since_epoch()).count();
      encode(millisecondsSinceEpoch , bl);
    }
  }
  ENCODE_FINISH(bl);
}

inline  void decode(
  NvmeGwTimerState& state,  ceph::bufferlist::const_iterator& bl) {
  DECODE_START(2, bl);
  dout(20) << "decode NvmeGwTimers version = " << struct_v << dendl;
  uint32_t size;
  decode(size, bl);
  for (uint32_t i = 0; i <size; i ++) {
    uint32_t tm_key;
    uint32_t tick;
    uint8_t val;
    if (struct_v >= 2) {
      decode(tm_key, bl);
      decode(tick, bl);
      decode(val,  bl);
      Tmdata tm;
      tm.timer_started = tick;
      tm.timer_value = val;
      uint64_t milliseconds;
      decode(milliseconds, bl);
      auto duration = std::chrono::milliseconds(milliseconds);
      tm.end_time = std::chrono::time_point<std::chrono::system_clock>(duration);
      state.data[tm_key] = tm;
    } else {
      decode(tick, bl);
      decode(val,  bl);
      Tmdata tm;
      tm.timer_started = tick;
      tm.timer_value = val;
      uint64_t milliseconds;
      decode(milliseconds, bl);
      if (tm.timer_started) {
        // relevant only entries with started timers in the state
        auto duration = std::chrono::milliseconds(milliseconds);
        tm.end_time = std::chrono::time_point<std::chrono::system_clock>(duration);
        state.data[i] = tm;
      }
    }
  }
  DECODE_FINISH(bl);
}

inline void encode(const NvmeAnaNonceMap& nonce_map,  ceph::bufferlist &bl,
  uint64_t features) {
  ENCODE_START(1, 1, bl);
  encode((uint32_t)nonce_map.size(), bl);
  for (auto& ana_group_nonces : nonce_map) {
    // ana group id
    encode(ana_group_nonces.first, bl);
    // encode the vector size
    encode ((uint32_t)ana_group_nonces.second.size(), bl);
    for (auto& nonce: ana_group_nonces.second) encode(nonce, bl);
  }
  ENCODE_FINISH(bl);
}

inline void decode(
  NvmeAnaNonceMap& nonce_map, ceph::buffer::list::const_iterator &bl) {
  dout(20) << "decode nonce map  " << dendl;
  uint32_t map_size;
  NvmeAnaGrpId ana_grp_id;
  uint32_t vector_size;
  std::string nonce;
  DECODE_START(1, bl);
  decode(map_size, bl);
  for (uint32_t i = 0; i<map_size; i++) {
    decode(ana_grp_id, bl);
    decode(vector_size,bl);
    for (uint32_t j = 0; j < vector_size; j++) {
      decode (nonce, bl);
      nonce_map[ana_grp_id].push_back(nonce);
    }
  }
  DECODE_FINISH(bl);
}

inline void encode(const NvmeGwMonStates& gws,  ceph::bufferlist &bl,
  uint64_t features) {
  uint8_t version = 1;
  if (HAVE_FEATURE(features, NVMEOFHA)) {
    version = 2;
  }
  if (HAVE_FEATURE(features, NVMEOFHAMAP)) {
    version = 3;
  }
  ENCODE_START(version, version, bl);
  encode ((uint32_t)gws.size(), bl); // number of gws in the group
  for (auto& gw : gws) {
    encode(gw.first, bl);// GW_id
    encode(gw.second.ana_grp_id, bl); // GW owns this group-id
    if (version >= 2) {
      encode((uint32_t)gw.second.sm_state.size(), bl);
      for (auto &state_it:gw.second.sm_state) {
        encode((uint32_t)state_it.first, bl); //key of map
        encode((uint32_t)state_it.second, bl);//value of map
      }
      encode((uint32_t)gw.second.availability, bl);
      encode((uint16_t)gw.second.performed_full_startup, bl);
      encode((uint16_t)gw.second.last_gw_map_epoch_valid, bl);
      encode(gw.second.subsystems, bl);

      encode((uint32_t)gw.second.blocklist_data.size(), bl);
      for (auto &blklst_itr: gw.second.blocklist_data) {
        encode((uint32_t)blklst_itr.first, bl);
        encode((uint32_t)blklst_itr.second.osd_epoch, bl);
        encode((uint32_t)blklst_itr.second.is_failover, bl);
      }
    } else {
      gw_states_per_group_t states[MAX_SUPPORTED_ANA_GROUPS];
      for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) states[i] = gw_states_per_group_t::GW_IDLE_STATE;
      for (auto &state_it:gw.second.sm_state) states[state_it.first] = state_it.second;
      for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) encode((uint32_t)states[i], bl);

      encode((uint32_t)gw.second.availability, bl);
      encode((uint16_t)gw.second.performed_full_startup, bl);
      encode((uint16_t)gw.second.last_gw_map_epoch_valid, bl);
      encode(gw.second.subsystems, bl); // TODO reuse but put features - encode version
      Blocklist_data bl_data[MAX_SUPPORTED_ANA_GROUPS];
      for (auto &blklst_itr: gw.second.blocklist_data) {
        bl_data[blklst_itr.first].osd_epoch   = blklst_itr.second.osd_epoch;
        bl_data[blklst_itr.first].is_failover = blklst_itr.second.is_failover;
      }
      for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
        encode((uint32_t)bl_data[i].osd_epoch, bl);
        encode((bool)bl_data[i].is_failover, bl);
      }
    }
    encode(gw.second.nonce_map, bl, features);
    if (version >= 3) {
      dout(20) << "encode addr_vect and beacon_index" << dendl;
      gw.second.addr_vect.encode(bl, features);
      encode(gw.second.beacon_index, bl);
    }
  }
  ENCODE_FINISH(bl);
}

inline void decode(
  NvmeGwMonStates& gws, ceph::buffer::list::const_iterator &bl) {
  gws.clear();
  uint32_t num_created_gws;
  DECODE_START(3, bl);
  dout(20) << "decode NvmeGwMonStates. struct_v: " << struct_v << dendl;
  decode(num_created_gws, bl);
  dout(20) << "decode NvmeGwMonStates. num gws  " << num_created_gws << dendl;
  std::set<uint32_t> created_anagrps;
  for (uint32_t i = 0; i<num_created_gws; i++) {
    std::string gw_name;
    decode(gw_name, bl);
    NvmeAnaGrpId ana_grp_id;
    decode(ana_grp_id, bl);
    dout(20) << "decode NvmeGwMonStates. GW-id " << gw_name << " ana grpid "<< ana_grp_id <<  dendl;
    NvmeGwMonState gw_created(ana_grp_id);
    uint32_t sm_state;
    uint32_t sm_key;
    uint32_t size;
    if (struct_v >= 2) {
      decode(size, bl);
      for (uint32_t i = 0; i <size; i ++) {
        decode(sm_key, bl);
        decode(sm_state, bl);
        gw_created.sm_state[sm_key] = ((gw_states_per_group_t)sm_state);
      }
    } else {
      created_anagrps.insert(ana_grp_id);
      for (uint32_t i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++) {
        decode(sm_state, bl);
        dout(20) << "decode NvmeGwMonStates state: "
                 << i << " " << sm_state << dendl;
        gw_created.sm_state[i] = ((gw_states_per_group_t)sm_state);
        //here create all 16 states but need to erase not relevant states after loop on created GW
      }
    }
    // common code
    uint32_t avail;
    decode(avail, bl);
    dout(20) << "decode NvmeGwMonStates avail : " << avail << dendl;
    gw_created.availability = (gw_availability_t)avail;
    uint16_t performed_startup;
    decode(performed_startup, bl);
    gw_created.performed_full_startup = (bool)performed_startup;
    uint16_t last_epoch_valid;
    decode(last_epoch_valid, bl);
    gw_created.last_gw_map_epoch_valid = (bool)last_epoch_valid;
    BeaconSubsystems   subsystems;
    decode(subsystems, bl);
    gw_created.subsystems = subsystems;

    if (struct_v >= 2) {
      decode(size, bl);
      for (uint32_t i=0; i<size; i++) {
        uint32_t blklist_key;
        uint32_t osd_epoch;
        uint32_t is_failover;
        decode(blklist_key, bl);
        decode(osd_epoch,   bl);
        decode(is_failover, bl);
        Blocklist_data blst((epoch_t)osd_epoch, (bool)is_failover);
        gw_created.blocklist_data[blklist_key] = blst;
      }
    } else {
      for (uint32_t i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++) {
        uint32_t osd_epoch;
        bool is_failover;
        decode(osd_epoch,   bl);
        dout(20) << "decode osd epoch  " << osd_epoch << dendl;
        decode(is_failover, bl);
        dout(20) << "decode is-failover  " << is_failover << dendl;
        Blocklist_data blst((epoch_t)osd_epoch, (bool)is_failover);
        // the same action as with "states"
        gw_created.blocklist_data[i] = blst;
      }
    }
    decode(gw_created.nonce_map, bl);
    if (struct_v >= 3) {
      dout(20) << "decode addr_vect and beacon_index" << dendl;
      gw_created.addr_vect.decode(bl);
      decode(gw_created.beacon_index, bl);
    }

    gws[gw_name] = gw_created;
  }
  if (struct_v == 1) {  //Fix allocations of states and blocklist_data
    //since only after full loop on gws we know what states are relevant
    for (auto &gw_it:gws) {
      auto &state = gw_it.second;
      for (uint32_t i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++) {
        if (created_anagrps.count(i) == 0) {
          state.sm_state.erase(i);
          state.blocklist_data.erase(i);
        }
      }
    }
  }
  DECODE_FINISH(bl);
}

inline void encode(const std::map<NvmeGroupKey, epoch_t>& gw_epoch,
                   ceph::bufferlist &bl) {
  ENCODE_START(1, 1, bl);
  encode ((uint32_t)gw_epoch.size(), bl); // number of groups
  for (auto& group_epoch: gw_epoch) {
    auto& group_key = group_epoch.first;
    encode(group_key.first, bl); // pool
    encode(group_key.second, bl); // group
    encode(group_epoch.second, bl);
  }
  ENCODE_FINISH(bl);
}

inline void decode(std::map<NvmeGroupKey, epoch_t>& gw_epoch,
                   ceph::buffer::list::const_iterator &bl) {
  gw_epoch.clear();
  uint32_t ngroups;
  DECODE_START(1, bl);
  decode(ngroups, bl);
  for(uint32_t i = 0; i<ngroups; i++){
    std::string pool, group;
    decode(pool, bl);
    decode(group, bl);
    epoch_t gepoch;
    decode(gepoch, bl);
    gw_epoch[std::make_pair(pool, group)] = gepoch;
}
  DECODE_FINISH(bl);
}

inline void encode(
  const std::map<NvmeGroupKey, NvmeGwMonStates>& created_gws,
  ceph::bufferlist &bl, uint64_t features) {
  ENCODE_START(1, 1, bl);
  encode ((uint32_t)created_gws.size(), bl); // number of groups
  for (auto& group_gws: created_gws) {
    auto& group_key = group_gws.first;
    encode(group_key.first, bl); // pool
    encode(group_key.second, bl); // group

    auto& gws = group_gws.second;
    encode(gws, bl, features); // encode group gws
  }
  ENCODE_FINISH(bl);
}

inline void decode(
  std::map<NvmeGroupKey, NvmeGwMonStates>& created_gws,
  ceph::buffer::list::const_iterator &bl) {
  created_gws.clear();
  uint32_t ngroups = 0;
  DECODE_START(1, bl);
  decode(ngroups, bl);
  for (uint32_t i = 0; i<ngroups; i++) {
    std::string pool, group;
    decode(pool, bl);
    decode(group, bl);
    NvmeGwMonStates cmap;
    decode(cmap, bl);
    created_gws[std::make_pair(pool, group)] = cmap;
  }
  DECODE_FINISH(bl);
}

inline void encode(
  const NvmeGwMonClientStates& subsyst_gwmap, ceph::bufferlist &bl, uint64_t features) {
  ENCODE_START(1, 1, bl);
  encode((uint32_t)subsyst_gwmap.size(), bl);
  for (auto& subsyst: subsyst_gwmap) {
    encode(subsyst.first, bl);
    encode(subsyst.second, bl, features);
  }
  ENCODE_FINISH(bl);
}

inline void decode(
  NvmeGwMonClientStates& subsyst_gwmap, ceph::buffer::list::const_iterator &bl) {
  subsyst_gwmap.clear();
  uint32_t num_gws;
  DECODE_START(1, bl);
  decode(num_gws, bl);

  for (uint32_t i = 0; i < num_gws; i++) {
    NvmeGwId gw_id;
    decode(gw_id, bl);
    NvmeGwClientState gw_st;
    decode(gw_st, bl);
    subsyst_gwmap[gw_id] = gw_st;
  }
  DECODE_FINISH(bl);
}

// Start encode  NvmeGroupKey, GMAP
inline void encode(
  const std::map<NvmeGroupKey, NvmeGwMonClientStates>& gmap,
  ceph::bufferlist &bl,
  uint64_t features) {
  ENCODE_START(1, 1, bl);
  encode ((uint32_t)gmap.size(), bl); // number of groups
  for (auto& group_state: gmap) {
    auto& group_key = group_state.first;
    encode(group_key.first, bl); // pool
    encode(group_key.second, bl); // group
    encode(group_state.second, bl, features);
  }
  ENCODE_FINISH(bl);
}

// Start decode NvmeGroupKey, NvmeGwMap
inline void decode(
  std::map<NvmeGroupKey, NvmeGwMonClientStates>& gmap,
  ceph::buffer::list::const_iterator &bl) {
  gmap.clear();
  uint32_t ngroups;
  DECODE_START(1, bl);
  decode(ngroups, bl);
  for (uint32_t i = 0; i<ngroups; i++) {
    std::string pool, group;
    decode(pool, bl);
    decode(group, bl);
    NvmeGwMonClientStates grp_map;
    decode(grp_map, bl);
    gmap[std::make_pair(pool, group)] = grp_map;
  }
  DECODE_FINISH(bl);
}

inline void encode(
  const std::map<NvmeGroupKey, NvmeGwTimers>& gmetadata,
  ceph::bufferlist &bl,  uint64_t features) {
  ENCODE_START(1, 1, bl);
  encode ((uint32_t)gmetadata.size(), bl); // number of groups
  for (auto& group_md: gmetadata) {
    auto& group_key = group_md.first;
    encode(group_key.first, bl); // pool
    encode(group_key.second, bl); // group

    encode(group_md.second, bl, features);
  }
  ENCODE_FINISH(bl);
}

inline void decode(
  std::map<NvmeGroupKey, NvmeGwTimers>& gmetadata,
  ceph::buffer::list::const_iterator &bl) {
  gmetadata.clear();
  uint32_t ngroups;
  DECODE_START(1, bl);
  decode(ngroups, bl);
  for (uint32_t i = 0; i<ngroups; i++) {
    std::string pool, group;
    decode(pool, bl);
    decode(group, bl);
    NvmeGwTimers gmd;
    decode(gmd, bl);
    gmetadata[std::make_pair(pool, group)] = gmd;
  }
  DECODE_FINISH(bl);
}

inline void encode(const NvmeGwTimers& group_md,  ceph::bufferlist &bl,
  uint64_t features) {
  ENCODE_START(1, 1, bl);
  encode ((uint32_t)group_md.size(), bl); // number of groups
  for (auto& gw_md: group_md) {
    encode(gw_md.first, bl); // gw
    encode(gw_md.second, bl, features); //  map of this gw
  }
  ENCODE_FINISH(bl);
}

inline void decode(NvmeGwTimers& md, ceph::buffer::list::const_iterator &bl) {
  uint32_t num_gws;
  DECODE_START(1, bl);
  decode(num_gws, bl);
  for (uint32_t i = 0; i < num_gws; i++) {
    std::string gw_id;
    decode(gw_id, bl);
    NvmeGwTimerState gw_meta;
    decode(gw_meta, bl);
    md[gw_id] = gw_meta;
  }
  DECODE_FINISH(bl);
}

inline void encode(const BeaconNamespace& ns,  ceph::bufferlist &bl) {
  ENCODE_START(1, 1, bl);
  encode(ns.anagrpid, bl);
  encode(ns.nonce, bl);
  ENCODE_FINISH(bl);
}

inline void decode(BeaconNamespace& ns, ceph::buffer::list::const_iterator &bl) {
  DECODE_START(1, bl);
  decode(ns.anagrpid, bl);
  decode(ns.nonce, bl);
  DECODE_FINISH(bl);
}

inline void encode(const BeaconListener& ls,  ceph::bufferlist &bl) {
  ENCODE_START(1, 1, bl);
  encode(ls.address_family, bl);
  encode(ls.address, bl);
  encode(ls.svcid, bl);
  ENCODE_FINISH(bl);
}

inline void decode(BeaconListener& ls, ceph::buffer::list::const_iterator &bl) {
  DECODE_START(1, bl);
  decode(ls.address_family, bl);
  decode(ls.address, bl);
  decode(ls.svcid, bl);
  DECODE_FINISH(bl);
}

inline void encode(const BeaconSubsystem& sub,  ceph::bufferlist &bl) {
  ENCODE_START(1, 1, bl);
  encode(sub.nqn, bl);
  encode((uint32_t)sub.listeners.size(), bl);
  for (const auto& ls: sub.listeners)
    encode(ls, bl);
  encode((uint32_t)sub.namespaces.size(), bl);
  for (const auto& ns: sub.namespaces)
    encode(ns, bl);
  ENCODE_FINISH(bl);
}

inline void decode(BeaconSubsystem& sub, ceph::buffer::list::const_iterator &bl) {
  DECODE_START(1, bl);
  dout(20) << "decode BeaconSubsystems " << dendl;
  decode(sub.nqn, bl);
  uint32_t s;
  sub.listeners.clear();
  decode(s, bl);
  for (uint32_t i = 0; i < s; i++) {
    BeaconListener ls;
    decode(ls, bl);
    sub.listeners.push_back(ls);
  }

  sub.namespaces.clear();
  decode(s, bl);
  for (uint32_t i = 0; i < s; i++) {
    BeaconNamespace ns;
    decode(ns, bl);
    sub.namespaces.push_back(ns);
  }
  DECODE_FINISH(bl);
}


#undef dout_subsys
#endif /* SRC_MON_NVMEOFGWSERIALIZEP_H_ */
