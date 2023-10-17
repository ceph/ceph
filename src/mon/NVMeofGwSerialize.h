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

inline std::ostream& operator<<(std::ostream& os, const gw_exported_states_per_group_t value) {
    switch (value) {
        case gw_exported_states_per_group_t::GW_EXPORTED_OPTIMIZED_STATE: os << "OPTIMIZED "; break;
        case gw_exported_states_per_group_t::GW_EXPORTED_INACCESSIBLE_STATE: os << "INACCESSIBLE "; break;
        default: os << "Invalid " << (int)value << " ";
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const gw_states_per_group_t value) {
    switch (value) {
        case gw_states_per_group_t::GW_IDLE_STATE:                  os << "IDLE "; break;
        case gw_states_per_group_t::GW_STANDBY_STATE:               os << "STANDBY "; break;
        case gw_states_per_group_t::GW_ACTIVE_STATE:                os << "ACTIVE "; break;
        case gw_states_per_group_t::GW_OWNER_WAIT_FAILBACK_PREPARED: os << "OWNER_FAILBACK_PREPARED "; break;
        case gw_states_per_group_t::GW_WAIT_FAILBACK_PREPARED:      os << "WAIT_FAILBACK_PREPARED "; break;
        case gw_states_per_group_t::GW_WAIT_BLOCKLIST_CMPL:       os <<   "WAIT_BLOCKLIST_CMPL "; break;
        default: os << "Invalid " << (int)value << " ";
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const gw_availability_t value) {
    switch (value) {

        case gw_availability_t::GW_CREATED: os << "CREATED"; break;
        case gw_availability_t::GW_AVAILABLE: os << "AVAILABLE"; break;
        case gw_availability_t::GW_UNAVAILABLE: os << "UNAVAILABLE"; break;

        default: os << "Invalid " << (int)value << " ";
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const SmState value) {
    os << "SM_STATE [ ";
    for (auto& state_itr: value )
        os << value.at(state_itr.first);
    os << "]";
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const BeaconNamespace value) {
    os << "BeaconNamespace( anagrpid:" << value.anagrpid << ", nonce:" << value.nonce <<" )";
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

inline std::ostream& operator<<(std::ostream& os, const NvmeGwClientState value) {
    os <<  "NvmeGwState { group id: " << value.group_id <<  " gw_map_epoch " <<  value.gw_map_epoch << " availablilty "<< value.availability
        << " GwSubsystems: [ ";
    for (const auto& sub: value.subsystems) os << sub.second << " ";
    os << " ] }";

    return os;
};

inline std::ostream& operator<<(std::ostream& os, const NvmeGroupKey value) {
    os << "NvmeGroupKey {" << value.first << "," << value.second << "}";
    return os;
};

inline std::ostream& operator<<(std::ostream& os, const NvmeGwMonClientStates value) {
    os << "NvmeGwMap ";
    for (auto& gw_state: value) {
        os << "\n" << MODULE_PREFFIX <<" { == gw_id: " << gw_state.first << " -> " <<  gw_state.second << "}";
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
    if(value.size()) os << "\n" << MODULE_PREFFIX;
    for (auto &nonce_map : value) {
        os  << "  ana_grp: " << nonce_map.first  << " [ " << nonce_map.second << "]\n"<< MODULE_PREFFIX ;
    }
    return os;
}

inline std::ostream& print_gw_created_t(std::ostream& os, const NvmeGwMonState value, size_t num_ana_groups) {
    os << "==Internal map ==NvmeGwCreated { ana_group_id " << value.ana_grp_id << " osd_epochs: ";
    for (auto& blklst_itr: value.blocklist_data)
    {
        os << " " << blklst_itr.first <<": " << blklst_itr.second.osd_epoch << ":" <<blklst_itr.second.is_failover ;
    }
    os << "\n" << MODULE_PREFFIX << "nonces: " << value.nonce_map << " }";

    for (auto& state_itr: value.sm_state )
    {
        os << " " << state_itr.first <<": " << state_itr.second << ",";
    }

    os << "]\n"<< MODULE_PREFFIX << "availability " << value.availability << " full-startup " << value.performed_full_startup  << " ]";

    return os;
}

inline std::ostream& operator<<(std::ostream& os, const NvmeGwMonState value) {
    os << "==Internal map ==G W_CREATED_T { ana_group_id " << value.ana_grp_id << " osd_epochs: ";
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
        os  <<  "gw_id: " << gw_created_map.first  << " [ " ;//<< gw_created_map.second << "] \n"<< MODULE_PREFFIX;
        print_gw_created_t(os, gw_created_map.second,  value.size());
        os << "] \n"<< MODULE_PREFFIX;
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const NVMeofGwMap value) {
    os << "NVMeofGwMap [ Created_gws: ";
    for (auto& group_gws: value.created_gws) {
        os <<  "\n" <<  MODULE_PREFFIX  << "{ " << group_gws.first << " } -> { " << group_gws.second << " }";
    }
    os << "]";
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

inline void encode(const GwSubsystems& subsystems,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode((uint32_t)subsystems.size(), bl);
    for (const auto& sub: subsystems) {
        encode(sub.second.nqn, bl);
        encode(sub.second.ana_state, bl);
    }
    ENCODE_FINISH(bl);
}

inline  void decode(GwSubsystems& subsystems,  ceph::bufferlist::const_iterator& bl) {
  uint32_t num_subsystems;
  DECODE_START(1, bl);
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

inline void encode(const NvmeGwClientState& state,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode(state.group_id, bl);
    encode(state.gw_map_epoch, bl);
    encode (state.subsystems, bl);
    encode((uint32_t)state.availability, bl);
    ENCODE_FINISH(bl);
}

inline  void decode(NvmeGwClientState& state,  ceph::bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(state.group_id, bl);
    decode(state.gw_map_epoch, bl);
    decode(state.subsystems, bl);
    uint32_t avail;
    decode(avail, bl);
    state.availability = (gw_availability_t)avail;
    DECODE_FINISH(bl);
}

inline  void encode(const NvmeGwTimerState& state,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode((uint32_t)state.data.size(), bl);
    for (auto &tm_itr:state.data) {
        encode((uint32_t)tm_itr.first, bl);// encode key
        uint32_t tick = tm_itr.second.timer_started;
        uint8_t  val  = tm_itr.second.timer_value;
        encode(tick, bl);
        encode(val,  bl);
        auto endtime  = tm_itr.second.end_time;
        // Convert the time point to milliseconds since the epoch
        uint64_t  millisecondsSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(endtime.time_since_epoch()).count();
        encode(millisecondsSinceEpoch , bl);
    }
    ENCODE_FINISH(bl);
}

inline  void decode(NvmeGwTimerState& state,  ceph::bufferlist::const_iterator& bl) {
    uint32_t size;
    DECODE_START(1, bl);
    decode(size, bl);
    for (uint32_t i = 0; i <size; i ++) {
        uint32_t tm_key;
        uint32_t tick;
        uint8_t val;
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
    }
    DECODE_FINISH(bl);
}

inline void encode(const NvmeAnaNonceMap& nonce_map,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode((uint32_t)nonce_map.size(), bl);
    for (auto& ana_group_nonces : nonce_map) {
        encode(ana_group_nonces.first, bl); // ana group id
        encode ((uint32_t)ana_group_nonces.second.size(), bl); // encode the vector size
        for (auto& nonce: ana_group_nonces.second) encode(nonce, bl);
    }
    ENCODE_FINISH(bl);
}

inline void decode(NvmeAnaNonceMap& nonce_map, ceph::buffer::list::const_iterator &bl) {
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

inline void encode(const NvmeGwMonStates& gws,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode ((uint32_t)gws.size(), bl); // number of gws in the group
    for (auto& gw : gws) {
        encode(gw.first, bl);// GW_id
        encode(gw.second.ana_grp_id, bl); // GW owns this group-id
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
        encode(gw.second.nonce_map, bl);
    }
    ENCODE_FINISH(bl);
}

inline void decode(NvmeGwMonStates& gws, ceph::buffer::list::const_iterator &bl) {
    gws.clear();
    uint32_t num_created_gws;
    DECODE_START(1, bl);
    decode(num_created_gws, bl);

    for (uint32_t i = 0; i<num_created_gws; i++) {
        std::string gw_name;
        decode(gw_name, bl);
        NvmeAnaGrpId ana_grp_id;
        decode(ana_grp_id, bl);

        NvmeGwMonState gw_created(ana_grp_id);
        uint32_t sm_state;
        uint32_t sm_key;
        NvmeGwId peer_name;
        uint32_t size;
        decode(size, bl);
        for (uint32_t i = 0; i <size; i ++) {
            decode(sm_key, bl);
            decode(sm_state, bl);
            gw_created.sm_state[sm_key] = ((gw_states_per_group_t)sm_state);
        }
        uint32_t avail;
        decode(avail, bl);
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
        decode(gw_created.nonce_map, bl);
        gws[gw_name] = gw_created;
    }
    DECODE_FINISH(bl);
}

inline void encode(const std::map<NvmeGroupKey, NvmeGwMonStates>& created_gws,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode ((uint32_t)created_gws.size(), bl); // number of groups
    for (auto& group_gws: created_gws) {
        auto& group_key = group_gws.first;
        encode(group_key.first, bl); // pool
        encode(group_key.second, bl); // group

        auto& gws = group_gws.second;
        encode (gws, bl); // encode group gws
    }
    ENCODE_FINISH(bl);
}

inline void decode(std::map<NvmeGroupKey, NvmeGwMonStates>& created_gws, ceph::buffer::list::const_iterator &bl) {
    created_gws.clear();
    uint32_t ngroups;
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

inline void encode(const NvmeGwMonClientStates& subsyst_gwmap,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode((uint32_t)subsyst_gwmap.size(), bl);
    for (auto& subsyst: subsyst_gwmap) {
        encode(subsyst.first, bl);
        encode(subsyst.second, bl);
    }
    ENCODE_FINISH(bl);
}

inline void decode(NvmeGwMonClientStates& subsyst_gwmap, ceph::buffer::list::const_iterator &bl) {
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
inline void encode(const std::map<NvmeGroupKey, NvmeGwMonClientStates>& gmap,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode ((uint32_t)gmap.size(), bl); // number of groups
    for (auto& group_state: gmap) {
        auto& group_key = group_state.first;
        encode(group_key.first, bl); // pool
        encode(group_key.second, bl); // group
        encode(group_state.second, bl);
    }
    ENCODE_FINISH(bl);
}
// Start decode NvmeGroupKey, NvmeGwMap
inline void decode(std::map<NvmeGroupKey, NvmeGwMonClientStates>& gmap, ceph::buffer::list::const_iterator &bl) {
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

inline void encode(const std::map<NvmeGroupKey, NvmeGwTimers>& gmetadata,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode ((uint32_t)gmetadata.size(), bl); // number of groups
    for (auto& group_md: gmetadata) {
        auto& group_key = group_md.first;
        encode(group_key.first, bl); // pool
        encode(group_key.second, bl); // group

        encode(group_md.second, bl);
    }
    ENCODE_FINISH(bl);
}

inline void decode(std::map<NvmeGroupKey, NvmeGwTimers>& gmetadata, ceph::buffer::list::const_iterator &bl) {
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

inline void encode(const NvmeGwTimers& group_md,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode ((uint32_t)group_md.size(), bl); // number of groups
    for (auto& gw_md: group_md) {
        encode(gw_md.first, bl); // gw
        encode(gw_md.second, bl); //  map of this gw
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
