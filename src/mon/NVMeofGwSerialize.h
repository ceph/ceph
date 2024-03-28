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

inline std::ostream& operator<<(std::ostream& os, const GW_EXPORTED_STATES_PER_AGROUP_E value) {
    switch (value) {
        case GW_EXPORTED_STATES_PER_AGROUP_E::GW_EXPORTED_OPTIMIZED_STATE: os << "OPTIMIZED "; break;
        case GW_EXPORTED_STATES_PER_AGROUP_E::GW_EXPORTED_INACCESSIBLE_STATE: os << "INACCESSIBLE "; break;
        default: os << "Invalid " << (int)value << " ";
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const GW_STATES_PER_AGROUP_E value) {
    switch (value) {
        case GW_STATES_PER_AGROUP_E::GW_IDLE_STATE:                  os << "IDLE "; break;
        case GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE:               os << "STANDBY "; break;
        case GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE:                os << "ACTIVE "; break;
        case GW_STATES_PER_AGROUP_E::GW_OWNER_WAIT_FAILBACK_PREPARED: os << "OWNER_FAILBACK_PREPARED "; break;
        case GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED:      os << "WAIT_FAILBACK_PREPARED "; break;
        case GW_STATES_PER_AGROUP_E::GW_WAIT_BLOCKLIST_CMPL:       os <<   "WAIT_BLOCKLIST_CMPL "; break;
        default: os << "Invalid " << (int)value << " ";
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const GW_AVAILABILITY_E value) {
    switch (value) {

        case GW_AVAILABILITY_E::GW_CREATED: os << "CREATED"; break;
        case GW_AVAILABILITY_E::GW_AVAILABLE: os << "AVAILABLE"; break;
        case GW_AVAILABILITY_E::GW_UNAVAILABLE: os << "UNAVAILABLE"; break;

        default: os << "Invalid " << (int)value << " ";
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const SM_STATE value) {
    os << "SM_STATE [ ";
    for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) os << value[i];
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

inline std::ostream& operator<<(std::ostream& os, const NvmeGwState value) {
    os <<  "NvmeGwState { group id: " << value.group_id <<  " gw_map_epoch " <<  value.gw_map_epoch
        << " GwSubsystems: [ ";
    for (const auto& sub: value.subsystems) os << sub.second << " ";
    os << " ] }";

    return os;
};

inline std::ostream& operator<<(std::ostream& os, const NvmeGroupKey value) {
    os << "NvmeGroupKey {" << value.first << "," << value.second << "}";
    return os;
};

inline std::ostream& operator<<(std::ostream& os, const NvmeGwMap value) {
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

inline std::ostream& print_gw_created_t(std::ostream& os, const NvmeGwCreated value, size_t num_ana_groups, NvmeAnaGrpId *anas) {
    os << "==Internal map ==NvmeGwCreated { ana_group_id " << value.ana_grp_id << " osd_epochs: ";
    for(size_t i = 0; i < num_ana_groups; i ++){
        os << " " << anas[i] <<": " << value.blocklist_data[anas[i]].osd_epoch << ":" <<value.blocklist_data[anas[i]].is_failover ;
    }
    os << "\n" << MODULE_PREFFIX << "nonces: " << value.nonce_map << " }";
    os << "\n" << MODULE_PREFFIX << "saved-nonces: " << value.copied_nonce_map << " }";
    for (size_t i = 0; i < num_ana_groups; i++) {
        os << " " << anas[i] <<": " << value.sm_state[anas[i]] << ",";
    }
    os <<  "]\n"<< MODULE_PREFFIX << " failover peers ";
    for (size_t i = 0; i < num_ana_groups; i++) {
        os << anas[i] <<": "  << value.failover_peer[anas[i]] << ",";
    }
    os << "]\n"<< MODULE_PREFFIX << "availability " << value.availability << "]";

    return os;
}

inline std::ostream& operator<<(std::ostream& os, const NvmeGwCreated value) {
    os << "==Internal map ==G W_CREATED_T { ana_group_id " << value.ana_grp_id << " osd_epochs: ";
    for(int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i ++){
        os << " " << value.blocklist_data[i].osd_epoch;
    }
    os << "\n" << MODULE_PREFFIX << "nonces: " << value.nonce_map << " }";

    for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
        os << value.sm_state[i] << ",";
    }
    os <<  "]\n"<< MODULE_PREFFIX << " failover peers ";
    for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
        os << value.failover_peer[i] << ",";
    }
    os <<  "]\n"<< MODULE_PREFFIX << " beacon-subsystems ";
    for (const auto& sub: value.subsystems) {
        os << sub << ",";
    }

    os << "]\n"<< MODULE_PREFFIX << "availability " << value.availability << "]";

    return os;
}

inline std::ostream& operator<<(std::ostream& os, const NvmeGwCreatedMap value) {
    if(value.size()) os << "\n" << MODULE_PREFFIX;;
    NvmeAnaGrpId anas[MAX_SUPPORTED_ANA_GROUPS];
    int i = 0;
    for(auto &it: value ){
       anas[i++] = it.second.ana_grp_id; // effective ana groups for these GWs within group_pool
    }
    for (auto &gw_created_map : value) {
        os  <<  "gw_id: " << gw_created_map.first  << " [ " ;//<< gw_created_map.second << "] \n"<< MODULE_PREFFIX;
        print_gw_created_t(os, gw_created_map.second,  value.size(), anas);
        os << "] \n"<< MODULE_PREFFIX;
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const NVMeofGwMap value) {
    os << "NVMeofGwMap [ Created_gws: ";
    for (auto& group_gws: value.Created_gws) {
        os <<  "\n" <<  MODULE_PREFFIX  << "{ " << group_gws.first << " } -> { " << group_gws.second << " }";
    }
    os << "]";
    return os;
}

inline void encode(const ANA_STATE& st,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode((uint32_t)st.size(), bl);
    for (const auto& gr: st){
        encode((uint32_t)gr.first, bl);
        encode((uint32_t)gr.second, bl);
    }
    ENCODE_FINISH(bl);
}

inline void decode(ANA_STATE& st, ceph::buffer::list::const_iterator &bl) {
    uint32_t n;
    DECODE_START(1, bl);
    decode(n, bl);
    st.resize(n);
    for (uint32_t i = 0; i < n; i++) {
        uint32_t a, b;
        decode(a, bl);
        decode(b, bl);
        st[i].first  = (GW_EXPORTED_STATES_PER_AGROUP_E)a;
        st[i].second = (epoch_t)b;
    }
    DECODE_FINISH(bl);
}

inline void encode(const GwSubsystems& subsystems,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode((uint32_t)subsystems.size(), bl);
    for (const auto& sub: subsystems){
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
  for (uint32_t i=0; i<num_subsystems; i++){
     std::string  nqn;
     decode(nqn, bl);
     ANA_STATE st;
     decode(st, bl);
     subsystems.insert({nqn, NqnState(nqn, st)});
  }
  DECODE_FINISH(bl);
}

inline void encode(const NvmeGwState& state,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode(state.group_id, bl);
    encode(state.gw_map_epoch, bl);
    encode (state.subsystems, bl);
    ENCODE_FINISH(bl);
}

inline  void decode(NvmeGwState& state,  ceph::bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(state.group_id, bl);
    decode(state.gw_map_epoch, bl);
    decode(state.subsystems, bl);
    DECODE_FINISH(bl);
}

inline  void encode(const NvmeGwMetaData& state,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode((uint32_t)MAX_SUPPORTED_ANA_GROUPS, bl);
    for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
        int    tick = state.data[i].timer_started;
        uint8_t val = state.data[i].timer_value;
        encode(tick, bl);
        encode(val,  bl);
        auto endtime = state.data[i].end_time;
        // Convert the time point to milliseconds since the epoch
        uint64_t  millisecondsSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(endtime.time_since_epoch()).count();
        encode(millisecondsSinceEpoch , bl);
    }
    ENCODE_FINISH(bl);
}

inline  void decode(NvmeGwMetaData& state,  ceph::bufferlist::const_iterator& bl) {
    uint32_t s;
      
    DECODE_START(1, bl);
    decode(s, bl);
    ceph_assert(s == (uint32_t)MAX_SUPPORTED_ANA_GROUPS);
    for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
        int tick;
        uint8_t val;
        decode(tick, bl);
        decode(val,  bl);
        state.data[i].timer_started = tick;
        state.data[i].timer_value = val;
        uint64_t milliseconds;
        decode(milliseconds, bl);
        auto duration = std::chrono::milliseconds(milliseconds);
        state.data[i].end_time = std::chrono::time_point<std::chrono::system_clock>(duration);
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
    for(uint32_t i = 0; i<map_size; i++){
        decode(ana_grp_id, bl);
        decode(vector_size,bl);
        for(uint32_t j = 0; j < vector_size; j++){
            decode (nonce, bl);
            nonce_map[ana_grp_id].push_back(nonce);
        }
    }
    DECODE_FINISH(bl);
}

inline void encode(const NvmeGwCreatedMap& gws,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode ((uint32_t)gws.size(), bl); // number of gws in the group
    for(auto& gw : gws){
        encode(gw.first, bl);// GW_id
        encode(gw.second.ana_grp_id, bl); // GW owns this group-id

        for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
            encode((uint32_t)(gw.second.sm_state[i]), bl);
        }
        for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
            encode((gw.second.failover_peer[i]), bl);
        }
        encode((uint32_t)gw.second.availability, bl);
        encode(gw.second.subsystems, bl);

        for(int i=0; i< MAX_SUPPORTED_ANA_GROUPS; i++){
            encode(gw.second.blocklist_data[i].osd_epoch, bl);
            encode(gw.second.blocklist_data[i].is_failover, bl);
        }
        encode(gw.second.nonce_map, bl);
        encode(gw.second.copied_nonce_map, bl);
    }
    ENCODE_FINISH(bl);
}

inline void decode(NvmeGwCreatedMap& gws, ceph::buffer::list::const_iterator &bl) {
    gws.clear();
    uint32_t num_created_gws;
    DECODE_START(1, bl);
    decode(num_created_gws, bl);

    for(uint32_t i = 0; i<num_created_gws; i++){
        std::string gw_name;
        decode(gw_name, bl);
        NvmeAnaGrpId ana_grp_id;
        decode(ana_grp_id, bl);

        NvmeGwCreated gw_created(ana_grp_id);
        uint32_t sm_state;
        NvmeGwId peer_name;
        for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
            decode(sm_state, bl);
            gw_created.sm_state[i] = (GW_STATES_PER_AGROUP_E)  sm_state;
        }
        for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
            decode(peer_name, bl);
            gw_created.failover_peer[i] = peer_name;
        }
        uint32_t avail;
        decode(avail, bl);
        gw_created.availability = (GW_AVAILABILITY_E)avail;
        BeaconSubsystems   subsystems;
        decode(subsystems, bl);
        gw_created.subsystems = subsystems;

        for(int i=0; i< MAX_SUPPORTED_ANA_GROUPS; i++){
            decode(gw_created.blocklist_data[i].osd_epoch, bl);
            decode(gw_created.blocklist_data[i].is_failover, bl);
        }
        decode(gw_created.nonce_map, bl);
        decode(gw_created.copied_nonce_map, bl);

        gws[gw_name] = gw_created;
    }
    DECODE_FINISH(bl);
}

inline void encode(const std::map<NvmeGroupKey, NvmeGwCreatedMap>& created_gws,  ceph::bufferlist &bl) {
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

inline void decode(std::map<NvmeGroupKey, NvmeGwCreatedMap>& created_gws, ceph::buffer::list::const_iterator &bl) {
    created_gws.clear();
    uint32_t ngroups;
    DECODE_START(1, bl);
    decode(ngroups, bl);
    for(uint32_t i = 0; i<ngroups; i++){
        std::string pool, group;
        decode(pool, bl);
        decode(group, bl);
        NvmeGwCreatedMap cmap;
        decode(cmap, bl);
        created_gws[std::make_pair(pool, group)] = cmap;
    }
    DECODE_FINISH(bl);
}

inline void encode(const NvmeGwMap& subsyst_gwmap,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode((uint32_t)subsyst_gwmap.size(), bl);
    for (auto& subsyst: subsyst_gwmap) {
        encode(subsyst.first, bl);
        encode(subsyst.second, bl);
    }
    ENCODE_FINISH(bl);
}

inline void decode(NvmeGwMap& subsyst_gwmap, ceph::buffer::list::const_iterator &bl) {
    subsyst_gwmap.clear();
    uint32_t num_gws;
    DECODE_START(1, bl);
    decode(num_gws, bl);

    for (uint32_t i = 0; i < num_gws; i++) {
        NvmeGwId gw_id;
        decode(gw_id, bl);
        NvmeGwState gw_st;
        decode(gw_st, bl);
        subsyst_gwmap[gw_id] = gw_st;
    }
    DECODE_FINISH(bl);
}

// Start encode  NvmeGroupKey, GMAP
inline void encode(const std::map<NvmeGroupKey, NvmeGwMap>& gmap,  ceph::bufferlist &bl) {
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
inline void decode(std::map<NvmeGroupKey, NvmeGwMap>& gmap, ceph::buffer::list::const_iterator &bl) {
    gmap.clear();
    uint32_t ngroups;
    DECODE_START(1, bl);
    decode(ngroups, bl);
    for(uint32_t i = 0; i<ngroups; i++){
        std::string pool, group;
        decode(pool, bl);
        decode(group, bl);
        NvmeGwMap grp_map;
        decode(grp_map, bl);
        gmap[std::make_pair(pool, group)] = grp_map;
    }
    DECODE_FINISH(bl);
}

inline void encode(const std::map<NvmeGroupKey, NvmeGwMetaDataMap>& gmetadata,  ceph::bufferlist &bl) {
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

inline void decode(std::map<NvmeGroupKey, NvmeGwMetaDataMap>& gmetadata, ceph::buffer::list::const_iterator &bl) {
    gmetadata.clear();
    uint32_t ngroups;
    DECODE_START(1, bl);
    decode(ngroups, bl);
    for(uint32_t i = 0; i<ngroups; i++){
        std::string pool, group;
        decode(pool, bl);
        decode(group, bl);
        NvmeGwMetaDataMap gmd;
        decode(gmd, bl);
        gmetadata[std::make_pair(pool, group)] = gmd;
    }
    DECODE_FINISH(bl);
}

inline void encode(const NvmeGwMetaDataMap& group_md,  ceph::bufferlist &bl) {
    ENCODE_START(1, 1, bl);
    encode ((uint32_t)group_md.size(), bl); // number of groups
    for (auto& gw_md: group_md) {
        encode(gw_md.first, bl); // gw
        encode(gw_md.second, bl); //  map of this gw
    }
    ENCODE_FINISH(bl);
}

inline void decode(NvmeGwMetaDataMap& md, ceph::buffer::list::const_iterator &bl) {
    uint32_t num_gws;
    DECODE_START(1, bl);
    decode(num_gws, bl);
    for (uint32_t i = 0; i < num_gws; i++) {
        std::string gw_id;
        decode(gw_id, bl);
        NvmeGwMetaData gw_meta;
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
