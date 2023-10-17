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

inline std::ostream& operator<<(std::ostream& os, const GW_STATE_T value) {
    os <<  "GW_STATE_T { group id: " << value.group_id <<  " gw_map_epoch " <<  value.gw_map_epoch
        << " GwSubsystems: [ ";
    for (const auto& sub: value.subsystems) os << sub.second << " ";
    os << " ] }";

    return os;
};

inline std::ostream& operator<<(std::ostream& os, const GROUP_KEY value) {
    os << "GROUP_KEY {" << value.first << "," << value.second << "}";
    return os;
};

inline std::ostream& operator<<(std::ostream& os, const GWMAP value) {
    os << "GWMAP ";
    for (auto& gw_state: value) {
        os << "\n" << MODULE_PREFFIX <<" { == gw_id: " << gw_state.first << " -> " <<  gw_state.second << "}";
    }
    os << "}";

    return os;
};

inline std::ostream& operator<<(std::ostream& os, const NONCE_VECTOR_T value) {
    for (auto & nonces : value) {
        os <<  nonces << " ";
    }
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const GW_ANA_NONCE_MAP value) {
    if(value.size()) os << "\n" << MODULE_PREFFIX;
    for (auto &nonce_map : value) {
        os  << "  ana_grp: " << nonce_map.first  << " [ " << nonce_map.second << "]\n"<< MODULE_PREFFIX ;
    }
    return os;
}

inline std::ostream& print_gw_created_t(std::ostream& os, const GW_CREATED_T value, size_t num_ana_groups, ANA_GRP_ID_T *anas) {
    os << "==Internal map ==GW_CREATED_T { ana_group_id " << value.ana_grp_id << " osd_epochs: ";
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

inline std::ostream& operator<<(std::ostream& os, const GW_CREATED_T value) {
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

inline std::ostream& operator<<(std::ostream& os, const GW_CREATED_MAP value) {
    if(value.size()) os << "\n" << MODULE_PREFFIX;;
    ANA_GRP_ID_T anas[MAX_SUPPORTED_ANA_GROUPS];
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
    encode(st.size(), bl);
    for (const auto& gr: st){
        encode((int)gr.first, bl);
        encode((int)gr.second, bl);
    }
}

inline void decode(ANA_STATE& st, ceph::buffer::list::const_iterator &bl) {
    size_t n;
    decode(n, bl);
    st.resize(n);
    for (size_t i = 0; i < n; i++) {
        int a;
        int b;
        decode(a, bl);
        decode(b, bl);
        st[i].first  = (GW_EXPORTED_STATES_PER_AGROUP_E)a;
        st[i].second = (epoch_t)b;
    }
}

inline void encode(const GwSubsystems& subsystems,  ceph::bufferlist &bl) {
    encode(subsystems.size(), bl);
    for (const auto& sub: subsystems){
        encode(sub.second.nqn, bl);
        encode(sub.second.ana_state, bl);
    }
}

inline  void decode(GwSubsystems& subsystems,  ceph::bufferlist::const_iterator& bl) {
  size_t num_subsystems;
  decode(num_subsystems, bl);
  subsystems.clear();
  for (size_t i=0; i<num_subsystems; i++){
     std::string  nqn;
     decode(nqn, bl);
     ANA_STATE st;
     decode(st, bl);
     subsystems.insert({nqn, NqnState(nqn, st)});
  }
}

inline void encode(const GW_STATE_T& state,  ceph::bufferlist &bl) {

    encode(state.group_id, bl);
    encode(state.gw_map_epoch, bl);
    encode (state.subsystems, bl);
}

inline  void decode(GW_STATE_T& state,  ceph::bufferlist::const_iterator& bl) {

    decode(state.group_id, bl);
    decode(state.gw_map_epoch, bl);
    decode(state.subsystems, bl);
}

inline  void encode(const GW_METADATA_T& state,  ceph::bufferlist &bl) {
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
}

inline  void decode(GW_METADATA_T& state,  ceph::bufferlist::const_iterator& bl) {
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
}

inline void encode(const GW_ANA_NONCE_MAP& nonce_map,  ceph::bufferlist &bl) {
    encode(nonce_map.size(), bl);
    for (auto& ana_group_nonces : nonce_map) {
        encode(ana_group_nonces.first, bl); // ana group id
        encode (ana_group_nonces.second.size(), bl); // encode the vector size
        for (auto& nonce: ana_group_nonces.second) encode(nonce, bl);
    }
}

inline void decode(GW_ANA_NONCE_MAP& nonce_map, ceph::buffer::list::const_iterator &bl) {
    size_t map_size;
    ANA_GRP_ID_T ana_grp_id;
    size_t vector_size;
    std::string nonce;

    decode(map_size, bl);
    for(size_t i = 0; i<map_size; i++){
        decode(ana_grp_id, bl);
        decode(vector_size,bl);
        for(size_t j = 0; j < vector_size; j++){
            decode (nonce, bl);
            nonce_map[ana_grp_id].push_back(nonce);
        }
    }
}

inline void encode(const GW_CREATED_MAP& gws,  ceph::bufferlist &bl) {
    encode (gws.size(), bl); // number of gws in the group
    for(auto& gw : gws){
        encode(gw.first, bl);// GW_id
        encode(gw.second.ana_grp_id, bl); // GW owns this group-id

        for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
            encode((int)(gw.second.sm_state[i]), bl);
        }
        for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
            encode((gw.second.failover_peer[i]), bl);
        }
        encode((int)gw.second.availability, bl);
        encode(gw.second.subsystems, bl);

        for(int i=0; i< MAX_SUPPORTED_ANA_GROUPS; i++){
            encode(gw.second.blocklist_data[i].osd_epoch, bl);
            encode(gw.second.blocklist_data[i].is_failover, bl);
        }
        encode(gw.second.nonce_map, bl);
        encode(gw.second.copied_nonce_map, bl);
    }
}

inline void decode(GW_CREATED_MAP& gws, ceph::buffer::list::const_iterator &bl) {
    gws.clear();
    size_t num_created_gws;
    decode(num_created_gws, bl);

    for(size_t i = 0; i<num_created_gws; i++){
        std::string gw_name;
        decode(gw_name, bl);
        ANA_GRP_ID_T ana_grp_id;
        decode(ana_grp_id, bl);

        GW_CREATED_T gw_created(ana_grp_id);
        int sm_state;
        GW_ID_T peer_name;
        for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
            decode(sm_state, bl);
            gw_created.sm_state[i] = (GW_STATES_PER_AGROUP_E)  sm_state;
        }
        for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
            decode(peer_name, bl);
            gw_created.failover_peer[i] = peer_name;
        }
        int avail;
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
}

inline void encode(const std::map<GROUP_KEY, GW_CREATED_MAP>& created_gws,  ceph::bufferlist &bl) {
    encode (created_gws.size(), bl); // number of groups
    for (auto& group_gws: created_gws) {
        auto& group_key = group_gws.first;
        encode(group_key.first, bl); // pool
        encode(group_key.second, bl); // group

        auto& gws = group_gws.second;
        encode (gws, bl); // encode group gws
    }
}

inline void decode(std::map<GROUP_KEY, GW_CREATED_MAP>& created_gws, ceph::buffer::list::const_iterator &bl) {
    created_gws.clear();
    size_t ngroups;
    decode(ngroups, bl);
    for(size_t i = 0; i<ngroups; i++){
        std::string pool, group;
        decode(pool, bl);
        decode(group, bl);
        GW_CREATED_MAP cmap;
        decode(cmap, bl);
        created_gws[std::make_pair(pool, group)] = cmap;
    }
}

inline void encode(const GWMAP& subsyst_gwmap,  ceph::bufferlist &bl) {
    encode(subsyst_gwmap.size(), bl);
   // dout(0) << "number gateways: " << subsyst_gwmap.size() << dendl;
    for (auto& subsyst: subsyst_gwmap) {
        encode(subsyst.first, bl);
     //   dout(0) << "gateway id: " << subsyst.first << dendl;
        encode(subsyst.second, bl);
   //     dout(0) << "gateway state: " << subsyst.second << dendl;
    }
}

inline void decode(GWMAP& subsyst_gwmap, ceph::buffer::list::const_iterator &bl) {
    subsyst_gwmap.clear();
    size_t num_gws;
    decode(num_gws, bl);
    //dout(0) << "number gateways: " << num_gws << dendl;

    for (size_t i = 0; i < num_gws; i++) {
        GW_ID_T gw_id;
        decode(gw_id, bl);
   //     dout(0) << "gw_id: " << gw_id << dendl;
        GW_STATE_T gw_st;
        decode(gw_st, bl);
    //    dout(0) << "gw_st: " << gw_st << dendl;
        subsyst_gwmap[gw_id] = gw_st;
    }
}

// Start encode  GROUP_KEY, GMAP
inline void encode(const std::map<GROUP_KEY, GWMAP>& gmap,  ceph::bufferlist &bl) {
    encode (gmap.size(), bl); // number of groups
    //dout(0) << "size: " << gmap.size() << dendl;
    for (auto& group_state: gmap) {
        auto& group_key = group_state.first;
        encode(group_key.first, bl); // pool
     //   dout(0) << "pool: " << group_key.first << dendl;
        encode(group_key.second, bl); // group
     //   dout(0) << "group: " << group_key.second << dendl;

        encode(group_state.second, bl);
    //    dout(0) << "GWMAP: " << group_state.second << dendl;
    }
}
// Start decode GROUP_KEY, GMAP
inline void decode(std::map<GROUP_KEY, GWMAP>& gmap, ceph::buffer::list::const_iterator &bl) {
    gmap.clear();
    size_t ngroups;
    decode(ngroups, bl);
   // dout(0) << "ngroups: " << ngroups << dendl;
    for(size_t i = 0; i<ngroups; i++){
        std::string pool, group;
        decode(pool, bl);
        decode(group, bl);
      //  dout(0) << "pool: " << pool << " group: " << group << dendl;
        GWMAP grp_map;
        decode(grp_map, bl);
     //   dout(0) << "GWMAP: " << grp_map << dendl;
        gmap[std::make_pair(pool, group)] = grp_map;
    }
}

inline void encode(const std::map<GROUP_KEY, GWMETADATA>& gmetadata,  ceph::bufferlist &bl) {
    encode (gmetadata.size(), bl); // number of groups
    for (auto& group_md: gmetadata) {
        auto& group_key = group_md.first;
        encode(group_key.first, bl); // pool
        encode(group_key.second, bl); // group

        encode(group_md.second, bl);
    }
}

inline void decode(std::map<GROUP_KEY, GWMETADATA>& gmetadata, ceph::buffer::list::const_iterator &bl) {
    gmetadata.clear();
    size_t ngroups;
    decode(ngroups, bl);
    for(size_t i = 0; i<ngroups; i++){
        std::string pool, group;
        decode(pool, bl);
        decode(group, bl);
        GWMETADATA gmd;
        decode(gmd, bl);
        gmetadata[std::make_pair(pool, group)] = gmd;
    }
}

inline void encode(const GWMETADATA& group_md,  ceph::bufferlist &bl) {
    encode (group_md.size(), bl); // number of groups
    for (auto& gw_md: group_md) {
        encode(gw_md.first, bl); // gw
        encode(gw_md.second, bl); //  map of this gw
    }
}

inline void decode(GWMETADATA& md, ceph::buffer::list::const_iterator &bl) {
    size_t num_gws;
    decode(num_gws, bl);
    for (size_t i = 0; i < num_gws; i++) {
        std::string gw_id;
        decode(gw_id, bl);
        GW_METADATA_T gw_meta;
        decode(gw_meta, bl);
        md[gw_id] = gw_meta;
    }
}

inline void encode(const BeaconNamespace& ns,  ceph::bufferlist &bl) {
    encode(ns.anagrpid, bl);
    encode(ns.nonce, bl);
}

inline void decode(BeaconNamespace& ns, ceph::buffer::list::const_iterator &bl) {
    decode(ns.anagrpid, bl);
    decode(ns.nonce, bl);
}

inline void encode(const BeaconListener& ls,  ceph::bufferlist &bl) {
    encode(ls.address_family, bl);
    encode(ls.address, bl);
    encode(ls.svcid, bl);
}

inline void decode(BeaconListener& ls, ceph::buffer::list::const_iterator &bl) {
    decode(ls.address_family, bl);
    decode(ls.address, bl);
    decode(ls.svcid, bl);
}

inline void encode(const BeaconSubsystem& sub,  ceph::bufferlist &bl) {
    encode(sub.nqn, bl);
    encode(sub.listeners.size(), bl);
    for (const auto& ls: sub.listeners)
        encode(ls, bl);
    encode(sub.namespaces.size(), bl);
    for (const auto& ns: sub.namespaces)
        encode(ns, bl);
}

inline void decode(BeaconSubsystem& sub, ceph::buffer::list::const_iterator &bl) {
    decode(sub.nqn, bl);
    size_t s;
    sub.listeners.clear();
    decode(s, bl);
    for (size_t i = 0; i < s; i++) {
        BeaconListener ls;
        decode(ls, bl);
        sub.listeners.push_back(ls);
    }

    sub.namespaces.clear();
    decode(s, bl);
    for (size_t i = 0; i < s; i++) {
        BeaconNamespace ns;
        decode(ns, bl);
        sub.namespaces.push_back(ns);
    }
}


#undef dout_subsys
#endif /* SRC_MON_NVMEOFGWSERIALIZEP_H_ */
