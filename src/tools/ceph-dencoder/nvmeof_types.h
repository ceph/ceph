// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_NVMEOF_TYPES_H
#define CEPH_NVMEOF_TYPES_H

#ifdef WITH_NVMEOF_GATEWAY_MONITOR_CLIENT
#include "mon/NVMeofGwMon.h"
#include "messages/MNVMeofGwMap.h"
#include "messages/MNVMeofGwBeacon.h"
TYPE(NVMeofGwMap)
// Implement the dencoder interface
class NVMeofGwMapDencoder {
 private:
   NVMeofGwMap m;
 public:
  NVMeofGwMapDencoder() = default;
  explicit NVMeofGwMapDencoder(const NVMeofGwMap& m) : m(m) {}

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(t, bl);
  }
  void decode(bufferlist::const_iterator &p) {
    using ceph::decode;
    decode(t, p);
  }
  void dump(Formatter* f) {
    f->dump_stream("NVMeofGwMap") << m;
  }

  static void generate_test_instances(std::list<NVMeofGwMapDencoder*>& ls) {
    std::string pool = "pool1";
    std::string group = "grp1";
    auto group_key = std::make_pair(pool, group);
    m.cfg_add_gw("GW1" ,group_key);
    m.cfg_add_gw("GW2" ,group_key);
    m.cfg_add_gw("GW3" ,group_key);
    NvmeNonceVector new_nonces = {"abc", "def","hij"};
    m.created_gws[group_key]["GW1"].nonce_map[1] = new_nonces;
    m.created_gws[group_key]["GW1"].performed_full_startup = true;
    for(int i=0; i< MAX_SUPPORTED_ANA_GROUPS; i++){
      m.created_gws[group_key]["GW1"].blocklist_data[i].osd_epoch = i*2;
      m.created_gws[group_key]["GW1"].blocklist_data[i].is_failover = false;
    }

    m.created_gws[group_key]["GW2"].nonce_map[2] = new_nonces;

    ls.push_back(new NVMeofGwMapDencoder(m));

  }
};
WRITE_CLASS_ENCODER(NVMeofGwMapDencoder)

TYPE(MNVMeofGwMap)
// Implement the dencoder interface
class MNVMeofGwMapDencoder {
 private:
   MNVMeofGwMap m;
 public:
  MNVMeofGwMapDencoder() = default;
  explicit MNVMeofGwMapDencoder(const MNVMeofGwMap& m) : m(m) {}

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(t, bl);
  }
  void decode(bufferlist::const_iterator &p) {
    using ceph::decode;
    decode(t, p);
  }
  void dump(Formatter* f) {
    f->dump_stream("MNVMeofGwMap") << m;
  }

  static void generate_test_instances(std::list<MNVMeofGwMapDencoder*>& ls) {
    std::map<NvmeGroupKey, NvmeGwMonClientStates> map;
    std::string pool = "pool1";
    std::string group = "grp1";
    std::string gw_id = "GW1";
    NvmeGwClientState state(1, 32, gw_availability_t::GW_UNAVAILABLE);
    std::string nqn = "nqn";
    ANA_STATE ana_state;
    NqnState nqn_state(nqn, ana_state);
    state.subsystems.insert({nqn, nqn_state});

    auto group_key = std::make_pair(pool, group);
    map[group_key][gw_id] = state;
    BeaconSubsystem sub = { nqn, {}, {} };
    NVMeofGwMap pending_map;
    pending_map.cfg_add_gw("GW1" ,group_key);
    pending_map.cfg_add_gw("GW2" ,group_key);
    pending_map.cfg_add_gw("GW3" ,group_key);
    NvmeNonceVector new_nonces = {"abc", "def","hij"};
    pending_map.created_gws[group_key]["GW1"].nonce_map[1] = new_nonces;
    pending_map.created_gws[group_key]["GW1"].subsystems.push_back(sub);
    for(int i=0; i< MAX_SUPPORTED_ANA_GROUPS; i++){
      pending_map.created_gws[group_key]["GW1"].blocklist_data[i].osd_epoch = i*2;
      pending_map.created_gws[group_key]["GW1"].blocklist_data[i].is_failover = false;
    }

    pending_map.created_gws[group_key]["GW2"].nonce_map[2] = new_nonces;
    pending_map.start_timer(gw_id, group_key, group, 30);

    m = MNVMeofGwMap(pending_map);
    ls.push_back(new MNVMeofGwMapDencoder(m));

  }
};
WRITE_CLASS_ENCODER(MNVMeofGwMapDencoder)

TYPE(MNVMeofGwBeacon)
// Implement the dencoder interface
class MNVMeofGwBeaconDencoder {
 private:
   MNVMeofGwBeacon m;
 public:
  MNVMeofGwBeaconDencoder() = default;
  explicit MNVMeofGwBeaconDencoder(const MNVMeofGwBeacon& m) : m(m) {}

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(t, bl);
  }
  void decode(bufferlist::const_iterator &p) {
    using ceph::decode;
    decode(t, p);
  }
  void dump(Formatter* f) {
    f->dump_stream("MNVMeofGwBeacon") << m;
  }

  static void generate_test_instances(std::list<MNVMeofGwBeaconDencoder*>& ls) {
    std::string gw_id = "GW";
    std::string gw_pool = "pool";
    std::string gw_group = "group";
    gw_availability_t availability = gw_availability_t::GW_AVAILABLE;
    std::string nqn = "nqn";
    BeaconSubsystem sub = { nqn, {}, {} };
    std::string nqn = "nqn";
    BeaconSubsystem sub = { nqn, {}, {} };
    BeaconSubsystems subs = { sub };
    epoch_t osd_epoch = 17;
    epoch_t gwmap_epoch = 42;
    m = MNVMeofGwBeacon(
      gw_id,
      gw_pool,
      gw_group,
      subs,
      availability,
      osd_epoch,
      gwmap_epoch);

    ls.push_back(new MNVMeofGwBeaconDencoder(m));

  }
};
WRITE_CLASS_ENCODER(MNVMeofGwBeaconDencoder)


#endif // WITH_NVMEOF_GATEWAY_MONITOR_CLIENT

#endif // CEPH_NVMEOF_TYPES_H
