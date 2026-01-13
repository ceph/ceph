// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_NVMEOFGWBEACON_H
#define CEPH_NVMEOFGWBEACON_H

#include <cstddef>
#include <vector>
#include "messages/PaxosServiceMessage.h"
#include "mon/MonCommand.h"
#include "mon/NVMeofGwMap.h"
#include "include/types.h"
#include "mon/NVMeofGwBeaconConstants.h"

class MNVMeofGwBeacon final : public PaxosServiceMessage {

protected:
    std::string       gw_id;
    std::string       gw_pool;
    std::string       gw_group;
    BeaconSubsystems  subsystems;                           // gateway susbsystem and their state machine states
    gw_availability_t availability;                         // in absence of  beacon  heartbeat messages it becomes inavailable
    epoch_t           last_osd_epoch;
    epoch_t           last_gwmap_epoch;
    uint64_t          sequence = 0;                         // sequence number for each beacon message
    uint64_t          affected_features = 0;

public:
  MNVMeofGwBeacon()
    : PaxosServiceMessage{MSG_MNVMEOF_GW_BEACON, 0, BEACON_VERSION_ENHANCED,
	  BEACON_VERSION_ENHANCED}, sequence(0)
  {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }

  MNVMeofGwBeacon(const std::string &gw_id_,
        const std::string& gw_pool_,
        const std::string& gw_group_,
        const BeaconSubsystems& subsystems_,
        const gw_availability_t& availability_,
        const epoch_t& last_osd_epoch_,
        const epoch_t& last_gwmap_epoch_,
        uint64_t sequence_ = 0,  // default sequence for backward compatibility
        uint64_t features = 0)
    : PaxosServiceMessage{MSG_MNVMEOF_GW_BEACON,
            0,
            features ? BEACON_VERSION_ENHANCED : BEACON_VERSION_LEGACY,
            features ? BEACON_VERSION_ENHANCED : BEACON_VERSION_LEGACY},
      gw_id(gw_id_), gw_pool(gw_pool_), gw_group(gw_group_), subsystems(subsystems_),
      availability(availability_), last_osd_epoch(last_osd_epoch_),
      last_gwmap_epoch(last_gwmap_epoch_), sequence(sequence_),
      affected_features(features)
  {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }

  const std::string& get_gw_id() const { return gw_id; }
  const std::string& get_gw_pool() const { return gw_pool; }
  const std::string& get_gw_group() const { return gw_group; }
  NvmeAnaNonceMap get_nonce_map() const {
    NvmeAnaNonceMap nonce_map;
    for (const auto& sub: subsystems) {
      for (const auto& ns: sub.namespaces) {
        auto& nonce_vec = nonce_map[ns.anagrpid-1];//Converting   ana groups to offsets
        if (std::find(nonce_vec.begin(), nonce_vec.end(), ns.nonce) == nonce_vec.end())
          nonce_vec.push_back(ns.nonce);
      }
    }
    return nonce_map;
  }

  const gw_availability_t& get_availability()   const   { return availability; }
  const epoch_t&           get_last_osd_epoch() const   { return last_osd_epoch; }
  const epoch_t&           get_last_gwmap_epoch() const { return last_gwmap_epoch; }
  const BeaconSubsystems&  get_subsystems()     const   { return subsystems; };
  uint64_t get_sequence() const { return sequence; }

private:
  ~MNVMeofGwBeacon() final {}

public:

  std::string_view get_type_name() const override { return "nvmeofgwbeacon"; }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(gw_id, payload);
    encode(gw_pool, payload);
    encode(gw_group, payload);
    encode(subsystems, payload, affected_features);
    encode((uint32_t)availability, payload);
    encode(last_osd_epoch, payload);
    encode(last_gwmap_epoch, payload);
    // Only encode sequence for enhanced beacons (version >= 2)
    if (get_header().version >= 2) {
      encode(sequence, payload);
    }
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    
    paxos_decode(p);
    decode(gw_id, p);
    decode(gw_pool, p);
    decode(gw_group, p);
    decode(subsystems, p);
    uint32_t tmp;
    decode(tmp, p);
    availability = static_cast<gw_availability_t>(tmp);
    decode(last_osd_epoch, p);
    decode(last_gwmap_epoch, p);
    // Only decode sequence for enhanced beacons (version >= 2)
    if (get_header().version >= 2 && !p.end()) {
      decode(sequence, p);
    } else {
      sequence = 0;  // Legacy beacons don't have sequence field
    }
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};


#endif
