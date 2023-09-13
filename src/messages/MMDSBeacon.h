// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MMDSBEACON_H
#define CEPH_MMDSBEACON_H

#include <string_view>

#include "msg/Message.h"
#include "messages/PaxosServiceMessage.h"

#include "include/types.h"

#include "mds/MDSMap.h"



/**
 * Unique ID for each type of metric we can send to the mon, so that if the mon
 * knows about the IDs then it can implement special behaviour for certain
 * messages.
 */
enum mds_metric_t {
  MDS_HEALTH_NULL = 0,
  MDS_HEALTH_TRIM,
  MDS_HEALTH_CLIENT_RECALL,
  MDS_HEALTH_CLIENT_LATE_RELEASE,
  MDS_HEALTH_CLIENT_RECALL_MANY,
  MDS_HEALTH_CLIENT_LATE_RELEASE_MANY,
  MDS_HEALTH_CLIENT_OLDEST_TID,
  MDS_HEALTH_CLIENT_OLDEST_TID_MANY,
  MDS_HEALTH_DAMAGE,
  MDS_HEALTH_READ_ONLY,
  MDS_HEALTH_SLOW_REQUEST,
  MDS_HEALTH_CACHE_OVERSIZED,
  MDS_HEALTH_SLOW_METADATA_IO,
  MDS_HEALTH_CLIENTS_LAGGY,
  MDS_HEALTH_DUMMY, // not a real health warning, for testing
};

inline const char *mds_metric_name(mds_metric_t m)
{
  switch (m) {
  case MDS_HEALTH_TRIM: return "MDS_TRIM";
  case MDS_HEALTH_CLIENT_RECALL: return "MDS_CLIENT_RECALL";
  case MDS_HEALTH_CLIENT_LATE_RELEASE: return "MDS_CLIENT_LATE_RELEASE";
  case MDS_HEALTH_CLIENT_RECALL_MANY: return "MDS_CLIENT_RECALL_MANY";
  case MDS_HEALTH_CLIENT_LATE_RELEASE_MANY: return "MDS_CLIENT_LATE_RELEASE_MANY";
  case MDS_HEALTH_CLIENT_OLDEST_TID: return "MDS_CLIENT_OLDEST_TID";
  case MDS_HEALTH_CLIENT_OLDEST_TID_MANY: return "MDS_CLIENT_OLDEST_TID_MANY";
  case MDS_HEALTH_DAMAGE: return "MDS_DAMAGE";
  case MDS_HEALTH_READ_ONLY: return "MDS_READ_ONLY";
  case MDS_HEALTH_SLOW_REQUEST: return "MDS_SLOW_REQUEST";
  case MDS_HEALTH_CACHE_OVERSIZED: return "MDS_CACHE_OVERSIZED";
  case MDS_HEALTH_SLOW_METADATA_IO: return "MDS_SLOW_METADATA_IO";
  case MDS_HEALTH_CLIENTS_LAGGY: return "MDS_CLIENTS_LAGGY";
  case MDS_HEALTH_DUMMY: return "MDS_DUMMY";
  default:
    return "???";
  }
}

inline const char *mds_metric_summary(mds_metric_t m)
{
  switch (m) {
  case MDS_HEALTH_TRIM:
    return "%num% MDSs behind on trimming";
  case MDS_HEALTH_CLIENT_RECALL:
    return "%num% clients failing to respond to cache pressure";
  case MDS_HEALTH_CLIENT_LATE_RELEASE:
    return "%num% clients failing to respond to capability release";
  case MDS_HEALTH_CLIENT_RECALL_MANY:
    return "%num% MDSs have many clients failing to respond to cache pressure";
  case MDS_HEALTH_CLIENT_LATE_RELEASE_MANY:
    return "%num% MDSs have many clients failing to respond to capability "
      "release";
  case MDS_HEALTH_CLIENT_OLDEST_TID:
    return "%num% clients failing to advance oldest client/flush tid";
  case MDS_HEALTH_CLIENT_OLDEST_TID_MANY:
    return "%num% MDSs have clients failing to advance oldest client/flush tid";
  case MDS_HEALTH_DAMAGE:
    return "%num% MDSs report damaged metadata";
  case MDS_HEALTH_READ_ONLY:
    return "%num% MDSs are read only";
  case MDS_HEALTH_SLOW_REQUEST:
    return "%num% MDSs report slow requests";
  case MDS_HEALTH_CACHE_OVERSIZED:
    return "%num% MDSs report oversized cache";
  case MDS_HEALTH_SLOW_METADATA_IO:
    return "%num% MDSs report slow metadata IOs";
  case MDS_HEALTH_CLIENTS_LAGGY:
    return "%num% client(s) laggy due to laggy OSDs";  
  default:
    return "???";
  }
}

/**
 * This structure is designed to allow some flexibility in how we emit health
 * complaints, such that:
 * - The mon doesn't have to have foreknowledge of all possible metrics: we can
 *   implement new messages in the MDS and have the mon pass them through to the user
 *   (enables us to do complex checks inside the MDS, and allows mon to be older version
 *   than MDS)
 * - The mon has enough information to perform reductions on some types of metric, for
 *   example complaints about the same client from multiple MDSs where we might want
 *   to reduce three "client X is stale on MDS y" metrics into one "client X is stale
 *   on 3 MDSs" message.
 */
struct MDSHealthMetric
{
  mds_metric_t type;
  health_status_t sev;
  std::string message;
  std::map<std::string, std::string> metadata;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph_assert(type != MDS_HEALTH_NULL);
    encode((uint16_t)type, bl);
    encode((uint8_t)sev, bl);
    encode(message, bl);
    encode(metadata, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    uint16_t raw_type;
    decode(raw_type, bl);
    type = (mds_metric_t)raw_type;
    ceph_assert(type != MDS_HEALTH_NULL);
    uint8_t raw_sev;
    decode(raw_sev, bl);
    sev = (health_status_t)raw_sev;
    decode(message, bl);
    decode(metadata, bl);
    DECODE_FINISH(bl);
  }

  bool operator==(MDSHealthMetric const &other) const
  {
    return (type == other.type && sev == other.sev && message == other.message);
  }

  MDSHealthMetric() : type(MDS_HEALTH_NULL), sev(HEALTH_OK) {}
  MDSHealthMetric(mds_metric_t type_, health_status_t sev_, std::string_view message_)
    : type(type_), sev(sev_), message(message_) {}
};
WRITE_CLASS_ENCODER(MDSHealthMetric)


/**
 * Health metrics send by the MDS to the mon, so that the mon can generate
 * user friendly warnings about undesirable states.
 */
struct MDSHealth
{
  std::vector<MDSHealthMetric> metrics;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(metrics, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(metrics, bl);
    DECODE_FINISH(bl);
  }

  bool operator==(MDSHealth const &other) const
  {
    return metrics == other.metrics;
  }
};
WRITE_CLASS_ENCODER(MDSHealth)


class MMDSBeacon final : public PaxosServiceMessage {
private:

  static constexpr int HEAD_VERSION = 8;
  static constexpr int COMPAT_VERSION = 6;

  uuid_d fsid;
  mds_gid_t global_id = MDS_GID_NONE;
  std::string name;

  MDSMap::DaemonState state = MDSMap::STATE_NULL;
  version_t seq = 0;

  CompatSet compat;

  MDSHealth health;

  std::map<std::string, std::string> sys_info;

  uint64_t mds_features = 0;

  std::string fs;

protected:
  MMDSBeacon() : PaxosServiceMessage(MSG_MDS_BEACON, 0, HEAD_VERSION, COMPAT_VERSION)
  {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
  MMDSBeacon(const uuid_d &f, mds_gid_t g, const std::string& n, epoch_t les,
	     MDSMap::DaemonState st, version_t se, uint64_t feat) :
    PaxosServiceMessage(MSG_MDS_BEACON, les, HEAD_VERSION, COMPAT_VERSION),
    fsid(f), global_id(g), name(n), state(st), seq(se),
    mds_features(feat) {
    set_priority(CEPH_MSG_PRIO_HIGH);
  }
  ~MMDSBeacon() final {}

public:
  const uuid_d& get_fsid() const { return fsid; }
  mds_gid_t get_global_id() const { return global_id; }
  const std::string& get_name() const { return name; }
  epoch_t get_last_epoch_seen() const { return version; }
  MDSMap::DaemonState get_state() const { return state; }
  version_t get_seq() const { return seq; }
  std::string_view get_type_name() const override { return "mdsbeacon"; }
  uint64_t get_mds_features() const { return mds_features; }

  CompatSet const& get_compat() const { return compat; }
  void set_compat(const CompatSet& c) { compat = c; }

  MDSHealth const& get_health() const { return health; }
  void set_health(const MDSHealth &h) { health = h; }

  const std::string& get_fs() const { return fs; }
  void set_fs(std::string_view s) { fs = s; }

  const std::map<std::string, std::string>& get_sys_info() const { return sys_info; }
  void set_sys_info(const std::map<std::string, std::string>& i) { sys_info = i; }

  void print(std::ostream& out) const override {
    out << "mdsbeacon(" << global_id << "/" << name
	<< " " << ceph_mds_state_name(state);
    if (fs.size()) {
      out << " fs=" << fs;
    }
    out << " seq=" << seq << " v" << version << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    paxos_encode();
    encode(fsid, payload);
    encode(global_id, payload);
    encode((__u32)state, payload);
    encode(seq, payload);
    encode(name, payload);
    encode(MDS_RANK_NONE, payload);
    encode(std::string(), payload);
    encode(compat, payload);
    encode(health, payload);
    if (state == MDSMap::STATE_BOOT) {
      encode(sys_info, payload);
    }
    encode(mds_features, payload);
    encode(FS_CLUSTER_ID_NONE, payload);
    encode(false, payload);
    encode(fs, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    paxos_decode(p);
    decode(fsid, p);
    decode(global_id, p);
    __u32 raw_state;
    decode(raw_state, p);
    state = (MDSMap::DaemonState)raw_state;
    decode(seq, p);
    decode(name, p);
    {
      mds_rank_t standby_for_rank;
      decode(standby_for_rank, p);
    }
    {
      std::string standby_for_name;
      decode(standby_for_name, p);
    }
    decode(compat, p);
    decode(health, p);
    if (state == MDSMap::STATE_BOOT) {
      decode(sys_info, p);
    }
    decode(mds_features, p);
    {
      fs_cluster_id_t standby_for_fscid;
      decode(standby_for_fscid, p);
    }
    if (header.version >= 7) {
      bool standby_replay;
      decode(standby_replay, p);
    }

    if (header.version < 7  && state == MDSMap::STATE_STANDBY_REPLAY) {
      // Old MDS daemons request the state, instead of explicitly
      // advertising that they are configured as a replay daemon.
      state = MDSMap::STATE_STANDBY;
    }
    if (header.version >= 8) {
      decode(fs, p);
    }
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
