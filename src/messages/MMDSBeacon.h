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
  MDS_HEALTH_DAMAGE
};

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

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    assert(type != MDS_HEALTH_NULL);
    ::encode((uint16_t)type, bl);
    ::encode((uint8_t)sev, bl);
    ::encode(message, bl);
    ::encode(metadata, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode((uint16_t&)type, bl);
    assert(type != MDS_HEALTH_NULL);
    ::decode((uint8_t&)sev, bl);
    ::decode(message, bl);
    ::decode(metadata, bl);
    DECODE_FINISH(bl);
  }

  bool operator==(MDSHealthMetric const &other) const
  {
    return (type == other.type && sev == other.sev && message == other.message);
  }

  MDSHealthMetric() : type(MDS_HEALTH_NULL), sev(HEALTH_OK) {}
  MDSHealthMetric(mds_metric_t type_, health_status_t sev_, std::string const &message_)
    : type(type_), sev(sev_), message(message_) {}
};
WRITE_CLASS_ENCODER(MDSHealthMetric)


/**
 * Health metrics send by the MDS to the mon, so that the mon can generate
 * user friendly warnings about undesirable states.
 */
struct MDSHealth
{
  std::list<MDSHealthMetric> metrics;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(metrics, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(metrics, bl);
    DECODE_FINISH(bl);
  }

  bool operator==(MDSHealth const &other) const
  {
    return metrics == other.metrics;
  }
};
WRITE_CLASS_ENCODER(MDSHealth)


class MMDSBeacon : public PaxosServiceMessage {

  static const int HEAD_VERSION = 6;
  static const int COMPAT_VERSION = 2;

  uuid_d fsid;
  mds_gid_t global_id;
  string name;

  MDSMap::DaemonState state;
  version_t seq;

  mds_rank_t      standby_for_rank;
  string          standby_for_name;
  fs_cluster_id_t standby_for_fscid;

  CompatSet compat;

  MDSHealth health;

  map<string, string> sys_info;

  uint64_t mds_features;

 public:
  MMDSBeacon() : PaxosServiceMessage(MSG_MDS_BEACON, 0, HEAD_VERSION, COMPAT_VERSION) { }
  MMDSBeacon(const uuid_d &f, mds_gid_t g, string& n, epoch_t les, MDSMap::DaemonState st, version_t se, uint64_t feat) :
    PaxosServiceMessage(MSG_MDS_BEACON, les, HEAD_VERSION, COMPAT_VERSION),
    fsid(f), global_id(g), name(n), state(st), seq(se),
    standby_for_rank(MDS_RANK_NONE), standby_for_fscid(FS_CLUSTER_ID_NONE),
    mds_features(feat) {
  }
private:
  ~MMDSBeacon() {}

public:
  uuid_d& get_fsid() { return fsid; }
  mds_gid_t get_global_id() { return global_id; }
  string& get_name() { return name; }
  epoch_t get_last_epoch_seen() { return version; }
  MDSMap::DaemonState get_state() { return state; }
  version_t get_seq() { return seq; }
  const char *get_type_name() const { return "mdsbeacon"; }
  mds_rank_t get_standby_for_rank() { return standby_for_rank; }
  const string& get_standby_for_name() { return standby_for_name; }
  const fs_cluster_id_t& get_standby_for_fscid() { return standby_for_fscid; }
  uint64_t get_mds_features() const { return mds_features; }

  CompatSet const& get_compat() const { return compat; }
  void set_compat(const CompatSet& c) { compat = c; }

  MDSHealth const& get_health() const { return health; }
  void set_health(const MDSHealth &h) { health = h; }

  void set_standby_for_rank(mds_rank_t r) { standby_for_rank = r; }
  void set_standby_for_name(string& n) { standby_for_name = n; }
  void set_standby_for_name(const char* c) { standby_for_name.assign(c); }
  void set_standby_for_fscid(fs_cluster_id_t f) { standby_for_fscid = f; }

  const map<string, string>& get_sys_info() const { return sys_info; }
  void set_sys_info(const map<string, string>& i) { sys_info = i; }

  void print(ostream& out) const {
    out << "mdsbeacon(" << global_id << "/" << name << " " << ceph_mds_state_name(state) 
	<< " seq " << seq << " v" << version << ")";
  }

  void encode_payload(uint64_t features) {
    paxos_encode();
    ::encode(fsid, payload);
    ::encode(global_id, payload);
    ::encode((__u32)state, payload);
    ::encode(seq, payload);
    ::encode(name, payload);
    ::encode(standby_for_rank, payload);
    ::encode(standby_for_name, payload);
    ::encode(compat, payload);
    ::encode(health, payload);
    if (state == MDSMap::STATE_BOOT) {
      ::encode(sys_info, payload);
    }
    ::encode(mds_features, payload);
    ::encode(standby_for_fscid, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    paxos_decode(p);
    ::decode(fsid, p);
    ::decode(global_id, p);
    ::decode((__u32&)state, p);
    ::decode(seq, p);
    ::decode(name, p);
    ::decode(standby_for_rank, p);
    ::decode(standby_for_name, p);
    if (header.version >= 2)
      ::decode(compat, p);
    if (header.version >= 3) {
      ::decode(health, p);
    }
    if (state == MDSMap::STATE_BOOT &&
	header.version >= 4) {
      ::decode(sys_info, p);
    }
    if (header.version >= 5) {
      ::decode(mds_features, p);
    }
    if (header.version >= 6) {
      ::decode(standby_for_fscid, p);
    }
  }
};

#endif
