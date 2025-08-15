// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <map>
#include <string>
#include <string_view>

#include "mds/MDSHealth.h"
#include "msg/Message.h"
#include "messages/PaxosServiceMessage.h"

#include "include/types.h"

#include "mds/MDSMap.h"

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
