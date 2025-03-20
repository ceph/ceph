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


#ifndef CEPH_MMONPROBE_H
#define CEPH_MMONPROBE_H

#include "include/ceph_features.h"
#include "common/ceph_releases.h"
#include "msg/Message.h"
#include "mon/MonMap.h"

class MMonProbe final : public Message {
public:
  static constexpr int HEAD_VERSION = 8;
  static constexpr int COMPAT_VERSION = 5;

  enum {
    OP_PROBE = 1,
    OP_REPLY = 2,
    OP_SLURP = 3,
    OP_SLURP_LATEST = 4,
    OP_DATA = 5,
    OP_MISSING_FEATURES = 6,
  };

  static const char *get_opname(int o) {
    switch (o) {
    case OP_PROBE: return "probe";
    case OP_REPLY: return "reply";
    case OP_SLURP: return "slurp";
    case OP_SLURP_LATEST: return "slurp_latest";
    case OP_DATA: return "data";
    case OP_MISSING_FEATURES: return "missing_features";
    default: ceph_abort(); return 0;
    }
  }
  
  uuid_d fsid;
  int32_t op = 0;
  std::string name;
  std::set<int32_t> quorum;
  int leader = -1;
  ceph::buffer::list monmap_bl;
  version_t paxos_first_version = 0;
  version_t paxos_last_version = 0;
  bool has_ever_joined = 0;
  uint64_t required_features = 0;
  ceph_release_t mon_release{ceph_release_t::unknown};

  MMonProbe()
    : Message{MSG_MON_PROBE, HEAD_VERSION, COMPAT_VERSION} {}
  MMonProbe(const uuid_d& f, int o, const std::string& n, bool hej, ceph_release_t mr)
    : Message{MSG_MON_PROBE, HEAD_VERSION, COMPAT_VERSION},
      fsid(f),
      op(o),
      name(n),
      paxos_first_version(0),
      paxos_last_version(0),
      has_ever_joined(hej),
      required_features(0),
      mon_release{mr} {}
private:
  ~MMonProbe() final {}

public:
  std::string_view get_type_name() const override { return "mon_probe"; }
  void print(std::ostream& out) const override {
    out << "mon_probe(" << get_opname(op) << " " << fsid << " name " << name;
    if (quorum.size())
      out << " quorum " << quorum;
    out << " leader " << leader;
    if (op == OP_REPLY) {
      out << " paxos("
	<< " fc " << paxos_first_version
	<< " lc " << paxos_last_version
	<< " )";
    }
    if (!has_ever_joined)
      out << " new";
    if (required_features)
      out << " required_features " << required_features;
    if (mon_release != ceph_release_t::unknown)
      out << " mon_release " << mon_release;
    out << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    if (monmap_bl.length() &&
	((features & CEPH_FEATURE_MONENC) == 0 ||
	 (features & CEPH_FEATURE_MSG_ADDR2) == 0)) {
      // reencode old-format monmap
      MonMap t;
      t.decode(monmap_bl);
      monmap_bl.clear();
      t.encode(monmap_bl, features);
    }

    encode(fsid, payload);
    encode(op, payload);
    encode(name, payload);
    encode(quorum, payload);
    encode(monmap_bl, payload);
    encode(has_ever_joined, payload);
    encode(paxos_first_version, payload);
    encode(paxos_last_version, payload);
    encode(required_features, payload);
    encode(mon_release, payload);
    encode(leader, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(op, p);
    decode(name, p);
    decode(quorum, p);
    decode(monmap_bl, p);
    decode(has_ever_joined, p);
    decode(paxos_first_version, p);
    decode(paxos_last_version, p);
    if (header.version >= 6)
      decode(required_features, p);
    else
      required_features = 0;
    if (header.version >= 7)
      decode(mon_release, p);
    else
      mon_release = ceph_release_t::unknown;
    if (header.version >= 8) {
      decode(leader, p);
    } else if (quorum.size()) {
      leader = *quorum.begin();
    }
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
