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


#ifndef CEPH_MMONELECTION_H
#define CEPH_MMONELECTION_H

#include "msg/Message.h"
#include "mon/MonMap.h"
#include "mon/mon_types.h"

class MMonElection : public MessageInstance<MMonElection> {
public:
  friend factory;

private:
  static constexpr int HEAD_VERSION = 8;
  static constexpr int COMPAT_VERSION = 5;

public:
  static constexpr int OP_PROPOSE = 1;
  static constexpr int OP_ACK     = 2;
  static constexpr int OP_NAK     = 3;
  static constexpr int OP_VICTORY = 4;
  static const char *get_opname(int o) {
    switch (o) {
    case OP_PROPOSE: return "propose";
    case OP_ACK: return "ack";
    case OP_NAK: return "nak";
    case OP_VICTORY: return "victory";
    default: ceph_abort(); return 0;
    }
  }
  
  uuid_d fsid;
  int32_t op;
  epoch_t epoch;
  bufferlist monmap_bl;
  set<int32_t> quorum;
  uint64_t quorum_features;
  mon_feature_t mon_features;
  uint8_t mon_release = 0;
  bufferlist sharing_bl;
  map<string,string> metadata;
  
  MMonElection() : MessageInstance(MSG_MON_ELECTION, HEAD_VERSION, COMPAT_VERSION),
    op(0), epoch(0),
    quorum_features(0),
    mon_features(0)
  { }

  MMonElection(int o, epoch_t e, MonMap *m)
    : MessageInstance(MSG_MON_ELECTION, HEAD_VERSION, COMPAT_VERSION),
      fsid(m->fsid), op(o), epoch(e),
      quorum_features(0),
      mon_features(0)
  {
    // encode using full feature set; we will reencode for dest later,
    // if necessary
    m->encode(monmap_bl, CEPH_FEATURES_ALL);
  }
private:
  ~MMonElection() override {}

public:  
  std::string_view get_type_name() const override { return "election"; }
  void print(ostream& out) const override {
    out << "election(" << fsid << " " << get_opname(op)
	<< " rel " << (int)mon_release << " e" << epoch << ")";
  }
  
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    if (monmap_bl.length() && (features != CEPH_FEATURES_ALL)) {
      // reencode old-format monmap
      MonMap t;
      t.decode(monmap_bl);
      monmap_bl.clear();
      t.encode(monmap_bl, features);
    }

    encode(fsid, payload);
    encode(op, payload);
    encode(epoch, payload);
    encode(monmap_bl, payload);
    encode(quorum, payload);
    encode(quorum_features, payload);
    encode((version_t)0, payload);  // defunct
    encode((version_t)0, payload);  // defunct
    encode(sharing_bl, payload);
    encode(mon_features, payload);
    encode(metadata, payload);
    encode(mon_release, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(fsid, p);
    decode(op, p);
    decode(epoch, p);
    decode(monmap_bl, p);
    decode(quorum, p);
    decode(quorum_features, p);
    {
      version_t v;  // defunct fields from old encoding
      decode(v, p);
      decode(v, p);
    }
    decode(sharing_bl, p);
    if (header.version >= 6)
      decode(mon_features, p);
    if (header.version >= 7)
      decode(metadata, p);
    if (header.version >= 8)
      decode(mon_release, p);
    else
      mon_release = infer_ceph_release_from_mon_features(mon_features);
  }
  
};

#endif
