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


#ifndef CEPH_MMONPAXOS_H
#define CEPH_MMONPAXOS_H

#include "messages/PaxosServiceMessage.h"
#include "mon/mon_types.h"
#include "include/ceph_features.h"

class MMonPaxos : public MessageInstance<MMonPaxos> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 3;

 public:
  // op types
  static constexpr int OP_COLLECT =   1; // proposer: propose round
  static constexpr int OP_LAST =      2; // voter:    accept proposed round
  static constexpr int OP_BEGIN =     3; // proposer: value proposed for this round
  static constexpr int OP_ACCEPT =    4; // voter:    accept propsed value
  static constexpr int OP_COMMIT =    5; // proposer: notify learners of agreed value
  static constexpr int OP_LEASE =     6; // leader: extend peon lease
  static constexpr int OP_LEASE_ACK = 7; // peon: lease ack
  static const char *get_opname(int op) {
    switch (op) {
    case OP_COLLECT: return "collect";
    case OP_LAST: return "last";
    case OP_BEGIN: return "begin";
    case OP_ACCEPT: return "accept";
    case OP_COMMIT: return "commit";
    case OP_LEASE: return "lease";
    case OP_LEASE_ACK: return "lease_ack";
    default: ceph_abort(); return 0;
    }
  }

  epoch_t epoch = 0;   // monitor epoch
  __s32 op = 0;          // paxos op

  version_t first_committed = 0;  // i've committed to
  version_t last_committed = 0;  // i've committed to
  version_t pn_from = 0;         // i promise to accept after
  version_t pn = 0;              // with with proposal
  version_t uncommitted_pn = 0;     // previous pn, if we are a LAST with an uncommitted value
  utime_t lease_timestamp;
  utime_t sent_timestamp;

  version_t latest_version = 0;
  bufferlist latest_value;

  map<version_t,bufferlist> values;

  bufferlist feature_map;

  MMonPaxos() : MessageInstance(MSG_MON_PAXOS, HEAD_VERSION, COMPAT_VERSION) { }
  MMonPaxos(epoch_t e, int o, utime_t now) : 
    MessageInstance(MSG_MON_PAXOS, HEAD_VERSION, COMPAT_VERSION),
    epoch(e),
    op(o),
    first_committed(0), last_committed(0), pn_from(0), pn(0), uncommitted_pn(0),
    sent_timestamp(now),
    latest_version(0) {
  }

private:
  ~MMonPaxos() override {}

public:  
  std::string_view get_type_name() const override { return "paxos"; }
  
  void print(ostream& out) const override {
    out << "paxos(" << get_opname(op) 
	<< " lc " << last_committed
	<< " fc " << first_committed
	<< " pn " << pn << " opn " << uncommitted_pn;
    if (latest_version)
      out << " latest " << latest_version << " (" << latest_value.length() << " bytes)";
    out <<  ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    header.version = HEAD_VERSION;
    encode(epoch, payload);
    encode(op, payload);
    encode(first_committed, payload);
    encode(last_committed, payload);
    encode(pn_from, payload);
    encode(pn, payload);
    encode(uncommitted_pn, payload);
    encode(lease_timestamp, payload);
    encode(sent_timestamp, payload);
    encode(latest_version, payload);
    encode(latest_value, payload);
    encode(values, payload);
    encode(feature_map, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(op, p);
    decode(first_committed, p);
    decode(last_committed, p);
    decode(pn_from, p);   
    decode(pn, p);   
    decode(uncommitted_pn, p);
    decode(lease_timestamp, p);
    decode(sent_timestamp, p);
    decode(latest_version, p);
    decode(latest_value, p);
    decode(values, p);
    if (header.version >= 4) {
      decode(feature_map, p);
    }
  }
};

#endif
