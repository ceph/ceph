// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef MOSDRECOVERYDELETEREPLY_H
#define MOSDRECOVERYDELETEREPLY_H

#include "MOSDFastDispatchOp.h"

struct MOSDPGRecoveryDeleteReply : public MOSDFastDispatchOp {
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;

  pg_shard_t from;
  spg_t pgid;
  epoch_t map_epoch, min_epoch;
  list<pair<hobject_t, eversion_t> > objects;

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDPGRecoveryDeleteReply()
    : MOSDFastDispatchOp(MSG_OSD_PG_RECOVERY_DELETE_REPLY, HEAD_VERSION, COMPAT_VERSION),
      map_epoch(0), min_epoch(0)
    {}

  void decode_payload() override {
    bufferlist::iterator p = payload.begin();
    decode(pgid.pgid, p);
    decode(map_epoch, p);
    decode(min_epoch, p);
    decode(objects, p);
    decode(pgid.shard, p);
    decode(from, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid.pgid, payload);
    encode(map_epoch, payload);
    encode(min_epoch, payload);
    encode(objects, payload);
    encode(pgid.shard, payload);
    encode(from, payload);
  }

  void print(ostream& out) const override {
    out << "MOSDPGRecoveryDeleteReply(" << pgid
        << " e" << map_epoch << "," << min_epoch << " " << objects << ")";
  }

  const char *get_type_name() const override { return "recovery_delete_reply"; }
};

#endif
