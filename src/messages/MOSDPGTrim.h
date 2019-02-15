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

#ifndef CEPH_MOSDPGTRIM_H
#define CEPH_MOSDPGTRIM_H

#include "msg/Message.h"
#include "messages/MOSDPeeringOp.h"

class MOSDPGTrim : public MessageInstance<MOSDPGTrim, MOSDPeeringOp> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 2;

public:
  epoch_t epoch = 0;
  spg_t pgid;
  eversion_t trim_to;

  epoch_t get_epoch() const { return epoch; }
  spg_t get_spg() const {
    return pgid;
  }
  epoch_t get_map_epoch() const {
    return epoch;
  }
  epoch_t get_min_epoch() const {
    return epoch;
  }
  PGPeeringEvent *get_event() override {
    return new PGPeeringEvent(
      epoch,
      epoch,
      MTrim(epoch, get_source().num(), pgid.shard, trim_to));
  }

  MOSDPGTrim() : MessageInstance(MSG_OSD_PG_TRIM, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDPGTrim(version_t mv, spg_t p, eversion_t tt) :
    MessageInstance(MSG_OSD_PG_TRIM, HEAD_VERSION, COMPAT_VERSION),
    epoch(mv), pgid(p), trim_to(tt) { }
private:
  ~MOSDPGTrim() override {}

public:
  std::string_view get_type_name() const override { return "pg_trim"; }
  void inner_print(ostream& out) const override {
    out << trim_to;
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(epoch, payload);
    encode(pgid.pgid, payload);
    encode(trim_to, payload);
    encode(pgid.shard, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(epoch, p);
    decode(pgid.pgid, p);
    decode(trim_to, p);
    decode(pgid.shard, p);
  }
};

#endif
