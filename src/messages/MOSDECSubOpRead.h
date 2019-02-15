// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef MOSDECSUBOPREAD_H
#define MOSDECSUBOPREAD_H

#include "MOSDFastDispatchOp.h"
#include "osd/ECMsgTypes.h"

class MOSDECSubOpRead : public MessageInstance<MOSDECSubOpRead, MOSDFastDispatchOp> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 1;

public:
  spg_t pgid;
  epoch_t map_epoch = 0, min_epoch = 0;
  ECSubRead op;

  int get_cost() const override {
    return 0;
  }
  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDECSubOpRead()
    : MessageInstance(MSG_OSD_EC_READ, HEAD_VERSION, COMPAT_VERSION)
    {}

  void decode_payload() override {
    auto p = payload.cbegin();
    decode(pgid, p);
    decode(map_epoch, p);
    decode(op, p);
    if (header.version >= 3) {
      decode(min_epoch, p);
      decode_trace(p);
    } else {
      min_epoch = map_epoch;
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid, payload);
    encode(map_epoch, payload);
    encode(op, payload, features);
    encode(min_epoch, payload);
    encode_trace(payload, features);
  }

  std::string_view get_type_name() const override { return "MOSDECSubOpRead"; }

  void print(ostream& out) const override {
    out << "MOSDECSubOpRead(" << pgid
	<< " " << map_epoch << "/" << min_epoch
	<< " " << op;
    out << ")";
  }
};

#endif
