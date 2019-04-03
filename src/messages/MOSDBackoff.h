// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_MOSDBACKOFF_H
#define CEPH_MOSDBACKOFF_H

#include "MOSDFastDispatchOp.h"
#include "osd/osd_types.h"

class MOSDBackoff : public MessageInstance<MOSDBackoff, MOSDFastDispatchOp> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  spg_t pgid;
  epoch_t map_epoch = 0;
  uint8_t op = 0;           ///< CEPH_OSD_BACKOFF_OP_*
  uint64_t id = 0;          ///< unique id within this session
  hobject_t begin, end;     ///< [) range to block, unless ==, block single obj

  spg_t get_spg() const override {
    return pgid;
  }
  epoch_t get_map_epoch() const override {
    return map_epoch;
  }

  MOSDBackoff()
    : MessageInstance(CEPH_MSG_OSD_BACKOFF, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDBackoff(spg_t pgid_, epoch_t ep, uint8_t op_, uint64_t id_,
	      hobject_t begin_, hobject_t end_)
    : MessageInstance(CEPH_MSG_OSD_BACKOFF, HEAD_VERSION, COMPAT_VERSION),
      pgid(pgid_),
      map_epoch(ep),
      op(op_),
      id(id_),
      begin(begin_),
      end(end_) { }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid, payload);
    encode(map_epoch, payload);
    encode(op, payload);
    encode(id, payload);
    encode(begin, payload);
    encode(end, payload);
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(pgid, p);
    decode(map_epoch, p);
    decode(op, p);
    decode(id, p);
    decode(begin, p);
    decode(end, p);
  }

  std::string_view get_type_name() const override { return "osd_backoff"; }

  void print(std::ostream& out) const override {
    out << "osd_backoff(" << pgid << " " << ceph_osd_backoff_op_name(op)
	<< " id " << id
	<< " [" << begin << "," << end << ")"
	<< " e" << map_epoch << ")";
  }
};

#endif
