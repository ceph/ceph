// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Sage Weil <sage@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MOSDREPSCRUBMAP_H
#define CEPH_MOSDREPSCRUBMAP_H

#include "MOSDFastDispatchOp.h"

/*
 * pass a ScrubMap from a shard back to the primary
 */

class MOSDRepScrubMap : public MessageInstance<MOSDRepScrubMap, MOSDFastDispatchOp> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

  spg_t pgid;            // primary spg_t
  epoch_t map_epoch = 0;
  pg_shard_t from;   // whose scrubmap this is
  bufferlist scrub_map_bl;
  bool preempted = false;

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDRepScrubMap()
    : MessageInstance(MSG_OSD_REP_SCRUBMAP, HEAD_VERSION, COMPAT_VERSION) {}

  MOSDRepScrubMap(spg_t pgid, epoch_t map_epoch, pg_shard_t from)
    : MessageInstance(MSG_OSD_REP_SCRUBMAP, HEAD_VERSION, COMPAT_VERSION),
      pgid(pgid),
      map_epoch(map_epoch),
      from(from) {}

private:
  ~MOSDRepScrubMap() {}

public:
  std::string_view get_type_name() const override { return "rep_scrubmap"; }
  void print(ostream& out) const override {
    out << "rep_scrubmap(" << pgid << " e" << map_epoch
	<< " from shard " << from
	<< (preempted ? " PREEMPTED":"") << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid, payload);
    encode(map_epoch, payload);
    encode(from, payload);
    encode(preempted, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(pgid, p);
    decode(map_epoch, p);
    decode(from, p);
    if (header.version >= 2) {
      decode(preempted, p);
    }
  }
};


#endif
