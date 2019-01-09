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

#ifndef CEPH_MOSDPGBACKFILLREMOVE_H
#define CEPH_MOSDPGBACKFILLREMOVE_H

#include "MOSDFastDispatchOp.h"

/*
 * instruct non-primary to remove some objects during backfill
 */

class MOSDPGBackfillRemove : public MessageInstance<MOSDPGBackfillRemove, MOSDFastDispatchOp> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  spg_t pgid;            ///< target spg_t
  epoch_t map_epoch = 0;
  list<pair<hobject_t,eversion_t>> ls;    ///< objects to remove

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDPGBackfillRemove()
    : MessageInstance(MSG_OSD_PG_BACKFILL_REMOVE, HEAD_VERSION,
			COMPAT_VERSION) {}

  MOSDPGBackfillRemove(spg_t pgid, epoch_t map_epoch)
    : MessageInstance(MSG_OSD_PG_BACKFILL_REMOVE, HEAD_VERSION,
			 COMPAT_VERSION),
      pgid(pgid),
      map_epoch(map_epoch) {}

private:
  ~MOSDPGBackfillRemove() {}

public:
  std::string_view get_type_name() const override { return "backfill_remove"; }
  void print(ostream& out) const override {
    out << "backfill_remove(" << pgid << " e" << map_epoch
	<< " " << ls << ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid, payload);
    encode(map_epoch, payload);
    encode(ls, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(pgid, p);
    decode(map_epoch, p);
    decode(ls, p);
  }
};



#endif
