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


#ifndef CEPH_MOSDREPSCRUB_H
#define CEPH_MOSDREPSCRUB_H

#include "MOSDFastDispatchOp.h"

/*
 * instruct an OSD initiate a replica scrub on a specific PG
 */

class MOSDRepScrub : public MessageInstance<MOSDRepScrub, MOSDFastDispatchOp> {
public:
  friend factory;

  static constexpr int HEAD_VERSION = 9;
  static constexpr int COMPAT_VERSION = 6;

  spg_t pgid;             // PG to scrub
  eversion_t scrub_from; // only scrub log entries after scrub_from
  eversion_t scrub_to;   // last_update_applied when message sent
  epoch_t map_epoch = 0, min_epoch = 0;
  bool chunky;           // true for chunky scrubs
  hobject_t start;       // lower bound of scrub, inclusive
  hobject_t end;         // upper bound of scrub, exclusive
  bool deep;             // true if scrub should be deep
  bool allow_preemption = false;
  int32_t priority = 0;
  bool high_priority = false;

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  epoch_t get_min_epoch() const override {
    return min_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDRepScrub()
    : MessageInstance(MSG_OSD_REP_SCRUB, HEAD_VERSION, COMPAT_VERSION),
      chunky(false),
      deep(false) { }

  MOSDRepScrub(spg_t pgid, eversion_t scrub_to, epoch_t map_epoch, epoch_t min_epoch,
               hobject_t start, hobject_t end, bool deep,
	       bool preemption, int prio, bool highprio)
    : MessageInstance(MSG_OSD_REP_SCRUB, HEAD_VERSION, COMPAT_VERSION),
      pgid(pgid),
      scrub_to(scrub_to),
      map_epoch(map_epoch),
      min_epoch(min_epoch),
      chunky(true),
      start(start),
      end(end),
      deep(deep),
      allow_preemption(preemption),
      priority(prio),
      high_priority(highprio) { }


private:
  ~MOSDRepScrub() override {}

public:
  std::string_view get_type_name() const override { return "replica scrub"; }
  void print(ostream& out) const override {
    out << "replica_scrub(pg: "	<< pgid
	<< ",from:" << scrub_from
	<< ",to:" << scrub_to
        << ",epoch:" << map_epoch << "/" << min_epoch
	<< ",start:" << start << ",end:" << end
        << ",chunky:" << chunky
        << ",deep:" << deep
        << ",version:" << header.version
	<< ",allow_preemption:" << (int)allow_preemption
	<< ",priority=" << priority
	<< (high_priority ? " (high)":"")
	<< ")";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(pgid.pgid, payload);
    encode(scrub_from, payload);
    encode(scrub_to, payload);
    encode(map_epoch, payload);
    encode(chunky, payload);
    encode(start, payload);
    encode(end, payload);
    encode(deep, payload);
    encode(pgid.shard, payload);
    encode((uint32_t)-1, payload); // seed
    encode(min_epoch, payload);
    encode(allow_preemption, payload);
    encode(priority, payload);
    encode(high_priority, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(pgid.pgid, p);
    decode(scrub_from, p);
    decode(scrub_to, p);
    decode(map_epoch, p);
    decode(chunky, p);
    decode(start, p);
    decode(end, p);
    decode(deep, p);
    decode(pgid.shard, p);
    {
      uint32_t seed;
      decode(seed, p);
    }
    if (header.version >= 7) {
      decode(min_epoch, p);
    } else {
      min_epoch = map_epoch;
    }
    if (header.version >= 8) {
      decode(allow_preemption, p);
    }
    if (header.version >= 9) {
      decode(priority, p);
      decode(high_priority, p);
    }
  }
};

#endif
