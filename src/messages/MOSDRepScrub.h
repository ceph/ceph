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

#include "msg/Message.h"

/*
 * instruct an OSD initiate a replica scrub on a specific PG
 */

struct MOSDRepScrub : public Message {

  static const int HEAD_VERSION = 6;
  static const int COMPAT_VERSION = 2;

  spg_t pgid;             // PG to scrub
  eversion_t scrub_from; // only scrub log entries after scrub_from
  eversion_t scrub_to;   // last_update_applied when message sent
  epoch_t map_epoch;
  bool chunky;           // true for chunky scrubs
  hobject_t start;       // lower bound of scrub, inclusive
  hobject_t end;         // upper bound of scrub, exclusive
  bool deep;             // true if scrub should be deep
  uint32_t seed;         // seed value for digest calculation

  MOSDRepScrub()
    : Message(MSG_OSD_REP_SCRUB, HEAD_VERSION, COMPAT_VERSION),
      chunky(false),
      deep(false),
      seed(0) { }

  MOSDRepScrub(spg_t pgid, eversion_t scrub_to, epoch_t map_epoch,
               hobject_t start, hobject_t end, bool deep, uint32_t seed)
    : Message(MSG_OSD_REP_SCRUB, HEAD_VERSION, COMPAT_VERSION),
      pgid(pgid),
      scrub_to(scrub_to),
      map_epoch(map_epoch),
      chunky(true),
      start(start),
      end(end),
      deep(deep),
      seed(seed) { }


private:
  ~MOSDRepScrub() {}

public:
  const char *get_type_name() const { return "replica scrub"; }
  void print(ostream& out) const {
    out << "replica scrub(pg: ";
    out << pgid << ",from:" << scrub_from << ",to:" << scrub_to
        << ",epoch:" << map_epoch << ",start:" << start << ",end:" << end
        << ",chunky:" << chunky
        << ",deep:" << deep
	<< ",seed:" << seed
        << ",version:" << header.version;
    out << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(pgid.pgid, payload);
    ::encode(scrub_from, payload);
    ::encode(scrub_to, payload);
    ::encode(map_epoch, payload);
    ::encode(chunky, payload);
    ::encode(start, payload);
    ::encode(end, payload);
    ::encode(deep, payload);
    ::encode(pgid.shard, payload);
    ::encode(seed, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(pgid.pgid, p);
    ::decode(scrub_from, p);
    ::decode(scrub_to, p);
    ::decode(map_epoch, p);

    if (header.version >= 3) {
      ::decode(chunky, p);
      ::decode(start, p);
      ::decode(end, p);
      if (header.version >= 4) {
        ::decode(deep, p);
      } else {
        deep = false;
      }
    } else { // v2 scrub: non-chunky
      chunky = false;
      deep = false;
    }

    if (header.version >= 5) {
      ::decode(pgid.shard, p);
    } else {
      pgid.shard = shard_id_t::NO_SHARD;
    }
    if (header.version >= 6) {
      ::decode(seed, p);
    } else {
      seed = 0;
    }
  }
};
REGISTER_MESSAGE(MOSDRepScrub, MSG_OSD_REP_SCRUB);
#endif
