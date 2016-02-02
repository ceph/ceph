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


#ifndef CEPH_MOSDREPOP_H
#define CEPH_MOSDREPOP_H

#include "msg/Message.h"
#include "osd/osd_types.h"

/*
 * OSD sub op - for internal ops on pobjects between primary and replicas(/stripes/whatever)
 */

class MOSDRepOp : public Message {

  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:
  epoch_t map_epoch;

  // metadata from original request
  osd_reqid_t reqid;

  spg_t pgid;

  bufferlist::iterator p;
  // Decoding flags. Decoding is only needed for messages catched by pipe reader.
  bool final_decode_needed;

  // subop
  pg_shard_t from;
  hobject_t poid;

  __u8 acks_wanted;

  // transaction to exec
  bufferlist logbl;
  pg_stat_t pg_stats;

  // subop metadata
  eversion_t version;

  // piggybacked osd/og state
  eversion_t pg_trim_to;   // primary->replica: trim to here
  eversion_t pg_trim_rollback_to;   // primary->replica: trim rollback
                                    // info to here

  hobject_t new_temp_oid;      ///< new temp object that we must now start tracking
  hobject_t discard_temp_oid;  ///< previously used temp object that we can now stop tracking

  /// non-empty if this transaction involves a hit_set history update
  boost::optional<pg_hit_set_history_t> updated_hit_set_history;

  int get_cost() const {
    return data.length();
  }

  virtual void decode_payload() {
    p = payload.begin();
    // splitted to partial and final
    ::decode(map_epoch, p);
    ::decode(reqid, p);
    ::decode(pgid, p);
  }

  void finish_decode() {
    if (!final_decode_needed)
      return; // Message is already final decoded
    ::decode(poid, p);

    ::decode(acks_wanted, p);
    ::decode(version, p);
    ::decode(logbl, p);
    ::decode(pg_stats, p);
    ::decode(pg_trim_to, p);


    ::decode(new_temp_oid, p);
    ::decode(discard_temp_oid, p);

    ::decode(from, p);
    ::decode(updated_hit_set_history, p);
    ::decode(pg_trim_rollback_to, p);
    final_decode_needed = false;
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(map_epoch, payload);
    ::encode(reqid, payload);
    ::encode(pgid, payload);
    ::encode(poid, payload);

    ::encode(acks_wanted, payload);
    ::encode(version, payload);
    ::encode(logbl, payload);
    ::encode(pg_stats, payload);
    ::encode(pg_trim_to, payload);
    ::encode(new_temp_oid, payload);
    ::encode(discard_temp_oid, payload);
    ::encode(from, payload);
    ::encode(updated_hit_set_history, payload);
    ::encode(pg_trim_rollback_to, payload);
  }

  MOSDRepOp()
    : Message(MSG_OSD_REPOP, HEAD_VERSION, COMPAT_VERSION),
      map_epoch(0),
      final_decode_needed(true), acks_wanted (0) {}
  MOSDRepOp(osd_reqid_t r, pg_shard_t from,
	    spg_t p, const hobject_t& po, int aw,
	    epoch_t mape, ceph_tid_t rtid, eversion_t v)
    : Message(MSG_OSD_REPOP, HEAD_VERSION, COMPAT_VERSION),
      map_epoch(mape),
      reqid(r),
      pgid(p),
      final_decode_needed(false),
      from(from),
      poid(po),
      acks_wanted(aw),
      version(v) {
    set_tid(rtid);
  }
private:
  ~MOSDRepOp() {}

public:
  const char *get_type_name() const { return "osd_repop"; }
  void print(ostream& out) const {
    out << "osd_repop(" << reqid
          << " " << pgid;
    if (!final_decode_needed) {
        out << " " << poid << " v " << version;
      if (updated_hit_set_history)
        out << ", has_updated_hit_set_history";
    }
    out << ")";
  }
};


#endif
