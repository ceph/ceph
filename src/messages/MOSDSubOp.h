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


#ifndef CEPH_MOSDSUBOP_H
#define CEPH_MOSDSUBOP_H

#include "msg/Message.h"
#include "osd/osd_types.h"

#include "include/ceph_features.h"

/*
 * OSD sub op - for internal ops on pobjects between primary and replicas(/stripes/whatever)
 */

class MOSDSubOp : public Message {

  static const int HEAD_VERSION = 12;
  static const int COMPAT_VERSION = 7;

public:
  epoch_t map_epoch;
  
  // metadata from original request
  osd_reqid_t reqid;
  
  // subop
  pg_shard_t from;
  spg_t pgid;
  hobject_t poid;
  object_locator_t oloc;
  
  __u8 acks_wanted;

  // op to exec
  vector<OSDOp> ops;
  utime_t mtime;

  bool old_exists;
  uint64_t old_size;
  eversion_t old_version;

  SnapSet snapset;

  // transaction to exec
  bufferlist logbl;
  pg_stat_t pg_stats;
  
  // subop metadata
  eversion_t version;

  // piggybacked osd/og state
  eversion_t pg_trim_to;   // primary->replica: trim to here
  eversion_t pg_trim_rollback_to;   // primary->replica: trim rollback
                                    // info to here
  osd_peer_stat_t peer_stat;

  map<string,bufferlist> attrset;

  interval_set<uint64_t> data_subset;
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator> clone_subsets;

  bool first, complete;

  interval_set<uint64_t> data_included;
  ObjectRecoveryInfo recovery_info;

  // reflects result of current push
  ObjectRecoveryProgress recovery_progress;

  // reflects progress before current push
  ObjectRecoveryProgress current_progress;

  map<string,bufferlist> omap_entries;
  bufferlist omap_header;


  hobject_t new_temp_oid;      ///< new temp object that we must now start tracking
  hobject_t discard_temp_oid;  ///< previously used temp object that we can now stop tracking

  /// non-empty if this transaction involves a hit_set history update
  boost::optional<pg_hit_set_history_t> updated_hit_set_history;

  int get_cost() const {
    if (ops.size() == 1 && ops[0].op.op == CEPH_OSD_OP_PULL)
      return ops[0].op.extent.length;
    return data.length();
  }

  virtual void decode_payload() {
    //since we drop incorrect_pools flag, now we only support
    //version >=7
    assert (header.version >= 7);
    bufferlist::iterator p = payload.begin();
    ::decode(map_epoch, p);
    ::decode(reqid, p);
    ::decode(pgid.pgid, p);
    ::decode(poid, p);

    __u32 num_ops;
    ::decode(num_ops, p);
    ops.resize(num_ops);
    unsigned off = 0;
    for (unsigned i = 0; i < num_ops; i++) {
      ::decode(ops[i].op, p);
      ops[i].indata.substr_of(data, off, ops[i].op.payload_len);
      off += ops[i].op.payload_len;
    }
    ::decode(mtime, p);
    //we don't need noop anymore
    bool noop_dont_need;
    ::decode(noop_dont_need, p);

    ::decode(acks_wanted, p);
    ::decode(version, p);
    ::decode(old_exists, p);
    ::decode(old_size, p);
    ::decode(old_version, p);
    ::decode(snapset, p);

    if (header.version <= 11) {
      SnapContext snapc_dont_need;
      ::decode(snapc_dont_need, p);
    }

    ::decode(logbl, p);
    ::decode(pg_stats, p);
    ::decode(pg_trim_to, p);
    ::decode(peer_stat, p);
    ::decode(attrset, p);

    ::decode(data_subset, p);
    ::decode(clone_subsets, p);
    
    ::decode(first, p);
    ::decode(complete, p);
    ::decode(oloc, p);
    ::decode(data_included, p);
    recovery_info.decode(p, pgid.pool());
    ::decode(recovery_progress, p);
    ::decode(current_progress, p);
    ::decode(omap_entries, p);
    ::decode(omap_header, p);

    if (header.version >= 8) {
      ::decode(new_temp_oid, p);
      ::decode(discard_temp_oid, p);
    }

    if (header.version >= 9) {
      ::decode(from, p);
      ::decode(pgid.shard, p);
    } else {
      from = pg_shard_t(
	get_source().num(),
	shard_id_t::NO_SHARD);
      pgid.shard = shard_id_t::NO_SHARD;
    }
    if (header.version >= 10) {
      ::decode(updated_hit_set_history, p);
    }
    if (header.version >= 11) {
      ::decode(pg_trim_rollback_to, p);
    } else {
      pg_trim_rollback_to = pg_trim_to;
    }
  }

  void finish_decode() { }

  virtual void encode_payload(uint64_t features) {
    ::encode(map_epoch, payload);
    ::encode(reqid, payload);
    ::encode(pgid.pgid, payload);
    ::encode(poid, payload);

    __u32 num_ops = ops.size();
    ::encode(num_ops, payload);
    for (unsigned i = 0; i < ops.size(); i++) {
      ops[i].op.payload_len = ops[i].indata.length();
      ::encode(ops[i].op, payload);
      data.append(ops[i].indata);
    }
    ::encode(mtime, payload);
    //encode a false here for backward compatiable
    ::encode(false, payload);
    ::encode(acks_wanted, payload);
    ::encode(version, payload);
    ::encode(old_exists, payload);
    ::encode(old_size, payload);
    ::encode(old_version, payload);
    ::encode(snapset, payload);

    if ((features & CEPH_FEATURE_OSDSUBOP_NO_SNAPCONTEXT) == 0) {
      header.version = 11;
      SnapContext dummy_snapc;
      ::encode(dummy_snapc, payload);
    }

    ::encode(logbl, payload);
    ::encode(pg_stats, payload);
    ::encode(pg_trim_to, payload);
    ::encode(peer_stat, payload);
    ::encode(attrset, payload);
    ::encode(data_subset, payload);
    ::encode(clone_subsets, payload);
    if (ops.size())
      header.data_off = ops[0].op.extent.offset;
    else
      header.data_off = 0;
    ::encode(first, payload);
    ::encode(complete, payload);
    ::encode(oloc, payload);
    ::encode(data_included, payload);
    ::encode(recovery_info, payload);
    ::encode(recovery_progress, payload);
    ::encode(current_progress, payload);
    ::encode(omap_entries, payload);
    ::encode(omap_header, payload);
    ::encode(new_temp_oid, payload);
    ::encode(discard_temp_oid, payload);
    ::encode(from, payload);
    ::encode(pgid.shard, payload);
    ::encode(updated_hit_set_history, payload);
    ::encode(pg_trim_rollback_to, payload);
  }

  MOSDSubOp()
    : Message(MSG_OSD_SUBOP, HEAD_VERSION, COMPAT_VERSION) { }
  MOSDSubOp(osd_reqid_t r, pg_shard_t from,
	    spg_t p, const hobject_t& po,  int aw,
	    epoch_t mape, ceph_tid_t rtid, eversion_t v)
    : Message(MSG_OSD_SUBOP, HEAD_VERSION, COMPAT_VERSION),
      map_epoch(mape),
      reqid(r),
      from(from),
      pgid(p),
      poid(po),
      acks_wanted(aw),
      old_exists(false), old_size(0),
      version(v),
      first(false), complete(false) {
    memset(&peer_stat, 0, sizeof(peer_stat));
    set_tid(rtid);
  }
private:
  ~MOSDSubOp() {}

public:
  const char *get_type_name() const { return "osd_sub_op"; }
  void print(ostream& out) const {
    out << "osd_sub_op(" << reqid
	<< " " << pgid
	<< " " << poid
	<< " " << ops;
    if (first)
      out << " first";
    if (complete)
      out << " complete";
    out << " v " << version
	<< " snapset=" << snapset;
    if (!data_subset.empty()) out << " subset " << data_subset;
    if (updated_hit_set_history)
      out << ", has_updated_hit_set_history";
    out << ")";
  }
};

REGISTER_MESSAGE(MOSDSubOp, MSG_OSD_SUBOP);
#endif
