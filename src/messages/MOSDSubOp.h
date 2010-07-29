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

/*
 * OSD sub op - for internal ops on pobjects between primary and replicas(/stripes/whatever)
 */

class MOSDSubOp : public Message {
public:
  epoch_t map_epoch;
  
  // metadata from original request
  osd_reqid_t reqid;
  
  // subop
  pg_t pgid;
  sobject_t poid;
  
  __u8 acks_wanted;

  // op to exec
  vector<OSDOp> ops;
  utime_t mtime;
  bool noop;

  bool old_exists;
  uint64_t old_size;
  eversion_t old_version;

  SnapSet snapset;
  SnapContext snapc;

  // transaction to exec
  bufferlist logbl;
  pg_stat_t pg_stats;
  
  // subop metadata
  eversion_t version;

  // piggybacked osd/og state
  eversion_t pg_trim_to;   // primary->replica: trim to here
  osd_peer_stat_t peer_stat;

  map<string,bufferptr> attrset;

  interval_set<uint64_t> data_subset;
  map<sobject_t, interval_set<uint64_t> > clone_subsets;

  bool first, complete;

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(map_epoch, p);
    ::decode(reqid, p);
    ::decode(pgid, p);
    ::decode(poid, p);

    __u32 num_ops;
    ::decode(num_ops, p);
    ops.resize(num_ops);
    unsigned off = 0;
    for (unsigned i = 0; i < num_ops; i++) {
      ::decode(ops[i].op, p);
      ops[i].data.substr_of(data, off, ops[i].op.payload_len);
      off += ops[i].op.payload_len;
    }
    ::decode(mtime, p);
    ::decode(noop, p);
    ::decode(acks_wanted, p);
    ::decode(version, p);
    ::decode(old_exists, p);
    ::decode(old_size, p);
    ::decode(old_version, p);
    ::decode(snapset, p);
    ::decode(snapc, p);
    ::decode(logbl, p);
    ::decode(pg_stats, p);
    ::decode(pg_trim_to, p);
    ::decode(peer_stat, p);
    ::decode(attrset, p);
    ::decode(data_subset, p);
    ::decode(clone_subsets, p);
    
    if (header.version >= 2) {
      ::decode(first, p);
      ::decode(complete, p);
    }
  }

  virtual void encode_payload() {
    header.version = 2;

    ::encode(map_epoch, payload);
    ::encode(reqid, payload);
    ::encode(pgid, payload);
    ::encode(poid, payload);

    __u32 num_ops = ops.size();
    ::encode(num_ops, payload);
    for (unsigned i = 0; i < ops.size(); i++) {
      ops[i].op.payload_len = ops[i].data.length();
      ::encode(ops[i].op, payload);
      data.append(ops[i].data);
    }
    ::encode(mtime, payload);
    ::encode(noop, payload);
    ::encode(acks_wanted, payload);
    ::encode(version, payload);
    ::encode(old_exists, payload);
    ::encode(old_size, payload);
    ::encode(old_version, payload);
    ::encode(snapset, payload);
    ::encode(snapc, payload);
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
  }


  MOSDSubOp(osd_reqid_t r, pg_t p, sobject_t po, bool noop_, int aw,
	    epoch_t mape, tid_t rtid, eversion_t v) :
    Message(MSG_OSD_SUBOP),
    map_epoch(mape),
    reqid(r),
    pgid(p),
    poid(po),
    acks_wanted(aw),
    noop(noop_),   
    old_exists(false), old_size(0),
    version(v),
    first(false), complete(false)
  {
    memset(&peer_stat, 0, sizeof(peer_stat));
    set_tid(rtid);
  }
  MOSDSubOp() {}
private:
  ~MOSDSubOp() {}

public:
  const char *get_type_name() { return "osd_sub_op"; }
  void print(ostream& out) {
    out << "osd_sub_op(" << reqid
	<< " " << pgid
	<< " " << poid
	<< " " << ops;
    if (noop)
      out << " (NOOP)";
    if (first)
      out << " first";
    if (complete)
      out << " complete";
    out << " v " << version
	<< " snapset=" << snapset << " snapc=" << snapc;    
    if (!data_subset.empty()) out << " subset " << data_subset;
    out << ")";
  }
};


#endif
