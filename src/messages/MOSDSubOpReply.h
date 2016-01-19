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


#ifndef CEPH_MOSDSUBOPREPLY_H
#define CEPH_MOSDSUBOPREPLY_H

#include "msg/Message.h"

#include "MOSDSubOp.h"
#include "os/ObjectStore.h"

/*
 * OSD op reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDSubOpReply : public Message {
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;
public:
  epoch_t map_epoch;
  
  // subop metadata
  osd_reqid_t reqid;
  pg_shard_t from;
  spg_t pgid;
  hobject_t poid;

  vector<OSDOp> ops;

  // result
  __u8 ack_type;
  int32_t result;
  
  // piggybacked osd state
  eversion_t last_complete_ondisk;
  osd_peer_stat_t peer_stat;

  map<string,bufferptr> attrset;

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(map_epoch, p);
    ::decode(reqid, p);
    ::decode(pgid.pgid, p);
    ::decode(poid, p);

    unsigned num_ops;
    ::decode(num_ops, p);
    ops.resize(num_ops);
    for (unsigned i = 0; i < num_ops; i++) {
      ::decode(ops[i].op, p);
    }
    ::decode(ack_type, p);
    ::decode(result, p);
    ::decode(last_complete_ondisk, p);
    ::decode(peer_stat, p);
    ::decode(attrset, p);

    if (!poid.is_max() && poid.pool == -1)
      poid.pool = pgid.pool();

    if (header.version >= 2) {
      ::decode(from, p);
      ::decode(pgid.shard, p);
    } else {
      from = pg_shard_t(
	get_source().num(),
	shard_id_t::NO_SHARD);
      pgid.shard = shard_id_t::NO_SHARD;
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
      ::encode(ops[i].op, payload);
    }
    ::encode(ack_type, payload);
    ::encode(result, payload);
    ::encode(last_complete_ondisk, payload);
    ::encode(peer_stat, payload);
    ::encode(attrset, payload);
    ::encode(from, payload);
    ::encode(pgid.shard, payload);
  }

  epoch_t get_map_epoch() { return map_epoch; }

  spg_t get_pg() { return pgid; }
  hobject_t get_poid() { return poid; }

  int get_ack_type() { return ack_type; }
  bool is_ondisk() { return ack_type & CEPH_OSD_FLAG_ONDISK; }
  bool is_onnvram() { return ack_type & CEPH_OSD_FLAG_ONNVRAM; }

  int get_result() { return result; }

  void set_last_complete_ondisk(eversion_t v) { last_complete_ondisk = v; }
  eversion_t get_last_complete_ondisk() { return last_complete_ondisk; }

  void set_peer_stat(const osd_peer_stat_t& stat) { peer_stat = stat; }
  const osd_peer_stat_t& get_peer_stat() { return peer_stat; }

  void set_attrset(map<string,bufferptr> &as) { attrset = as; }
  map<string,bufferptr>& get_attrset() { return attrset; } 

public:
  MOSDSubOpReply(
    MOSDSubOp *req, pg_shard_t from, int result_, epoch_t e, int at) :
    Message(MSG_OSD_SUBOPREPLY, HEAD_VERSION, COMPAT_VERSION),
    map_epoch(e),
    reqid(req->reqid),
    from(from),
    pgid(req->pgid.pgid, req->from.shard),
    poid(req->poid),
    ops(req->ops),
    ack_type(at),
    result(result_) {
    memset(&peer_stat, 0, sizeof(peer_stat));
    set_tid(req->get_tid());
  }
  MOSDSubOpReply() : Message(MSG_OSD_SUBOPREPLY) {}
private:
  ~MOSDSubOpReply() {}

public:
  const char *get_type_name() const { return "osd_subop_reply"; }
  
  void print(ostream& out) const {
    out << "osd_sub_op_reply(" << reqid
	<< " " << pgid 
	<< " " << poid << " " << ops;
    if (ack_type & CEPH_OSD_FLAG_ONDISK)
      out << " ondisk";
    if (ack_type & CEPH_OSD_FLAG_ONNVRAM)
      out << " onnvram";
    if (ack_type & CEPH_OSD_FLAG_ACK)
      out << " ack";
    out << ", result = " << result;
    out << ")";
  }

};


#endif
