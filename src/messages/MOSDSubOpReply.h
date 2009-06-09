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


#ifndef __MOSDSUBOPREPLY_H
#define __MOSDSUBOPREPLY_H

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
public:
  epoch_t map_epoch;
  
  // subop metadata
  osd_reqid_t reqid;
  pg_t pgid;
  tid_t rep_tid;
  sobject_t poid;

  vector<OSDOp> ops;

  // result
  __u8 ack_type;
  int32_t result;
  
  // piggybacked osd state
  eversion_t pg_complete_thru;
  osd_peer_stat_t peer_stat;

  map<string,bufferptr> attrset;

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(map_epoch, p);
    ::decode(reqid, p);
    ::decode(pgid, p);
    ::decode(rep_tid, p);
    ::decode(poid, p);

    unsigned num_ops;
    ::decode(num_ops, p);
    ops.resize(num_ops);
    unsigned off = 0;
    for (unsigned i = 0; i < num_ops; i++) {
      ::decode(ops[i].op, p);
      ops[i].data.substr_of(data, off, ops[i].op.payload_len);
      off += ops[i].op.payload_len;
    }
    ::decode(ack_type, p);
    ::decode(result, p);
    ::decode(pg_complete_thru, p);
    ::decode(peer_stat, p);
    ::decode(attrset, p);
  }
  virtual void encode_payload() {
    ::encode(map_epoch, payload);
    ::encode(reqid, payload);
    ::encode(pgid, payload);
    ::encode(rep_tid, payload);
    ::encode(poid, payload);
    __u32 num_ops = ops.size();
    ::encode(num_ops, payload);
    for (unsigned i = 0; i < ops.size(); i++) {
      ops[i].op.payload_len = ops[i].data.length();
      ::encode(ops[i].op, payload);
      data.append(ops[i].data);
    }
    ::encode(ack_type, payload);
    ::encode(result, payload);
    ::encode(pg_complete_thru, payload);
    ::encode(peer_stat, payload);
    ::encode(attrset, payload);
  }

  epoch_t get_map_epoch() { return map_epoch; }

  pg_t get_pg() { return pgid; }
  tid_t get_rep_tid() { return rep_tid; }
  sobject_t get_poid() { return poid; }

  int get_ack_type() { return ack_type; }
  bool is_ondisk() { return ack_type & CEPH_OSD_FLAG_ONDISK; }
  bool is_onnvram() { return ack_type & CEPH_OSD_FLAG_ONNVRAM; }

  int get_result() { return result; }

  void set_pg_complete_thru(eversion_t v) { pg_complete_thru = v; }
  eversion_t get_pg_complete_thru() { return pg_complete_thru; }

  void set_peer_stat(const osd_peer_stat_t& stat) { peer_stat = stat; }
  const osd_peer_stat_t& get_peer_stat() { return peer_stat; }

  void set_attrset(map<string,bufferptr> &as) { attrset = as; }
  map<string,bufferptr>& get_attrset() { return attrset; } 

public:
  MOSDSubOpReply(MOSDSubOp *req, int result_, epoch_t e, int at) :
    Message(MSG_OSD_SUBOPREPLY),
    map_epoch(e),
    reqid(req->reqid),
    pgid(req->pgid),
    rep_tid(req->rep_tid),
    poid(req->poid),
    ops(req->ops),
    ack_type(at),
    result(result_) {
    memset(&peer_stat, 0, sizeof(peer_stat));
  }
  MOSDSubOpReply() {}

  const char *get_type_name() { return "osd_op_reply"; }
  
  void print(ostream& out) {
    out << "osd_sub_op_reply(" << reqid
	<< " " << pgid 
	<< " " << poid << " " << ops;
    if (ack_type & CEPH_OSD_FLAG_ONDISK)
      out << " ondisk";
    if (ack_type & CEPH_OSD_FLAG_ONNVRAM)
      out << " onnvram";
    if (ack_type & CEPH_OSD_FLAG_ACK)
      out << " ack";
    out << " = " << result;
    out << ")";
  }

};


#endif
