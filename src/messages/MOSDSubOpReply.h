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
#include "osd/ObjectStore.h"

/*
 * OSD op reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDSubOpReply : public Message {
  struct st_t {
    epoch_t map_epoch;

    // subop metadata
    osd_reqid_t reqid;
    pg_t pgid;
    tid_t rep_tid;
    int32_t op;
    pobject_t poid;
    off_t length, offset;

    // result
    bool commit;
    int32_t result;

    // piggybacked osd state
    eversion_t pg_complete_thru;
    osd_peer_stat_t peer_stat;
  } st;

  map<string,bufferptr> attrset;

 public:
  epoch_t get_map_epoch() { return st.map_epoch; }

  pg_t get_pg() { return st.pgid; }
  tid_t get_rep_tid() { return st.rep_tid; }
  int get_op()  { return st.op; }
  pobject_t get_poid() { return st.poid; }
  const off_t get_length() { return st.length; }
  const off_t get_offset() { return st.offset; }

  bool get_commit() { return st.commit; }
  int get_result() { return st.result; }

  void set_pg_complete_thru(eversion_t v) { st.pg_complete_thru = v; }
  eversion_t get_pg_complete_thru() { return st.pg_complete_thru; }

  void set_peer_stat(const osd_peer_stat_t& stat) { st.peer_stat = stat; }
  const osd_peer_stat_t& get_peer_stat() { return st.peer_stat; }

  void set_attrset(map<string,bufferptr> &as) { attrset = as; }
  map<string,bufferptr>& get_attrset() { return attrset; } 

public:
  MOSDSubOpReply(MOSDSubOp *req, int result, epoch_t e, bool commit) :
    Message(MSG_OSD_SUBOPREPLY) {
    st.map_epoch = e;
    st.reqid = req->get_reqid();
    st.pgid = req->get_pg();
    st.rep_tid = req->get_rep_tid();
    st.op = req->get_op();
    st.poid = req->get_poid();
    st.commit = commit;
    st.result = result;
    st.length = req->get_length();
    st.offset = req->get_offset();
  }
  MOSDSubOpReply() {}


  // marshalling
  virtual void decode_payload() {
    int off = 0;
    ::_decode(st, payload, off);
    ::_decode(attrset, payload, off);
  }
  virtual void encode_payload() {
    ::_encode(st, payload);
    ::_encode(attrset, payload);
    env.data_off = st.offset;
  }

  const char *get_type_name() { return "osd_op_reply"; }
  
  void print(ostream& out) {
    out << "osd_sub_op_reply(" << st.reqid
	<< " " << MOSDOp::get_opname(st.op)
	<< " " << st.poid;
    if (st.length) out << " " << st.offset << "~" << st.length;
    if (st.op >= 10) {
      if (st.commit)
	out << " commit";
      else
	out << " ack";
    }
    out << " = " << st.result;
    out << ")";
  }

};


#endif
