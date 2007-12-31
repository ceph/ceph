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


#ifndef __MOSDOPREPLY_H
#define __MOSDOPREPLY_H

#include "msg/Message.h"

#include "MOSDOp.h"
#include "osd/ObjectStore.h"

/*
 * OSD op reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDOpReply : public Message {
  struct st_t {
    // req
    osdreqid_t reqid;

    tid_t rep_tid;
    
    object_t oid;
    ObjectLayout layout;  // pgid, etc.
    
    int32_t op;
    
    // reply
    int32_t    result;
    bool   commit;
    off_t length, offset;
    off_t object_size;
    eversion_t version;
    
    eversion_t pg_complete_thru;
    
    epoch_t map_epoch;

    osd_peer_stat_t peer_stat;
  } st;

  map<string,bufferptr> attrset;

 public:
  const osdreqid_t& get_reqid() { return st.reqid; }
  long     get_tid() { return st.reqid.tid; }
  long     get_rep_tid() { return st.rep_tid; }
  object_t get_oid() { return st.oid; }
  pg_t     get_pg() { return st.layout.pgid; }
  int      get_op()  { return st.op; }
  bool     get_commit() { return st.commit; }
  
  int    get_result() { return st.result; }
  off_t get_length() { return st.length; }
  off_t get_offset() { return st.offset; }
  off_t get_object_size() { return st.object_size; }
  eversion_t get_version() { return st.version; }
  map<string,bufferptr>& get_attrset() { return attrset; }

  eversion_t get_pg_complete_thru() { return st.pg_complete_thru; }
  void set_pg_complete_thru(eversion_t v) { st.pg_complete_thru = v; }

  void set_result(int r) { st.result = r; }
  void set_length(off_t s) { st.length = s; }
  void set_offset(off_t o) { st.offset = o; }
  void set_object_size(off_t s) { st.object_size = s; }
  void set_version(eversion_t v) { st.version = v; }
  void set_attrset(map<string,bufferptr> &as) { attrset = as; }

  void set_op(int op) { st.op = op; }
  void set_rep_tid(tid_t t) { st.rep_tid = t; }

  void set_peer_stat(const osd_peer_stat_t& stat) { st.peer_stat = stat; }
  const osd_peer_stat_t& get_peer_stat() { return st.peer_stat; }

  // osdmap
  epoch_t get_map_epoch() { return st.map_epoch; }


public:
  MOSDOpReply(MOSDOp *req, int result, epoch_t e, bool commit) :
    Message(CEPH_MSG_OSD_OPREPLY) {
    memset(&st, 0, sizeof(st));
    this->st.reqid = req->st.reqid;
    this->st.op = req->st.op;
    this->st.rep_tid = req->st.rep_tid;

    this->st.oid = req->st.oid;
    this->st.layout = req->st.layout;
    this->st.result = result;
    this->st.commit = commit;

    this->st.length = req->st.length;   // speculative... OSD should ensure these are correct
    this->st.offset = req->st.offset;
    this->st.version = req->st.version;

    this->st.map_epoch = e;
  }
  MOSDOpReply() {}


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
    out << "osd_op_reply(" << st.reqid
	<< " " << MOSDOp::get_opname(st.op)
	<< " " << st.oid;
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
