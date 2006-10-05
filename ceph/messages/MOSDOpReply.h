// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


typedef struct {
  // req
  long pcid;
  tid_t tid;
  tid_t rep_tid;

  object_t oid;
  pg_t pg;

  int op;
  
  // reply
  int    result;
  bool   commit;
  size_t length, offset;
  size_t object_size;
  eversion_t version;

  eversion_t pg_complete_thru;

  epoch_t map_epoch;

  size_t _data_len;//, _oc_len;
} MOSDOpReply_st;


class MOSDOpReply : public Message {
  MOSDOpReply_st st;
  bufferlist data;
  map<string,bufferptr> attrset;

 public:
  long     get_tid() { return st.tid; }
  long     get_rep_tid() { return st.rep_tid; }
  object_t get_oid() { return st.oid; }
  pg_t     get_pg() { return st.pg; }
  int      get_op()  { return st.op; }
  bool     get_commit() { return st.commit; }
  
  int    get_result() { return st.result; }
  size_t get_length() { return st.length; }
  size_t get_offset() { return st.offset; }
  size_t get_object_size() { return st.object_size; }
  eversion_t get_version() { return st.version; }
  map<string,bufferptr>& get_attrset() { return attrset; }

  eversion_t get_pg_complete_thru() { return st.pg_complete_thru; }
  void set_pg_complete_thru(eversion_t v) { st.pg_complete_thru = v; }

  void set_result(int r) { st.result = r; }
  void set_length(size_t s) { st.length = s; }
  void set_offset(size_t o) { st.offset = o; }
  void set_object_size(size_t s) { st.object_size = s; }
  void set_version(eversion_t v) { st.version = v; }
  void set_attrset(map<string,bufferptr> &as) { attrset = as; }

  void set_op(int op) { st.op = op; }
  void set_tid(tid_t t) { st.tid = t; }
  void set_rep_tid(tid_t t) { st.rep_tid = t; }

  // data payload
  void set_data(bufferlist &d) {
    data.claim(d);
    //st._data_len = data.length();
  }
  bufferlist& get_data() {
    return data;
  }

  // osdmap
  epoch_t get_map_epoch() { return st.map_epoch; }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid()          { return st.pcid; }

public:
  MOSDOpReply(MOSDOp *req, int result, epoch_t e, bool commit) :
    Message(MSG_OSD_OPREPLY) {
    memset(&st, 0, sizeof(st));
    this->st.pcid = req->st.pcid;

    this->st.op = req->st.op;
    this->st.tid = req->st.tid;
    this->st.rep_tid = req->st.rep_tid;

    this->st.oid = req->st.oid;
    this->st.pg = req->st.pg;
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
    payload.copy(0, sizeof(st), (char*)&st);
    payload.splice(0, sizeof(st));
    int off = 0;
    ::_decode(attrset, payload, off);
    if (st._data_len) payload.splice(off, st._data_len, &data);
  }
  virtual void encode_payload() {
    st._data_len = data.length();
    payload.push_back( new buffer((char*)&st, sizeof(st)) );
    ::_encode(attrset, payload);
    payload.claim_append( data );
  }

  virtual char *get_type_name() { return "oopr"; }
};

#endif
