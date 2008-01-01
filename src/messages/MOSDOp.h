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


#ifndef __MOSDOP_H
#define __MOSDOP_H

#include "msg/Message.h"
#include "osd/osd_types.h"

/*
 * OSD op
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDOp : public Message {
public:
  static const char* get_opname(int op) {
    switch (op) {
    case CEPH_OSD_OP_READ: return "read";
    case CEPH_OSD_OP_STAT: return "stat";

    case CEPH_OSD_OP_WRNOOP: return "wrnoop"; 
    case CEPH_OSD_OP_WRITE: return "write"; 
    case CEPH_OSD_OP_ZERO: return "zero"; 
    case CEPH_OSD_OP_DELETE: return "delete"; 
    case CEPH_OSD_OP_TRUNCATE: return "truncate"; 
    case CEPH_OSD_OP_WRLOCK: return "wrlock"; 
    case CEPH_OSD_OP_WRUNLOCK: return "wrunlock"; 
    case CEPH_OSD_OP_RDLOCK: return "rdlock"; 
    case CEPH_OSD_OP_RDUNLOCK: return "rdunlock"; 
    case CEPH_OSD_OP_UPLOCK: return "uplock"; 
    case CEPH_OSD_OP_DNLOCK: return "dnlock"; 

    case CEPH_OSD_OP_MININCLOCK: return "mininclock";

    case CEPH_OSD_OP_BALANCEREADS: return "balance-reads";
    case CEPH_OSD_OP_UNBALANCEREADS: return "unbalance-reads";

    case CEPH_OSD_OP_PULL: return "pull";
    case CEPH_OSD_OP_PUSH: return "push";
    default: assert(0);
    }
    return 0;
  }

private:
  struct st_ {
    // who's asking?
    entity_inst_t client;
    osdreqid_t    reqid;  // minor weirdness: entity_name_t is in reqid_t too.
    
    // for replication
    tid_t rep_tid;
    
    object_t oid;
    objectrev_t rev;
    ObjectLayout layout;
    
    epoch_t map_epoch;
    
    eversion_t pg_trim_to;   // primary->replica: trim to here
    
    int32_t op;
    off_t offset, length;

    eversion_t version;
    eversion_t old_version;
    
    bool   want_ack;
    bool   want_commit;
    bool   retry_attempt;
    
    int shed_count;
    osd_peer_stat_t peer_stat;
  } st;

  map<string,bufferptr> attrset;


  friend class MOSDOpReply;

public:
  const osdreqid_t&    get_reqid() { return st.reqid; }
  const tid_t          get_client_tid() { return st.reqid.tid; }
  int                  get_client_inc() { return st.reqid.inc; }

  const entity_name_t& get_client() { return st.client.name; }
  const entity_inst_t& get_client_inst() { return st.client; }
  void set_client_inst(const entity_inst_t& i) { st.client = i; }

  bool wants_reply() {
    if (st.op < 100) return true;
    return false;  // no reply needed for primary-lock, -unlock.
  }

  const tid_t       get_rep_tid() { return st.rep_tid; }
  void set_rep_tid(tid_t t) { st.rep_tid = t; }

  bool get_retry_attempt() const { return st.retry_attempt; }
  void set_retry_attempt(bool a) { st.retry_attempt = a; }

  const object_t get_oid() { return st.oid; }
  const pg_t     get_pg() { return st.layout.pgid; }
  const ObjectLayout& get_layout() { return st.layout; }
  const epoch_t  get_map_epoch() { return st.map_epoch; }

  const eversion_t  get_version() { return st.version; }
  //const eversion_t  get_old_version() { return st.old_version; }
  
  void set_rev(objectrev_t r) { st.rev = r; }
  objectrev_t get_rev() { return st.rev; }

  const eversion_t get_pg_trim_to() { return st.pg_trim_to; }
  void set_pg_trim_to(eversion_t v) { st.pg_trim_to = v; }
  
  const int    get_op() { return st.op; }
  void set_op(int o) { st.op = o; }
  bool is_read() { 
    return st.op < 10;
  }

  const off_t get_length() { return st.length; }
  const off_t get_offset() { return st.offset; }

  map<string,bufferptr>& get_attrset() { return attrset; }
  void set_attrset(map<string,bufferptr> &as) { attrset.swap(as); }

  const bool wants_ack() { return st.want_ack; }
  const bool wants_commit() { return st.want_commit; }

  void set_peer_stat(const osd_peer_stat_t& stat) { st.peer_stat = stat; }
  const osd_peer_stat_t& get_peer_stat() { return st.peer_stat; }
  void inc_shed_count() { st.shed_count++; }
  int get_shed_count() { return st.shed_count; }
  


  MOSDOp(entity_inst_t asker, int inc, long tid,
         object_t oid, ObjectLayout ol, epoch_t mapepoch, int op) :
    Message(CEPH_MSG_OSD_OP) {
    memset(&st, 0, sizeof(st));
    this->st.client = asker;
    this->st.reqid.name = asker.name;
    this->st.reqid.inc = inc;
    this->st.reqid.tid = tid;

    this->st.oid = oid;
    this->st.layout = ol;
    this->st.map_epoch = mapepoch;
    this->st.op = op;

    this->st.rep_tid = 0;

    this->st.want_ack = true;
    this->st.want_commit = true;
  }
  MOSDOp() {}

  void set_layout(const ObjectLayout& l) { st.layout = l; }

  void set_length(off_t l) { st.length = l; }
  void set_offset(off_t o) { st.offset = o; }
  void set_version(eversion_t v) { st.version = v; }
  void set_old_version(eversion_t ov) { st.old_version = ov; }
  
  void set_want_ack(bool b) { st.want_ack = b; }
  void set_want_commit(bool b) { st.want_commit = b; }

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

  const char *get_type_name() { return "osd_op"; }
  void print(ostream& out) {
    out << "osd_op(" << st.reqid
	<< " " << get_opname(st.op)
	<< " " << st.oid;
    if (st.length) out << " " << st.offset << "~" << st.length;
    if (st.retry_attempt) out << " RETRY";
    out << ")";
  }
};


#endif
