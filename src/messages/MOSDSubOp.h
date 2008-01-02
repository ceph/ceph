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


#ifndef __MOSDSUBOP_H
#define __MOSDSUBOP_H

#include "msg/Message.h"
#include "osd/osd_types.h"

/*
 * OSD sub op - for internal ops on pobjects between primary and replicas(/stripes/whatever)
 */

class MOSDSubOp : public Message {
public:
private:
  struct st_ {
    epoch_t map_epoch;

    // metadata from original request
    osd_reqid_t reqid;

    // subop
    pg_t pgid;
    pobject_t poid;
    int32_t op;
    off_t offset, length;

    // subop metadata
    tid_t rep_tid;
    eversion_t version;

    // piggybacked osd/og state
    eversion_t pg_trim_to;   // primary->replica: trim to here
    osd_peer_stat_t peer_stat;
  } st;

  map<string,bufferptr> attrset;


public:
  const epoch_t get_map_epoch() { return st.map_epoch; }

  const osd_reqid_t&    get_reqid() { return st.reqid; }

  bool wants_reply() {
    if (st.op < 100) return true;
    return false;  // no reply needed for primary-lock, -unlock.
  }

  const pg_t get_pg() { return st.pgid; }
  const pobject_t get_poid() { return st.poid; }
  const int get_op() { return st.op; }
  bool is_read() { return st.op < 10; }
  const off_t get_length() { return st.length; }
  const off_t get_offset() { return st.offset; }

  const tid_t get_rep_tid() { return st.rep_tid; }
  const eversion_t get_version() { return st.version; }
  const eversion_t get_pg_trim_to() { return st.pg_trim_to; }
  void set_pg_trim_to(eversion_t v) { st.pg_trim_to = v; }

  map<string,bufferptr>& get_attrset() { return attrset; }
  void set_attrset(map<string,bufferptr> &as) { attrset.swap(as); }

  void set_peer_stat(const osd_peer_stat_t& stat) { st.peer_stat = stat; }
  const osd_peer_stat_t& get_peer_stat() { return st.peer_stat; }
 
  MOSDSubOp(osd_reqid_t r, pg_t p, pobject_t po, int o, off_t of, off_t le,
	    epoch_t mape, tid_t rtid, eversion_t v) :
    Message(MSG_OSD_SUBOP) {
    memset(&st, 0, sizeof(st));
    st.reqid = r;

    st.pgid = p;
    st.poid = po;
    st.op = o;
    st.offset = of;
    st.length = le;
    st.map_epoch = mape;
    st.rep_tid = rtid;
    st.version = v;
  }
  MOSDSubOp() {}

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

  const char *get_type_name() { return "osd_sub_op"; }
  void print(ostream& out) {
    out << "osd_sub_op(" << st.reqid
	<< " " << MOSDOp::get_opname(st.op)
	<< " " << st.poid
	<< " v" << st.version;    
    if (st.length) out << " " << st.offset << "~" << st.length;
    out << ")";
  }
};


#endif
