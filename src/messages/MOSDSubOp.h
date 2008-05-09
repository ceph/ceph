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
  uint32_t inc_lock;
  
  // piggybacked osd/og state
  eversion_t pg_trim_to;   // primary->replica: trim to here
  osd_peer_stat_t peer_stat;

  map<string,bufferptr> attrset;

public:
 virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(map_epoch, p);
    ::decode(reqid, p);
    ::decode(pgid, p);
    ::decode(poid, p);
    ::decode(op, p);
    ::decode(offset, p);
    ::decode(length, p);
    ::decode(rep_tid, p);
    ::decode(version, p);
    ::decode(inc_lock, p);
    ::decode(pg_trim_to, p);
    ::decode(peer_stat, p);
    ::decode(attrset, p);
  }

  virtual void encode_payload() {
    ::encode(map_epoch, payload);
    ::encode(reqid, payload);
    ::encode(pgid, payload);
    ::encode(poid, payload);
    ::encode(op, payload);
    ::encode(offset, payload);
    ::encode(length, payload);
    ::encode(rep_tid, payload);
    ::encode(version, payload);
    ::encode(inc_lock, payload);
    ::encode(pg_trim_to, payload);
    ::encode(peer_stat, payload);
    ::encode(attrset, payload);
    env.data_off = offset;
  }



  const epoch_t get_map_epoch() { return map_epoch; }

  const osd_reqid_t&    get_reqid() { return reqid; }

  bool wants_reply() {
    if (op < 100) return true;
    return false;  // no reply needed for primary-lock, -unlock.
  }

  const pg_t get_pg() { return pgid; }
  const pobject_t get_poid() { return poid; }
  const int get_op() { return op; }
  bool is_read() { return op < 10; }
  const off_t get_length() { return length; }
  const off_t get_offset() { return offset; }

  const tid_t get_rep_tid() { return rep_tid; }
  const eversion_t get_version() { return version; }
  const eversion_t get_pg_trim_to() { return pg_trim_to; }
  void set_pg_trim_to(eversion_t v) { pg_trim_to = v; }

  unsigned get_inc_lock() { return inc_lock; }
  void set_inc_lock(unsigned i) { inc_lock = i; }

  map<string,bufferptr>& get_attrset() { return attrset; }
  void set_attrset(map<string,bufferptr> &as) { attrset.swap(as); }

  void set_peer_stat(const osd_peer_stat_t& stat) { peer_stat = stat; }
  const osd_peer_stat_t& get_peer_stat() { return peer_stat; }
 
  MOSDSubOp(osd_reqid_t r, pg_t p, pobject_t po, int o, off_t of, off_t le,
	    epoch_t mape, tid_t rtid, unsigned il, eversion_t v) :
    Message(MSG_OSD_SUBOP),
    map_epoch(mape),
    reqid(r),
    pgid(p),
    poid(po),
    op(o),
    offset(of),
    length(le),
    rep_tid(rtid),
    version(v),
    inc_lock(il)
  {
    memset(&peer_stat, 0, sizeof(peer_stat));
  }
  MOSDSubOp() {}

  const char *get_type_name() { return "osd_sub_op"; }
  void print(ostream& out) {
    out << "osd_sub_op(" << reqid
	<< " " << MOSDOp::get_opname(op)
	<< " " << poid
	<< " v" << version;    
    if (length) out << " " << offset << "~" << length;
    out << ")";
  }
};


#endif
