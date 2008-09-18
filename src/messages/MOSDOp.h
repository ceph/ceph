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

#define EINCLOCKED 100

class MOSDOp : public Message {
private:
  ceph_osd_request_head head;
  vector<snapid_t> snaps;

  friend class MOSDOpReply;

public:
  snapid_t get_snap_seq() { return snapid_t(head.snap_seq); }
  vector<snapid_t> &get_snaps() { return snaps; }
  void set_snap_seq(snapid_t s) { head.snap_seq = s; }

  osd_reqid_t get_reqid() { return osd_reqid_t(get_orig_source(),
					       head.client_inc,
					       head.tid); }
  int get_client_inc() { return head.client_inc; }
  tid_t get_client_tid() { return head.tid; }
  
  object_t get_oid() { return object_t(head.oid); }
  pg_t     get_pg() { return pg_t(head.layout.ol_pgid); }
  ceph_object_layout get_layout() { return head.layout; }
  epoch_t  get_map_epoch() { return head.osdmap_epoch; }

  eversion_t get_version() { return head.reassert_version; }
  
  const int    get_op() { return head.op; }
  void set_op(int o) { head.op = o; }
  bool is_read() { return ceph_osd_op_is_read(get_op()); }

  loff_t get_length() const { return head.length; }
  loff_t get_offset() const { return head.offset; }

  unsigned get_inc_lock() const { return head.inc_lock; }

  void set_peer_stat(const osd_peer_stat_t& stat) { head.peer_stat = stat; }
  const ceph_osd_peer_stat& get_peer_stat() { return head.peer_stat; }

  void inc_shed_count() { head.shed_count = get_shed_count() + 1; }
  int get_shed_count() { return head.shed_count; }
  


  MOSDOp(int inc, long tid,
         object_t oid, ceph_object_layout ol, epoch_t mapepoch, int op,
	 int flags) :
    Message(CEPH_MSG_OSD_OP) {
    memset(&head, 0, sizeof(head));
    head.tid = tid;
    head.client_inc = inc;
    head.oid = oid;
    head.layout = ol;
    head.osdmap_epoch = mapepoch;
    head.op = op;
    head.flags = flags;
  }
  MOSDOp() {}

  void set_inc_lock(__u32 l) {
    head.inc_lock = l;
  }

  void set_layout(const ceph_object_layout& l) { head.layout = l; }

  void set_length(loff_t l) { head.length = l; }
  void set_offset(loff_t o) { head.offset = o; }
  void set_version(eversion_t v) { head.reassert_version = v; }
  
  int get_flags() const { return head.flags; }
  bool wants_ack() const { return get_flags() & CEPH_OSD_OP_ACK; }
  bool wants_commit() const { return get_flags() & CEPH_OSD_OP_SAFE; }
  bool is_retry_attempt() const { return get_flags() & CEPH_OSD_OP_RETRY; }

  void set_want_ack(bool b) { head.flags = get_flags() | CEPH_OSD_OP_ACK; }
  void set_want_commit(bool b) { head.flags = get_flags() | CEPH_OSD_OP_SAFE; }
  void set_retry_attempt(bool a) { 
    if (a)
      head.flags = head.flags | CEPH_OSD_OP_RETRY;
    else
      head.flags = head.flags & ~CEPH_OSD_OP_RETRY;
  }

  // marshalling
  virtual void encode_payload() {
    head.num_snaps = snaps.size();
    ::encode(head, payload);
    for (unsigned i=0; i<snaps.size(); i++)
      ::encode(snaps[i], payload);
    env.data_off = get_offset();
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    snaps.resize(head.num_snaps);
    for (unsigned i=0; i<snaps.size(); i++)
      ::decode(snaps[i], p);
  }


  const char *get_type_name() { return "osd_op"; }
  void print(ostream& out) {
    out << "osd_op(" << get_reqid()
	<< " " << ceph_osd_op_name(get_op())
	<< " " << head.oid;
    if (get_length()) out << " " << get_offset() << "~" << get_length();
    out << " " << pg_t(head.layout.ol_pgid);
    if (is_retry_attempt()) out << " RETRY";
    if (!snaps.empty())
      out << " snaps=" << snaps;
    out << ")";
  }
};


#endif
