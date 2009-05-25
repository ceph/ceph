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
private:
  ceph_osd_request_head head;
public:
  vector<ceph_osd_op> ops;
  vector<snapid_t> snaps;
  osd_peer_stat_t peer_stat;

  friend class MOSDOpReply;

  // read
  snapid_t get_snapid() { return snapid_t(head.snapid); }
  void set_snapid(snapid_t s) { head.snapid = s; }
  // writ
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
  
  utime_t get_mtime() { return head.mtime; }
  
  bool may_read() { return head.flags & CEPH_OSD_FLAG_READ; }
  bool may_write() { return head.flags & CEPH_OSD_FLAG_WRITE; }

  void set_peer_stat(const osd_peer_stat_t& stat) {
    peer_stat = stat;
    head.flags = (head.flags | CEPH_OSD_FLAG_PEERSTAT);
  }
  const osd_peer_stat_t& get_peer_stat() {
    assert(head.flags & CEPH_OSD_FLAG_PEERSTAT);
    return peer_stat; 
  }

  //void inc_shed_count() { head.shed_count = get_shed_count() + 1; }
  //int get_shed_count() { return head.shed_count; }
 


  MOSDOp(int inc, long tid,
         object_t oid, ceph_object_layout ol, epoch_t mapepoch,
	 int flags) :
    Message(CEPH_MSG_OSD_OP) {
    memset(&head, 0, sizeof(head));
    head.tid = tid;
    head.client_inc = inc;
    head.oid = oid;
    head.layout = ol;
    head.osdmap_epoch = mapepoch;
    head.flags = flags;
  }
  MOSDOp() {}

  void set_layout(const ceph_object_layout& l) { head.layout = l; }
  void set_version(eversion_t v) { head.reassert_version = v; }
  void set_mtime(utime_t mt) { head.mtime = mt; }

  // ops
  void add_simple_op(int o, __u64 off, __u64 len) {
    ceph_osd_op op;
    op.op = o;
    op.offset = off;
    op.length = len;
    ops.push_back(op);
  }
  void write(__u64 off, __u64 len, bufferlist& bl) {
    add_simple_op(CEPH_OSD_OP_WRITE, off, len);
    data.claim(bl);
    header.data_off = off;
  }
  void writefull(bufferlist& bl) {
    add_simple_op(CEPH_OSD_OP_WRITEFULL, 0, bl.length());
    data.claim(bl);
    header.data_off = 0;
  }
  void zero(__u64 off, __u64 len) {
    add_simple_op(CEPH_OSD_OP_ZERO, off, len);
  }
  void truncate(__u64 off) {
    add_simple_op(CEPH_OSD_OP_TRUNCATE, off, 0);
  }
  void remove() {
    add_simple_op(CEPH_OSD_OP_DELETE, 0, 0);
  }

  void read(__u64 off, __u64 len) {
    add_simple_op(CEPH_OSD_OP_READ, off, len);
  }
  void stat() {
    add_simple_op(CEPH_OSD_OP_STAT, 0, 0);
  }

  // flags
  int get_flags() const { return head.flags; }

  bool wants_ack() const { return get_flags() & CEPH_OSD_FLAG_ACK; }
  bool wants_ondisk() const { return get_flags() & CEPH_OSD_FLAG_ONDISK; }
  bool wants_onnvram() const { return get_flags() & CEPH_OSD_FLAG_ONNVRAM; }

  void set_want_ack(bool b) { head.flags = get_flags() | CEPH_OSD_FLAG_ACK; }
  void set_want_onnvram(bool b) { head.flags = get_flags() | CEPH_OSD_FLAG_ONNVRAM; }
  void set_want_ondisk(bool b) { head.flags = get_flags() | CEPH_OSD_FLAG_ONDISK; }

  bool is_retry_attempt() const { return get_flags() & CEPH_OSD_FLAG_RETRY; }
  void set_retry_attempt(bool a) { 
    if (a)
      head.flags = head.flags | CEPH_OSD_FLAG_RETRY;
    else
      head.flags = head.flags & ~CEPH_OSD_FLAG_RETRY;
  }

  // marshalling
  virtual void encode_payload() {
    head.num_snaps = snaps.size();
    head.num_ops = ops.size();
    ::encode(head, payload);
    ::encode_nohead(ops, payload);
    ::encode_nohead(snaps, payload);
    if (head.flags & CEPH_OSD_FLAG_PEERSTAT)
      ::encode(peer_stat, payload);
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    decode_nohead(head.num_ops, ops, p);
    decode_nohead(head.num_snaps, snaps, p);
    if (head.flags & CEPH_OSD_FLAG_PEERSTAT)
      ::decode(peer_stat, p);
  }


  const char *get_type_name() { return "osd_op"; }
  void print(ostream& out) {
    out << "osd_op(" << get_reqid();
    out << " " << head.oid;

    out << " ";
    if (may_read())
      out << "r";
    if (may_write())
      out << "w";

    if (head.snapid != CEPH_NOSNAP)
      out << "@" << snapid_t((__u64)head.snapid);

    out << " " << ops;
    out << " " << pg_t(head.layout.ol_pgid);
    if (is_retry_attempt()) out << " RETRY";
    if (!snaps.empty())
      out << " snaps=" << snaps;
    out << ")";
  }
};


#endif
