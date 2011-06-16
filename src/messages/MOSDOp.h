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


#ifndef CEPH_MOSDOP_H
#define CEPH_MOSDOP_H

#include "msg/Message.h"
#include "osd/osd_types.h"

/*
 * OSD op
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class OSD;

class MOSDOp : public Message {
private:
  uint32_t client_inc;
  __u32 osdmap_epoch;
  __u32 flags;
  utime_t mtime;
  eversion_t reassert_version;

  object_t oid;
  object_locator_t oloc;
  pg_t pgid;
public:
  vector<OSDOp> ops;
private:

  snapid_t snapid;
  snapid_t snap_seq;
  vector<snapid_t> snaps;

  osd_peer_stat_t peer_stat;
public:
  int rmw_flags;

  friend class MOSDOpReply;

  // read
  snapid_t get_snapid() { return snapid; }
  void set_snapid(snapid_t s) { snapid = s; }
  // writ
  snapid_t get_snap_seq() { return snap_seq; }
  vector<snapid_t> &get_snaps() { return snaps; }
  void set_snap_seq(snapid_t s) { snap_seq = s; }

  osd_reqid_t get_reqid() { return osd_reqid_t(get_orig_source(),
					       client_inc,
					       header.tid); }
  int get_client_inc() { return client_inc; }
  tid_t get_client_tid() { return header.tid; }
  
  object_t& get_oid() { return oid; }

  pg_t     get_pg() const { return pgid; }

  object_locator_t get_object_locator() const {
    return oloc;
  }

  epoch_t  get_map_epoch() { return osdmap_epoch; }

  eversion_t get_version() { return reassert_version; }
  
  utime_t get_mtime() { return mtime; }

  int get_rmw_flags() { assert(rmw_flags); return rmw_flags; }
  bool may_read() { assert(rmw_flags); return rmw_flags & CEPH_OSD_FLAG_READ; }
  bool may_write() { assert(rmw_flags); return rmw_flags & CEPH_OSD_FLAG_WRITE; }
  bool may_exec() { assert(rmw_flags); return rmw_flags & (CEPH_OSD_FLAG_EXEC | CEPH_OSD_FLAG_EXEC_PUBLIC); }
  bool require_exec_caps() { assert(rmw_flags); return rmw_flags & CEPH_OSD_FLAG_EXEC; }

  void set_peer_stat(const osd_peer_stat_t& st) {
    peer_stat = st;
    flags |= CEPH_OSD_FLAG_PEERSTAT;
  }
  const osd_peer_stat_t& get_peer_stat() {
    assert(flags & CEPH_OSD_FLAG_PEERSTAT);
    return peer_stat; 
  }

  MOSDOp(int inc, long tid,
         object_t& _oid, object_locator_t& _oloc, pg_t _pgid, epoch_t _osdmap_epoch,
	 int _flags) :
    Message(CEPH_MSG_OSD_OP),
    client_inc(inc),
    osdmap_epoch(_osdmap_epoch), flags(_flags),
    oid(_oid), oloc(_oloc), pgid(_pgid),
    rmw_flags(flags) {
    set_tid(tid);
  }
  MOSDOp() : rmw_flags(0) {}
private:
  ~MOSDOp() {}

public:
  void set_version(eversion_t v) { reassert_version = v; }
  void set_mtime(utime_t mt) { mtime = mt; }

  // ops
  void add_simple_op(int o, uint64_t off, uint64_t len) {
    OSDOp osd_op;
    osd_op.op.op = o;
    osd_op.op.extent.offset = off;
    osd_op.op.extent.length = len;
    ops.push_back(osd_op);
  }
  void write(uint64_t off, uint64_t len, bufferlist& bl) {
    add_simple_op(CEPH_OSD_OP_WRITE, off, len);
    data.claim(bl);
    header.data_off = off;
  }
  void writefull(bufferlist& bl) {
    add_simple_op(CEPH_OSD_OP_WRITEFULL, 0, bl.length());
    data.claim(bl);
    header.data_off = 0;
  }
  void zero(uint64_t off, uint64_t len) {
    add_simple_op(CEPH_OSD_OP_ZERO, off, len);
  }
  void truncate(uint64_t off) {
    add_simple_op(CEPH_OSD_OP_TRUNCATE, off, 0);
  }
  void remove() {
    add_simple_op(CEPH_OSD_OP_DELETE, 0, 0);
  }

  void read(uint64_t off, uint64_t len) {
    add_simple_op(CEPH_OSD_OP_READ, off, len);
  }
  void stat() {
    add_simple_op(CEPH_OSD_OP_STAT, 0, 0);
  }

  // flags
  int get_flags() const { return flags; }

  bool wants_ack() const { return flags & CEPH_OSD_FLAG_ACK; }
  bool wants_ondisk() const { return flags & CEPH_OSD_FLAG_ONDISK; }
  bool wants_onnvram() const { return flags & CEPH_OSD_FLAG_ONNVRAM; }

  void set_want_ack(bool b) { flags |= CEPH_OSD_FLAG_ACK; }
  void set_want_onnvram(bool b) { flags |= CEPH_OSD_FLAG_ONNVRAM; }
  void set_want_ondisk(bool b) { flags |= CEPH_OSD_FLAG_ONDISK; }

  bool is_retry_attempt() const { return flags & CEPH_OSD_FLAG_RETRY; }
  void set_retry_attempt(bool a) { 
    if (a)
      flags |= CEPH_OSD_FLAG_RETRY;
    else
      flags &= ~CEPH_OSD_FLAG_RETRY;
  }

  // marshalling
  virtual void encode_payload() {

    for (unsigned i = 0; i < ops.size(); i++) {
      if (ceph_osd_op_type_multi(ops[i].op.op)) {
	bufferlist bl;
	::encode(ops[i].soid, bl);
	ops[i].op.payload_len = bl.length();
	data.append(bl);
      } else {
	ops[i].op.payload_len = ops[i].data.length();
	data.append(ops[i].data);
      }
    }

    if (!connection->has_feature(CEPH_FEATURE_OBJECTLOCATOR)) {
      // here is the old structure we are encoding to: //
#if 0
struct ceph_osd_request_head {
	__le32 client_inc;                 /* client incarnation */
	struct ceph_object_layout layout;  /* pgid */
	__le32 osdmap_epoch;               /* client's osdmap epoch */

	__le32 flags;

	struct ceph_timespec mtime;        /* for mutations only */
	struct ceph_eversion reassert_version; /* if we are replaying op */

	__le32 object_len;     /* length of object name */

	__le64 snapid;         /* snapid to read */
	__le64 snap_seq;       /* writer's snap context */
	__le32 num_snaps;

	__le16 num_ops;
	struct ceph_osd_op ops[];  /* followed by ops[], obj, ticket, snaps */
} __attribute__ ((packed));
#endif

      ::encode(client_inc, payload);

      __u32 su = 0;
      ::encode(pgid, payload);
      ::encode(su, payload);

      ::encode(osdmap_epoch, payload);
      ::encode(flags, payload);
      ::encode(mtime, payload);
      ::encode(reassert_version, payload);

      __u32 oid_len = oid.name.length();
      ::encode(oid_len, payload);
      ::encode(snapid, payload);
      ::encode(snap_seq, payload);
      __u32 num_snaps = snaps.size();
      ::encode(num_snaps, payload);
      
      //::encode(ops, payload);
      __u16 num_ops = ops.size();
      ::encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	::encode(ops[i].op, payload);

      ::encode_nohead(oid.name, payload);
      ::encode_nohead(snaps, payload);
      if (flags & CEPH_OSD_FLAG_PEERSTAT)
	::encode(peer_stat, payload);
    } else {
      header.version = 2;
      ::encode(client_inc, payload);
      ::encode(osdmap_epoch, payload);
      ::encode(flags, payload);
      ::encode(mtime, payload);
      ::encode(reassert_version, payload);

      ::encode(oloc, payload);
      ::encode(pgid, payload);
      ::encode(oid, payload);

      __u16 num_ops = ops.size();
      ::encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	::encode(ops[i].op, payload);

      ::encode(snapid, payload);
      ::encode(snap_seq, payload);
      ::encode(snaps, payload);

      if (flags & CEPH_OSD_FLAG_PEERSTAT)
	::encode(peer_stat, payload);
    }
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();

    if (header.version < 2) {
      // old decode
      ::decode(client_inc, p);

      __u32 su;
      ::decode(pgid, p);
      ::decode(su, p);
      oloc.pool = pgid.pool();

      ::decode(osdmap_epoch, p);
      ::decode(flags, p);
      ::decode(mtime, p);
      ::decode(reassert_version, p);

      __u32 oid_len;
      ::decode(oid_len, p);
      ::decode(snapid, p);
      ::decode(snap_seq, p);
      __u32 num_snaps;
      ::decode(num_snaps, p);
      
      //::decode(ops, p);
      __u16 num_ops;
      ::decode(num_ops, p);
      ops.resize(num_ops);
      for (unsigned i = 0; i < num_ops; i++)
	::decode(ops[i].op, p);

      decode_nohead(oid_len, oid.name, p);
      decode_nohead(num_snaps, snaps, p);

      if (flags & CEPH_OSD_FLAG_PEERSTAT)
	::decode(peer_stat, p);
    } else {
      // new decode 
      ::decode(client_inc, p);
      ::decode(osdmap_epoch, p);
      ::decode(flags, p);
      ::decode(mtime, p);
      ::decode(reassert_version, p);

      ::decode(oloc, p);
      ::decode(pgid, p);
      ::decode(oid, p);

      //::decode(ops, p);
      __u16 num_ops;
      ::decode(num_ops, p);
      ops.resize(num_ops);
      for (unsigned i = 0; i < num_ops; i++)
	::decode(ops[i].op, p);

      ::decode(snapid, p);
      ::decode(snap_seq, p);
      ::decode(snaps, p);

      if (flags & CEPH_OSD_FLAG_PEERSTAT)
	::decode(peer_stat, p);
    }

    unsigned off = 0;
    for (unsigned i = 0; i < ops.size(); i++) {
      if (ceph_osd_op_type_multi(ops[i].op.op)) {
	bufferlist t;
	t.substr_of(data, off, ops[i].op.payload_len);
	off += ops[i].op.payload_len;
        if (t.length()) {
	  bufferlist::iterator p = t.begin();
	  ::decode(ops[i].soid, p);
	}
      } else {
	ops[i].data.substr_of(data, off, ops[i].op.payload_len);
	off += ops[i].op.payload_len;
      }
    }
  }


  const char *get_type_name() { return "osd_op"; }
  void print(ostream& out) {
    out << "osd_op(" << get_reqid();
    out << " " << oid;

#if 0
    out << " ";
    if (may_read())
      out << "r";
    if (may_write())
      out << "w";
#endif
    if (snapid != CEPH_NOSNAP)
      out << "@" << snapid;

    if (oloc.key.size())
      out << " " << oloc;

    out << " " << ops;
    out << " " << pgid;
    if (is_retry_attempt()) out << " RETRY";
    if (get_snap_seq())
      out << " snapc " << get_snap_seq() << "=" << snaps;
    if (get_flags() & CEPH_OSD_FLAG_ORDERSNAP)
      out << " ordersnap";
    if (get_flags() & CEPH_OSD_FLAG_BALANCE_READS)
      out << " balance_reads";
    if (get_flags() & CEPH_OSD_FLAG_PARALLELEXEC)
      out << " parallelexec";
    if (get_flags() & CEPH_OSD_FLAG_LOCALIZE_READS)
      out << " localize_reads";
    if (get_flags() & CEPH_OSD_FLAG_RWORDERED)
      out << " rwordered";
    out << ")";
  }
};


#endif
