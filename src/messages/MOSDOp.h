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
#include "include/ceph_features.h"
#include <atomic>

/*
 * OSD op
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class OSD;

class MOSDOp : public Message {

  static const int HEAD_VERSION = 7;
  static const int COMPAT_VERSION = 3;

private:
  uint32_t client_inc;
  __u32 osdmap_epoch;
  __u32 flags;
  utime_t mtime;
  eversion_t reassert_version;
  int32_t retry_attempt;   // 0 is first attempt.  -1 if we don't know.

  object_t oid;
  object_locator_t oloc;
  pg_t pgid;
  bufferlist::iterator p;
  // Decoding flags. Decoding is only needed for messages catched by pipe reader.
  // Transition from true -> false without locks being held
  // Can never see final_decode_needed == false and partial_decode_needed == true
  atomic<bool> partial_decode_needed;
  atomic<bool> final_decode_needed;
  //
public:
  vector<OSDOp> ops;
private:

  snapid_t snapid;
  snapid_t snap_seq;
  vector<snapid_t> snaps;

  uint64_t features;

  osd_reqid_t reqid; // reqid explicitly set by sender

public:
  friend class MOSDOpReply;

  ceph_tid_t get_client_tid() { return header.tid; }
  void set_snapid(const snapid_t& s) { snapid = s; }
  void set_snaps(const vector<snapid_t>& i) {
    snaps = i;
  }
  void set_snap_seq(const snapid_t& s) { snap_seq = s; }
  void set_reqid(const osd_reqid_t rid) {
    reqid = rid;
  }

  // Fields decoded in partial decoding
  const pg_t& get_pg() const {
    assert(!partial_decode_needed);
    return pgid;
  }
  epoch_t get_map_epoch() const {
    assert(!partial_decode_needed);
    return osdmap_epoch;
  }
  int get_flags() const {
    assert(!partial_decode_needed);
    return flags;
  }
  const eversion_t& get_version() const {
    assert(!partial_decode_needed);
    return reassert_version;
  }
  osd_reqid_t get_reqid() const {
    assert(!partial_decode_needed);
    if (reqid.name != entity_name_t() || reqid.tid != 0) {
      return reqid;
    } else {
      if (!final_decode_needed)
	assert(reqid.inc == (int32_t)client_inc);  // decode() should have done this
      return osd_reqid_t(get_orig_source(),
                         reqid.inc,
			 header.tid);
    }
  }

  // Fields decoded in final decoding
  int get_client_inc() const {
    assert(!final_decode_needed);
    return client_inc;
  }
  utime_t get_mtime() const {
    assert(!final_decode_needed);
    return mtime;
  }
  const object_locator_t& get_object_locator() const {
    assert(!final_decode_needed);
    return oloc;
  }
  object_t& get_oid() {
    assert(!final_decode_needed);
    return oid;
  }
  const snapid_t& get_snapid() {
    assert(!final_decode_needed);
    return snapid;
  }
  const snapid_t& get_snap_seq() const {
    assert(!final_decode_needed);
    return snap_seq;
  }
  const vector<snapid_t> &get_snaps() const {
    assert(!final_decode_needed);
    return snaps;
  }

  /**
   * get retry attempt
   *
   * 0 is the first attempt.
   *
   * @return retry attempt, or -1 if we don't know
   */
  int get_retry_attempt() const {
    return retry_attempt;
  }
  uint64_t get_features() const {
    if (features)
      return features;
    return get_connection()->get_features();
  }

  MOSDOp()
    : Message(CEPH_MSG_OSD_OP, HEAD_VERSION, COMPAT_VERSION),
      partial_decode_needed(true),
      final_decode_needed(true) { }
  MOSDOp(int inc, long tid,
         object_t& _oid, object_locator_t& _oloc, pg_t& _pgid,
	 epoch_t _osdmap_epoch,
	 int _flags, uint64_t feat)
    : Message(CEPH_MSG_OSD_OP, HEAD_VERSION, COMPAT_VERSION),
      client_inc(inc),
      osdmap_epoch(_osdmap_epoch), flags(_flags), retry_attempt(-1),
      oid(_oid), oloc(_oloc), pgid(_pgid),
      partial_decode_needed(false),
      final_decode_needed(false),
      features(feat) {
    set_tid(tid);

    // also put the client_inc in reqid.inc, so that get_reqid() can
    // be used before the full message is decoded.
    reqid.inc = inc;
  }
private:
  ~MOSDOp() {}

public:
  void set_version(eversion_t v) { reassert_version = v; }
  void set_mtime(utime_t mt) { mtime = mt; }
  void set_mtime(ceph::real_time mt) {
    mtime = ceph::real_clock::to_timespec(mt);
  }

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

  bool has_flag(__u32 flag) { return flags & flag; };
  bool wants_ack() const { return flags & CEPH_OSD_FLAG_ACK; }
  bool wants_ondisk() const { return flags & CEPH_OSD_FLAG_ONDISK; }
  bool wants_onnvram() const { return flags & CEPH_OSD_FLAG_ONNVRAM; }

  void set_want_ack(bool b) { flags |= CEPH_OSD_FLAG_ACK; }
  void set_want_onnvram(bool b) { flags |= CEPH_OSD_FLAG_ONNVRAM; }
  void set_want_ondisk(bool b) { flags |= CEPH_OSD_FLAG_ONDISK; }

  bool is_retry_attempt() const { return flags & CEPH_OSD_FLAG_RETRY; }
  void set_retry_attempt(unsigned a) { 
    if (a)
      flags |= CEPH_OSD_FLAG_RETRY;
    else
      flags &= ~CEPH_OSD_FLAG_RETRY;
    retry_attempt = a;
  }

  // marshalling
  virtual void encode_payload(uint64_t features) {

    OSDOp::merge_osd_op_vector_in_data(ops, data);

    if ((features & CEPH_FEATURE_OBJECTLOCATOR) == 0) {
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
      header.version = 1;

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
    } else if ((features & CEPH_FEATURE_NEW_OSDOP_ENCODING) == 0) {
      header.version = 6;
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

      ::encode(retry_attempt, payload);
      ::encode(features, payload);
      ::encode(reqid, payload);
    } else {
      // new, reordered, v7 message encoding
      header.version = HEAD_VERSION;
      ::encode(pgid, payload);
      ::encode(osdmap_epoch, payload);
      ::encode(flags, payload);
      ::encode(reassert_version, payload);
      ::encode(reqid, payload);
      ::encode(client_inc, payload);
      ::encode(mtime, payload);
      ::encode(oloc, payload);
      ::encode(oid, payload);

      __u16 num_ops = ops.size();
      ::encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	::encode(ops[i].op, payload);

      ::encode(snapid, payload);
      ::encode(snap_seq, payload);
      ::encode(snaps, payload);

      ::encode(retry_attempt, payload);
      ::encode(features, payload);
    }
  }

  virtual void decode_payload() {
    assert(partial_decode_needed && final_decode_needed);
    p = payload.begin();

    // Always keep here the newest version of decoding order/rule
    if (header.version == HEAD_VERSION) {
	  ::decode(pgid, p);
	  ::decode(osdmap_epoch, p);
	  ::decode(flags, p);
	  ::decode(reassert_version, p);
	  ::decode(reqid, p);
    } else if (header.version < 2) {
      // old decode
      ::decode(client_inc, p);

      old_pg_t opgid;
      ::decode_raw(opgid, p);
      pgid = opgid;

      __u32 su;
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

      // recalculate pgid hash value
      pgid.set_ps(ceph_str_hash(CEPH_STR_HASH_RJENKINS,
				oid.name.c_str(),
				oid.name.length()));

      retry_attempt = -1;
      features = 0;
      OSDOp::split_osd_op_vector_in_data(ops, data);

      // we did the full decode
      final_decode_needed = false;

      // put client_inc in reqid.inc for get_reqid()'s benefit
      reqid = osd_reqid_t();
      reqid.inc = client_inc;
    } else if (header.version < 7) {
      ::decode(client_inc, p);
      ::decode(osdmap_epoch, p);
      ::decode(flags, p);
      ::decode(mtime, p);
      ::decode(reassert_version, p);

      ::decode(oloc, p);

      if (header.version < 3) {
	old_pg_t opgid;
	::decode_raw(opgid, p);
	pgid = opgid;
      } else {
	::decode(pgid, p);
      }

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

      if (header.version >= 4)
        ::decode(retry_attempt, p);
      else
        retry_attempt = -1;

      if (header.version >= 5)
        ::decode(features, p);
      else
	features = 0;

      if (header.version >= 6)
	::decode(reqid, p);
      else
	reqid = osd_reqid_t();

      OSDOp::split_osd_op_vector_in_data(ops, data);

      // we did the full decode
      final_decode_needed = false;

      // put client_inc in reqid.inc for get_reqid()'s benefit
      if (reqid.name == entity_name_t() && reqid.tid == 0)
	reqid.inc = client_inc;
    }

    partial_decode_needed = false;
  }

  void finish_decode() {
    assert(!partial_decode_needed); // partial decoding required
    if (!final_decode_needed)
      return; // Message is already final decoded
    assert(header.version >= 7);

    ::decode(client_inc, p);
    ::decode(mtime, p);
    ::decode(oloc, p);
    ::decode(oid, p);

    __u16 num_ops;
    ::decode(num_ops, p);
    ops.resize(num_ops);
    for (unsigned i = 0; i < num_ops; i++)
      ::decode(ops[i].op, p);

    ::decode(snapid, p);
    ::decode(snap_seq, p);
    ::decode(snaps, p);

    ::decode(retry_attempt, p);

    ::decode(features, p);

    OSDOp::split_osd_op_vector_in_data(ops, data);

    final_decode_needed = false;
  }

  void clear_buffers() {
    ops.clear();
  }

  const char *get_type_name() const { return "osd_op"; }
  void print(ostream& out) const {
    out << "osd_op(";
    if (!partial_decode_needed) {
      out << get_reqid() << ' ';
      out << pgid;
      if (!final_decode_needed) {
	out << ' ';
	if (!oloc.nspace.empty())
	  out << oloc.nspace << "/";
	out << oid
	    << " " << ops
	    << " snapc " << get_snap_seq() << "=" << snaps;
	if (oloc.key.size())
	  out << " " << oloc;
	if (is_retry_attempt())
	  out << " RETRY=" << get_retry_attempt();
      } else {
	out << " (undecoded)";
      }
      out << " " << ceph_osd_flag_string(get_flags());
      if (reassert_version != eversion_t())
	out << " reassert_version=" << reassert_version;
      out << " e" << osdmap_epoch;
    }
    out << ")";
  }
};


#endif
