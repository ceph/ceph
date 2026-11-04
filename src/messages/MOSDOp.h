// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <optional>
#include <vector>

#include "MOSDFastDispatchOp.h"
#include "include/ceph_features.h"
#include "include/ceph_fs.h" // for CEPH_MSG_OSD_OP
#include "include/rados_encoder.h"
#include "common/ceph_releases.h"
#include "common/hobject.h"
#include "common/rdma_token.h"

/*
 * OSD op
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDOpReply;

namespace _mosdop {
template<typename V>
class MOSDOp final : public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 9;
  static constexpr int COMPAT_VERSION = 3;

private:
  uint32_t client_inc = 0;
  __u32 osdmap_epoch = 0;
  __u32 flags = 0;
  utime_t mtime;
  int32_t retry_attempt = -1;   // 0 is first attempt.  -1 if we don't know.

  hobject_t hobj;
  spg_t pgid;
  ceph::buffer::list::const_iterator p;
  // Decoding flags. Decoding is only needed for messages caught by pipe reader.
  // Transition from true -> false without locks being held
  // Can never see final_decode_needed == false and partial_decode_needed == true
  std::atomic<bool> partial_decode_needed;
  std::atomic<bool> final_decode_needed;
  //
public:
  V ops;
private:
  snapid_t snap_seq;
  std::vector<snapid_t> snaps;

  uint64_t features;
  bool bdata_encode;
  osd_reqid_t reqid; // reqid explicitly set by sender

  /// per-op advisory out-of-band delivery descriptors (see
  /// common/rdma_token.h): empty when no op requests delivery,
  /// otherwise aligned with ops (mirroring the reply's oob_results);
  /// encoded after the v9 fields only when non-empty, and decoded in
  /// the finish_decode() tail of a v9 request when bytes remain there
  std::vector<ceph::rdma::delivery_t> rdma_deliveries;
  /// the payloads of the writes that the primary pulls out of client
  /// memory (delivery_t::FLAG_PULL), which a look at the rest of a
  /// received v9 write found (peek_pulled_len()); fixed in
  /// decode_payload(), before the OSD queues the request, and never
  /// changed after
  uint64_t pulled_len = 0;

  /**
   * The bytes the pulled writes of a v9 request take out of client
   * memory, from the part of its payload at q, which finish_decode()
   * decodes later, on the op thread: the OSD charges the request's cost
   * to its scheduler before that. As PrimaryLogPG counts them: the
   * lengths of the WRITEs and WRITEFULLs whose descriptor pulls, summed
   * with saturation, when the descriptors mirror the ops; 0 for a
   * request without them, and for one this cannot read, which
   * finish_decode() fails.
   */
  static uint64_t peek_pulled_len(ceph::buffer::list::const_iterator q) {
    using ceph::decode;
    try {
      uint32_t client_inc;
      decode(client_inc, q);
      utime_t mtime;
      decode(mtime, q);
      // the object's locator, skipped by its encoded length. Every
      // client encodes it with one (struct_v 3 or later), but
      // finish_decode() takes an older one too, which has no length:
      // that one is decoded in full, so that the sum still counts
      // every pull of a request the OSD runs
      const auto oloc_at = q;
      __u8 oloc_v, oloc_compat;
      decode(oloc_v, q);
      if (oloc_v < 3) {
	q = oloc_at;
	object_locator_t oloc;
	decode(oloc, q);
      } else {
	decode(oloc_compat, q);
	uint32_t oloc_len;
	decode(oloc_len, q);
	q += oloc_len;
      }
      // the object's name, which is not needed here
      uint32_t name_len;
      decode(name_len, q);
      q += name_len;
      __u16 num_ops;
      decode(num_ops, q);
      // the ops, read again below alongside their descriptors
      auto ops_at = q;
      q += num_ops * sizeof(ceph_osd_op);
      snapid_t snap, seq;
      decode(snap, q);
      decode(seq, q);
      uint32_t num_snaps;
      decode(num_snaps, q);
      if (num_snaps > q.get_remaining() / sizeof(ceph_le64)) {
	return 0;
      }
      q += num_snaps * sizeof(ceph_le64);
      int32_t retry;
      decode(retry, q);
      uint64_t feat;
      decode(feat, q);
      if (q.end()) {
	return 0;
      }
      uint32_t num_deliveries;
      decode(num_deliveries, q);
      if (num_deliveries != num_ops) {
	return 0;
      }
      uint64_t len = 0;
      for (unsigned i = 0; i < num_ops; i++) {
	ceph_osd_op op;
	decode(op, ops_at);
	ceph::rdma::delivery_t d;
	decode(d, q);
	if (!d.is_pull() ||
	    (op.op != CEPH_OSD_OP_WRITE && op.op != CEPH_OSD_OP_WRITEFULL)) {
	  continue;
	}
	const uint64_t l = op.extent.length;
	len = l > UINT64_MAX - len ? UINT64_MAX : len + l;
      }
      return len;
    } catch (const ceph::buffer::error&) {
      return 0;
    }
  }

public:
  /// what the OSD's scheduler charges for the request: its data, and
  /// the payloads its writes pull, which reach the OSD all the same
  int get_cost() const override {
    constexpr uint64_t max_cost = std::numeric_limits<int>::max();
    const uint64_t cost = std::min(pulled_len, max_cost) + data.length();
    return static_cast<int>(std::min(cost, max_cost));
  }

  /// the require_osd_release from which every OSD decodes the delivery
  /// descriptors (set_rdma_deliveries()): a client sends them only to a
  /// cluster at this release or later. An OSD of an earlier release
  /// skips them, answers a read inline, and fails a pulled write, whose
  /// payload it never gets. Ceph main has no release after umbrella
  /// yet: when it opens one, this becomes that release
  static constexpr ceph_release_t RDMA_DELIVERY_RELEASE =
    ceph_release_t::umbrella;

  bool has_rdma_delivery() const {
    return !rdma_deliveries.empty();
  }
  /// the bytes the request's writes pull out of client memory, as
  /// decode_payload() found them (peek_pulled_len()): what the scheduler
  /// charges for them, and what PrimaryLogPG holds to
  /// osd_max_write_size. 0 for a request not flagged as a write
  uint64_t get_pulled_len() const {
    return pulled_len;
  }
  /// whether an op of the request carries a pull descriptor
  /// (delivery_t::is_pull()), whatever its length
  bool has_pulls() const {
    return ceph::rdma::any_pull(rdma_deliveries);
  }
  const std::vector<ceph::rdma::delivery_t>& get_rdma_deliveries() const {
    return rdma_deliveries;
  }
  void set_rdma_deliveries(std::vector<ceph::rdma::delivery_t>&& v) {
    rdma_deliveries = std::move(v);
  }

  /**
   * Whether the client asks for the return data of a request
   * (CEPH_OSD_FLAG_RETURNVEC) for its writes that the primary pulls:
   * the primary puts the record of each pull in its op's return data,
   * which the reply then carries, and which the PG log keeps with the
   * write, as it keeps the records of a request without the flag; the
   * reply to a resend that the PG answers as a dup reports the pull from
   * it. Only when the flag changes nothing that another op of the
   * request returns: with it, a write returns the data and the result of
   * a CALL or a read, and fails when one returns more than
   * osd_max_write_op_reply_len. deliveries is aligned with ops, or
   * empty.
   */
  template <typename Ops, typename Deliveries>
  static bool wants_pull_records(const Ops& ops,
				 const Deliveries& deliveries) {
    bool pulls = false;
    for (size_t i = 0; i < ops.size(); i++) {
      if (i < deliveries.size() && deliveries[i].is_pull()) {
	pulls = true;
	continue;
      }
      switch (ops[i].op.op) {
      // ops that return no data, and 0 when they succeed
      case CEPH_OSD_OP_CREATE:
      case CEPH_OSD_OP_SETALLOCHINT:
      case CEPH_OSD_OP_WRITE:
      case CEPH_OSD_OP_WRITEFULL:
      case CEPH_OSD_OP_APPEND:
      case CEPH_OSD_OP_TRUNCATE:
      case CEPH_OSD_OP_ZERO:
      case CEPH_OSD_OP_SETXATTR:
      case CEPH_OSD_OP_RMXATTR:
      case CEPH_OSD_OP_OMAPSETVALS:
      case CEPH_OSD_OP_OMAPSETHEADER:
      case CEPH_OSD_OP_OMAPRMKEYS:
      case CEPH_OSD_OP_OMAPCLEAR:
	break;
      default:
	return false;
      }
    }
    return pulls;
  }

  friend MOSDOpReply;

  ceph_tid_t get_client_tid() { return header.tid; }
  void set_snapid(const snapid_t& s) {
    hobj.snap = s;
  }
  void set_snaps(const std::vector<snapid_t>& i) {
    snaps = i;
  }
  void set_snap_seq(const snapid_t& s) { snap_seq = s; }
  void set_reqid(const osd_reqid_t rid) {
    reqid = rid;
  }
  void set_spg(spg_t p) {
    pgid = p;
  }

  // Fields decoded in partial decoding
  pg_t get_pg() const {
    ceph_assert(!partial_decode_needed);
    return pgid.pgid;
  }
  spg_t get_spg() const override {
    ceph_assert(!partial_decode_needed);
    return pgid;
  }
  pg_t get_raw_pg() const {
    ceph_assert(!partial_decode_needed);
    return pg_t(hobj.get_hash(), pgid.pgid.pool());
  }
  epoch_t get_map_epoch() const override {
    ceph_assert(!partial_decode_needed);
    return osdmap_epoch;
  }
  int get_flags() const {
    ceph_assert(!partial_decode_needed);
    return flags;
  }
  osd_reqid_t get_reqid() const {
    ceph_assert(!partial_decode_needed);
    if (reqid.name != entity_name_t() || reqid.tid != 0) {
      return reqid;
    } else {
      if (!final_decode_needed)
	ceph_assert(reqid.inc == (int32_t)client_inc);  // decode() should have done this
      return osd_reqid_t(get_orig_source(),
                         reqid.inc,
			 header.tid);
    }
  }

  // Fields decoded in final decoding
  int get_client_inc() const {
    ceph_assert(!final_decode_needed);
    return client_inc;
  }
  utime_t get_mtime() const {
    ceph_assert(!final_decode_needed);
    return mtime;
  }
  object_locator_t get_object_locator() const {
    ceph_assert(!final_decode_needed);
    if (hobj.oid.name.empty())
      return object_locator_t(hobj.pool, hobj.nspace, hobj.get_hash());
    else
      return object_locator_t(hobj);
  }
  const object_t& get_oid() const {
    ceph_assert(!final_decode_needed);
    return hobj.oid;
  }
  const hobject_t &get_hobj() const {
    return hobj;
  }
  snapid_t get_snapid() const {
    ceph_assert(!final_decode_needed);
    return hobj.snap;
  }
  const snapid_t& get_snap_seq() const {
    ceph_assert(!final_decode_needed);
    return snap_seq;
  }
  const std::vector<snapid_t> &get_snaps() const {
    ceph_assert(!final_decode_needed);
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
#ifdef WITH_CRIMSON
    ceph_abort("In crimson, conn is independently maintained outside Message");
#else
    return get_connection()->get_features();
#endif
  }

  MOSDOp()
    : MOSDFastDispatchOp(CEPH_MSG_OSD_OP, HEAD_VERSION, COMPAT_VERSION),
      partial_decode_needed(true),
      final_decode_needed(true),
      bdata_encode(false) { }
  MOSDOp(int inc, ceph_tid_t tid, const hobject_t& ho, spg_t& _pgid,
	 epoch_t _osdmap_epoch,
	 int _flags, uint64_t feat)
    : MOSDFastDispatchOp(CEPH_MSG_OSD_OP, HEAD_VERSION, COMPAT_VERSION),
      client_inc(inc),
      osdmap_epoch(_osdmap_epoch), flags(_flags), retry_attempt(-1),
      hobj(ho),
      pgid(_pgid),
      partial_decode_needed(false),
      final_decode_needed(false),
      features(feat),
      bdata_encode(false) {
    set_tid(tid);

    // also put the client_inc in reqid.inc, so that get_reqid() can
    // be used before the full message is decoded.
    reqid.inc = inc;
  }
private:
  ~MOSDOp() final {}

public:
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
  void write(uint64_t off, uint64_t len, ceph::buffer::list& bl) {
    add_simple_op(CEPH_OSD_OP_WRITE, off, len);
    data = std::move(bl);
    header.data_off = off;
  }
  void writefull(ceph::buffer::list& bl) {
    add_simple_op(CEPH_OSD_OP_WRITEFULL, 0, bl.length());
    data = std::move(bl);
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

  bool has_flag(__u32 flag) const { return flags & flag; };

  bool is_retry_attempt() const { return flags & CEPH_OSD_FLAG_RETRY; }
  void set_retry_attempt(unsigned a) { 
    if (a)
      flags |= CEPH_OSD_FLAG_RETRY;
    else
      flags &= ~CEPH_OSD_FLAG_RETRY;
    retry_attempt = a;
  }

  // marshalling
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    if( false == bdata_encode ) {
      OSDOp::merge_osd_op_vector_in_data(ops, data);
      bdata_encode = true;
    }

    if ((features & CEPH_FEATURE_OBJECTLOCATOR) == 0) {
      // here is the old structure we are encoding to: //
#if 0
struct ceph_osd_request_head {
	ceph_le32 client_inc;              /* client incarnation */
	struct ceph_object_layout layout;  /* pgid */
	ceph_le32 osdmap_epoch;            /* client's osdmap epoch */

	ceph_le32 flags;

	struct ceph_timespec mtime;        /* for mutations only */
	struct ceph_eversion reassert_version; /* if we are replaying op */

	ceph_le32 object_len;     /* length of object name */

	ceph_le64 snapid;         /* snapid to read */
	ceph_le64 snap_seq;       /* writer's snap context */
	ceph_le32 num_snaps;

	ceph_le16 num_ops;
	struct ceph_osd_op ops[];  /* followed by ops[], obj, ticket, snaps */
} __attribute__ ((packed));
#endif
      header.version = 1;

      encode(client_inc, payload);

      __u32 su = 0;
      encode(get_raw_pg(), payload);
      encode(su, payload);

      encode(osdmap_epoch, payload);
      encode(flags, payload);
      encode(mtime, payload);
      encode(eversion_t(), payload);  // reassert_version

      __u32 oid_len = hobj.oid.name.length();
      encode(oid_len, payload);
      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      __u32 num_snaps = snaps.size();
      encode(num_snaps, payload);
      
      //::encode(ops, payload);
      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	encode(ops[i].op, payload);

      ceph::encode_nohead(hobj.oid.name, payload);
      ceph::encode_nohead(snaps, payload);
    } else if ((features & CEPH_FEATURE_NEW_OSDOP_ENCODING) == 0) {
      header.version = 6;
      encode(client_inc, payload);
      encode(osdmap_epoch, payload);
      encode(flags, payload);
      encode(mtime, payload);
      encode(eversion_t(), payload); // reassert_version
      encode(get_object_locator(), payload);
      encode(get_raw_pg(), payload);

      encode(hobj.oid, payload);

      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
        encode(ops[i].op, payload);

      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      encode(snaps, payload);

      encode(retry_attempt, payload);
      encode(features, payload);
      if (reqid.name != entity_name_t() || reqid.tid != 0) {
	encode(reqid, payload);
      } else {
	// don't include client_inc in the reqid for the legacy v6
	// encoding or else we'll confuse older peers.
	encode(osd_reqid_t(), payload);
      }
    } else if (!HAVE_FEATURE(features, RESEND_ON_SPLIT)) {
      // reordered, v7 message encoding
      header.version = 7;
      encode(get_raw_pg(), payload);
      encode(osdmap_epoch, payload);
      encode(flags, payload);
      encode(eversion_t(), payload); // reassert_version
      encode(reqid, payload);
      encode(client_inc, payload);
      encode(mtime, payload);
      encode(get_object_locator(), payload);
      encode(hobj.oid, payload);

      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	encode(ops[i].op, payload);

      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      encode(snaps, payload);

      encode(retry_attempt, payload);
      encode(features, payload);
    } else if (!HAVE_FEATURE(features, SERVER_SQUID)) {
      // v8 encoding with hobject_t hash separate from pgid, no
      // reassert version
      header.version = 8;

      encode(pgid, payload);
      encode(hobj.get_hash(), payload);
      encode(osdmap_epoch, payload);
      encode(flags, payload);
      encode(reqid, payload);
      encode_trace(payload, features);

      // -- above decoded up front; below decoded post-dispatch thread --

      encode(client_inc, payload);
      encode(mtime, payload);
      encode(get_object_locator(), payload);
      encode(hobj.oid, payload);

      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	encode(ops[i].op, payload);

      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      encode(snaps, payload);

      encode(retry_attempt, payload);
      encode(features, payload);
    } else {
      // latest v9 opentelemetry trace
      header.version = HEAD_VERSION;

      encode(pgid, payload);
      encode(hobj.get_hash(), payload);
      encode(osdmap_epoch, payload);
      encode(flags, payload);
      encode(reqid, payload);
      encode_trace(payload, features);
      encode_otel_trace(payload, features);

      // -- above decoded up front; below decoded post-dispatch thread --

      encode(client_inc, payload);
      encode(mtime, payload);
      encode(get_object_locator(), payload);
      encode(hobj.oid, payload);

      __u16 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < ops.size(); i++)
	encode(ops[i].op, payload);

      encode(hobj.snap, payload);
      encode(snap_seq, payload);
      encode(snaps, payload);

      encode(retry_attempt, payload);
      encode(features, payload);

      if (!rdma_deliveries.empty()) {
	// the per-op rdma delivery descriptors trail the v9 fields: an
	// OSD that does not know them stops decoding before them and
	// serves the request inline, which is always correct as they
	// are advisory
	encode(rdma_deliveries, payload);
      }
    }
  }

  void decode_payload() override {
    using ceph::decode;
    ceph_assert(partial_decode_needed && final_decode_needed);
    p = std::cbegin(payload);

    // Always keep here the newest version of decoding order/rule
    if (header.version == HEAD_VERSION) {
      decode(pgid, p);
      uint32_t hash;
      decode(hash, p);
      hobj.set_hash(hash);
      decode(osdmap_epoch, p);
      decode(flags, p);
      decode(reqid, p);
      decode_trace(p);
      decode_otel_trace(p);
      if (flags & CEPH_OSD_FLAG_WRITE) {
	pulled_len = peek_pulled_len(p);
      }
    } else if (header.version == 8) {
      decode(pgid, p);      // actual pgid
      uint32_t hash;
      decode(hash, p); // raw hash value
      hobj.set_hash(hash);
      decode(osdmap_epoch, p);
      decode(flags, p);
      decode(reqid, p);
      decode_trace(p);
    } else if (header.version == 7) {
      decode(pgid.pgid, p);      // raw pgid
      hobj.set_hash(pgid.pgid.ps());
      decode(osdmap_epoch, p);
      decode(flags, p);
      eversion_t reassert_version;
      decode(reassert_version, p);
      decode(reqid, p);
    } else if (header.version < 2) {
      // old decode
      decode(client_inc, p);

      old_pg_t opgid;
      ceph::decode_raw(opgid, p);
      pgid.pgid = opgid;

      __u32 su;
      decode(su, p);

      decode(osdmap_epoch, p);
      decode(flags, p);
      decode(mtime, p);
      eversion_t reassert_version;
      decode(reassert_version, p);

      __u32 oid_len;
      decode(oid_len, p);
      decode(hobj.snap, p);
      decode(snap_seq, p);
      __u32 num_snaps;
      decode(num_snaps, p);
      
      //::decode(ops, p);
      __u16 num_ops;
      decode(num_ops, p);
      ops.resize(num_ops);
      for (unsigned i = 0; i < num_ops; i++)
	decode(ops[i].op, p);

      ceph::decode_nohead(oid_len, hobj.oid.name, p);
      ceph::decode_nohead(num_snaps, snaps, p);

      // recalculate pgid hash value
      pgid.pgid.set_ps(ceph_str_hash(CEPH_STR_HASH_RJENKINS,
				     hobj.oid.name.c_str(),
				     hobj.oid.name.length()));
      hobj.pool = pgid.pgid.pool();
      hobj.set_hash(pgid.pgid.ps());

      retry_attempt = -1;
      features = 0;
      OSDOp::split_osd_op_vector_in_data(ops, data);

      // we did the full decode
      final_decode_needed = false;

      // put client_inc in reqid.inc for get_reqid()'s benefit
      reqid = osd_reqid_t();
      reqid.inc = client_inc;
    } else if (header.version < 7) {
      decode(client_inc, p);
      decode(osdmap_epoch, p);
      decode(flags, p);
      decode(mtime, p);
      eversion_t reassert_version;
      decode(reassert_version, p);

      object_locator_t oloc;
      decode(oloc, p);

      if (header.version < 3) {
	old_pg_t opgid;
	ceph::decode_raw(opgid, p);
	pgid.pgid = opgid;
      } else {
	decode(pgid.pgid, p);
      }

      decode(hobj.oid, p);

      //::decode(ops, p);
      __u16 num_ops;
      decode(num_ops, p);
      ops.resize(num_ops);
      for (unsigned i = 0; i < num_ops; i++)
        decode(ops[i].op, p);

      decode(hobj.snap, p);
      decode(snap_seq, p);
      decode(snaps, p);

      if (header.version >= 4)
        decode(retry_attempt, p);
      else
        retry_attempt = -1;

      if (header.version >= 5)
        decode(features, p);
      else
	features = 0;

      if (header.version >= 6)
	decode(reqid, p);
      else
	reqid = osd_reqid_t();

      hobj.pool = pgid.pgid.pool();
      hobj.set_key(oloc.key);
      hobj.nspace = oloc.nspace;
      hobj.set_hash(pgid.pgid.ps());

      OSDOp::split_osd_op_vector_in_data(ops, data);

      // we did the full decode
      final_decode_needed = false;

      // put client_inc in reqid.inc for get_reqid()'s benefit
      if (reqid.name == entity_name_t() && reqid.tid == 0)
	reqid.inc = client_inc;
    } else {
      // a version newer than this one can read: refuse it, not read it as v9
      throw ceph::buffer::malformed_input(
	"MOSDOp version " + std::to_string(header.version) + " is unknown");
    }

    partial_decode_needed = false;
  }

  bool finish_decode() {
    using ceph::decode;
    ceph_assert(!partial_decode_needed); // partial decoding required
    if (!final_decode_needed)
      return false; // Message is already final decoded
    ceph_assert(header.version >= 7);

    decode(client_inc, p);
    decode(mtime, p);
    object_locator_t oloc;
    decode(oloc, p);
    decode(hobj.oid, p);

    __u16 num_ops;
    decode(num_ops, p);
    ops.resize(num_ops);
    for (unsigned i = 0; i < num_ops; i++)
      decode(ops[i].op, p);

    decode(hobj.snap, p);
    decode(snap_seq, p);
    decode(snaps, p);

    decode(retry_attempt, p);

    decode(features, p);

    if (header.version == 9 && !p.end()) {
      // per-op rdma delivery descriptors, present when some op
      // requests delivery (see encode_payload())
      decode(rdma_deliveries, p);
    }

    hobj.pool = pgid.pgid.pool();
    hobj.set_key(oloc.key);
    hobj.nspace = oloc.nspace;

    OSDOp::split_osd_op_vector_in_data(ops, data);

    final_decode_needed = false;
    return true;
  }

  void clear_buffers() override {
    OSDOp::clear_data(ops);
    bdata_encode = false;
  }

  std::string_view get_type_name() const override { return "osd_op"; }
  void print(std::ostream& out) const override {
    out << "osd_op(";
    if (!partial_decode_needed) {
      out << get_reqid() << ' ';
      out << pgid;
      if (!final_decode_needed) {
	out << ' ';
	out << hobj
	    << " " << ops
	    << " snapc " << get_snap_seq() << "=" << snaps;
	if (is_retry_attempt())
	  out << " RETRY=" << get_retry_attempt();
      } else {
	out << " " << get_raw_pg() << " (undecoded)";
      }
      out << " " << ceph_osd_flag_string(get_flags());
      out << " e" << osdmap_epoch;
    }
    out << ")";
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
}

using MOSDOp = _mosdop::MOSDOp<std::vector<OSDOp>>;


#endif
