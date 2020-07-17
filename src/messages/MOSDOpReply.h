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


#ifndef CEPH_MOSDOPREPLY_H
#define CEPH_MOSDOPREPLY_H

#include "msg/Message.h"

#include "MOSDOp.h"
#include "common/errno.h"

/*
 * OSD op reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDOpReply : public Message {
private:
  static constexpr int HEAD_VERSION = 8;
  static constexpr int COMPAT_VERSION = 2;

  object_t oid;
  pg_t pgid;
  std::vector<OSDOp> ops;
  bool bdata_encode;
  int64_t flags = 0;
  errorcode32_t result;
  eversion_t bad_replay_version;
  eversion_t replay_version;
  version_t user_version = 0;
  epoch_t osdmap_epoch = 0;
  int32_t retry_attempt = -1;
  bool do_redirect;
  request_redirect_t redirect;

public:
  const object_t& get_oid() const { return oid; }
  const pg_t&     get_pg() const { return pgid; }
  int      get_flags() const { return flags; }

  bool     is_ondisk() const { return get_flags() & CEPH_OSD_FLAG_ONDISK; }
  bool     is_onnvram() const { return get_flags() & CEPH_OSD_FLAG_ONNVRAM; }
  
  int get_result() const { return result; }
  const eversion_t& get_replay_version() const { return replay_version; }
  const version_t& get_user_version() const { return user_version; }
  
  void set_result(int r) { result = r; }

  void set_reply_versions(eversion_t v, version_t uv) {
    replay_version = v;
    user_version = uv;
    /* We go through some shenanigans here for backwards compatibility
     * with old clients, who do not look at our replay_version and
     * user_version but instead see what we now call the
     * bad_replay_version. On pools without caching
     * the user_version infrastructure is a slightly-laggy copy of
     * the regular pg version/at_version infrastructure; the difference
     * being it is not updated on watch ops like that is -- but on updates
     * it is set equal to at_version. This means that for non-watch write ops
     * on classic pools, all three of replay_version, user_version, and
     * bad_replay_version are identical. But for watch ops the replay_version
     * has been updated, while the user_at_version has not, and the semantics
     * we promised old clients are that the version they see is not an update.
     * So set the bad_replay_version to be the same as the user_at_version. */
    bad_replay_version = v;
    if (uv) {
      bad_replay_version.version = uv;
    }
  }

  /* Don't fill in replay_version for non-write ops */
  void set_enoent_reply_versions(const eversion_t& v, const version_t& uv) {
    user_version = uv;
    bad_replay_version = v;
  }

  void set_redirect(const request_redirect_t& redir) { redirect = redir; }
  const request_redirect_t& get_redirect() const { return redirect; }
  bool is_redirect_reply() const { return do_redirect; }

  void add_flags(int f) { flags |= f; }

  void claim_op_out_data(std::vector<OSDOp>& o) {
    ceph_assert(ops.size() == o.size());
    for (unsigned i = 0; i < o.size(); i++) {
      ops[i].outdata = std::move(o[i].outdata);
    }
  }
  void claim_ops(std::vector<OSDOp>& o) {
    o.swap(ops);
    bdata_encode = false;
  }
  void set_op_returns(const std::vector<pg_log_op_return_item_t>& op_returns) {
    if (op_returns.size()) {
      ceph_assert(ops.empty() || ops.size() == op_returns.size());
      ops.resize(op_returns.size());
      for (unsigned i = 0; i < op_returns.size(); ++i) {
	ops[i].rval = op_returns[i].rval;
	ops[i].outdata = op_returns[i].bl;
      }
    }
  }

  /**
   * get retry attempt
   *
   * If we don't know the attempt (because the server is old), return -1.
   */
  int get_retry_attempt() const {
    return retry_attempt;
  }
  
  // osdmap
  epoch_t get_map_epoch() const { return osdmap_epoch; }

  /*osd_reqid_t get_reqid() { return osd_reqid_t(get_dest(),
					       head.client_inc,
					       head.tid); }
  */

public:
  MOSDOpReply()
    : Message{CEPH_MSG_OSD_OPREPLY, HEAD_VERSION, COMPAT_VERSION},
    bdata_encode(false) {
    do_redirect = false;
  }
  MOSDOpReply(const MOSDOp *req, int r, epoch_t e, int acktype,
	      bool ignore_out_data)
    : Message{CEPH_MSG_OSD_OPREPLY, HEAD_VERSION, COMPAT_VERSION},
      oid(req->hobj.oid), pgid(req->pgid.pgid), ops(req->ops),
      bdata_encode(false) {

    set_tid(req->get_tid());
    result = r;
    flags =
      (req->flags & ~(CEPH_OSD_FLAG_ONDISK|CEPH_OSD_FLAG_ONNVRAM|CEPH_OSD_FLAG_ACK)) | acktype;
    osdmap_epoch = e;
    user_version = 0;
    retry_attempt = req->get_retry_attempt();
    do_redirect = false;

    for (unsigned i = 0; i < ops.size(); i++) {
      // zero out input data
      ops[i].indata.clear();
      if (ignore_out_data) {
	// original request didn't set the RETURNVEC flag
	ops[i].outdata.clear();
      }
    }
  }
private:
  ~MOSDOpReply() override {}

public:
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    if(false == bdata_encode) {
      OSDOp::merge_osd_op_vector_out_data(ops, data);
      bdata_encode = true;
    }

    if ((features & CEPH_FEATURE_PGID64) == 0) {
      header.version = 1;
      ceph_osd_reply_head head;
      memset(&head, 0, sizeof(head));
      head.layout.ol_pgid = pgid.get_old_pg().v;
      head.flags = flags;
      head.osdmap_epoch = osdmap_epoch;
      head.reassert_version = bad_replay_version;
      head.result = result;
      head.num_ops = ops.size();
      head.object_len = oid.name.length();
      encode(head, payload);
      for (unsigned i = 0; i < head.num_ops; i++) {
	encode(ops[i].op, payload);
      }
      ceph::encode_nohead(oid.name, payload);
    } else {
      header.version = HEAD_VERSION;
      encode(oid, payload);
      encode(pgid, payload);
      encode(flags, payload);
      encode(result, payload);
      encode(bad_replay_version, payload);
      encode(osdmap_epoch, payload);

      __u32 num_ops = ops.size();
      encode(num_ops, payload);
      for (unsigned i = 0; i < num_ops; i++)
	encode(ops[i].op, payload);

      encode(retry_attempt, payload);

      for (unsigned i = 0; i < num_ops; i++)
	encode(ops[i].rval, payload);

      encode(replay_version, payload);
      encode(user_version, payload);
      if ((features & CEPH_FEATURE_NEW_OSDOPREPLY_ENCODING) == 0) {
        header.version = 6;
        encode(redirect, payload);
      } else {
        do_redirect = !redirect.empty();
        encode(do_redirect, payload);
        if (do_redirect) {
          encode(redirect, payload);
        }
      }
      encode_trace(payload, features);
    }
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();

    // Always keep here the newest version of decoding order/rule
    if (header.version == HEAD_VERSION) {
      decode(oid, p);
      decode(pgid, p);
      decode(flags, p);
      decode(result, p);
      decode(bad_replay_version, p);
      decode(osdmap_epoch, p);

      __u32 num_ops = ops.size();
      decode(num_ops, p);
      ops.resize(num_ops);
      for (unsigned i = 0; i < num_ops; i++)
	decode(ops[i].op, p);
      decode(retry_attempt, p);

      for (unsigned i = 0; i < num_ops; ++i)
	decode(ops[i].rval, p);

      OSDOp::split_osd_op_vector_out_data(ops, data);

      decode(replay_version, p);
      decode(user_version, p);
      decode(do_redirect, p);
      if (do_redirect)
	decode(redirect, p);
      decode_trace(p);
    } else if (header.version < 2) {
      ceph_osd_reply_head head;
      decode(head, p);
      ops.resize(head.num_ops);
      for (unsigned i = 0; i < head.num_ops; i++) {
	decode(ops[i].op, p);
      }
      ceph::decode_nohead(head.object_len, oid.name, p);
      pgid = pg_t(head.layout.ol_pgid);
      result = (int32_t)head.result;
      flags = head.flags;
      replay_version = head.reassert_version;
      user_version = replay_version.version;
      osdmap_epoch = head.osdmap_epoch;
      retry_attempt = -1;
    } else {
      decode(oid, p);
      decode(pgid, p);
      decode(flags, p);
      decode(result, p);
      decode(bad_replay_version, p);
      decode(osdmap_epoch, p);

      __u32 num_ops = ops.size();
      decode(num_ops, p);
      ops.resize(num_ops);
      for (unsigned i = 0; i < num_ops; i++)
	decode(ops[i].op, p);

      if (header.version >= 3)
	decode(retry_attempt, p);
      else
	retry_attempt = -1;

      if (header.version >= 4) {
	for (unsigned i = 0; i < num_ops; ++i)
	  decode(ops[i].rval, p);

	OSDOp::split_osd_op_vector_out_data(ops, data);
      }

      if (header.version >= 5) {
	decode(replay_version, p);
	decode(user_version, p);
      } else {
	replay_version = bad_replay_version;
	user_version = replay_version.version;
      }

      if (header.version == 6) {
	decode(redirect, p);
        do_redirect = !redirect.empty();
      }
      if (header.version >= 7) {
        decode(do_redirect, p);
        if (do_redirect) {
	  decode(redirect, p);
        }
      }
      if (header.version >= 8) {
        decode_trace(p);
      }
    }
  }

  std::string_view get_type_name() const override { return "osd_op_reply"; }

  void print(std::ostream& out) const override {
    out << "osd_op_reply(" << get_tid()
	<< " " << oid << " " << ops
	<< " v" << get_replay_version()
	<< " uv" << get_user_version();
    if (is_ondisk())
      out << " ondisk";
    else if (is_onnvram())
      out << " onnvram";
    else
      out << " ack";
    out << " = " << get_result();
    if (get_result() < 0) {
      out << " (" << cpp_strerror(get_result()) << ")";
    }
    if (is_redirect_reply()) {
      out << " redirect: { " << redirect << " }";
    }
    out << ")";
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};


#endif
