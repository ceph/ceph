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
#include "os/ObjectStore.h"

/*
 * OSD op reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDOpReply : public Message {

  static const int HEAD_VERSION = 4;
  static const int COMPAT_VERSION = 2;

  object_t oid;
  pg_t pgid;
  vector<OSDOp> ops;
  int64_t flags;
  int32_t result;
  eversion_t reassert_version;
  epoch_t osdmap_epoch;
  int32_t retry_attempt;

public:
  object_t get_oid() const { return oid; }
  pg_t     get_pg() const { return pgid; }
  int      get_flags() const { return flags; }

  bool     is_ondisk() const { return get_flags() & CEPH_OSD_FLAG_ONDISK; }
  bool     is_onnvram() const { return get_flags() & CEPH_OSD_FLAG_ONNVRAM; }
  
  int get_result() const { return result; }
  eversion_t get_version() { return reassert_version; }
  
  void set_result(int r) { result = r; }
  void set_version(eversion_t v) { reassert_version = v; }

  void add_flags(int f) { flags |= f; }

  void claim_op_out_data(vector<OSDOp>& o) {
    assert(ops.size() == o.size());
    for (unsigned i = 0; i < o.size(); i++) {
      ops[i].outdata.claim(o[i].outdata);
    }
  }
  void claim_ops(vector<OSDOp>& o) {
    o.swap(ops);
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
  epoch_t get_map_epoch() { return osdmap_epoch; }

  /*osd_reqid_t get_reqid() { return osd_reqid_t(get_dest(),
					       head.client_inc,
					       head.tid); }
  */

public:
  MOSDOpReply()
    : Message(CEPH_MSG_OSD_OPREPLY, HEAD_VERSION, COMPAT_VERSION) { }
  MOSDOpReply(MOSDOp *req, int r, epoch_t e, int acktype)
    : Message(CEPH_MSG_OSD_OPREPLY, HEAD_VERSION, COMPAT_VERSION) {
    set_tid(req->get_tid());
    ops = req->ops;
    result = r;
    flags =
      (req->flags & ~(CEPH_OSD_FLAG_ONDISK|CEPH_OSD_FLAG_ONNVRAM|CEPH_OSD_FLAG_ACK)) | acktype;
    oid = req->oid;
    pgid = req->pgid;
    osdmap_epoch = e;
    reassert_version = req->reassert_version;
    retry_attempt = req->get_retry_attempt();

    // zero out ops payload_len
    for (unsigned i = 0; i < ops.size(); i++)
      ops[i].op.payload_len = 0;
  }
private:
  ~MOSDOpReply() {}

public:
  virtual void encode_payload(uint64_t features) {

    OSDOp::merge_osd_op_vector_out_data(ops, data);

    if ((features & CEPH_FEATURE_PGID64) == 0) {
      header.version = 1;
      ceph_osd_reply_head head;
      memset(&head, 0, sizeof(head));
      head.layout.ol_pgid = pgid.get_old_pg().v;
      head.flags = flags;
      head.osdmap_epoch = osdmap_epoch;
      head.result = result;
      head.num_ops = ops.size();
      head.object_len = oid.name.length();
      ::encode(head, payload);
      for (unsigned i = 0; i < head.num_ops; i++) {
	::encode(ops[i].op, payload);
      }
      ::encode_nohead(oid.name, payload);
    } else {
      ::encode(oid, payload);
      ::encode(pgid, payload);
      ::encode(flags, payload);
      ::encode(result, payload);
      ::encode(reassert_version, payload);
      ::encode(osdmap_epoch, payload);

      __u32 num_ops = ops.size();
      ::encode(num_ops, payload);
      for (unsigned i = 0; i < num_ops; i++)
	::encode(ops[i].op, payload);

      ::encode(retry_attempt, payload);

      for (unsigned i = 0; i < num_ops; i++)
	::encode(ops[i].rval, payload);
    }
  }
  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    if (header.version < 2) {
      ceph_osd_reply_head head;
      ::decode(head, p);
      ops.resize(head.num_ops);
      for (unsigned i = 0; i < head.num_ops; i++) {
	::decode(ops[i].op, p);
      }
      ::decode_nohead(head.object_len, oid.name, p);
      pgid = pg_t(head.layout.ol_pgid);
      result = head.result;
      flags = head.flags;
      reassert_version = head.reassert_version;
      osdmap_epoch = head.osdmap_epoch;
      retry_attempt = -1;
    } else {
      ::decode(oid, p);
      ::decode(pgid, p);
      ::decode(flags, p);
      ::decode(result, p);
      ::decode(reassert_version, p);
      ::decode(osdmap_epoch, p);

      __u32 num_ops = ops.size();
      ::decode(num_ops, p);
      ops.resize(num_ops);
      for (unsigned i = 0; i < num_ops; i++)
	::decode(ops[i].op, p);

      if (header.version >= 3)
	::decode(retry_attempt, p);
      else
	retry_attempt = -1;

      if (header.version >= 4) {
	for (unsigned i = 0; i < num_ops; ++i)
	  ::decode(ops[i].rval, p);

	OSDOp::split_osd_op_vector_out_data(ops, data);
      }
    }
  }

  const char *get_type_name() const { return "osd_op_reply"; }
  
  void print(ostream& out) const {
    out << "osd_op_reply(" << get_tid()
	<< " " << oid << " " << ops;
    if (is_ondisk())
      out << " ondisk";
    else if (is_onnvram())
      out << " onnvram";
    else
      out << " ack";
    out << " = " << get_result();
    if (get_result() < 0) {
      char buf[80];
      out << " (" << strerror_r(-get_result(), buf, sizeof(buf)) << ")";
    }
    out << ")";
  }

};


#endif
