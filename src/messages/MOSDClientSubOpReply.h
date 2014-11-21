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


#ifndef CEPH_MOSDCLIENTSUBOPREPLY_H
#define CEPH_MOSDCLIENTSUBOPREPLY_H

#include "msg/Message.h"

#include "os/ObjectStore.h"

/*
 * OSD Client Subop reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDClientSubOpReply : public Message {
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;
public:
  epoch_t map_epoch;
  
  // subop metadata
  osd_reqid_t reqid;
  pg_shard_t from;
  spg_t pgid;

  // result
  __u8 ack_type;
  int32_t result;
  
  // piggybacked osd state
  eversion_t last_complete_ondisk;


  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(map_epoch, p);
    ::decode(reqid, p);
    ::decode(pgid, p);

    ::decode(ack_type, p);
    ::decode(result, p);
    ::decode(last_complete_ondisk, p);

    ::decode(from, p);
  }
  virtual void encode_payload(uint64_t features) {
    ::encode(map_epoch, payload);
    ::encode(reqid, payload);
    ::encode(pgid, payload);
    ::encode(ack_type, payload);
    ::encode(result, payload);
    ::encode(last_complete_ondisk, payload);
    ::encode(from, payload);
  }

  epoch_t get_map_epoch() { return map_epoch; }

  spg_t get_pg() { return pgid; }

  int get_ack_type() { return ack_type; }
  bool is_ondisk() { return ack_type & CEPH_OSD_FLAG_ONDISK; }
  bool is_onnvram() { return ack_type & CEPH_OSD_FLAG_ONNVRAM; }

  int get_result() { return result; }

  void set_last_complete_ondisk(eversion_t v) { last_complete_ondisk = v; }
  eversion_t get_last_complete_ondisk() { return last_complete_ondisk; }

public:
  MOSDClientSubOpReply(
    MOSDClientSubOp *req, pg_shard_t from, int result_, epoch_t e, int at) :
    Message(MSG_OSD_CLIENT_SUBOPREPLY, HEAD_VERSION, COMPAT_VERSION),
    map_epoch(e),
    reqid(req->reqid),
    from(from),
    pgid(req->pgid.pgid, req->from.shard),
    ack_type(at),
    result(result_) {
    set_tid(req->get_tid());
  }
  MOSDClientSubOpReply() : Message(MSG_OSD_CLIENT_SUBOPREPLY) {}
private:
  ~MOSDClientSubOpReply() {}

public:
  const char *get_type_name() const { return "osd_client_subop_reply"; }
  
  void print(ostream& out) const {
    out << "osd_client_sub_op_reply(" << reqid
	<< " " << pgid;
    if (ack_type & CEPH_OSD_FLAG_ONDISK)
      out << " ondisk";
    if (ack_type & CEPH_OSD_FLAG_ONNVRAM)
      out << " onnvram";
    if (ack_type & CEPH_OSD_FLAG_ACK)
      out << " ack";
    out << ", result = " << result;
    out << ")";
  }

};


#endif
