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


#ifndef CEPH_MOSDREPOPREPLY_H
#define CEPH_MOSDREPOPREPLY_H

#include "msg/Message.h"

#include "os/ObjectStore.h"

/*
 * OSD Client Subop reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDRepOpReply : public Message {
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

  bufferlist::iterator p;
  // Decoding flags. Decoding is only needed for messages catched by pipe reader.
  bool final_decode_needed;

  virtual void decode_payload() {
    p = payload.begin();
    ::decode(map_epoch, p);
    ::decode(reqid, p);
    ::decode(pgid, p);
  }

  void finish_decode() {
    if (!final_decode_needed)
      return; // Message is already final decoded
    ::decode(ack_type, p);
    ::decode(result, p);
    ::decode(last_complete_ondisk, p);

    ::decode(from, p);
    final_decode_needed = false;
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
  MOSDRepOpReply(
    MOSDRepOp *req, pg_shard_t from, int result_, epoch_t e, int at) :
    Message(MSG_OSD_REPOPREPLY, HEAD_VERSION, COMPAT_VERSION),
    map_epoch(e),
    reqid(req->reqid),
    from(from),
    pgid(req->pgid.pgid, req->from.shard),
    ack_type(at),
    result(result_),
    final_decode_needed(false) {
    set_tid(req->get_tid());
  }
  MOSDRepOpReply() 
    : Message(MSG_OSD_REPOPREPLY), map_epoch(0),  
      ack_type(0), result(0),
      final_decode_needed(true) {}
private:
  ~MOSDRepOpReply() {}

public:
  const char *get_type_name() const { return "osd_repop_reply"; }

  void print(ostream& out) const {
    out << "osd_repop_reply(" << reqid
        << " " << pgid;
    if (!final_decode_needed) {
      if (ack_type & CEPH_OSD_FLAG_ONDISK)
        out << " ondisk";
      if (ack_type & CEPH_OSD_FLAG_ONNVRAM)
        out << " onnvram";
      if (ack_type & CEPH_OSD_FLAG_ACK)
        out << " ack";
      out << ", result = " << result;
    }
    out << ")";
  }

};

REGISTER_MESSAGE(MOSDRepOpReply, MSG_OSD_REPOPREPLY);
#endif
