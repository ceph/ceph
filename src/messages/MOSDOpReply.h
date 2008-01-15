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


#ifndef __MOSDOPREPLY_H
#define __MOSDOPREPLY_H

#include "msg/Message.h"

#include "MOSDOp.h"
#include "osd/ObjectStore.h"

/*
 * OSD op reply
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

class MOSDOpReply : public Message {
  ceph_osd_reply_head head;

 public:
  long     get_tid() { return head.tid; }
  object_t get_oid() { return head.oid; }
  pg_t     get_pg() { return head.layout.ol_pgid; }
  int      get_op()  { return head.op; }
  bool     is_safe() { return head.flags & CEPH_OSD_OP_SAFE; }
  
  int    get_result() { return head.result; }
  off_t get_length() { return head.length; }
  off_t get_offset() { return head.offset; }
  eversion_t get_version() { return head.reassert_version; }

  void set_result(int r) { head.result = r; }
  void set_length(off_t s) { head.length = s; }
  void set_offset(off_t o) { head.offset = o; }
  void set_version(eversion_t v) { head.reassert_version = v; }

  void set_op(int op) { head.op = op; }

  // osdmap
  epoch_t get_map_epoch() { return head.osdmap_epoch; }


public:
  MOSDOpReply(MOSDOp *req, int result, epoch_t e, bool commit) :
    Message(CEPH_MSG_OSD_OPREPLY) {
    memset(&head, 0, sizeof(head));
    head.tid = req->head.tid;
    head.op = req->head.op;
    head.flags = commit ? CEPH_OSD_OP_SAFE:0;
    head.oid = req->head.oid;
    head.layout = req->head.layout;
    head.osdmap_epoch = e;
    head.result = result;
    head.offset = req->head.offset;
    head.length = req->head.length;  // speculative... OSD should ensure these are correct
    head.reassert_version = req->head.reassert_version;
  }
  MOSDOpReply() {}


  // marshalling
  virtual void decode_payload() {
    int off = 0;
    ::_decode(head, payload, off);
  }
  virtual void encode_payload() {
    ::_encode(head, payload);
    env.data_off = head.offset;
  }

  const char *get_type_name() { return "osd_op_reply"; }
  
  void print(ostream& out) {
    out << "osd_op_reply(" << get_tid()
	<< " " << MOSDOp::get_opname(head.op)
	<< " " << head.oid;
    if (head.length) out << " " << head.offset << "~" << head.length;
    if (head.op >= 10) {
      if (is_safe())
	out << " commit";
      else
	out << " ack";
    }
    out << " = " << head.result;
    out << ")";
  }

};


#endif
