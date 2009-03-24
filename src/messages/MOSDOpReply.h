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
#include "os/ObjectStore.h"

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
  vector<ceph_osd_op> ops;

  long     get_tid() { return head.tid; }
  object_t get_oid() { return head.oid; }
  pg_t     get_pg() { return pg_t(head.layout.ol_pgid); }
  int      get_flags() { return head.flags; }

  bool     is_ondisk() { return get_flags() & CEPH_OSD_FLAG_ONDISK; }
  bool     is_onnvram() { return get_flags() & CEPH_OSD_FLAG_ONNVRAM; }
  
  __s32 get_result() { return head.result; }
  eversion_t get_version() { return head.reassert_version; }

  bool is_modify() { return head.flags & CEPH_OSD_FLAG_MODIFY; }

  void set_result(int r) { head.result = r; }
  void set_version(eversion_t v) { head.reassert_version = v; }

  // osdmap
  epoch_t get_map_epoch() { return head.osdmap_epoch; }

  osd_reqid_t get_reqid() { return osd_reqid_t(get_dest(),
					       head.client_inc,
					       head.tid); }

public:
  MOSDOpReply(MOSDOp *req, __s32 result, epoch_t e, int acktype) :
    Message(CEPH_MSG_OSD_OPREPLY) {
    memset(&head, 0, sizeof(head));
    head.tid = req->head.tid;
    head.client_inc = req->head.client_inc;
    ops = req->ops;
    head.result = result;
    head.flags =
      (req->head.flags & ~(CEPH_OSD_FLAG_ONDISK|CEPH_OSD_FLAG_ONNVRAM|CEPH_OSD_FLAG_ACK)) | acktype;
    head.oid = req->head.oid;
    head.layout = req->head.layout;
    head.osdmap_epoch = e;
    head.reassert_version = req->head.reassert_version;
  }
  MOSDOpReply() {}


  // marshalling
  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    ::decode_nohead(head.num_ops, ops, p);
  }
  virtual void encode_payload() {
    head.num_ops = ops.size();
    ::encode(head, payload);
    ::encode_nohead(ops, payload);
  }

  const char *get_type_name() { return "osd_op_reply"; }
  
  void print(ostream& out) {
    out << "osd_op_reply(" << get_reqid()
	<< " " << head.oid << " " << ops;
    if (is_modify()) {
      if (is_ondisk())
	out << " ondisk";
      else if (is_onnvram())
	out << " onnvram";
      else
	out << " ack";
    }
    out << " = " << get_result();
    out << ")";
  }

};


#endif
