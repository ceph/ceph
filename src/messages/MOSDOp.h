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

#define EINCLOCKED 100

class MOSDOp : public Message {
public:
  static const char* get_opname(int op) {
    switch (op) {
    case CEPH_OSD_OP_READ: return "read";
    case CEPH_OSD_OP_STAT: return "stat";

    case CEPH_OSD_OP_WRNOOP: return "wrnoop"; 
    case CEPH_OSD_OP_WRITE: return "write"; 
    case CEPH_OSD_OP_ZERO: return "zero"; 
    case CEPH_OSD_OP_DELETE: return "delete"; 
    case CEPH_OSD_OP_TRUNCATE: return "truncate"; 
    case CEPH_OSD_OP_WRLOCK: return "wrlock"; 
    case CEPH_OSD_OP_WRUNLOCK: return "wrunlock"; 
    case CEPH_OSD_OP_RDLOCK: return "rdlock"; 
    case CEPH_OSD_OP_RDUNLOCK: return "rdunlock"; 
    case CEPH_OSD_OP_UPLOCK: return "uplock"; 
    case CEPH_OSD_OP_DNLOCK: return "dnlock"; 

    case CEPH_OSD_OP_MININCLOCK: return "mininclock";

    case CEPH_OSD_OP_BALANCEREADS: return "balance-reads";
    case CEPH_OSD_OP_UNBALANCEREADS: return "unbalance-reads";

    case CEPH_OSD_OP_PULL: return "pull";
    case CEPH_OSD_OP_PUSH: return "push";
    default: assert(0);
    }
    return 0;
  }

private:
  ceph_osd_request_head head;


  friend class MOSDOpReply;

public:
  osd_reqid_t get_reqid() { return osd_reqid_t(head.client_inst.name, 
					       le32_to_cpu(head.client_inc),
					       le64_to_cpu(head.tid)); }
  int get_client_inc() { return le32_to_cpu(head.client_inc); }
  tid_t get_client_tid() { return le64_to_cpu(head.tid); }
  
  entity_name_t get_client() { return head.client_inst.name; }
  entity_inst_t get_client_inst() { return head.client_inst; }
  void set_client_addr(const entity_addr_t& a) { head.client_inst.addr = a; }

  object_t get_oid() { return object_t(head.oid); }
  pg_t     get_pg() { return pg_t(le64_to_cpu(head.layout.ol_pgid)); }
  ceph_object_layout get_layout() { return head.layout; }
  epoch_t  get_map_epoch() { return le32_to_cpu(head.osdmap_epoch); }

  eversion_t get_version() { return head.reassert_version; }
  
  const int    get_op() { return le32_to_cpu(head.op); }
  void set_op(int o) { head.op = cpu_to_le32(o); }
  bool is_read() { 
    return get_op() < 10;
  }

  off_t get_length() const { return le64_to_cpu(head.length); }
  off_t get_offset() const { return le64_to_cpu(head.offset); }

  unsigned get_inc_lock() const { return le32_to_cpu(head.inc_lock); }

  void set_peer_stat(const osd_peer_stat_t& stat) { head.peer_stat = stat; }
  const ceph_osd_peer_stat& get_peer_stat() { return head.peer_stat; }

  void inc_shed_count() { head.shed_count++; }
  int get_shed_count() { return head.shed_count; }
  


  MOSDOp(entity_inst_t asker, int inc, long tid,
         object_t oid, ceph_object_layout ol, epoch_t mapepoch, int op,
	 int flags) :
    Message(CEPH_MSG_OSD_OP) {
    memset(&head, 0, sizeof(head));
    head.client_inst.name = asker.name;
    head.client_inst.addr = asker.addr;
    head.tid = cpu_to_le64(tid);
    head.client_inc = cpu_to_le32(inc);
    head.oid = oid;
    head.layout = ol;
    head.osdmap_epoch = cpu_to_le32(mapepoch);
    head.op = cpu_to_le32(op);
    head.flags = cpu_to_le32(flags);
  }
  MOSDOp() {}

  void set_inc_lock(__u32 l) {
    head.inc_lock = cpu_to_le32(l);
  }

  void set_layout(const ceph_object_layout& l) { head.layout = l; }

  void set_length(off_t l) { head.length = cpu_to_le64(l); }
  void set_offset(off_t o) { head.offset = cpu_to_le64(o); }
  void set_version(eversion_t v) { head.reassert_version = v; }
  
  int get_flags() const { return le32_to_cpu(head.flags); }
  bool wants_ack() const { return get_flags() & CEPH_OSD_OP_ACK; }
  bool wants_commit() const { return get_flags() & CEPH_OSD_OP_SAFE; }
  bool is_retry_attempt() const { return get_flags() & CEPH_OSD_OP_RETRY; }

  void set_want_ack(bool b) { head.flags = cpu_to_le32(get_flags() | CEPH_OSD_OP_ACK); }
  void set_want_commit(bool b) { head.flags = cpu_to_le32(get_flags() | CEPH_OSD_OP_SAFE); }
  void set_retry_attempt(bool a) { head.flags = cpu_to_le32(get_flags() | CEPH_OSD_OP_RETRY); }

  // marshalling
  virtual void decode_payload() {
    int off = 0;
    ::_decode(head, payload, off);
  }

  virtual void encode_payload() {
    ::_encode(head, payload);
    env.data_off = cpu_to_le32(get_offset());
  }

  const char *get_type_name() { return "osd_op"; }
  void print(ostream& out) {
    out << "osd_op(" << get_reqid()
	<< " " << get_opname(get_op())
	<< " " << head.oid;
    if (get_length()) out << " " << get_offset() << "~" << get_length();
    if (is_retry_attempt()) out << " RETRY";
    out << ")";
  }
};


#endif
