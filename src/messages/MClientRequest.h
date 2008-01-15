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


#ifndef __MCLIENTREQUEST_H
#define __MCLIENTREQUEST_H

/**
 *
 * MClientRequest - container for a client METADATA request.  created/sent by clients.  
 *    can be forwarded around between MDS's.
 *
 *   int client - the originating client
 *   long tid   - transaction id, unique among requests for that client.  probably just a counter!
 *                -> the MDS passes the Request to the Reply constructor, so this always matches.
 *  
 *   int op - the metadata op code.  MDS_OP_RENAME, etc.
 *   int caller_uid, _gid - guess
 * 
 * fixed size arguments are in a union.
 * there's also a string argument, for e.g. symlink().
 *  
 */

#include "msg/Message.h"
#include "include/filepath.h"
#include "mds/mdstypes.h"

#include <sys/types.h>
#include <utime.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>


static inline const char* ceph_mds_op_name(int op) {
  switch (op) {
  case CEPH_MDS_OP_STAT:  return "stat";
  case CEPH_MDS_OP_LSTAT: return "lstat";
  case CEPH_MDS_OP_FSTAT: return "fstat";
  case CEPH_MDS_OP_UTIME: return "utime";
  case CEPH_MDS_OP_CHMOD: return "chmod";
  case CEPH_MDS_OP_CHOWN: return "chown";
  case CEPH_MDS_OP_READDIR: return "readdir";
  case CEPH_MDS_OP_MKNOD: return "mknod";
  case CEPH_MDS_OP_LINK: return "link";
  case CEPH_MDS_OP_UNLINK: return "unlink";
  case CEPH_MDS_OP_RENAME: return "rename";
  case CEPH_MDS_OP_MKDIR: return "mkdir";
  case CEPH_MDS_OP_RMDIR: return "rmdir";
  case CEPH_MDS_OP_SYMLINK: return "symlink";
  case CEPH_MDS_OP_OPEN: return "open";
  case CEPH_MDS_OP_TRUNCATE: return "truncate";
  case CEPH_MDS_OP_FSYNC: return "fsync";
  default: return "unknown";
  }
}

// metadata ops.
//  >=1000 --> an update, non-idempotent (i.e. an update)

class MClientRequest : public Message {
public:
  struct ceph_mds_request_head head;

  // path arguments
  filepath path, path2;

 public:
  // cons
  MClientRequest() : Message(CEPH_MSG_CLIENT_REQUEST) {}
  MClientRequest(int op, entity_inst_t ci) : Message(CEPH_MSG_CLIENT_REQUEST) {
    memset(&head, 0, sizeof(head));
    this->head.op = op;
    this->head.client_inst.name = ci.name.v;
    this->head.client_inst.addr = ci.addr.v;
  }

  void set_mdsmap_epoch(epoch_t e) { head.mdsmap_epoch = e; }
  epoch_t get_mdsmap_epoch() { return head.mdsmap_epoch; }

  metareqid_t get_reqid() {
    // FIXME: for now, assume clients always have 1 incarnation
    return metareqid_t(head.client_inst.name, head.tid); 
  }

  int get_open_file_mode() {
    if (head.args.open.flags & O_LAZY) 
      return FILE_MODE_LAZY;
    if (head.args.open.flags & O_WRONLY) 
      return FILE_MODE_W;
    if (head.args.open.flags & O_RDWR) 
      return FILE_MODE_RW;
    if (head.args.open.flags & O_APPEND) 
      return FILE_MODE_W;
    return FILE_MODE_R;
  }
  bool open_file_mode_is_readonly() {
    return get_open_file_mode() == FILE_MODE_R;
  }
  bool is_idempotent() {
    if (head.op == CEPH_MDS_OP_OPEN) 
      return open_file_mode_is_readonly();
    return (head.op < 1000);
  }
  bool auth_is_best() {
    if (!is_idempotent()) return true;
    if (head.op == CEPH_MDS_OP_READDIR) return true;
    return false;    
  }
  bool follow_trailing_symlink() {
    switch (head.op) {
    case CEPH_MDS_OP_LSTAT:
    case CEPH_MDS_OP_FSTAT:
    case CEPH_MDS_OP_LINK:
    case CEPH_MDS_OP_UNLINK:
    case CEPH_MDS_OP_RENAME:
      return false;
      
    case CEPH_MDS_OP_STAT:
    case CEPH_MDS_OP_UTIME:
    case CEPH_MDS_OP_CHMOD:
    case CEPH_MDS_OP_CHOWN:
    case CEPH_MDS_OP_READDIR:
    case CEPH_MDS_OP_OPEN:
    case CEPH_MDS_OP_TRUNCATE:

    case CEPH_MDS_OP_FSYNC:
    case CEPH_MDS_OP_MKNOD:
    case CEPH_MDS_OP_MKDIR:
    case CEPH_MDS_OP_RMDIR:
    case CEPH_MDS_OP_SYMLINK:
      return true;

    default:
      assert(0);
      return false;
    }
  }



  // normal fields
  void set_tid(tid_t t) { head.tid = t; }
  void set_oldest_client_tid(tid_t t) { head.oldest_client_tid = t; }
  void inc_num_fwd() { head.num_fwd++; }
  void set_retry_attempt(int a) { head.retry_attempt = a; }
  void set_path(string& p) { path.set_path(p); }
  void set_path(const char *p) { path.set_path(p); }
  void set_filepath(const filepath& fp) { path = fp; }
  void set_path2(string& p) { path2.set_path(p); }
  void set_path2(const char *p) { path2.set_path(p); }
  void set_filepath2(const filepath& fp) { path2 = fp; }
  void set_caller_uid(int u) { head.caller_uid = u; }
  void set_caller_gid(int g) { head.caller_gid = g; }
  void set_mds_wants_replica_in_dirino(inodeno_t dirino) { 
    head.mds_wants_replica_in_dirino = dirino; }
  
  void set_client_inst(const entity_inst_t& i) { 
    head.client_inst.name = i.name.v; 
    head.client_inst.addr = i.addr.v; 
  }
  entity_inst_t get_client_inst() { 
    return entity_inst_t(head.client_inst);
  }
  entity_name_t get_client() { return head.client_inst.name; }
  tid_t get_tid() { return head.tid; }
  tid_t get_oldest_client_tid() { return head.oldest_client_tid; }
  int get_num_fwd() { return head.num_fwd; }
  int get_retry_attempt() { return head.retry_attempt; }
  int get_op() { return head.op; }
  int get_caller_uid() { return head.caller_uid; }
  int get_caller_gid() { return head.caller_gid; }

  const string& get_path() { return path.get_path(); }
  filepath& get_filepath() { return path; }
  const string& get_path2() { return path2.get_path(); }
  filepath& get_filepath2() { return path2; }

  inodeno_t get_mds_wants_replica_in_dirino() { 
    return head.mds_wants_replica_in_dirino; }

  void decode_payload() {
    int off = 0;
    ::_decode(head, payload, off);
    path._decode(payload, off);
    path2._decode(payload, off);
  }

  void encode_payload() {
    ::_encode(head, payload);
    path._encode(payload);
    path2._encode(payload);
  }

  const char *get_type_name() { return "creq"; }
  void print(ostream& out) {
    out << "clientreq(" << get_client() 
	<< "." << get_tid() 
	<< " " << ceph_mds_op_name(get_op());
    if (!get_filepath().empty()) 
      out << " " << get_filepath();
    if (!get_filepath2().empty())
      out << " " << get_filepath2();
    if (head.retry_attempt)
      out << " RETRY=" << head.retry_attempt;
    out << ")";
  }

};

#endif
