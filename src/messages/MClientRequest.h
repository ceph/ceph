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
  case CEPH_MDS_OP_FINDINODE: return "findinode";
  case CEPH_MDS_OP_STAT:  return "stat";
  case CEPH_MDS_OP_LSTAT:  return "lstat";
  case CEPH_MDS_OP_UTIME: return "utime";
  case CEPH_MDS_OP_LUTIME: return "lutime";
  case CEPH_MDS_OP_CHMOD: return "chmod";
  case CEPH_MDS_OP_LCHMOD: return "lchmod";
  case CEPH_MDS_OP_CHOWN: return "chown";
  case CEPH_MDS_OP_LCHOWN: return "lchown";
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
  case CEPH_MDS_OP_LTRUNCATE: return "ltruncate";
  case CEPH_MDS_OP_FSYNC: return "fsync";
  default: return "unknown";
  }
}

// metadata ops.

static inline ostream& operator<<(ostream &out, const ceph_inopath_item &i) {
  return out << i.ino << "." << i.dname_hash;
}

class MClientRequest : public Message {
public:
  struct ceph_mds_request_head head;

  // path arguments
  filepath path, path2;
  vector<ceph_inopath_item> inopath;

 public:
  // cons
  MClientRequest() : Message(CEPH_MSG_CLIENT_REQUEST) {}
  MClientRequest(int op, entity_inst_t ci) : Message(CEPH_MSG_CLIENT_REQUEST) {
    memset(&head, 0, sizeof(head));
    this->head.op = op;
    this->head.client_inst.name = ci.name;
    this->head.client_inst.addr = ci.addr;
  }

  void set_mdsmap_epoch(epoch_t e) { head.mdsmap_epoch = e; }
  epoch_t get_mdsmap_epoch() { return head.mdsmap_epoch; }

  metareqid_t get_reqid() {
    // FIXME: for now, assume clients always have 1 incarnation
    return metareqid_t(head.client_inst.name, head.tid); 
  }

  bool open_file_mode_is_readonly() {
    return file_mode_is_readonly(ceph_flags_to_mode(head.args.open.flags));
  }
  bool is_idempotent() {
    if (head.op == CEPH_MDS_OP_OPEN) 
      return open_file_mode_is_readonly();
    return (head.op & CEPH_MDS_OP_WRITE) == 0;
  }
  bool auth_is_best() {
    if (!is_idempotent()) return true;
    if (head.op == CEPH_MDS_OP_READDIR) return true;
    return false;    
  }
  bool follow_trailing_symlink() {
    return head.op & CEPH_MDS_OP_FOLLOW_LINK;
  }



  // normal fields
  void set_tid(tid_t t) { head.tid = t; }
  void set_oldest_client_tid(tid_t t) { head.oldest_client_tid = t; }
  void inc_num_fwd() { head.num_fwd = head.num_fwd + 1; }
  void set_retry_attempt(int a) { head.retry_attempt = a; }
  void set_path(string& p) { path.set_path(p); }
  void set_path(const char *p) { path.set_path(p); }
  void set_filepath(const filepath& fp) { path = fp; }
  void set_path2(string& p) { path2.set_path(p); }
  void set_path2(const char *p) { path2.set_path(p); }
  void set_filepath2(const filepath& fp) { path2 = fp; }
  void set_caller_uid(unsigned u) { head.caller_uid = u; }
  void set_caller_gid(unsigned g) { head.caller_gid = g; }
  void set_mds_wants_replica_in_dirino(inodeno_t dirino) { 
    head.mds_wants_replica_in_dirino = dirino; }
  
  void set_client_inst(const entity_inst_t& i) { 
    head.client_inst.name = i.name; 
    head.client_inst.addr = i.addr; 
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
  unsigned get_caller_uid() { return head.caller_uid; }
  unsigned get_caller_gid() { return head.caller_gid; }

  const string& get_path() { return path.get_path(); }
  filepath& get_filepath() { return path; }
  const string& get_path2() { return path2.get_path(); }
  filepath& get_filepath2() { return path2; }

  inodeno_t get_mds_wants_replica_in_dirino() { 
    return inodeno_t(head.mds_wants_replica_in_dirino); }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    if (head.op == CEPH_MDS_OP_FINDINODE) {
      ::decode(inopath, p);
    } else {
      ::decode(path, p);
      ::decode(path2, p);
    }
  }

  void encode_payload() {
    ::encode(head, payload);
    if (head.op == CEPH_MDS_OP_FINDINODE) {
      ::encode(path, payload);
      ::encode(path2, payload);
    } else {
      ::encode(inopath, payload);
    }
  }

  const char *get_type_name() { return "creq"; }
  void print(ostream& out) {
    out << "client_request(" << get_client() 
	<< "." << get_tid() 
	<< " " << ceph_mds_op_name(get_op());
    //if (!get_filepath().empty()) 
    out << " " << get_filepath();
    if (!get_filepath2().empty())
      out << " " << get_filepath2();
    if (!inopath.empty())
      out << " " << inopath;
    if (head.retry_attempt)
      out << " RETRY=" << head.retry_attempt;
    out << ")";
  }

};

#endif
