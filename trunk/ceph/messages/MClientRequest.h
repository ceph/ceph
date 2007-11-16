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


// metadata ops.
//  >=1000 --> an update, non-idempotent (i.e. an update)
#define MDS_OP_STATFS   1

#define MDS_OP_STAT     100
#define MDS_OP_LSTAT    101
#define MDS_OP_FSTAT    102
#define MDS_OP_UTIME    1102
#define MDS_OP_CHMOD    1104
#define MDS_OP_CHOWN    1105  

#define MDS_OP_READDIR  200
#define MDS_OP_MKNOD    1201
#define MDS_OP_LINK     1202
#define MDS_OP_UNLINK   1203
#define MDS_OP_RENAME   1204

#define MDS_OP_MKDIR    1220
#define MDS_OP_RMDIR    1221
#define MDS_OP_SYMLINK  1222

#define MDS_OP_OPEN     301
#define MDS_OP_TRUNCATE 1306
#define MDS_OP_FSYNC    307

#define MDS_OP_RELEASE  308  // used only by SyntheticClient op_dist thinger


class MClientRequest : public Message {
public:
  struct ceph_client_request_head head;

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
    if (head.op == MDS_OP_OPEN) 
      return open_file_mode_is_readonly();
    return (head.op < 1000);
  }
  bool auth_is_best() {
    if (!is_idempotent()) return true;
    if (head.op == MDS_OP_READDIR) return true;
    return false;    
  }
  bool follow_trailing_symlink() {
    switch (head.op) {
    case MDS_OP_LSTAT:
    case MDS_OP_FSTAT:
    case MDS_OP_LINK:
    case MDS_OP_UNLINK:
    case MDS_OP_RENAME:
      return false;
      
    case MDS_OP_STAT:
    case MDS_OP_UTIME:
    case MDS_OP_CHMOD:
    case MDS_OP_CHOWN:
    case MDS_OP_READDIR:
    case MDS_OP_OPEN:
    case MDS_OP_TRUNCATE:

    case MDS_OP_FSYNC:
    case MDS_OP_MKNOD:
    case MDS_OP_MKDIR:
    case MDS_OP_RMDIR:
    case MDS_OP_SYMLINK:
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
  const string& get_path2() { return path.get_path(); }
  filepath& get_filepath2() { return path2; }

  inodeno_t get_mds_wants_replica_in_dirino() { 
    return head.mds_wants_replica_in_dirino; }

  inodeno_t get_cwd_ino() { return head.cwd_ino ? head.cwd_ino:MDS_INO_ROOT; }

  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(head), (char*)&head);
    off += sizeof(head);
    path._decode(payload, off);
    path2._decode(payload, off);
  }

  void encode_payload() {
    payload.append((char*)&head, sizeof(head));
    path._encode(payload);
    path2._encode(payload);
  }

  char *get_type_name() { return "creq"; }
  void print(ostream& out) {
    out << "clientreq(" << get_client() 
	<< "." << get_tid() 
	<< " ";
    switch(get_op()) {
    case MDS_OP_STATFS: 
      out << "statfs"; break;

    case MDS_OP_STAT: 
      out << "stat"; break;
    case MDS_OP_LSTAT: 
      out << "lstat"; break;
    case MDS_OP_FSTAT: 
      out << "fstat"; break;
    case MDS_OP_UTIME: 
      out << "utime"; break;
    case MDS_OP_CHMOD: 
      out << "chmod"; break;
    case MDS_OP_CHOWN: 
      out << "chown"; break;
      
    case MDS_OP_READDIR: 
      out << "readdir"; break;
    case MDS_OP_MKNOD: 
      out << "mknod"; break;
    case MDS_OP_LINK: 
      out << "link"; break;
    case MDS_OP_UNLINK:
      out << "unlink"; break;
    case MDS_OP_RENAME:
      out << "rename"; break;
      
    case MDS_OP_MKDIR: 
      out << "mkdir"; break;
    case MDS_OP_RMDIR: 
      out << "rmdir"; break;
    case MDS_OP_SYMLINK: 
      out << "symlink"; break;
      
    case MDS_OP_OPEN: 
      out << "open"; break;
    case MDS_OP_TRUNCATE: 
      out << "truncate"; break;
    case MDS_OP_FSYNC: 
      out << "fsync"; break;
      //    case MDS_OP_RELEASE: 
      //out << "release"; break;
    default: 
      out << "unknown=" << get_op();
      assert(0);
    }
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
