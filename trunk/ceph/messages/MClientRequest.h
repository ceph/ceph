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
  struct {
    tid_t tid;
    tid_t oldest_client_tid;
    int num_fwd;
    int retry_attempt;
    inodeno_t  mds_wants_replica_in_dirino;

    entity_inst_t client_inst;

    int op;
    int caller_uid, caller_gid;
    inodeno_t cwd_ino;
  } st;

  // path arguments
  filepath path;
  string sarg;

 public:
  // fixed size arguments.  in a union.
  // note: nothing with a constructor can go here; use underlying base 
  // types for _inodeno_t, _frag_t.
  union {
    struct {
      int mask;
    } stat;
    struct {
      _inodeno_t ino;
      int mask;
    } fstat;
    struct {
      _frag_t frag;
    } readdir;
    struct {
      _utime_t mtime;
      _utime_t atime;
    } utime;
    struct {
      mode_t mode;
    } chmod; 
    struct {
      uid_t uid;
      gid_t gid;
    } chown;
    struct {
      mode_t mode;
      dev_t rdev;
    } mknod; 
    struct {
      mode_t mode;
    } mkdir; 
    struct {
      int flags;
      mode_t mode;
    } open;
    struct {
      _inodeno_t ino;  // optional
      off_t length;
    } truncate;
    struct {
      _inodeno_t ino;
    } fsync;
  } args;

  // cons
  MClientRequest() : Message(CEPH_MSG_CLIENT_REQUEST) {}
  MClientRequest(int op, entity_inst_t ci) : Message(CEPH_MSG_CLIENT_REQUEST) {
    memset(&st, 0, sizeof(st));
    memset(&args, 0, sizeof(args));
    this->st.op = op;
    this->st.client_inst = ci;
  }

  metareqid_t get_reqid() {
    // FIXME: for now, assume clients always have 1 incarnation
    return metareqid_t(st.client_inst.name.num(), st.tid); 
  }

  int get_open_file_mode() {
    if (args.open.flags & O_LAZY) 
      return FILE_MODE_LAZY;
    if (args.open.flags & O_WRONLY) 
      return FILE_MODE_W;
    if (args.open.flags & O_RDWR) 
      return FILE_MODE_RW;
    if (args.open.flags & O_APPEND) 
      return FILE_MODE_W;
    return FILE_MODE_R;
  }
  bool open_file_mode_is_readonly() {
    return get_open_file_mode() == FILE_MODE_R;
  }
  bool is_idempotent() {
    if (st.op == MDS_OP_OPEN) 
      return open_file_mode_is_readonly();
    return (st.op < 1000);
  }
  bool auth_is_best() {
    if (!is_idempotent()) return true;
    if (st.op == MDS_OP_READDIR) return true;
    return false;    
  }
  bool follow_trailing_symlink() {
    switch (st.op) {
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
  void set_tid(tid_t t) { st.tid = t; }
  void set_oldest_client_tid(tid_t t) { st.oldest_client_tid = t; }
  void inc_num_fwd() { st.num_fwd++; }
  void set_retry_attempt(int a) { st.retry_attempt = a; }
  void set_path(string& p) { path.set_path(p); }
  void set_path(const char *p) { path.set_path(p); }
  void set_path(const filepath& fp) { path = fp; }
  void set_caller_uid(int u) { st.caller_uid = u; }
  void set_caller_gid(int g) { st.caller_gid = g; }
  void set_sarg(string& arg) { this->sarg = arg; }
  void set_sarg(const char *arg) { this->sarg = arg; }
  void set_mds_wants_replica_in_dirino(inodeno_t dirino) { 
    st.mds_wants_replica_in_dirino = dirino; }
  
  void set_client_inst(const entity_inst_t& i) { st.client_inst = i; }
  const entity_inst_t& get_client_inst() { return st.client_inst; }

  int get_client() { return st.client_inst.name.num(); }
  tid_t get_tid() { return st.tid; }
  tid_t get_oldest_client_tid() { return st.oldest_client_tid; }
  int get_num_fwd() { return st.num_fwd; }
  int get_retry_attempt() { return st.retry_attempt; }
  int get_op() { return st.op; }
  int get_caller_uid() { return st.caller_uid; }
  int get_caller_gid() { return st.caller_gid; }
  //inodeno_t get_ino() { return st.ino; }
  const string& get_path() { return path.get_path(); }
  filepath& get_filepath() { return path; }
  string& get_sarg() { return sarg; }
  inodeno_t get_mds_wants_replica_in_dirino() { 
    return st.mds_wants_replica_in_dirino; }

  inodeno_t get_cwd_ino() { return st.cwd_ino ? st.cwd_ino:inodeno_t(MDS_INO_ROOT); }

  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);
    payload.copy(off, sizeof(args), (char*)&args);
    off += sizeof(args);
    path._decode(payload, off);
    ::_decode(sarg, payload, off);
  }

  void encode_payload() {
    payload.append((char*)&st, sizeof(st));
    payload.append((char*)&args, sizeof(args));
    path._encode(payload);
    ::_encode(sarg, payload);
  }

  char *get_type_name() { return "creq"; }
  void print(ostream& out) {
    out << "clientreq(client" << get_client() 
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
    if (get_path().length()) 
      out << " " << get_path();
    if (get_sarg().length())
      out << " " << get_sarg();
    if (st.retry_attempt)
      out << " RETRY=" << st.retry_attempt;
    out << ")";
  }

};

#endif
