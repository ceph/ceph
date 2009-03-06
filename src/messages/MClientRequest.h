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
  MClientRequest(int op) : Message(CEPH_MSG_CLIENT_REQUEST) {
    memset(&head, 0, sizeof(head));
    head.op = op;
  }

  void set_mdsmap_epoch(epoch_t e) { head.mdsmap_epoch = e; }
  epoch_t get_mdsmap_epoch() { return head.mdsmap_epoch; }

  metareqid_t get_reqid() {
    // FIXME: for now, assume clients always have 1 incarnation
    return metareqid_t(get_orig_source(), head.tid); 
  }

  /*bool open_file_mode_is_readonly() {
    return file_mode_is_readonly(ceph_flags_to_mode(head.args.open.flags));
    }*/
  bool is_write() {
    return
      (head.op & CEPH_MDS_OP_WRITE) || 
      (head.op == CEPH_MDS_OP_OPEN && !(head.args.open.flags & (O_CREAT|O_TRUNC)));
  }
  bool can_forward() {
    if (is_write() ||
	head.op == CEPH_MDS_OP_OPEN)   // do not forward _any_ open request.
      return false;
    return true;
  }
  bool auth_is_best() {
    if (is_write()) 
      return true;
    if (head.op == CEPH_MDS_OP_OPEN ||
	head.op == CEPH_MDS_OP_READDIR) 
      return true;
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
  void set_filepath(const filepath& fp) { path = fp; }
  void set_filepath2(const filepath& fp) { path2 = fp; }
  void set_string2(const char *s) { path2.set_path(s, 0); }
  void set_caller_uid(unsigned u) { head.caller_uid = u; }
  void set_caller_gid(unsigned g) { head.caller_gid = g; }
  void set_mds_wants_replica_in_dirino(inodeno_t dirino) { 
    head.mds_wants_replica_in_dirino = dirino; }
  
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
      ::encode(inopath, payload);
    } else {
      ::encode(path, payload);
      ::encode(path2, payload);
    }
  }

  const char *get_type_name() { return "creq"; }
  void print(ostream& out) {
    out << "client_request(" << get_orig_source() 
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
