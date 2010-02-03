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

class MClientRequest : public Message {
public:
  struct ceph_mds_request_head head;

  struct Release {
    mutable ceph_mds_request_release item;
    nstring dname;

    Release() : item(), dname() {}
    Release(const ceph_mds_request_release& rel, nstring name) :
      item(rel), dname(name) {}

    void encode(bufferlist& bl) const {
      item.dname_len = dname.length();
      ::encode(item, bl);
      ::encode_nohead(dname, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(item, bl);
      ::decode_nohead(item.dname_len, dname, bl);
    }
  };
  vector<Release> releases;

  // path arguments
  filepath path, path2;



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
    return metareqid_t(get_orig_source(), header.tid); 
  }

  /*bool open_file_mode_is_readonly() {
    return file_mode_is_readonly(ceph_flags_to_mode(head.args.open.flags));
    }*/
  bool is_write() {
    return
      (head.op & CEPH_MDS_OP_WRITE) || 
      (head.op == CEPH_MDS_OP_OPEN && !(head.args.open.flags & (O_CREAT|O_TRUNC))) ||
      (head.op == CEPH_MDS_OP_CREATE && !(head.args.open.flags & (O_CREAT|O_TRUNC)));
  }
  bool can_forward() {
    if (is_write() ||
	head.op == CEPH_MDS_OP_OPEN ||   // do not forward _any_ open request.
	head.op == CEPH_MDS_OP_CREATE)   // do not forward _any_ open request.
      return false;
    return true;
  }
  bool auth_is_best() {
    if (is_write()) 
      return true;
    if (head.op == CEPH_MDS_OP_OPEN ||
	head.op == CEPH_MDS_OP_CREATE ||
	head.op == CEPH_MDS_OP_READDIR) 
      return true;
    return false;    
  }

  int get_flags() {
    return head.flags;
  }
  bool is_replay() {
    return get_flags() & CEPH_MDS_FLAG_REPLAY;
  }

  // normal fields
  void set_oldest_client_tid(tid_t t) { head.oldest_client_tid = t; }
  void inc_num_fwd() { head.num_fwd = head.num_fwd + 1; }
  void set_retry_attempt(int a) { head.num_retry = a; }
  void set_filepath(const filepath& fp) { path = fp; }
  void set_filepath2(const filepath& fp) { path2 = fp; }
  void set_string2(const char *s) { path2.set_path(s, 0); }
  void set_caller_uid(unsigned u) { head.caller_uid = u; }
  void set_caller_gid(unsigned g) { head.caller_gid = g; }
  void set_dentry_wanted() {
    head.flags = head.flags | CEPH_MDS_FLAG_WANT_DENTRY;
  }
  void set_replayed_op() {
    head.flags = head.flags | CEPH_MDS_FLAG_REPLAY;
  }
    
  tid_t get_oldest_client_tid() { return head.oldest_client_tid; }
  int get_num_fwd() { return head.num_fwd; }
  int get_retry_attempt() { return head.num_retry; }
  int get_op() { return head.op; }
  unsigned get_caller_uid() { return head.caller_uid; }
  unsigned get_caller_gid() { return head.caller_gid; }

  const string& get_path() { return path.get_path(); }
  filepath& get_filepath() { return path; }
  const string& get_path2() { return path2.get_path(); }
  filepath& get_filepath2() { return path2; }

  int get_dentry_wanted() { return get_flags() & CEPH_MDS_FLAG_WANT_DENTRY; }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    ::decode(path, p);
    ::decode(path2, p);
    ::decode_nohead(head.num_releases, releases, p);
  }

  void encode_payload() {
    head.num_releases = releases.size();
    ::encode(head, payload);
    ::encode(path, payload);
    ::encode(path2, payload);
    ::encode_nohead(releases, payload);
  }

  const char *get_type_name() { return "creq"; }
  void print(ostream& out) {
    out << "client_request(" << get_orig_source() 
	<< ":" << get_tid() 
	<< " " << ceph_mds_op_name(get_op());
    if (head.op == CEPH_MDS_OP_GETATTR)
      out << " " << ccap_string(head.args.getattr.mask);
    if (head.op == CEPH_MDS_OP_SETATTR) {
      if (head.args.setattr.mask & CEPH_SETATTR_MODE)
	out << " mode=0" << ios::oct << head.args.setattr.mode << ios::dec;
      if (head.args.setattr.mask & CEPH_SETATTR_UID)
	out << " uid=" << head.args.setattr.uid;
      if (head.args.setattr.mask & CEPH_SETATTR_GID)
	out << " gid=" << head.args.setattr.gid;
      if (head.args.setattr.mask & CEPH_SETATTR_SIZE)
	out << " size=" << head.args.setattr.size;
      if (head.args.setattr.mask & CEPH_SETATTR_MTIME)
	out << " mtime=" << utime_t(head.args.setattr.mtime);
      if (head.args.setattr.mask & CEPH_SETATTR_ATIME)
	out << " atime=" << utime_t(head.args.setattr.atime);
    }
    //if (!get_filepath().empty()) 
    out << " " << get_filepath();
    if (!get_filepath2().empty())
      out << " " << get_filepath2();
    if (head.num_retry)
      out << " RETRY=" << (int)head.num_retry;
    if (get_flags() & CEPH_MDS_FLAG_REPLAY)
      out << " REPLAY";
    out << ")";
  }

};

WRITE_CLASS_ENCODER(MClientRequest::Release)

#endif
