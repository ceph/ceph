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


#ifndef CEPH_MCLIENTREQUEST_H
#define CEPH_MCLIENTREQUEST_H

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

#include <string_view>

#include "msg/Message.h"
#include "include/filepath.h"
#include "mds/mdstypes.h"
#include "include/ceph_features.h"

#include <sys/types.h>
#include <utime.h>
#include <sys/stat.h>
#include <fcntl.h>


// metadata ops.

class MClientRequest : public MessageInstance<MClientRequest> {
public:
  friend factory;
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 1;

public:
  mutable struct ceph_mds_request_head head; /* XXX HACK! */
  utime_t stamp;

  struct Release {
    mutable ceph_mds_request_release item;
    string dname;

    Release() : item(), dname() {}
    Release(const ceph_mds_request_release& rel, string name) :
      item(rel), dname(name) {}

    void encode(bufferlist& bl) const {
      using ceph::encode;
      item.dname_len = dname.length();
      encode(item, bl);
      encode_nohead(dname, bl);
    }
    void decode(bufferlist::const_iterator& bl) {
      using ceph::decode;
      decode(item, bl);
      decode_nohead(item.dname_len, dname, bl);
    }
  };
  mutable vector<Release> releases; /* XXX HACK! */

  // path arguments
  filepath path, path2;
  vector<uint64_t> gid_list;

  /* XXX HACK */
  mutable bool queued_for_replay = false;

protected:
  // cons
  MClientRequest()
    : MessageInstance(CEPH_MSG_CLIENT_REQUEST, HEAD_VERSION, COMPAT_VERSION) {}
  MClientRequest(int op)
    : MessageInstance(CEPH_MSG_CLIENT_REQUEST, HEAD_VERSION, COMPAT_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = op;
  }
  ~MClientRequest() override {}

public:
  void set_mdsmap_epoch(epoch_t e) { head.mdsmap_epoch = e; }
  epoch_t get_mdsmap_epoch() const { return head.mdsmap_epoch; }
  epoch_t get_osdmap_epoch() const {
    ceph_assert(head.op == CEPH_MDS_OP_SETXATTR);
    if (header.version >= 3)
      return head.args.setxattr.osdmap_epoch;
    else
      return 0;
  }
  void set_osdmap_epoch(epoch_t e) {
    ceph_assert(head.op == CEPH_MDS_OP_SETXATTR);
    head.args.setxattr.osdmap_epoch = e;
  }

  metareqid_t get_reqid() const {
    // FIXME: for now, assume clients always have 1 incarnation
    return metareqid_t(get_orig_source(), header.tid); 
  }

  /*bool open_file_mode_is_readonly() {
    return file_mode_is_readonly(ceph_flags_to_mode(head.args.open.flags));
    }*/
  bool may_write() const {
    return
      (head.op & CEPH_MDS_OP_WRITE) || 
      (head.op == CEPH_MDS_OP_OPEN && (head.args.open.flags & (O_CREAT|O_TRUNC)));
  }

  int get_flags() const {
    return head.flags;
  }
  bool is_replay() const {
    return get_flags() & CEPH_MDS_FLAG_REPLAY;
  }

  // normal fields
  void set_stamp(utime_t t) { stamp = t; }
  void set_oldest_client_tid(ceph_tid_t t) { head.oldest_client_tid = t; }
  void inc_num_fwd() { head.num_fwd = head.num_fwd + 1; }
  void set_retry_attempt(int a) { head.num_retry = a; }
  void set_filepath(const filepath& fp) { path = fp; }
  void set_filepath2(const filepath& fp) { path2 = fp; }
  void set_string2(const char *s) { path2.set_path(std::string_view(s), 0); }
  void set_caller_uid(unsigned u) { head.caller_uid = u; }
  void set_caller_gid(unsigned g) { head.caller_gid = g; }
  void set_gid_list(int count, const gid_t *gids) {
    gid_list.reserve(count);
    for (int i = 0; i < count; ++i) {
      gid_list.push_back(gids[i]);
    }
  }
  void set_dentry_wanted() {
    head.flags = head.flags | CEPH_MDS_FLAG_WANT_DENTRY;
  }
  void set_replayed_op() {
    head.flags = head.flags | CEPH_MDS_FLAG_REPLAY;
  }

  utime_t get_stamp() const { return stamp; }
  ceph_tid_t get_oldest_client_tid() const { return head.oldest_client_tid; }
  int get_num_fwd() const { return head.num_fwd; }
  int get_retry_attempt() const { return head.num_retry; }
  int get_op() const { return head.op; }
  unsigned get_caller_uid() const { return head.caller_uid; }
  unsigned get_caller_gid() const { return head.caller_gid; }
  const vector<uint64_t>& get_caller_gid_list() const { return gid_list; }

  const string& get_path() const { return path.get_path(); }
  const filepath& get_filepath() const { return path; }
  const string& get_path2() const { return path2.get_path(); }
  const filepath& get_filepath2() const { return path2; }

  int get_dentry_wanted() const { return get_flags() & CEPH_MDS_FLAG_WANT_DENTRY; }

  void mark_queued_for_replay() const { queued_for_replay = true; }
  bool is_queued_for_replay() const { return queued_for_replay; }

  void decode_payload() override {
    auto p = payload.cbegin();

    if (header.version >= 4) {
      decode(head, p);
    } else {
      struct ceph_mds_request_head_legacy old_mds_head;

      decode(old_mds_head, p);
      copy_from_legacy_head(&head, &old_mds_head);
      head.version = 0;

      /* Can't set the btime from legacy struct */
      if (head.op == CEPH_MDS_OP_SETATTR) {
	int localmask = head.args.setattr.mask;

	localmask &= ~CEPH_SETATTR_BTIME;

	head.args.setattr.btime = { 0 };
	head.args.setattr.mask = localmask;
      }
    }

    decode(path, p);
    decode(path2, p);
    decode_nohead(head.num_releases, releases, p);
    if (header.version >= 2)
      decode(stamp, p);
    if (header.version >= 4) // epoch 3 was for a ceph_mds_request_args change
      decode(gid_list, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    head.num_releases = releases.size();
    head.version = CEPH_MDS_REQUEST_HEAD_VERSION;

    if (features & CEPH_FEATURE_FS_BTIME) {
      encode(head, payload);
    } else {
      struct ceph_mds_request_head_legacy old_mds_head;

      copy_to_legacy_head(&old_mds_head, &head);
      encode(old_mds_head, payload);
    }

    encode(path, payload);
    encode(path2, payload);
    encode_nohead(releases, payload);
    encode(stamp, payload);
    encode(gid_list, payload);
  }

  std::string_view get_type_name() const override { return "creq"; }
  void print(ostream& out) const override {
    out << "client_request(" << get_orig_source() 
	<< ":" << get_tid() 
	<< " " << ceph_mds_op_name(get_op());
    if (head.op == CEPH_MDS_OP_GETATTR)
      out << " " << ccap_string(head.args.getattr.mask);
    if (head.op == CEPH_MDS_OP_SETATTR) {
      if (head.args.setattr.mask & CEPH_SETATTR_MODE)
	out << " mode=0" << std::oct << head.args.setattr.mode << std::dec;
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
    if (head.op == CEPH_MDS_OP_SETFILELOCK ||
	head.op == CEPH_MDS_OP_GETFILELOCK) {
      out << " rule " << (int)head.args.filelock_change.rule
	  << ", type " << (int)head.args.filelock_change.type
	  << ", owner " << head.args.filelock_change.owner
	  << ", pid " << head.args.filelock_change.pid
	  << ", start " << head.args.filelock_change.start
	  << ", length " << head.args.filelock_change.length
	  << ", wait " << (int)head.args.filelock_change.wait;
    }
    //if (!get_filepath().empty()) 
    out << " " << get_filepath();
    if (!get_filepath2().empty())
      out << " " << get_filepath2();
    if (stamp != utime_t())
      out << " " << stamp;
    if (head.num_retry)
      out << " RETRY=" << (int)head.num_retry;
    if (get_flags() & CEPH_MDS_FLAG_REPLAY)
      out << " REPLAY";
    if (queued_for_replay)
      out << " QUEUED_FOR_REPLAY";
    out << " caller_uid=" << head.caller_uid
	<< ", caller_gid=" << head.caller_gid
	<< '{';
    for (auto i = gid_list.begin(); i != gid_list.end(); ++i)
      out << *i << ',';
    out << '}'
	<< ")";
  }

};

WRITE_CLASS_ENCODER(MClientRequest::Release)

#endif
