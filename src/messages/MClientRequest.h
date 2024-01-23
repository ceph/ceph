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

#include "include/filepath.h"
#include "mds/mdstypes.h"
#include "include/ceph_features.h"
#include "mds/cephfs_features.h"
#include "messages/MMDSOp.h"

#include <sys/types.h>
#include <utime.h>
#include <sys/stat.h>
#include <fcntl.h>

struct SnapPayload {
  std::map<std::string, std::string> metadata;

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(metadata, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator &iter) {
    DECODE_START(1, iter);
    decode(metadata, iter);
    DECODE_FINISH(iter);
  }
};

WRITE_CLASS_ENCODER(SnapPayload)

// metadata ops.

class MClientRequest final : public MMDSOp {
private:
  static constexpr int HEAD_VERSION = 5;
  static constexpr int COMPAT_VERSION = 1;

public:
  mutable struct ceph_mds_request_head head; /* XXX HACK! */
  utime_t stamp;
  feature_bitset_t mds_features;

  struct Release {
    mutable ceph_mds_request_release item;
    std::string dname;

    Release() : item(), dname() {}
    Release(const ceph_mds_request_release& rel, std::string name) :
      item(rel), dname(name) {}

    void encode(ceph::buffer::list& bl) const {
      using ceph::encode;
      item.dname_len = dname.length();
      encode(item, bl);
      ceph::encode_nohead(dname, bl);
    }
    void decode(ceph::buffer::list::const_iterator& bl) {
      using ceph::decode;
      decode(item, bl);
      ceph::decode_nohead(item.dname_len, dname, bl);
    }
  };
  mutable std::vector<Release> releases; /* XXX HACK! */

  // path arguments
  filepath path, path2;
  std::string alternate_name;
  std::vector<uint64_t> gid_list;

  /* XXX HACK */
  mutable bool queued_for_replay = false;

protected:
  // cons
  MClientRequest()
    : MMDSOp(CEPH_MSG_CLIENT_REQUEST, HEAD_VERSION, COMPAT_VERSION) {
    memset(&head, 0, sizeof(head));
    head.owner_uid = -1;
    head.owner_gid = -1;
  }
  MClientRequest(int op, feature_bitset_t features = 0)
    : MMDSOp(CEPH_MSG_CLIENT_REQUEST, HEAD_VERSION, COMPAT_VERSION) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    mds_features = features;
    head.owner_uid = -1;
    head.owner_gid = -1;
  }
  ~MClientRequest() final {}

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
  bool is_async() const {
    return get_flags() & CEPH_MDS_FLAG_ASYNC;
  }

  // normal fields
  void set_stamp(utime_t t) { stamp = t; }
  void set_oldest_client_tid(ceph_tid_t t) { head.oldest_client_tid = t; }
  void inc_num_fwd() { head.ext_num_fwd = head.ext_num_fwd + 1; }
  void set_retry_attempt(int a) { head.ext_num_retry = a; }
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
  void set_async_op() {
    head.flags = head.flags | CEPH_MDS_FLAG_ASYNC;
  }

  void set_alternate_name(std::string _alternate_name) {
    alternate_name = std::move(_alternate_name);
  }
  void set_alternate_name(bufferptr&& cipher) {
    alternate_name = std::move(cipher.c_str());
  }

  utime_t get_stamp() const { return stamp; }
  ceph_tid_t get_oldest_client_tid() const { return head.oldest_client_tid; }
  int get_num_fwd() const { return head.ext_num_fwd; }
  int get_retry_attempt() const { return head.ext_num_retry; }
  int get_op() const { return head.op; }
  unsigned get_caller_uid() const { return head.caller_uid; }
  unsigned get_caller_gid() const { return head.caller_gid; }
  unsigned get_owner_uid() const { return head.owner_uid; }
  unsigned get_owner_gid() const { return head.owner_gid; }
  const std::vector<uint64_t>& get_caller_gid_list() const { return gid_list; }

  const std::string& get_path() const { return path.get_path(); }
  const filepath& get_filepath() const { return path; }
  const std::string& get_path2() const { return path2.get_path(); }
  const filepath& get_filepath2() const { return path2; }
  std::string_view get_alternate_name() const { return std::string_view(alternate_name); }

  int get_dentry_wanted() const { return get_flags() & CEPH_MDS_FLAG_WANT_DENTRY; }

  void mark_queued_for_replay() const { queued_for_replay = true; }
  bool is_queued_for_replay() const { return queued_for_replay; }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();

    if (header.version >= 4) {
      decode(head, p);
    } else {
      struct ceph_mds_request_head_legacy old_mds_head;

      decode(old_mds_head, p);
      copy_from_legacy_head(&head, &old_mds_head);
      head.version = 0;

      head.ext_num_retry = head.num_retry;
      head.ext_num_fwd = head.num_fwd;

      head.owner_uid = head.caller_uid;
      head.owner_gid = head.caller_gid;

      /* Can't set the btime from legacy struct */
      if (head.op == CEPH_MDS_OP_SETATTR) {
	int localmask = head.args.setattr.mask;

	localmask &= ~CEPH_SETATTR_BTIME;

	head.args.setattr.btime = { ceph_le32(0), ceph_le32(0) };
	head.args.setattr.mask = localmask;
      }
    }

    decode(path, p);
    decode(path2, p);
    ceph::decode_nohead(head.num_releases, releases, p);
    if (header.version >= 2)
      decode(stamp, p);
    if (header.version >= 4) // epoch 3 was for a ceph_mds_request_args change
      decode(gid_list, p);
    if (header.version >= 5)
      decode(alternate_name, p);
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    head.num_releases = releases.size();
    /*
     * If the peer is old version, we must skip all the
     * new members, because the old version of MDS or
     * client will just copy the 'head' memory and isn't
     * that smart to skip them.
     */
    if (!mds_features.test(CEPHFS_FEATURE_32BITS_RETRY_FWD)) {
      head.version = 1;
    } else if (!mds_features.test(CEPHFS_FEATURE_HAS_OWNER_UIDGID)) {
      head.version = 2;
    } else {
      head.version = CEPH_MDS_REQUEST_HEAD_VERSION;
    }

    if (features & CEPH_FEATURE_FS_BTIME) {
      encode(head, payload);
    } else {
      struct ceph_mds_request_head_legacy old_mds_head;

      copy_to_legacy_head(&old_mds_head, &head);
      encode(old_mds_head, payload);
    }

    encode(path, payload);
    encode(path2, payload);
    ceph::encode_nohead(releases, payload);
    encode(stamp, payload);
    encode(gid_list, payload);
    encode(alternate_name, payload);
  }

  std::string_view get_type_name() const override { return "creq"; }
  void print(std::ostream& out) const override {
    out << "client_request(" << get_orig_source()
	<< ":" << get_tid()
	<< " " << ceph_mds_op_name(get_op());
    if (IS_CEPH_MDS_OP_NEWINODE(head.op)) {
      out << " owner_uid=" << head.owner_uid
	  << ", owner_gid=" << head.owner_gid;
    }
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
    if (alternate_name.size())
      out << " (" << alternate_name << ") ";
    if (!get_filepath2().empty())
      out << " " << get_filepath2();
    if (stamp != utime_t())
      out << " " << stamp;
    if (head.ext_num_fwd)
      out << " FWD=" << (int)head.ext_num_fwd;
    if (head.ext_num_retry)
      out << " RETRY=" << (int)head.ext_num_retry;
    if (is_async())
      out << " ASYNC";
    if (is_replay())
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
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

WRITE_CLASS_ENCODER(MClientRequest::Release)

#endif
