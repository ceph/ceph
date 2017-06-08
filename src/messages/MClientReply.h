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


#ifndef CEPH_MCLIENTREPLY_H
#define CEPH_MCLIENTREPLY_H

#include "include/types.h"
#include "MClientRequest.h"

#include "msg/Message.h"
#include "include/ceph_features.h"
#include "common/errno.h"

/***
 *
 * MClientReply - container message for MDS reply to a client's MClientRequest
 *
 * key fields:
 *  long tid - transaction id, so the client can match up with pending request
 *  int result - error code, or fh if it was open
 *
 * for most requests:
 *  trace is a vector of InodeStat's tracing from root to the file/dir/whatever
 *  the operation referred to, so that the client can update it's info about what
 *  metadata lives on what MDS.
 *
 * for readdir replies:
 *  dir_contents is a vector of InodeStat*'s.  
 * 
 * that's mostly it, i think!
 *
 */


struct LeaseStat {
  // this matches ceph_mds_reply_lease
  __u16 mask;
  __u32 duration_ms;  
  __u32 seq;

  LeaseStat() : mask(0), duration_ms(0), seq(0) {}

  void encode(bufferlist &bl) const {
    ::encode(mask, bl);
    ::encode(duration_ms, bl);
    ::encode(seq, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(mask, bl);
    ::decode(duration_ms, bl);
    ::decode(seq, bl);
  }
};
WRITE_CLASS_ENCODER(LeaseStat)

inline ostream& operator<<(ostream& out, const LeaseStat& l) {
  return out << "lease(mask " << l.mask << " dur " << l.duration_ms << ")";
}

struct DirStat {
  // mds distribution hints
  frag_t frag;
  __s32 auth;
  set<__s32> dist;
  
  DirStat() : auth(CDIR_AUTH_PARENT) {}
  DirStat(bufferlist::iterator& p) {
    decode(p);
  }

  void encode(bufferlist& bl) {
    ::encode(frag, bl);
    ::encode(auth, bl);
    ::encode(dist, bl);
  }
  void decode(bufferlist::iterator& p) {
    ::decode(frag, p);
    ::decode(auth, p);
    ::decode(dist, p);
  }

  // see CDir::encode_dirstat for encoder.
};

struct InodeStat {
  vinodeno_t vino;
  version_t version;
  ceph_mds_reply_cap cap;

  ceph_file_layout layout;
  unsigned mode, uid, gid, nlink, rdev;
  loff_t size, max_size;
  version_t truncate_seq;
  uint64_t truncate_size;
  utime_t ctime, mtime, atime;
  version_t time_warp_seq;
  bufferlist inline_data;
  version_t inline_version;

  frag_info_t dirstat;
  nest_info_t rstat;
  
  string  symlink;   // symlink content (if symlink)
  fragtree_t dirfragtree;

  version_t xattr_version;
  bufferlist xattrbl;

  ceph_dir_layout dir_layout;

  quota_info_t quota;
  //map<string, bufferptr> xattrs;

 public:
  InodeStat() {}
  InodeStat(bufferlist::iterator& p, uint64_t features) {
    decode(p, features);
  }

  void decode(bufferlist::iterator &p, uint64_t features) {
    struct ceph_mds_reply_inode e;
    ::decode(e, p);
    vino.ino = inodeno_t(e.ino);
    vino.snapid = snapid_t(e.snapid);
    version = e.version;
    layout = e.layout;
    cap = e.cap;
    size = e.size;
    max_size = e.max_size;
    truncate_seq = e.truncate_seq;
    truncate_size = e.truncate_size;
    ctime.decode_timeval(&e.ctime);
    mtime.decode_timeval(&e.mtime);
    atime.decode_timeval(&e.atime);
    time_warp_seq = e.time_warp_seq;
    mode = e.mode;
    uid = e.uid;
    gid = e.gid;
    nlink = e.nlink;
    rdev = e.rdev;

    dirstat.nfiles = e.files;
    dirstat.nsubdirs = e.subdirs;

    rstat.rctime.decode_timeval(&e.rctime);
    rstat.rbytes = e.rbytes;
    rstat.rfiles = e.rfiles;
    rstat.rsubdirs = e.rsubdirs;

    dirfragtree.decode_nohead(e.fragtree.nsplits, p);

    ::decode(symlink, p);
    
    if (features & CEPH_FEATURE_DIRLAYOUTHASH)
      ::decode(dir_layout, p);
    else
      memset(&dir_layout, 0, sizeof(dir_layout));

    xattr_version = e.xattr_version;
    ::decode(xattrbl, p);

    if (features & CEPH_FEATURE_MDS_INLINE_DATA) {
      ::decode(inline_version, p);
      ::decode(inline_data, p);
    } else {
      inline_version = CEPH_INLINE_NONE;
    }

    if (features & CEPH_FEATURE_MDS_QUOTA)
      ::decode(quota, p);
    else
      memset(&quota, 0, sizeof(quota));
  }
  
  // see CInode::encode_inodestat for encoder.
};


class MClientReply : public Message {
  // reply data
public:
  struct ceph_mds_reply_head head;
  bufferlist trace_bl;
  bufferlist extra_bl;
  bufferlist snapbl;

 public:
  int get_op() const { return head.op; }

  void set_mdsmap_epoch(epoch_t e) { head.mdsmap_epoch = e; }
  epoch_t get_mdsmap_epoch() const { return head.mdsmap_epoch; }

  int get_result() const { return (__s32)(__u32)head.result; }

  void set_result(int r) { head.result = r; }

  void set_unsafe() { head.safe = 0; }

  bool is_safe() const { return head.safe; }

  MClientReply() : Message(CEPH_MSG_CLIENT_REPLY) {}
  MClientReply(MClientRequest *req, int result = 0) : 
    Message(CEPH_MSG_CLIENT_REPLY) {
    memset(&head, 0, sizeof(head));
    header.tid = req->get_tid();
    head.op = req->get_op();
    head.result = result;
    head.safe = 1;
  }
private:
  ~MClientReply() {}

public:
  const char *get_type_name() const { return "creply"; }
  void print(ostream& o) const {
    o << "client_reply(???:" << get_tid();
    o << " = " << get_result();
    if (get_result() <= 0) {
      o << " " << cpp_strerror(get_result());
    }
    if (head.op & CEPH_MDS_OP_WRITE) {
      if (head.safe)
	o << " safe";
      else
	o << " unsafe";
    }
    o << ")";
  }

  // serialization
  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    ::decode(trace_bl, p);
    ::decode(extra_bl, p);
    ::decode(snapbl, p);
    assert(p.end());
  }
  virtual void encode_payload(uint64_t features) {
    ::encode(head, payload);
    ::encode(trace_bl, payload);
    ::encode(extra_bl, payload);
    ::encode(snapbl, payload);
  }


  // dir contents
  void set_extra_bl(bufferlist& bl) {
    extra_bl.claim(bl);
  }
  bufferlist &get_extra_bl() {
    return extra_bl;
  }

  // trace
  void set_trace(bufferlist& bl) {
    trace_bl.claim(bl);
  }
  bufferlist& get_trace_bl() {
    return trace_bl;
  }
};

#endif
