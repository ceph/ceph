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


#ifndef __MCLIENTREPLY_H
#define __MCLIENTREPLY_H

#include "include/types.h"
#include "include/encodable.h"
#include "MClientRequest.h"

#include "msg/Message.h"
#include "mds/CInode.h"
#include "mds/CDir.h"
#include "mds/CDentry.h"

#include <vector>
using namespace std;

class CInode;

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
} __attribute__ ((packed));

struct DirStat {
  // mds distribution hints
  frag_t frag;
  __s32 auth;
  set<__s32> dist;
  __u8 is_rep;
  
  DirStat() {}
  DirStat(bufferlist::iterator& p) {
    _decode(p);
  }

  void _decode(bufferlist::iterator& p) {
    ::_decode_simple(frag, p);
    ::_decode_simple(auth, p);
    ::_decode_simple(is_rep, p);
    ::_decode_simple(dist, p);
  }

  static void _encode(bufferlist& bl, CDir *dir, int whoami) {
    /*
     * note: encoding matches struct ceph_client_reply_dirfrag
     */
    frag_t frag = dir->get_frag();
    __s32 auth;
    set<__s32> dist;
    __u8 is_rep;
    
    auth = dir->get_dir_auth().first;
    if (dir->is_auth()) 
      dir->get_dist_spec(dist, whoami);
    is_rep = dir->is_rep();

    ::_encode_simple(frag, bl);
    ::_encode_simple(auth, bl);
    ::_encode_simple(is_rep, bl);
    ::_encode_simple(dist, bl);
  }  
};

struct InodeStat {
  //inode_t inode;
  inodeno_t ino;
  version_t version;
  ceph_file_layout layout;
  utime_t ctime, mtime, atime;
  unsigned mode, uid, gid, nlink, rdev;
  loff_t size, max_size;
  
  string  symlink;   // symlink content (if symlink)
  fragtree_t dirfragtree;

 public:
  InodeStat() {}
  InodeStat(bufferlist::iterator& p) {
    _decode(p);
  }

  void _decode(bufferlist::iterator &p) {
    struct ceph_mds_reply_inode e;
    ::_decode_simple(e, p);
    ino = inodeno_t(e.ino);
    version = e.version;
    layout = e.layout;
    ctime.decode_timeval(&e.ctime);
    mtime.decode_timeval(&e.mtime);
    atime.decode_timeval(&e.atime);
    mode = e.mode;
    uid = e.uid;
    gid = e.gid;
    nlink = e.nlink;
    size = e.size;
    max_size = e.max_size;
    rdev = e.rdev;

    int n = e.fragtree.nsplits;
    while (n) {
      __u32 s, by;
      ::_decode_simple(s, p);
      ::_decode_simple(by, p);
      dirfragtree._splits[s] = by;
      n--;
    }
    ::_decode_simple(symlink, p);
  }

  static void _encode(bufferlist &bl, CInode *in) {
    /*
     * note: encoding matches struct ceph_client_reply_inode
     */
    struct ceph_mds_reply_inode e;
    memset(&e, 0, sizeof(e));
    e.ino = in->inode.ino;
    e.version = in->inode.version;
    e.layout = in->inode.layout;
    in->inode.ctime.encode_timeval(&e.ctime);
    in->inode.mtime.encode_timeval(&e.mtime);
    in->inode.atime.encode_timeval(&e.atime);
    e.mode = in->inode.mode;
    e.uid = in->inode.uid;
    e.gid = in->inode.gid;
    e.nlink = in->inode.nlink;
    e.size = in->inode.size;
    e.max_size = in->inode.max_size;
    e.rdev = in->inode.rdev;
    e.fragtree.nsplits = in->dirfragtree._splits.size();
    ::_encode_simple(e, bl);
    for (map<frag_t,int32_t>::iterator p = in->dirfragtree._splits.begin();
	 p != in->dirfragtree._splits.end();
	 p++) {
      ::_encode_simple(p->first, bl);
      ::_encode_simple(p->second, bl);
    }
    ::_encode_simple(in->symlink, bl);
  }
  
};


class MClientReply : public Message {
  // reply data
  struct ceph_mds_reply_head st;
  bufferlist trace_bl;
  bufferlist dir_bl;

 public:
  long get_tid() { return st.tid; }
  int get_op() { return st.op; }

  void set_mdsmap_epoch(epoch_t e) { st.mdsmap_epoch = e; }
  epoch_t get_mdsmap_epoch() { return st.mdsmap_epoch; }

  int get_result() { return (__s32)(__u32)st.result; }

  unsigned get_file_caps() { return st.file_caps; }
  unsigned get_file_caps_seq() { return st.file_caps_seq; }
  //uint64_t get_file_data_version() { return st.file_data_version; }
  
  void set_result(int r) { st.result = r; }
  void set_file_caps(unsigned char c) { st.file_caps = c; }
  void set_file_caps_seq(long s) { st.file_caps_seq = s; }
  //void set_file_data_version(uint64_t v) { st.file_data_version = v; }

  MClientReply() {}
  MClientReply(MClientRequest *req, int result = 0) : 
    Message(CEPH_MSG_CLIENT_REPLY) {
    memset(&st, 0, sizeof(st));
    this->st.tid = req->get_tid();
    this->st.op = req->get_op();
    this->st.result = result;
  }
  const char *get_type_name() { return "creply"; }
  void print(ostream& o) {
    o << "client_reply(" << env.dst.name << "." << st.tid;
    o << " = " << get_result();
    if (get_result() <= 0)
      o << " " << strerror(-get_result());
    o << ")";
  }

  // serialization
  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::_decode_simple(st, p);
    ::_decode_simple(trace_bl, p);
    ::_decode_simple(dir_bl, p);
    assert(p.end());
  }
  virtual void encode_payload() {
    ::_encode_simple(st, payload);
    ::_encode_simple(trace_bl, payload);
    ::_encode_simple(dir_bl, payload);
  }


  // dir contents
  void set_dir_bl(bufferlist& bl) {
    dir_bl.claim(bl);
  }
  bufferlist &get_dir_bl() {
    return dir_bl;
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
