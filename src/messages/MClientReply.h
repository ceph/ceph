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
  inode_t inode;
  string  symlink;   // symlink content (if symlink)
  fragtree_t dirfragtree;
  uint32_t mask;

 public:
  InodeStat() {}
  InodeStat(bufferlist::iterator& p) {
    _decode(p);
  }

  void _decode(bufferlist::iterator &p) {
    struct ceph_mds_reply_inode e;
    ::_decode_simple(e, p);
    inode.ino = e.ino;
    inode.layout = e.layout;
    inode.ctime.decode_timeval(&e.ctime);
    inode.mtime.decode_timeval(&e.mtime);
    inode.atime.decode_timeval(&e.atime);
    inode.mode = e.mode;
    inode.uid = e.uid;
    inode.gid = e.gid;
    inode.nlink = e.nlink;
    inode.size = e.size;
    inode.rdev = e.rdev;

    int n = e.fragtree.nsplits;
    while (n) {
      __u32 s, by;
      ::_decode_simple(s, p);
      ::_decode_simple(by, p);
      dirfragtree._splits[s] = by;
      n--;
    }
    ::_decode_simple(symlink, p);
    mask = e.mask;
  }

  static void _encode(bufferlist &bl, CInode *in) {
    int mask = STAT_MASK_INO|STAT_MASK_TYPE|STAT_MASK_BASE;

    // mask
    if (in->authlock.can_rdlock(0)) mask |= STAT_MASK_AUTH;
    if (in->linklock.can_rdlock(0)) mask |= STAT_MASK_LINK;
    if (in->filelock.can_rdlock(0)) mask |= STAT_MASK_FILE;

    /*
     * note: encoding matches struct ceph_client_reply_inode
     */
    struct ceph_mds_reply_inode e;
    memset(&e, 0, sizeof(e));
    e.ino = in->inode.ino;
    e.layout = in->inode.layout;
    in->inode.ctime.encode_timeval(&e.ctime);
    in->inode.mtime.encode_timeval(&e.mtime);
    in->inode.atime.encode_timeval(&e.atime);
    e.mode = in->inode.mode;
    e.uid = in->inode.uid;
    e.gid = in->inode.gid;
    e.nlink = in->inode.nlink;
    e.size = in->inode.size;
    e.rdev = in->inode.rdev;
    e.mask = mask;
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
 
  list<InodeStat*> trace_in;
  list<DirStat*>   trace_dir;
  list<string>     trace_dn;
  bufferlist trace_bl;

  DirStat *dir_dir;
  list<InodeStat*> dir_in;
  list<string> dir_dn;
  bufferlist dir_bl;

 public:
  long get_tid() { return st.tid; }
  int get_op() { return st.op; }

  void set_mdsmap_epoch(epoch_t e) { st.mdsmap_epoch = e; }
  epoch_t get_mdsmap_epoch() { return st.mdsmap_epoch; }

  int get_result() { return st.result; }

  inodeno_t get_ino() { return trace_in.back()->inode.ino; }
  const inode_t& get_inode() { return trace_in.back()->inode; }

  unsigned char get_file_caps() { return st.file_caps; }
  long get_file_caps_seq() { return st.file_caps_seq; }
  //uint64_t get_file_data_version() { return st.file_data_version; }
  
  void set_result(int r) { st.result = r; }
  void set_file_caps(unsigned char c) { st.file_caps = c; }
  void set_file_caps_seq(long s) { st.file_caps_seq = s; }
  //void set_file_data_version(uint64_t v) { st.file_data_version = v; }

  MClientReply() : dir_dir(0) {}
  MClientReply(MClientRequest *req, int result = 0) : 
    Message(CEPH_MSG_CLIENT_REPLY), dir_dir(0) {
    memset(&st, 0, sizeof(st));
    this->st.tid = req->get_tid();
    this->st.op = req->get_op();

    this->st.result = result;
  }
  virtual ~MClientReply() {
    list<InodeStat*>::iterator it;
    
    for (it = trace_in.begin(); it != trace_in.end(); ++it) 
      delete *it;
    for (it = dir_in.begin(); it != dir_in.end(); ++it) 
      delete *it;
  }
  const char *get_type_name() { return "creply"; }
  void print(ostream& o) {
    o << "creply(" << env.dst.name << "." << st.tid;
    o << " = " << st.result;
    if (st.result <= 0)
      o << " " << strerror(-st.result);
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
  void take_dir_items(bufferlist& bl) {
    dir_bl.claim(bl);
  }
  void _decode_dir() {
    bufferlist::iterator p = dir_bl.begin();
    dir_dir = new DirStat(p);
    __u32 num;
    ::_decode_simple(num, p);
    while (num--) {
      string dn;
      ::_decode_simple(dn, p);
      dir_dn.push_back(dn);
      dir_in.push_back(new InodeStat(p));
    }
    assert(p.end());
  }

  const list<InodeStat*>& get_dir_in() { 
    if (dir_in.empty() && dir_bl.length()) _decode_dir();
    return dir_in;
  }
  const list<string>& get_dir_dn() { 
    if (dir_dn.empty() && dir_bl.length()) _decode_dir();    
    return dir_dn; 
  }
  const DirStat* get_dir_dir() {
    return dir_dir;
  }


  // trace
  void set_trace_dist(CInode *in, int whoami) {
    // inode, dentry, dir, ..., inode
    bufferlist bl;
    __u32 numi = 0;
    if (in) 
      while (true) {
	// inode
	InodeStat::_encode(bl, in);
	numi++;
	CDentry *dn = in->get_parent_dn();
	if (!dn) break;

	// dentry, dir
	::_encode_simple(in->get_parent_dn()->get_name(), bl);
	DirStat::_encode(bl, dn->get_dir(), whoami);
	in = dn->get_dir()->get_inode();
      }
    ::_encode_simple(numi, trace_bl);
    trace_bl.claim_append(bl);
  }
  void _decode_trace() {
    bufferlist::iterator p = trace_bl.begin();
    __u32 numi;
    ::_decode_simple(numi, p);
    if (numi) 
      while (1) {
	// inode
	trace_in.push_front(new InodeStat(p));
	if (--numi == 0) break;

	// dentry, dir
	string ref_dn;
	::_decode_simple(ref_dn, p);
	trace_dn.push_front(ref_dn);
	trace_dir.push_front(new DirStat(p));
      }
    assert(p.end());
  }

  const list<InodeStat*>& get_trace_in() { 
    if (trace_in.empty() && trace_bl.length()) _decode_trace();
    return trace_in; 
  }
  const list<DirStat*>& get_trace_dir() { 
    if (trace_in.empty() && trace_bl.length()) _decode_trace();
    return trace_dir; 
  }
  const list<string>& get_trace_dn() { 
    if (trace_in.empty() && trace_bl.length()) _decode_trace();
    return trace_dn; 
  }


};

#endif
