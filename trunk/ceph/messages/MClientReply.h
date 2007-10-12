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
  int auth;
  set<int> dist;
  bool is_rep;
  
  DirStat() {}
  DirStat(bufferlist::iterator& p) {
    _decode(p);
  }

  void _decode(bufferlist::iterator& p) {
    ::_decode_simple(frag, p);
    ::_decode_simple(auth, p);
    ::_decode_simple(dist, p);
    ::_decode_simple(is_rep, p);
  }

  static void _encode(bufferlist& bl, CDir *dir, int whoami) {
    frag_t frag = dir->get_frag();
    int auth;
    set<int> dist;
    bool is_rep;
    
    auth = dir->get_dir_auth().first;
    if (dir->is_auth()) 
      dir->get_dist_spec(dist, whoami);
    is_rep = dir->is_rep();

    ::_encode_simple(frag, bl);
    ::_encode_simple(auth, bl);
    ::_encode_simple(dist, bl);
    ::_encode_simple(is_rep, bl);
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
    ::_decode_simple(mask, p);
    ::_decode_simple(inode, p);
    ::_decode_simple(symlink, p);
    dirfragtree._decode(p);
  }

  static void _encode(bufferlist &bl, CInode *in) {
    int mask = STAT_MASK_INO|STAT_MASK_TYPE|STAT_MASK_BASE;

    // mask
    if (in->authlock.can_rdlock(0)) mask |= STAT_MASK_AUTH;
    if (in->linklock.can_rdlock(0)) mask |= STAT_MASK_LINK;
    if (in->filelock.can_rdlock(0)) mask |= STAT_MASK_FILE;

    ::_encode_simple(mask, bl);
    ::_encode_simple(in->inode, bl);
    ::_encode_simple(in->symlink, bl);
    in->dirfragtree._encode(bl);
  }
  
};


class MClientReply : public Message {
  // reply data
  struct st_ {
    long tid;
    int op;
    int result;  // error code
    unsigned char file_caps;  // for open
    long          file_caps_seq;
    uint64_t file_data_version;  // for client buffercache consistency
  } st;
 
  string path;

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

  int get_result() { return st.result; }
  const string& get_path() { return path; }

  inodeno_t get_ino() { return trace_in.back()->inode.ino; }
  const inode_t& get_inode() { return trace_in.back()->inode; }

  unsigned char get_file_caps() { return st.file_caps; }
  long get_file_caps_seq() { return st.file_caps_seq; }
  uint64_t get_file_data_version() { return st.file_data_version; }
  
  void set_result(int r) { st.result = r; }
  void set_file_caps(unsigned char c) { st.file_caps = c; }
  void set_file_caps_seq(long s) { st.file_caps_seq = s; }
  void set_file_data_version(uint64_t v) { st.file_data_version = v; }

  MClientReply() : dir_dir(0) {};
  MClientReply(MClientRequest *req, int result = 0) : 
    Message(MSG_CLIENT_REPLY), dir_dir(0) {
    memset(&st, 0, sizeof(st));
    this->st.tid = req->get_tid();
    this->st.op = req->get_op();
    this->path = req->get_path();

    this->st.result = result;
  }
  virtual ~MClientReply() {
    list<InodeStat*>::iterator it;
    
    for (it = trace_in.begin(); it != trace_in.end(); ++it) 
      delete *it;
    for (it = dir_in.begin(); it != dir_in.end(); ++it) 
      delete *it;
  }
  virtual char *get_type_name() { return "creply"; }
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
    ::_decode_simple(path, p);
    ::_decode_simple(trace_bl, p);
    ::_decode_simple(dir_bl, p);
    assert(p.end());
  }
  virtual void encode_payload() {
    ::_encode_simple(st, payload);
    ::_encode_simple(path, payload);
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
    while (!p.end()) {
      string dn;
      ::_decode_simple(dn, p);
      dir_dn.push_back(dn);
      dir_in.push_back(new InodeStat(p));
    }
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
    while (in) {
      InodeStat::_encode(trace_bl, in);
      CDentry *dn = in->get_parent_dn();
      if (!dn) break;
      ::_encode_simple(in->get_parent_dn()->get_name(), trace_bl);
      DirStat::_encode(trace_bl, dn->get_dir(), whoami);
      in = dn->get_dir()->get_inode();
    }
  }
  void _decode_trace() {
    bufferlist::iterator p = trace_bl.begin();
    while (!p.end()) {
      // inode
      trace_in.push_front(new InodeStat(p));
      if (!p.end()) {
	// dentry
	string ref_dn;
	::_decode_simple(ref_dn, p);
	trace_dn.push_front(ref_dn);

	// dir
	trace_dir.push_front(new DirStat(p));
      }
    }
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
