// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

class InodeStat {

 public:
  inode_t inode;
  string  symlink;   // symlink content (if symlink)
  fragtree_t dirfragtree;

  // mds distribution hints
  map<frag_t,int>       dirfrag_auth;
  map<frag_t,set<int> > dirfrag_dist;
  set<frag_t>           dirfrag_rep;

 public:
  InodeStat() {}
  InodeStat(CInode *in, int whoami) :
    inode(in->inode)
  {
    // inode.mask
    inode.mask = INODE_MASK_BASE;
    if (in->authlock.can_rdlock(0)) inode.mask |= INODE_MASK_AUTH;
    if (in->linklock.can_rdlock(0)) inode.mask |= INODE_MASK_LINK;
    if (in->filelock.can_rdlock(0)) inode.mask |= INODE_MASK_FILE;
    
    // symlink content?
    if (in->is_symlink()) 
      symlink = in->symlink;
    
    // dirfragtree
    dirfragtree = in->dirfragtree;
      
    // dirfrag info
    list<CDir*> ls;
    in->get_dirfrags(ls);
    for (list<CDir*>::iterator p = ls.begin();
	 p != ls.end();
	 ++p) {
      CDir *dir = *p;
      dirfrag_auth[dir->dirfrag().frag] = dir->get_dir_auth().first;
      if (dir->is_auth()) 
	dir->get_dist_spec(dirfrag_dist[dir->dirfrag().frag], whoami);
      if (dir->is_rep())
	dirfrag_rep.insert(dir->dirfrag().frag);
    }
  }
  
  void _encode(bufferlist &bl) {
    ::_encode(inode, bl);
    ::_encode(dirfrag_auth, bl);
    ::_encode(dirfrag_dist, bl);
    ::_encode(dirfrag_rep, bl);
    ::_encode(symlink, bl);
    dirfragtree._encode(bl);
  }
  
  void _decode(bufferlist &bl, int& off) {
    ::_decode(inode, bl, off);
    ::_decode(dirfrag_auth, bl, off);
    ::_decode(dirfrag_dist, bl, off);
    ::_decode(dirfrag_rep, bl, off);
    ::_decode(symlink, bl, off);
    dirfragtree._decode(bl, off);
  }
};


class MClientReply : public Message {
  // reply data
  struct {
    long tid;
    int op;
    int result;  // error code
    unsigned char file_caps;  // for open
    long          file_caps_seq;
    uint64_t file_data_version;  // for client buffercache consistency
    
    int _num_trace_in;
    int _dir_size;
  } st;
 
  string path;
  list<InodeStat*> trace_in;
  list<string>     trace_dn;

  list<InodeStat*> dir_in;
  list<string>     dir_dn;

 public:
  long get_tid() { return st.tid; }
  int get_op() { return st.op; }

  int get_result() { return st.result; }
  const string& get_path() { return path; }

  inodeno_t get_ino() { return trace_in.back()->inode.ino; }
  const inode_t& get_inode() { return trace_in.back()->inode; }

  const list<InodeStat*>& get_trace_in() { return trace_in; }
  const list<string>&     get_trace_dn() { return trace_dn; }

  const list<InodeStat*>& get_dir_in() { return dir_in; }
  const list<string>&     get_dir_dn() { return dir_dn; }

  unsigned char get_file_caps() { return st.file_caps; }
  long get_file_caps_seq() { return st.file_caps_seq; }
  uint64_t get_file_data_version() { return st.file_data_version; }
  
  void set_result(int r) { st.result = r; }
  void set_file_caps(unsigned char c) { st.file_caps = c; }
  void set_file_caps_seq(long s) { st.file_caps_seq = s; }
  void set_file_data_version(uint64_t v) { st.file_data_version = v; }

  MClientReply() {};
  MClientReply(MClientRequest *req, int result = 0) : 
    Message(MSG_CLIENT_REPLY) {
    memset(&st, 0, sizeof(st));
    this->st.tid = req->get_tid();
    this->st.op = req->get_op();
    this->path = req->get_path();

    this->st.result = result;

    st._dir_size = 0;
    st._num_trace_in = 0;
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
    if (st.result) o << " = " << st.result;
    o << ")";
  }

  // serialization
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);

    _decode(path, payload, off);

    for (int i=0; i<st._num_trace_in; ++i) {
      if (i) {
        string ref_dn;
        ::_decode(ref_dn, payload, off);
        trace_dn.push_back(ref_dn);
      }        
      InodeStat *ci = new InodeStat;
      ci->_decode(payload, off);
      trace_in.push_back(ci);
    }

    for (int i=0; i<st._dir_size; ++i) {
      InodeStat *ci = new InodeStat;
      ci->_decode(payload, off);
      dir_in.push_back(ci);
      string dn;
      ::_decode(dn, payload, off);
      dir_dn.push_back(dn);
    }
  }
  virtual void encode_payload() {
    payload.append((char*)&st, sizeof(st));
    _encode(path, payload);

    // trace
    list<string>::iterator pdn = trace_dn.begin();
    list<InodeStat*>::iterator pin;
    for (pin = trace_in.begin();
         pin != trace_in.end();
         ++pin) {
      if (pin != trace_in.begin()) {
        ::_encode(*pdn, payload);
        ++pdn;
      }
      (*pin)->_encode(payload);
    }

    // dir contents
    pdn = dir_dn.begin();
    for (pin = dir_in.begin();
         pin != dir_in.end();
         ++pin, ++pdn) {
      (*pin)->_encode(payload);
      ::_encode(*pdn, payload);
    }
  }

  // builders
  /*
  void add_dir_item(string& dn, InodeStat *in) {
    dir_dn.push_back(dn);
    dir_in.push_back(in);
    ++st._dir_size;
    }*/
  void take_dir_items(list<InodeStat*>& inls,
                      list<string>& dnls,
                      int num) {
    dir_in.swap(inls);
    dir_dn.swap(dnls);
    st._dir_size = num;
  }
  void copy_dir_items(const list<InodeStat*>& inls,
                      const list<string>& dnls) {
    list<string>::const_iterator pdn = dnls.begin();
    list<InodeStat*>::const_iterator pin = inls.begin();
    while (pin != inls.end()) {
      // copy!
      InodeStat *i = new InodeStat;
      *i = **pin;
      dir_in.push_back(i);
      dir_dn.push_back(*pdn);
      ++pin;
      ++pdn;
      ++st._dir_size;
    }
  }

  void set_trace_dist(CInode *in, int whoami) {
    st._num_trace_in = 0;
    while (in) {
      // add this inode to trace, along with referring dentry name
      if (in->get_parent_dn()) 
        trace_dn.push_front(in->get_parent_dn()->get_name());
      trace_in.push_front(new InodeStat(in, whoami));
      ++st._num_trace_in;
      
      in = in->get_parent_inode();
    }
  }

};

#endif
