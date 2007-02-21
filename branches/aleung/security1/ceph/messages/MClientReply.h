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
 *  trace is a vector of c_inoe_info's tracing from root to the file/dir/whatever
 *  the operation referred to, so that the client can update it's info about what
 *  metadata lives on what MDS.
 *
 * for readdir replies:
 *  dir_contents is a vector c_inode_info*'s.  
 * 
 * that's mostly it, i think!
 *
 */

class InodeStat {

 public:
  inode_t inode;
  string  symlink;   // symlink content (if symlink)


  // mds distribution hints
  int      dir_auth;
  bool     hashed, replicated;
  bool     spec_defined;
  set<int> dist;    // where am i replicated?

 public:
  InodeStat() {}
  InodeStat(CInode *in, int whoami) :
    inode(in->inode)
  {
    // inode.mask
    inode.mask = INODE_MASK_BASE;
    if (in->filelock.can_read(in->is_auth()))
      inode.mask |= INODE_MASK_PERM;
    if (in->hardlock.can_read(in->is_auth()))
      inode.mask |= INODE_MASK_SIZE | INODE_MASK_MTIME;      // fixme when we separate this out.
    
    // symlink content?
    if (in->is_symlink()) 
      symlink = in->symlink;
      
    // replicated where?
    if (in->dir && in->dir->is_auth()) {
      spec_defined = true;
      in->dir->get_dist_spec(this->dist, whoami);
    } else 
      spec_defined = false;

    if (in->dir)
      dir_auth = in->dir->get_dir_auth();
    else
      dir_auth = -1;

    // dir info
    hashed = (in->dir && in->dir->is_hashed());   // FIXME not quite right.
    replicated = (in->dir && in->dir->is_rep());
  }
  
  void _encode(bufferlist &bl) {
    bl.append((char*)&inode, sizeof(inode));
    bl.append((char*)&spec_defined, sizeof(spec_defined));
    bl.append((char*)&dir_auth, sizeof(dir_auth));
    bl.append((char*)&hashed, sizeof(hashed));
    bl.append((char*)&replicated, sizeof(replicated));

    ::_encode(symlink, bl);
    ::_encode(dist, bl);    // distn
  }
  
  void _decode(bufferlist &bl, int& off) {
    bl.copy(off, sizeof(inode), (char*)&inode);
    off += sizeof(inode);
    bl.copy(off, sizeof(spec_defined), (char*)&spec_defined);
    off += sizeof(spec_defined);
    bl.copy(off, sizeof(dir_auth), (char*)&dir_auth);
    off += sizeof(dir_auth);
    bl.copy(off, sizeof(hashed), (char*)&hashed);
    off += sizeof(hashed);
    bl.copy(off, sizeof(replicated), (char*)&replicated);
    off += sizeof(replicated);

    ::_decode(symlink, bl, off);
    ::_decode(dist, bl, off);
  }
};


typedef struct {
  long pcid;
  long tid;
  int op;
  int result;  // error code
  unsigned char file_caps;  // for open
  long          file_caps_seq;
  __uint64_t file_data_version;  // for client buffercache consistency

  int _num_trace_in;
  int _dir_size;
} MClientReply_st;

class MClientReply : public Message {
  // reply data
  MClientReply_st st;
 
  string path;
  list<InodeStat*> trace_in;
  list<string>     trace_dn;

  list<InodeStat*> dir_in;
  list<string>     dir_dn;

  // security capability
  ExtCap ext_cap;

 public:
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

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
  __uint64_t get_file_data_version() { return st.file_data_version; }

  ExtCap get_ext_cap() { return ext_cap; }
  ExtCap *get_ptr_cap() { return &ext_cap; }
  
  void set_result(int r) { st.result = r; }
  void set_file_caps(unsigned char c) { st.file_caps = c; }
  void set_file_caps_seq(long s) { st.file_caps_seq = s; }
  void set_file_data_version(__uint64_t v) { st.file_data_version = v; }

  void set_ext_cap(ExtCap *ecap) { ext_cap = (*ecap); }
  //void set_ext_cap(ExtCap *ecap) {
  //  ext_cap = (*ecap);
  //  memcpy(&(ext_cap.allocSig), &(ecap->allocSig), ecap->allocSig.size());
  //}

  MClientReply() {};
  MClientReply(MClientRequest *req, int result = 0) : 
    Message(MSG_CLIENT_REPLY) {
    memset(&st, 0, sizeof(st));
    this->st.pcid = req->get_pcid();    // match up procedure call id!!!
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

    //_decode(ext_cap, payload, off);
    ext_cap._decode(payload, off);
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
    //_encode(ext_cap, payload);
    ext_cap._encode(payload);
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
