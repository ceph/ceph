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


#ifndef __MHASHDIRPREP_H
#define __MHASHDIRPREP_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MHashDirPrep : public Message {
  inodeno_t ino;
  bool assim;

  // subdir dentry names + inodes 
  map<string,CInodeDiscover*>    inodes;

 public:
  inodeno_t get_ino() { return ino; }
  map<string,CInodeDiscover*>& get_inodes() { return inodes; }

  bool did_assim() { return assim; }
  void mark_assim() { assert(!assim); assim = true; }

  MHashDirPrep() : assim(false) { }
  MHashDirPrep(inodeno_t ino) :
    Message(MSG_MDS_HASHDIRPREP),
    assim(false) {
    this->ino = ino;
  }
  ~MHashDirPrep() {
    for (map<string,CInodeDiscover*>::iterator it = inodes.begin();
         it != inodes.end();
         it++) 
      delete it->second;
  }


  virtual char *get_type_name() { return "HP"; }

  void add_inode(const string& dentry, CInodeDiscover *in) {
    inodes[dentry] = in;
  }

  void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    
    // inodes
    int ni;
    payload.copy(off, sizeof(int), (char*)&ni);
    off += sizeof(int);
    for (int i=0; i<ni; i++) {
      // dentry
      string dname;
      _decode(dname, payload, off);
      
      // inode
      CInodeDiscover *in = new CInodeDiscover;
      in->_decode(payload, off);
      
      inodes[dname] = in;
    }
  }

  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));

    // inodes
    int ni = inodes.size();
    payload.append((char*)&ni, sizeof(int));
    for (map<string,CInodeDiscover*>::iterator iit = inodes.begin();
         iit != inodes.end();
         iit++) {
      _encode(iit->first, payload);   // dentry
      iit->second->_encode(payload);  // inode
    }
  }
};

#endif
