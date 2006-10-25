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

#ifndef __EINODEUPDATE_H
#define __EINODEUPDATE_H

#include <assert.h>
#include "config.h"
#include "include/types.h"

#include "ETraced.h"
#include "../MDStore.h"



class EInodeUpdate : public ETraced {
 protected:
  inode_t inode;

 public:
  EInodeUpdate(CInode *in) : ETraced(EVENT_INODEUPDATE, in) {
    this->inode = in->get_inode();
  }
  EInodeUpdate() : ETraced(EVENT_INODEUPDATE) { }
  
  void print(ostream& out) {
    out << "up inode " << inode.ino << " ";
    ETraced::print(out);
    out << " v " << inode.version;    
  }

  virtual void encode_payload(bufferlist& bl) {
    encode_trace(bl);
    bl.append((char*)&inode, sizeof(inode));
  }
  void decode_payload(bufferlist& bl, int& off) {
    decode_trace(bl, off);
    bl.copy(off, sizeof(inode), (char*)&inode);
    off += sizeof(inode);
  }

  
  bool can_expire(MDS *mds) {
    // am i obsolete?
    CInode *in = mds->mdcache->get_inode(inode.ino);

    //assert(in);
    if (!in) {
      dout(7) << "inode " << inode.ino << " not in cache, must have exported" << endl;
      return true;
    }
    dout(7) << "EInodeUpdate obsolete? on " << *in << endl;
    if (!in->is_auth())
      return true;  // not my inode anymore!
    if (in->get_version() != inode.version)
      return true;  // i'm obsolete!  (another log entry follows)

    CDir *parent = in->get_parent_dir();
    if (!parent) return true;  // root?
    if (!parent->is_dirty()) return true; // dir is clean!

    // frozen -> exporting -> obsolete    (FOR NOW?)
    if (in->is_frozen())
      return true; 

    return false;  
  }

  virtual void retire(MDS *mds, Context *c) {
    // commit my containing directory
    CInode *in = mds->mdcache->get_inode(inode.ino);
    assert(in);
    CDir *parent = in->get_parent_dir();

    if (parent) {
      // okay!
      dout(7) << "commiting containing dir for " << *in << ", which is " << *parent << endl;
      mds->mdstore->commit_dir(parent, c);
    } else {
      // oh, i'm the root inode
      dout(7) << "don't know how to commit the root inode" << endl;
      if (c) {
        c->finish(0);
        delete c;
      }
    }

  }
  
};

#endif
