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


#ifndef __IDALLOCATOR_H
#define __IDALLOCATOR_H

#include "include/types.h"
#include "include/rangeset.h"
#include "include/buffer.h"
#include "include/Context.h"

class MDS;

#define ID_INO    1  // inode
//#define ID_FH     2  // file handle

typedef __uint64_t idno_t;

class IdAllocator {
  MDS *mds;

  map< int, rangeset<idno_t> > free;   // type -> rangeset
  map< int, set<idno_t> >      dirty;  // dirty ids
  
  bool opened, opening;
  
  inode_t id_inode;

 public:
  IdAllocator(MDS *mds);

  idno_t get_id(int type);
  void reclaim_id(int type, idno_t id);

  // load/save from disk (hack)
  bool is_open() { return opened; }
  bool is_opening() { return opening; }

  bool is_dirty(int type, idno_t id) {
    return dirty[type].count(id) ? true:false;
  }

  void reset();
  void save(Context *onfinish=0);

  void shutdown() {
    if (is_open()) save(0);
  }

  void load(Context *onfinish);
  void load_2(int, bufferlist&, Context *onfinish);

};

#endif
