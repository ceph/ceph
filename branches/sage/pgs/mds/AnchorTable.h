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


#ifndef __ANCHORTABLE_H
#define __ANCHORTABLE_H

#include "Anchor.h"
#include "include/Context.h"

#include <ext/hash_map>
using namespace __gnu_cxx;

class MDS;


class AnchorTable {
  MDS *mds;
  hash_map<inodeno_t, Anchor*>  anchor_map;

  bool opening, opened;
  list<Context*> waiting_for_open;

 public:
  inode_t table_inode;

 public:
  AnchorTable(MDS *mds); 

 protected:
  void init_inode();  // call this before doing anything.

  // 
  bool have_ino(inodeno_t ino) { 
    return true;                  // always in memory for now.
  } 
  void fetch_ino(inodeno_t ino, Context *onfinish) {
    assert(!opened);
    load(onfinish);
  }

  // adjust table
  bool add(inodeno_t ino, inodeno_t dirino, string& ref_dn);
  void inc(inodeno_t ino);
  void dec(inodeno_t ino);

  
  // high level interface
  void lookup(inodeno_t ino, vector<Anchor*>& trace);
  void create(inodeno_t ino, vector<Anchor*>& trace);
  void destroy(inodeno_t ino);

  // messages
 public:
  void dispatch(class Message *m);
 protected:
  void handle_anchor_request(class MAnchorRequest *m);  


 public:

  // load/save entire table for now!
  void reset();
  void save(Context *onfinish);
  void load(Context *onfinish);
  void load_2(size_t size, bufferlist& bl);


};

#endif
