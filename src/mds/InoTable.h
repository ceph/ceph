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


#ifndef __INOTABLE_H
#define __INOTABLE_H

#include "MDSTable.h"
#include "include/interval_set.h"

class MDS;

class InoTable : public MDSTable {
  interval_set<inodeno_t> free;   // unused ids

 public:
  InoTable(MDS *m) : MDSTable(m, "inotable") { }

  // alloc or reclaim ids
  inodeno_t alloc_id(inodeno_t id=0);
  void alloc_ids(vector<inodeno_t>& inos);
  void alloc_ids(deque<inodeno_t>& inos, int want);
  void release_ids(vector<inodeno_t>& inos);
  void release_ids(deque<inodeno_t>& inos);

  void init_inode();
  void reset_state();
  void encode_state(bufferlist& bl) {
    ::encode(free.m, bl);
  }
  void decode_state(bufferlist::iterator& bl) {
    ::decode(free.m, bl);
  }
};

#endif
