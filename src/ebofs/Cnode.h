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


#ifndef CEPH_EBOFS_CNODE_H
#define CEPH_EBOFS_CNODE_H

#include "Onode.h"

/*
 * collection node
 *
 * holds attribute metadata for collections.
 * colletion membership is stored in b+tree tables, independent of tte cnode.
 */

class Cnode : public LRUObject
{
 private:
  int ref;
  bool dirty;

 public:
  coll_t coll_id;
  extent_t cnode_loc;
  epoch_t last_alloc_epoch;

  map<string,bufferptr> attr;

 public:
  Cnode(coll_t cid) : ref(0), dirty(false), coll_id(cid), last_alloc_epoch(0) {
    cnode_loc.length = 0;
  }
  ~Cnode() {
  }

  block_t get_cnode_id() { return cnode_loc.start; }
  int get_cnode_len() { return cnode_loc.length; }

  void get() {
    if (ref == 0) lru_pin();
    ref++;
  }
  void put() {
    ref--;
    if (ref == 0) lru_unpin();
  }
  int get_ref_count() { return ref; }

  void mark_dirty() {
    if (!dirty) {
      dirty = true;
      get();
    }
  }
  void mark_clean() {
    if (dirty) {
      dirty = false;
      put();
    }
  }
  bool is_dirty() { return dirty; }


  int get_attr_bytes() {
    int s = 0;
    for (map<string, bufferptr>::iterator i = attr.begin();
         i != attr.end();
         i++) {
      s += i->first.length() + 1;
      s += i->second.length() + sizeof(int);
    }
    return s;
  }
  
  //
  //???void clear();

  
};

inline ostream& operator<<(ostream& out, Cnode& cn)
{
  out << "cnode(" << hex << cn.coll_id << dec;
  if (cn.is_dirty()) out << " dirty";
  //out << " " << &cn;
  out << ")";
  return out;
}

#endif
