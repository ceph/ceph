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


#ifndef CEPH_EBOFS_ALLOCATOR_H
#define CEPH_EBOFS_ALLOCATOR_H

#include "types.h"

#include "include/interval_set.h"

class Ebofs;

class Allocator {
public:
  const static block_t NEAR_LAST = 0;     
  const static block_t NEAR_LAST_FWD = 1;   
  
  const static int DIR_ANY = 0;
  const static int DIR_FWD = 2;
  const static int DIR_BACK = 1;

protected:
  Ebofs *fs;
  block_t      last_pos;
  

  interval_set<block_t> limbo;

  static int pick_bucket(block_t num) {
    int b = 0;
    while (num > 1) {
      b++;
      num = num >> EBOFS_FREE_BUCKET_BITS;
    }
    if (b >= EBOFS_NUM_FREE_BUCKETS)
      b = EBOFS_NUM_FREE_BUCKETS-1;
    return b;
  }

  int find(extent_t& ex, int bucket, block_t num, block_t near, int dir = DIR_ANY);

  void dump_freelist();

 public:
  int _release_into_limbo(extent_t& ex);

  int _release_loner(extent_t& ex);  // release loner extent
  int _release_merge(extent_t& ex);  // release any extent (searches for adjacent)

  //int _alloc_loner_inc(extent_t& ex);
  //int _alloc_loner_dec(extent_t& ex);


 public:
  Allocator(Ebofs *f) : fs(f), last_pos(0) {}
  
  int allocate(extent_t& ex, block_t num, block_t near=NEAR_LAST);
  int release(extent_t& ex);  // alias for alloc_dec

  int alloc_inc(extent_t ex);
  int alloc_dec(extent_t ex);

  int unallocate(extent_t& ex) {  // skip limbo
    return _release_merge(ex);
  }
  
  int commit_limbo();  // limbo -> fs->limbo_tab
  int release_limbo(); // fs->limbo_tab -> free_tabs

};

#endif
