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


#ifndef __EBOFS_TYPES_H
#define __EBOFS_TYPES_H

#include "include/buffer.h"
#include "include/Context.h"
#include "include/pobject.h"
#include "common/Cond.h"

#include <ext/hash_map>
#include <set>
#include <list>
#include <vector>
using namespace std;
using namespace __gnu_cxx;

#include <tr1/unordered_map>
using std::tr1::unordered_map;


#include "include/object.h"

#include "csum.h"

#include "include/intarith.h"

// disk
typedef uint64_t block_t;        // disk location/sector/block

static const unsigned EBOFS_BLOCK_SIZE = 4096;
static const unsigned EBOFS_BLOCK_MASK = 4095;
static const unsigned EBOFS_BLOCK_BITS = 12;    // 1<<12 == 4096

struct extent_t {
  block_t start, length;

  //extent_t() : start(0), length(0) {}
  //extent_t(block_t s, block_t l) : start(s), length(l) {}

  block_t last() const { return start + length - 1; }
  block_t end() const { return start + length; }
} __attribute__ ((packed));

inline ostream& operator<<(ostream& out, const extent_t& ex)
{
  return out << ex.start << "~" << ex.length;
}


// objects

struct ebofs_onode {
  csum_t onode_csum;  // from after onode_csum to base + onode_bytes
  __u32 onode_bytes;    

  extent_t onode_loc;       /* this is actually the block we live in */
  pobject_t object_id;    /* for kicks */
  uint64_t readonly;  

  int64_t object_size;     /* file size in bytes.  should this be 64-bit? */
  __u32 alloc_blocks;   // allocated
  csum_t data_csum;
  
  __u16 inline_bytes;
  __u16 num_collections;
  __u32 num_attr;        // num attr in onode
  __u32 num_extents;     /* number of extents used.  if 0, data is in the onode */
  __u32 num_bad_byte_extents; // corrupt partial byte extents
} __attribute__ ((packed));

struct ebofs_cnode {
  csum_t cnode_csum;
  __u32  cnode_bytes;

  extent_t     cnode_loc;       /* this is actually the block we live in */
  coll_t     coll_id;
  __u32      num_attr;        // num attr in cnode
} __attribute__ ((packed));

struct ebofs_inode_ptr {
  extent_t loc;
  csum_t csum;
  ebofs_inode_ptr() {}
  ebofs_inode_ptr(const extent_t& l, csum_t c) : loc(l), csum(c) {}
} __attribute__ ((packed));

static inline ostream& operator<<(ostream& out, const ebofs_inode_ptr& ptr) {
  return out << ptr.loc << "=" << hex << ptr.csum << dec;
}


// tree/set nodes
//typedef int    nodeid_t;
typedef int64_t nodeid_t;     // actually, a block number.  FIXME.

static const unsigned EBOFS_NODE_BLOCKS = 1;
static const unsigned EBOFS_NODE_BYTES = EBOFS_NODE_BLOCKS * EBOFS_BLOCK_SIZE;
static const unsigned EBOFS_MAX_NODE_REGIONS = 10;   // pick a better value!
static const unsigned EBOFS_NODE_DUP = 3;

struct ebofs_nodepool {
  extent_t node_usemap_even;   // for even sb versions
  extent_t node_usemap_odd;    // for odd sb versions
  
  __u32  num_regions;
  extent_t region_loc[EBOFS_MAX_NODE_REGIONS];
} __attribute__ ((packed));

// table

struct ebofs_node_ptr {
  nodeid_t nodeid;
  //uint64_t start[EBOFS_NODE_DUP];
  //uint64_t length;
  csum_t csum;
} __attribute__ ((packed));

struct ebofs_table {
  ebofs_node_ptr root;
  __u32    num_keys;
  __u32    depth;
} __attribute__ ((packed));


// super
typedef uint64_t version_t;

static const uint64_t EBOFS_MAGIC = 0x000EB0F5;

static const int EBOFS_NUM_FREE_BUCKETS = 5;   /* see alloc.h for bucket constraints */
static const int EBOFS_FREE_BUCKET_BITS = 2;

struct ebofs_super {
  uint64_t s_magic;
  uint64_t fsid;   /* _ebofs_ fsid, mind you, not ceph_fsid_t. */

  epoch_t epoch;             // version of this superblock.
  uint64_t op_seq;              // seq # of last operation we _did_ apply+commit to the store.

  uint64_t num_blocks;        /* # blocks in filesystem */

  // some basic stats, for kicks
  uint64_t free_blocks;       /* unused blocks */
  uint64_t limbo_blocks;      /* limbo blocks */
  //unsigned num_objects;
  //unsigned num_fragmented;
  
  struct ebofs_nodepool nodepool;
  
  // tables
  struct ebofs_table free_tab[EBOFS_NUM_FREE_BUCKETS];  
  struct ebofs_table limbo_tab;
  struct ebofs_table alloc_tab;
  struct ebofs_table object_tab;      // object directory
  struct ebofs_table collection_tab;  // collection directory
  struct ebofs_table co_tab;

  csum_t super_csum;

  csum_t calc_csum() {
    return ::calc_csum_unaligned((char*)this, (unsigned long)&super_csum-(unsigned long)this);
  }
  bool is_corrupt() { 
    csum_t actual = calc_csum();
    if (actual != super_csum) 
      return true;
    else 
      return false;
  }
  bool is_valid_magic() { return s_magic == EBOFS_MAGIC; }
  bool is_valid() { return is_valid_magic() && !is_corrupt(); }
} __attribute__ ((packed));


#endif
