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


#ifndef __EBOFS_TYPES_H
#define __EBOFS_TYPES_H

#include <cassert>
#include "include/buffer.h"
#include "include/Context.h"
#include "common/Cond.h"

#include <ext/hash_map>
#include <set>
#include <list>
#include <vector>
using namespace std;
using namespace __gnu_cxx;


#include "include/object.h"


#ifndef MIN
# define MIN(a,b)  ((a)<=(b) ? (a):(b))
#endif
#ifndef MAX
# define MAX(a,b)  ((a)>=(b) ? (a):(b))
#endif


/*
namespace __gnu_cxx {
  template<> struct hash<unsigned long long> {
    size_t operator()(unsigned long long __x) const { 
      static hash<unsigned long> H;
      return H((__x >> 32) ^ (__x & 0xffffffff)); 
    }
  };
  
  template<> struct hash< std::string >
  {
    size_t operator()( const std::string& x ) const
    {
      static hash<const char*> H;
      return H(x.c_str());
    }
  };
}
*/


// disk
typedef __uint64_t block_t;        // disk location/sector/block

static const int EBOFS_BLOCK_SIZE = 4096;
static const int EBOFS_BLOCK_BITS = 12;    // 1<<12 == 4096

class Extent {
 public:
  block_t start, length;

  Extent() : start(0), length(0) {}
  Extent(block_t s, block_t l) : start(s), length(l) {}

  block_t last() const { return start + length - 1; }
  block_t end() const { return start + length; }
};

inline ostream& operator<<(ostream& out, Extent& ex)
{
  return out << ex.start << "~" << ex.length;
}


// tree/set nodes
typedef int    nodeid_t;

static const int EBOFS_NODE_BLOCKS = 1;
static const int EBOFS_NODE_BYTES = EBOFS_NODE_BLOCKS * EBOFS_BLOCK_SIZE;
static const int EBOFS_MAX_NODE_REGIONS = 10;   // pick a better value!

struct ebofs_nodepool {
  Extent node_usemap_even;   // for even sb versions
  Extent node_usemap_odd;    // for odd sb versions
  
  int    num_regions;
  Extent region_loc[EBOFS_MAX_NODE_REGIONS];
};


// objects

typedef __uint64_t coll_t;

struct ebofs_onode {
  Extent     onode_loc;       /* this is actually the block we live in */

  object_t   object_id;       /* for kicks */
  off_t      object_size;     /* file size in bytes.  should this be 64-bit? */
  unsigned   object_blocks;
  bool       readonly;
  
  int        num_collections;
  int        num_attr;        // num attr in onode
  int        num_extents;     /* number of extents used.  if 0, data is in the onode */
};

struct ebofs_cnode {
  Extent     cnode_loc;       /* this is actually the block we live in */
  coll_t     coll_id;
  int        num_attr;        // num attr in cnode
};


// table
struct ebofs_table {
  nodeid_t root;      /* root node of btree */
  int      num_keys;
  int      depth;
};


// super
typedef __uint64_t version_t;

static const unsigned EBOFS_MAGIC = 0x000EB0F5;

static const int EBOFS_NUM_FREE_BUCKETS = 5;   /* see alloc.h for bucket constraints */
static const int EBOFS_FREE_BUCKET_BITS = 2;


struct ebofs_super {
  unsigned s_magic;
  
  unsigned epoch;             // version of this superblock.

  unsigned num_blocks;        /* # blocks in filesystem */

  // some basic stats, for kicks
  unsigned free_blocks;       /* unused blocks */
  unsigned limbo_blocks;      /* limbo blocks */
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
};


#endif
