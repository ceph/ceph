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


#ifndef __EBOFS_ONODE_H
#define __EBOFS_ONODE_H

#include "include/lru.h"

#include "types.h"
#include "BufferCache.h"

#include "include/interval_set.h"

#include "include/nstring.h"

/*
 * object node (like an inode)
 *
 * holds object metadata, including
 *  size
 *  allocation (extent list)
 *  attributes
 *
 */

struct ExtentCsum {
  extent_t ex;
  vector<csum_t> csum;
  
  void resize_tail() {
    unsigned old = csum.size();
    csum.resize(ex.length);
    for (block_t j=old; j<ex.length; j++)
      csum[j] = 0;
  }
  void resize_head() {
    if (ex.length < csum.size()) {
      memmove(&csum[0], &csum[csum.size()-ex.length], ex.length*sizeof(csum_t));
      csum.resize(ex.length);
    } else if (ex.length > csum.size()) {
      int old = csum.size();
      csum.resize(ex.length);
      memmove(&csum[ex.length-old], &csum[0], ex.length*sizeof(csum_t));
      for (block_t b = 0; b<ex.length-old; b++)
	csum[b] = 0;
    }
  }
};
inline ostream& operator<<(ostream& out, ExtentCsum &ec) {
  out << ec.ex;
  out << '=';
  out << hex << ec.csum << dec;
  return out;
}

class Onode : public LRUObject {
private:
  int ref;

public:
  pobject_t object_id;
  version_t version;      // incremented on each modify.

  // data
  extent_t onode_loc;
  epoch_t last_alloc_epoch; // epoch i last allocated for

  uint64_t object_size;
  uint64_t alloc_blocks, last_block;
  csum_t data_csum;
  bool readonly;

  // onode
  set<coll_t>            collections;
  map<nstring, bufferptr> attr;

  map<block_t, ExtentCsum>   extent_map;
  interval_set<uint64_t> bad_byte_extents;

  interval_set<block_t> uncommitted;

  ObjectCache  *oc;

  bool          dirty;
  bool          dangling;   // not in onode_map
  bool          deleted;    // deleted

  //list<Context*>   commit_waiters;

 public:
  Onode(pobject_t oid) : ref(0), object_id(oid), version(0), last_alloc_epoch(0),
			object_size(0), alloc_blocks(0), last_block(0), data_csum(0),
			readonly(0),
			oc(0),
			dirty(false), dangling(false), deleted(false) { 
    onode_loc.start = 0;
    onode_loc.length = 0;
  }
  ~Onode() {
    if (oc) delete oc;
  }

  block_t get_onode_id() { return onode_loc.start; }
  int get_onode_len() { return onode_loc.length; }

  int get_ref_count() { return ref; }
  void get() {
    if (ref == 0) lru_pin();
    ref++;
    //cout << "ebofs.onode.get " << hex << object_id << dec << " " << ref << std::endl;
  }
  void put() {
    ref--;
    if (ref == 0) lru_unpin();
    //cout << "ebofs.onode.put " << hex << object_id << dec << " " << ref << std::endl;
  }

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
  bool is_deleted() { return deleted; }
  bool is_dangling() { return dangling; }

  
  bool have_oc() {
    return oc != 0;
  }
  ObjectCache *get_oc(BufferCache *bc) {
    if (!oc) {
      oc = new ObjectCache(object_id, this, bc);
      oc->get();
      get();
    }
    return oc;
  }
  void close_oc() {
    if (oc) {
      //cout << "close_oc on " << object_id << std::endl;
      assert(oc->is_empty());
      if (oc->put() == 0){
        //cout << "************************* hosing oc" << std::endl;
        delete oc;
      }
      oc = 0;
      put();
    }
  }


  // allocation
  void verify_extents() {
    if (0) {  // do crazy stupid sanity checking
      block_t count = 0, pos = 0;
      interval_set<block_t> is;    
      csum_t csum = 0;
          
      set<block_t> s;
      //cout << "verify_extentsing.  data_csum=" << hex << data_csum << dec << std::endl;

      for (map<block_t,ExtentCsum>::iterator p = extent_map.begin();
           p != extent_map.end();
           p++) {
        //cout << " verify_extents " << p->first << ": " << p->second << std::endl;
        assert(pos == p->first);
	pos += p->second.ex.length;
	if (p->second.ex.start) {
	  count += p->second.ex.length;
	  for (unsigned j=0;j<p->second.ex.length;j++) {
	    assert(s.count(p->second.ex.start+j) == 0);
	    s.insert(p->second.ex.start+j);
	    csum += p->second.csum[j];
	  }
	}
      }
      //cout << " verify_extents got csum " << hex << csum << " want " << data_csum << dec << std::endl;

      assert(s.size() == count);
      assert(count == alloc_blocks);
      assert(pos == last_block);
      assert(csum == data_csum);
    }
  }

  csum_t *get_extent_csum_ptr(block_t offset, block_t len) {
    map<block_t,ExtentCsum>::iterator p = extent_map.lower_bound(offset);
    if (p == extent_map.end() || p->first > offset)
      p--;
    assert(p->first <= offset);
    assert(p->second.ex.start != 0);
    assert(offset+len <= p->first + p->second.ex.length);
    return &p->second.csum[offset-p->first];
  }

  /*
   * set_extent - adjust extent map.
   *  assume new extent will have csum of 0.
   *  factor clobbered extents out of csums.
   */
  void set_extent(block_t offset, extent_t ex) {
    //cout << "set_extent " << offset << " -> " << ex << " ... " << last_block << std::endl;

    verify_extents();

    // at the end?
    if (offset == last_block) {
      //cout << " appending " << ex << std::endl;
      if (!extent_map.empty() && 
	  ((extent_map.rbegin()->first && 
	    ex.start &&
	    extent_map.rbegin()->second.ex.end() == ex.start) ||
	   (!extent_map.rbegin()->first && 
	    !ex.start))) {
        extent_map.rbegin()->second.ex.length += ex.length;
	if (ex.start)
	  extent_map.rbegin()->second.resize_tail();
      } else {
        extent_map[last_block].ex = ex;
	if (ex.start)
	  extent_map[last_block].resize_tail();
      }
      last_block += ex.length;
      if (ex.start) 
	alloc_blocks += ex.length;
      return;
    }

    // past the end?
    if (offset > last_block) {
      if (ex.start) {
	extent_map[last_block].ex.start = 0;
	extent_map[last_block].ex.length = offset - last_block;
	extent_map[offset].ex = ex;
	extent_map[offset].resize_tail();
	last_block = offset+ex.length;
	alloc_blocks += ex.length;
      } else {
	// ignore attempt to set a trailing "hole"
      }
      return;
    }

    // remove any extent bits we overwrite
    if (!extent_map.empty()) {
      // preceeding extent?
      map<block_t,ExtentCsum>::iterator p = extent_map.lower_bound(offset);
      if (p != extent_map.begin()) {
        p--;
	ExtentCsum &left = p->second;
        if (p->first + left.ex.length > offset) {
          //cout << " preceeding left was " << left << std::endl;
	  block_t newlen = offset - p->first;
          if (p->first + left.ex.length > offset+ex.length) {
            // cutting chunk out of middle, add trailing bit
            ExtentCsum &right = extent_map[offset+ex.length] = left;
            right.ex.length -= offset+ex.length - p->first;
	    if (right.ex.start) {
	      right.ex.start += offset+ex.length - p->first;
	      alloc_blocks += right.ex.length;
	      right.resize_head();
	      for (unsigned j=0; j<right.ex.length; j++)
		data_csum += right.csum[j];
	    }
            //cout << " tail right is " << right << std::endl;
	  }
	  if (left.ex.start) {
	    alloc_blocks -= left.ex.length - newlen;
	    for (unsigned i=newlen; i<left.ex.length; i++)
	      data_csum -= left.csum[i];
	  }
          left.ex.length = newlen;     // cut tail off preceeding extent
	  if (left.ex.start) 
	    left.resize_tail();
          //cout << " preceeding left now " << left << std::endl;
        }
        p++;
      }
      
      // overlapping extents
      while (p != extent_map.end() &&
             p->first < offset + ex.length) {
        map<block_t,ExtentCsum>::iterator next = p;
        next++;

        // completely subsumed?
	ExtentCsum &o = p->second;
        if (p->first + o.ex.length <= offset+ex.length) {
          //cout << " erasing " << o << std::endl;
	  if (o.ex.start) {
	    alloc_blocks -= o.ex.length;
	    for (unsigned i=0; i<o.ex.length; i++)
	      data_csum -= o.csum[i];
	  }
          extent_map.erase(p);
          p = next;
          continue;
        }

        // spans next extent, cut off head
        ExtentCsum &n = extent_map[ offset+ex.length ] = o;
        //cout << " cutting head off " << o;
	unsigned overlap = offset+ex.length - p->first;
        n.ex.length -= overlap;
        if (n.ex.start) {
	  n.ex.start += overlap;
	  alloc_blocks -= overlap;
	  for (unsigned j=0; j<overlap; j++)
	    data_csum -= n.csum[j];
	  n.resize_head();
	}
        extent_map.erase(p);
        //cout << ", now " << n << std::endl;
        break;
      }
    }

    // add ourselves
    ExtentCsum &n = extent_map[ offset ];
    n.ex = ex;
    if (ex.start) {
      alloc_blocks += ex.length;
      n.resize_tail();
    }

    // extend object?
    if (offset + ex.length > last_block)
      last_block = offset+ex.length;
    
    verify_extents();
  }
  
  int truncate_extents(block_t len, vector<extent_t>& extra) {
    //cout << " truncate to " << len << " .. last_block " << last_block << std::endl;

    verify_extents();

    map<block_t,ExtentCsum>::iterator p = extent_map.lower_bound(len);
    if (p != extent_map.begin() &&
        (p == extent_map.end() ||( p->first > len && p->first))) {
      p--;
      ExtentCsum &o = p->second;
      if (o.ex.length > len - p->first) {
	int newlen = len - p->first;
	if (o.ex.start) {
	  extent_t ex;
	  ex.start = o.ex.start + newlen;
	  ex.length = o.ex.length - newlen;
	  //cout << " truncating ex " << p->second.ex << " to " << newlen << ", releasing " << ex << std::endl;
	  for (unsigned i=newlen; i<o.ex.length; i++)
	    data_csum -= o.csum[i];
	  o.ex.length = newlen;
	  o.resize_tail();
	  extra.push_back(ex);
	  alloc_blocks -= ex.length;
	} else
	  o.ex.length = newlen;
        assert(o.ex.length > 0);
      }
      p++;
    }
    
    while (p != extent_map.end()) {
      assert(p->first >= len);
      ExtentCsum &o = p->second;
      if (o.ex.start) {
	for (unsigned i=0; i<o.ex.length; i++)
	  data_csum -= o.csum[i];
	extra.push_back(o.ex);
	alloc_blocks -= o.ex.length;
      }
      map<block_t,ExtentCsum>::iterator n = p;
      n++;
      extent_map.erase(p);
      p = n;
    }    
    
    last_block = len;
    verify_extents();
    return 0;
  }


  /* map_extents(start, len, ls)
   *  map teh given page range into extents (and csums) on disk.
   */
  int map_extents(block_t start, block_t len, vector<extent_t>& ls, vector<csum_t> *csum) {
    //cout << "map_extents " << start << " " << len << std::endl;
    verify_extents();

    map<block_t,ExtentCsum>::iterator p;
    
    // hack hack speed up common cases!
    if (start == 0) {
      p = extent_map.begin();
    } else if (start+len == last_block && len == 1 && !extent_map.empty()) {
      // append hack.
      p = extent_map.end();
      p--;
      if (p->first < start) p++;
    } else {
      // normal
      p = extent_map.lower_bound(start);
    }

    if (p != extent_map.begin() &&
        (p == extent_map.end() || (p->first > start && p->first))) {
      p--;
      if (p->second.ex.length > start - p->first) {
        extent_t ex;
	int off = (start - p->first);
        ex.length = MIN(len, p->second.ex.length - off);
	if (p->second.ex.start) {
	  ex.start = p->second.ex.start + off;
	  if (csum)
	    for (unsigned i=off; i<ex.length; i++)
	      csum->push_back(p->second.csum[i]);
	} else
	  ex.start = 0;
        ls.push_back(ex);
        
        //cout << " got (tail of?) " << p->second << " : " << ex << std::endl;
        
        start += ex.length;
        len -= ex.length;
      }
      p++;
    }

    while (len > 0 &&
           p != extent_map.end()) {
      assert(p->first == start);
      ExtentCsum e = p->second;
      e.ex.length = MIN(len, e.ex.length);
      ls.push_back(e.ex);
      if (e.ex.start && csum) 
	for (unsigned i=0; i<e.ex.length; i++)
	  csum->push_back(p->second.csum[i]);
      //cout << " got (head of?) " << p->second << " : " << ex << std::endl;
      start += e.ex.length;
      len -= e.ex.length;
      p++;
    }    

    return 0;
  }



  /* map_alloc_regions(start, len, map)
   *  map range into regions that need to be (re)allocated on disk
   *  because they overlap "safe" (or unallocated) parts of the object
   */
  /*
  void map_alloc_regions(block_t start, block_t len, 
                         interval_set<block_t>& alloc) {
    interval_set<block_t> already_uncom;

    alloc.insert(start, len);   // start with whole range
    already_uncom.intersection_of(alloc, uncommitted);
    alloc.subtract(already_uncom);   // take out the bits that aren't yet committed
  }
  */

  block_t get_first_block() {
    if (!alloc_blocks) return 0;
    map<block_t,ExtentCsum>::iterator p = extent_map.begin();
    while (1) {
      if (p->second.ex.start)
	return p->second.ex.start;
      p++;
    }
    assert(0);
  }



  // pack/unpack
  int get_ondisk_bytes() {
    return sizeof(ebofs_onode) + 
      get_collection_bytes() + 
      get_attr_bytes() + 
      get_extent_bytes() +
      get_bad_byte_bytes();
  }
  int get_collection_bytes() {
    return sizeof(coll_t) * collections.size();
  }
  int get_attr_bytes() {
    int s = 0;
    for (map<nstring, bufferptr>::iterator i = attr.begin();
         i != attr.end();
         i++) {
      s += i->first.length() + 1;
      s += i->second.length() + sizeof(int);
    }
    return s;
  }
  int get_extent_bytes() {
    return sizeof(extent_t) * extent_map.size() + sizeof(csum_t)*alloc_blocks;
  }
  int get_bad_byte_bytes() {
    return sizeof(extent_t) * bad_byte_extents.m.size();
  }
};


inline ostream& operator<<(ostream& out, Onode& on)
{
  out << "onode(" << hex << on.object_id << dec << " len=" << on.object_size;
  out << " ref=" << on.get_ref_count();
  if (on.is_dirty()) out << " dirty";
  if (on.is_dangling()) out << " dangling";
  if (on.is_deleted()) out << " deleted";
  out << " uncom=" << on.uncommitted;
  if (!on.bad_byte_extents.empty()) out << " badbytes=" << on.bad_byte_extents;
  //  out << " " << &on;
  out << ")";
  return out;
}



#endif
