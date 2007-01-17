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


#ifndef __EBOFS_ONODE_H
#define __EBOFS_ONODE_H

#include "include/lru.h"

#include "types.h"
#include "BufferCache.h"

#include "include/interval_set.h"


/*
 * object node (like an inode)
 *
 * holds object metadata, including
 *  size
 *  allocation (extent list)
 *  attributes
 *
 */

class Onode : public LRUObject {
private:
  int ref;

public:
  object_t object_id;
  version_t version;      // incremented on each modify.

  // data
  bool     readonly;
  Extent   onode_loc;
  off_t    object_size;
  unsigned object_blocks;

  // onode
  set<coll_t>            collections;
  map<string, bufferptr> attr;
  //vector<Extent>        extents;
  map<block_t, Extent>  extent_map;

  interval_set<block_t> uncommitted;

  ObjectCache  *oc;

  bool          dirty;
  bool          dangling;   // not in onode_map
  bool          deleted;    // deleted

  list<Context*>   commit_waiters;

 public:
  Onode(object_t oid) : ref(0), object_id(oid), version(0),
			readonly(false),
			object_size(0), object_blocks(0), oc(0),
			dirty(false), dangling(false), deleted(false) { 
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
    //cout << "ebofs.onode.get " << hex << object_id << dec << " " << ref << endl;
  }
  void put() {
    ref--;
    if (ref == 0) lru_unpin();
    //cout << "ebofs.onode.put " << hex << object_id << dec << " " << ref << endl;
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
      //cout << "close_oc on " << object_id << endl;
      assert(oc->is_empty());
      if (oc->put() == 0){
        //cout << "************************* hosing oc" << endl;
        delete oc;
      }
      oc = 0;
      put();
    }
  }


  // allocation
  void verify_extents() {
    if (0) {  // do crazy stupid sanity checking
      block_t count = 0;
      interval_set<block_t> is;    
          
      set<block_t> s;
      cout << "verifying" << endl;

      for (map<block_t,Extent>::iterator p = extent_map.begin();
           p != extent_map.end();
           p++) {
        cout << " " << p->first << ": " << p->second << endl;
        assert(count == p->first);
        count += p->second.length;
        for (unsigned j=0;j<p->second.length;j++) {
          assert(s.count(p->second.start+j) == 0);
          s.insert(p->second.start+j);
        }
      }

      assert(s.size() == count);
      assert(count == object_blocks);
    }
  }
  void set_extent(block_t offset, Extent ex) {
    //cout << "set_extent " << offset << " -> " << ex << " ... " << object_blocks << endl;
    assert(offset <= object_blocks);
    verify_extents();

    // at the end?
    if (offset == object_blocks) {
      //cout << " appending " << ex << endl;
      if (!extent_map.empty() && extent_map.rbegin()->second.end() == ex.start) {
        //cout << "appending " << ex << " to " << extent_map.rbegin()->second << endl;
        extent_map.rbegin()->second.length += ex.length;
      } else
        extent_map[object_blocks] = ex;
      object_blocks += ex.length;
      return;
    }

    // removing any extent bits we overwrite
    if (!extent_map.empty()) {
      // preceeding extent?
      map<block_t,Extent>::iterator p = extent_map.lower_bound(offset);
      if (p != extent_map.begin()) {
        p--;
        if (p->first + p->second.length > offset) {
          //cout << " preceeding was " << p->second << endl;
          if (p->first + p->second.length > offset+ex.length) {
            // cutting chunk out of middle, add last bit
            Extent &n = extent_map[offset+ex.length] = p->second;
            n.start += offset+ex.length - p->first;
            n.length -= offset+ex.length - p->first;
            //cout << " tail frag is " << n << endl;
          } 
          p->second.length = offset - p->first;     // cut tail off preceeding extent
          //cout << " preceeding now " << p->second << endl;
        }
        p++;
      }      
      
      // overlapping extents
      while (p != extent_map.end() &&
             p->first < offset + ex.length) {
        map<block_t,Extent>::iterator next = p;
        next++;

        // completely subsumed?
        if (p->first + p->second.length <= offset+ex.length) {
          //cout << " erasing " << p->second << endl;
          extent_map.erase(p);
          p = next;
          continue;
        }

        // spans new extent, cut off head
        Extent &n = extent_map[ offset+ex.length ] = p->second;
        //cout << " cut head off " << p->second;
        n.start += offset+ex.length - p->first;
        n.length -= offset+ex.length - p->first;
        extent_map.erase(p);
        //cout << ", now " << n << endl;
        break;
      }
    }

    extent_map[ offset ] = ex;

    // extend object?
    if (offset + ex.length > object_blocks)
      object_blocks = offset+ex.length;
    
    verify_extents();
  }
  

  /* map_extents(start, len, ls)
   *  map teh given page range into extents on disk.
   */
  int map_extents(block_t start, block_t len, vector<Extent>& ls) {
    //cout << "map_extents " << start << " " << len << endl;
    verify_extents();

    //assert(start+len <= object_blocks);

    map<block_t,Extent>::iterator p = extent_map.lower_bound(start);
    if (p != extent_map.begin() &&
        (p == extent_map.end() || p->first > start && p->first)) {
      p--;
      if (p->second.length > start - p->first) {
        Extent ex;
        ex.start = p->second.start + (start - p->first);
        ex.length = MIN(len, p->second.length - (start - p->first));
        ls.push_back(ex);
        
        //cout << " got (tail of?) " << p->second << " : " << ex << endl;
        
        start += ex.length;
        len -= ex.length;
      }
      p++;
    }

    while (len > 0 &&
           p != extent_map.end()) {
      assert(p->first == start);
      Extent ex = p->second;
      ex.length = MIN(len, ex.length);
      ls.push_back(ex);
      //cout << " got (head of?) " << p->second << " : " << ex << endl;
      start += ex.length;
      len -= ex.length;
      p++;
    }    

    return 0;
  }

  int truncate_extents(block_t len, vector<Extent>& extra) {
    verify_extents();

    map<block_t,Extent>::iterator p = extent_map.lower_bound(len);
    if (p != extent_map.begin() &&
        (p == extent_map.end() || p->first > len && p->first)) {
      p--;
      if (p->second.length > len - p->first) {
        Extent ex;
        ex.start = p->second.start + (len - p->first);
        ex.length = p->second.length - (len - p->first);
        extra.push_back(ex);

        p->second.length = len - p->first;
        assert(p->second.length > 0);
        
        //cout << " got (tail of?) " << p->second << " : " << ex << endl;
      }
      p++;
    }
    
    while (p != extent_map.end()) {
      assert(p->first >= len);
      extra.push_back(p->second);
      map<block_t,Extent>::iterator n = p;
      n++;
      extent_map.erase(p);
      p = n;
    }    
    
    object_blocks = len;
    verify_extents();
    return 0;
  }

  int truncate_front_extents(block_t len, vector<Extent>& extra) {
    verify_extents();
    
    while (len > 0) {
      Extent& ex = extent_map.begin()->second;  // look, this is a reference!
      if (ex.length > len) {
        // partial first extent
        Extent frontbit( ex.start, len );
        extra.push_back(frontbit);
        ex.length -= len;
        ex.start += len;
        break;
      }

      // pull off entire first extent.
      assert(ex.length <= len);
      len -= ex.length;
      extra.push_back(ex);
      extent_map.erase(extent_map.begin());
    }

    object_blocks -= len;
    verify_extents();
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



  // pack/unpack
  int get_collection_bytes() {
    return sizeof(coll_t) * collections.size();
  }
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
  int get_extent_bytes() {
    return sizeof(Extent) * extent_map.size();
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
  //  out << " " << &on;
  out << ")";
  return out;
}



#endif
