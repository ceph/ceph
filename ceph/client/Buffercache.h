// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Carlos Maltzahn <carlosm@soe.ucsc.edu>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#ifndef __BUFFERCACHE_H
#define __BUFFERCACHE_H

#include <time.h>

#include "include/types.h"
#include "include/buffer.h"
#include "include/bufferlist.h"
#include "include/lru.h"
#include "config.h"
#include "common/Cond.h"

#include "Client.h"

// stl
#include <list>
#include <map>
using namespace std;

// Bufferhead states
#define BUFHD_STATE_CLEAN	  1
#define BUFHD_STATE_DIRTY	  2
#define BUFHD_STATE_RX            3
#define BUFHD_STATE_TX            4

#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "client" << "." << pthread_self() << " " 

class Buffercache;
class Filecache;

class Bufferhead : public LRUObject {
  int ref;

 public: // FIXME: make more private and write some accessors
  int get() {
	assert(ref >= 0);
	if (ref == 0) lru_pin();
	return ++ref;
  }
  int put() {
	assert(ref > 0);
	if (ref == 1) lru_unpin();
	return --ref;
  }
  
  off_t offset;
  off_t miss_len;  // only valid during misses 
  class Inode *inode;
  time_t dirty_since;
  int state; 
  bufferlist bl;
  // read_waiters: threads waiting for reads from the buffer
  // write_waiters: threads waiting for writes into the buffer
  list<Cond*> read_waiters, write_waiters;
  Buffercache *bc;
  Filecache *fc;
  bool visited;
  bool is_hole() { return bl.length() == 0; }
  
  // cons/destructors
  Bufferhead(class Inode *inode, Buffercache *bc);
  Bufferhead(class Inode *inode, off_t off, Buffercache *bc);
  ~Bufferhead(); 
  
  //Bufferhead(inodeno_t ino, off_t off, off_t len, int state);
  // ~Bufferhead(); FIXME: need to mesh with allocator scheme
  
  void set_offset(off_t offset);

  off_t length() {
    if (is_hole() || state == BUFHD_STATE_RX) return miss_len;
    return bl.length();
  }

  void alloc_buffers(off_t size);

  /** wait_for_(read|write) 
   * put Cond on local stack, block until woken up.
   * _caller_ pins to avoid any race weirdness
   */
  void wait_for_read(Mutex& lock) {
	assert(state == BUFHD_STATE_RX || state == BUFHD_STATE_TX);
	Cond cond;
	get();
	read_waiters.push_back(&cond);
	cond.Wait(lock);
	put();
  }
  void wait_for_write(Mutex& lock) {
	assert(state == BUFHD_STATE_RX || state == BUFHD_STATE_TX);
	Cond cond;
	get();
	write_waiters.push_back(&cond);
	cond.Wait(lock);
	put();
  }
  
  void wakeup_read_waiters() { 
    for (list<Cond*>::iterator it = read_waiters.begin();
		 it != read_waiters.end();
		 it++) {
	  (*it)->Signal();
	}
    read_waiters.clear(); 
  }
  void wakeup_write_waiters() {
    for (list<Cond*>::iterator it = write_waiters.begin();
		 it != write_waiters.end();
		 it++) {
	  (*it)->Signal();
	}
    write_waiters.clear(); 
  }
  
  void miss_start(off_t miss_len);
  void miss_finish();
  void dirty();
  void dirtybuffers_erase();
  void flush_start();
  void flush_finish();
  void claim_append(Bufferhead* other);
  void splice(off_t offset, off_t length, Bufferhead *claim_by);

  friend ostream& operator<<(ostream& out, Bufferhead& bh);
};

inline ostream& operator<<(ostream& out, Bufferhead& bh) {
  out << "Bufferhead(ino=" << bh.inode->ino() << endl;
        out << "\t offset=" << bh.offset << endl;
        out << "\t length=" << bh.length() << endl;
        out << "\t state=" << bh.state << endl;
        out << "\t visited=" << bh.visited << endl;
        out << "\t is_hole=" << bh.is_hole() << endl;
        out << "\t bl=" << bh.bl << endl;
  out << ")" << endl;
  return out;
}                                   
  

class Dirtybuffers {
 private:
  multimap<time_t, Bufferhead*> _dbufs;
  map<Bufferhead*, time_t> _revind;
  Buffercache *bc;
  // DEBUG
  //time_t former_age;

 public:
  Dirtybuffers(Buffercache *bc) { 
    //former_age = 0;
    //dout(5) << "Dirtybuffers() former_age: " << former_age << endl; 
    this->bc = bc;
  }
  Dirtybuffers(const Dirtybuffers& other);
  Dirtybuffers& operator=(const Dirtybuffers& other);
  int size() { assert(_dbufs.size() == _revind.size()); return _dbufs.size(); }
  multimap<time_t, Bufferhead*>::iterator find(Bufferhead *bh);
  void erase(Bufferhead* bh);
  void insert(Bufferhead* bh);
  bool empty() { assert(_revind.empty() == _dbufs.empty()); return _dbufs.empty(); }
  bool exist(Bufferhead* bh);
  void get_expired(time_t ttl, off_t left_dirty, set<Bufferhead*>& to_flush);
  time_t get_age() { 
    time_t age;
    if (_dbufs.empty()) {
      age = 0;
    } else {
      age = time(NULL) - _dbufs.begin()->second->dirty_since;
    }
    //dout(10) << "former age: " << former_age << " age: " << age << endl;
    //assert((!(former_age > 30)) || (age > 0));
    //former_age = age;
    return age;
  }
  friend ostream& operator<<(ostream& out, Dirtybuffers& db);
};

inline ostream& operator<<(ostream& out, Dirtybuffers& db) {
  out << "Dirtybuffers(size=" << db.size() << endl;
  for (multimap<time_t, Bufferhead*>::iterator it = db._dbufs.begin();
           it != db._dbufs.end();
           it++)
        out << "\t" << it->first << ": " << *(it->second) << endl;
  out << ")" << endl;
  return out;
}                                   

class Filecache {
 private:
  list<Cond*> inflight_waiters;
  list<Context*> inflight_waiter_callbacks;

 public: 
  class Inode *inode;
  map<off_t, Bufferhead*> buffer_map;
  set<Bufferhead*> dirty_buffers;
  set<Bufferhead*> inflight_buffers;
  Buffercache *bc;

  Filecache(Buffercache *bc, class Inode *inode) { 
    this->bc = bc;
    this->inode = inode;
    buffer_map.clear();
  }
  Filecache(const Filecache& other); 
  Filecache& operator=(const Filecache& other);

  ~Filecache() {
    dout(6) << "bc: delete fc of ino: " << inode->ino() << endl;
    map<off_t, Bufferhead*> to_delete = buffer_map;
    buffer_map.clear();
    for (map<off_t, Bufferhead*>::iterator it = to_delete.begin();
         it != to_delete.end();
         it++) {
      delete it->second; 
    }
  }

#if 0
  off_t length() {
    off_t len = 0;
    for (map<off_t, Bufferhead*>::iterator it = buffer_map.begin();
         it != buffer_map.end();
         it++) {
      len += it->second->bl.length();
    }
    return len;
  }
#endif 

  bool is_dirty() { return !dirty_buffers.empty(); }
  bool is_inflight() { return !inflight_buffers.empty(); }
  bool is_flushed() { return !is_dirty() && !is_inflight(); }

  void insert(off_t offset, Bufferhead* bh);

  void splice(off_t offset, off_t size);

  void wait_for_inflight(Mutex& lock) {
	Cond cond;
	inflight_waiters.push_back(&cond);
	cond.Wait(lock);
  }
  void add_inflight_waiter(Context *c) {
	inflight_waiter_callbacks.push_back(c);
  }

  void wakeup_inflight_waiters() {
	// callbacks first
	list<Context*> ls;
	ls.splice(ls.begin(), inflight_waiter_callbacks);
	finish_contexts(ls, 0);

	// then threads
    for (list<Cond*>::iterator it = inflight_waiters.begin();
		 it != inflight_waiters.end();
		 it++) {
	  (*it)->Signal();
	}
    inflight_waiters.clear(); 
  }

  map<off_t, Bufferhead*>::iterator get_buf(off_t off);
  map<off_t, Bufferhead*>::iterator overlap(off_t len, off_t off);
  int copy_out(off_t size, off_t offset, char *dst);    
  map<off_t, Bufferhead*>::iterator map_existing(off_t len, off_t start_off, 
                    map<off_t, Bufferhead*>& hits, 
		    map<off_t, Bufferhead*>& rx,
		    map<off_t, Bufferhead*>& tx,
                    map<off_t, off_t>& holes);
  off_t consolidation_opp(time_t ttl, off_t clean_goal, 
                           off_t offset, list<off_t>& offlist);
  void get_dirty(set<Bufferhead*>& to_flush);
};

class Buffercache { 
 private:
  off_t dirty_size, rx_size, tx_size, clean_size;
  list<Cond*> inflight_waiters;

 public:
  map<inodeno_t, Filecache*> bcache_map;
  LRU lru;
  Dirtybuffers *dirty_buffers;
  set<Bufferhead*> inflight_buffers;

  Buffercache() : dirty_size(0), rx_size(0), tx_size(0), clean_size(0) { 
    dirty_buffers = new Dirtybuffers(this);
  }
  
  // FIXME: constructor & destructor need to mesh with allocator scheme
  ~Buffercache() {
    for (map<inodeno_t, Filecache*>::iterator it = bcache_map.begin();
         it != bcache_map.end();
         it++) {
      // FIXME: make sure all buffers are cleaned  and then free them
      delete it->second; 
    }
  }
  Buffercache(const Buffercache& other);
  Buffercache& operator=(const Buffercache& other);
  
  Filecache *get_fc(Inode *inode) {
    if (!bcache_map.count(inode->ino())) {
      bcache_map[inode->ino()] = new Filecache(this, inode);
    } 
    return bcache_map[inode->ino()];
  }
      
  void wait_for_inflight(Mutex& lock) {
	Cond cond;
	inflight_waiters.push_back(&cond);
	cond.Wait(lock);
  }

  void wakeup_inflight_waiters() {
    for (list<Cond*>::iterator it = inflight_waiters.begin();
		 it != inflight_waiters.end();
		 it++) {
	  (*it)->Signal();
	}
    inflight_waiters.clear(); 
  }

  void clean_to_dirty(off_t size) {
    clean_size -= size;
    assert(clean_size >= 0);
    dirty_size += size;
  }
  void dirty_to_tx(off_t size) {
    dirty_size -= size;
    assert(dirty_size >= 0);
    tx_size += size;
  }
  void tx_to_dirty(off_t size) {
    tx_size -= size;
    assert(tx_size >= 0);
    dirty_size += size;
  }
  void tx_to_clean(off_t size) {
    tx_size -= size;
    assert(tx_size >= 0);
    clean_size += size;
  }
  void increase_size(off_t size) {
    clean_size += size;
  }
  void decrease_size(off_t size) {
    clean_size -= size;
    assert(clean_size >= 0);
  }
  off_t get_clean_size() { return clean_size; }
  off_t get_dirty_size() { return dirty_size; }
  off_t get_rx_size() { return rx_size; }
  off_t get_tx_size() { return tx_size; }
  off_t get_total_size() { return clean_size + dirty_size + rx_size + tx_size; }
  void get_reclaimable(off_t min_size, list<Bufferhead*>&);

  void insert(Bufferhead *bh);
  void dirty(Inode *inode, off_t size, off_t offset, const char *src);
  off_t touch_continuous(map<off_t, Bufferhead*>& hits, off_t size, off_t offset);
  void map_or_alloc(class Inode *inode, off_t len, off_t off, 
                    map<off_t, Bufferhead*>& buffers, 
		    map<off_t, Bufferhead*>& rx,
		    map<off_t, Bufferhead*>& tx);
  void consolidate(map<Inode*, map<off_t, list<off_t> > > cons_map);
  void release_file(inodeno_t ino);       
  off_t reclaim(off_t min_size);
};

     
#endif

