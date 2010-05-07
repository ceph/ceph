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


#ifndef __EBOFS_BUFFERCACHE_H
#define __EBOFS_BUFFERCACHE_H

#include "include/lru.h"
#include "include/Context.h"

#include "common/Clock.h"

#include "types.h"
#include "BlockDevice.h"

#include "include/interval_set.h"
#include "include/xlist.h"

class ObjectCache;
class BufferCache;
class Onode;

class BufferHead : public LRUObject {
 public:
  /*
   * - buffer_heads should always break across disk extent boundaries
   * - partial buffer_heads are always 1 block.
   */
  const static int STATE_MISSING = 0; //     missing; data is on disk, but not loaded.
  const static int STATE_CLEAN = 1;   // Rw  clean
  const static int STATE_DIRTY = 2;   // RW  dirty
  const static int STATE_TX = 3;      // Rw  flushing to disk
  const static int STATE_RX = 4;      //  w  reading from disk
  const static int STATE_PARTIAL = 5; // reading from disk, + partial content map.  always 1 block.
  const static int STATE_CORRUPT = 6; //     data on disk doesn't match onode checksum

 public:
  ObjectCache *oc;

  bufferlist data;   // if empty, defined as zero (hole)

  ioh_t     rx_ioh;         // 
  extent_t  rx_from;
  ioh_t     tx_ioh;         // 
  block_t   tx_block;

  map<uint64_t, bufferlist>     partial;   // partial dirty content overlayed onto incoming data

  map<block_t, list<Context*> > waitfor_read;
  
  set<BufferHead*>  shadows;     // shadow bh's that clone()ed me.
  BufferHead*       shadow_of;


 private:
  int        ref;
  int        state;

 public:
  version_t  epoch_modified;
  
  version_t  version;        // current version in cache
  version_t  last_flushed;   // last version flushed to disk
 
  extent_t   object_loc;     // block position _in_object_

  utime_t    dirty_stamp;
  //xlist<BufferHead*>::item xlist_dirty;

  bool       want_to_expire;  // wants to be at bottom of lru

 public:
  BufferHead(ObjectCache *o, block_t start, block_t len) :
    oc(o), //cancellable_ioh(0), tx_epoch(0),
    rx_ioh(0), tx_ioh(0), tx_block(0),
    shadow_of(0),
    ref(0), state(STATE_MISSING), epoch_modified(0), version(0), last_flushed(0),
    //object_loc(start, len),
    //xlist_dirty(this),
    want_to_expire(false) {
    object_loc.start = start;
    object_loc.length = len;
  }
  ~BufferHead() {
    unpin_shadows();
  }
  
  ObjectCache *get_oc() { return oc; }

  int get() {
    assert(ref >= 0);
    if (ref == 0) lru_pin();
    return ++ref;
  }
  int put() {
    assert(ref > 0);
    if (ref == 1) lru_unpin();
    --ref;
    return ref;
  }
  int get_num_ref() { return ref; }

  block_t start() { return object_loc.start; }
  //void set_start(block_t s) { object_loc.start = s; }
  block_t length() { return object_loc.length; }
  void reset_length(block_t l) { object_loc.length = l; }
  block_t end() { return start() + length(); }
  block_t last() { return end()-1; }
  
  version_t get_version() { return version; }
  void set_version(version_t v) { version = v; }
  version_t get_last_flushed() { return last_flushed; }
  void set_last_flushed(version_t v) { 
    if (v <= last_flushed) cout << "last_flushed begin set to " << v << ", was " << last_flushed << std::endl;
    assert(v > last_flushed);
    last_flushed = v; 
  }

  utime_t get_dirty_stamp() { return dirty_stamp; }
  void set_dirty_stamp(utime_t t) { dirty_stamp = t; }

  void set_state(int s) {
    if (s == STATE_PARTIAL || s == STATE_RX || s == STATE_TX) get();
    if (state == STATE_PARTIAL || state == STATE_RX || state == STATE_TX) put();

    if ((state == STATE_TX && s != STATE_TX) ||
	(state == STATE_PARTIAL && s != STATE_PARTIAL)) 
      unpin_shadows();

    state = s;
  }
  int get_state() { return state; }

  bool is_missing() { return state == STATE_MISSING; }
  bool is_dirty() { return state == STATE_DIRTY; }
  bool is_clean() { return state == STATE_CLEAN; }
  bool is_tx() { return state == STATE_TX; }
  bool is_rx() { return state == STATE_RX; }
  bool is_partial() { return state == STATE_PARTIAL; }
  bool is_corrupt() { return state == STATE_CORRUPT; }

  bool is_hole() { return is_clean() && data.length() == 0; }
  
  void add_shadow(BufferHead *dup) {
    shadows.insert(dup);
    dup->shadow_of = this;
    dup->get();
  }
  void remove_shadow(BufferHead *dup) {
    shadows.erase(dup);
    dup->shadow_of = 0;
    dup->put();
  }
  void unpin_shadows() {
    for (set<BufferHead*>::iterator p = shadows.begin();
	 p != shadows.end();
	 ++p) {
      //cout << "unpin shadow " << *p << std::endl;
      (*p)->shadow_of = 0;
      (*p)->put();
    }
    shadows.clear();
  }

  void copy_partial_substr(uint64_t start, uint64_t end, bufferlist& bl) {
    map<uint64_t, bufferlist>::iterator i = partial.begin();
    
    // skip first bits (fully to left)
    while ((i->first + i->second.length() < start) &&
           i != partial.end()) 
      i++;
    assert(i != partial.end());
    assert(i->first <= start);
    
    // first
    unsigned bhoff = MAX(start, i->first) - i->first;
    unsigned bhlen = MIN(end-start, i->second.length());
    bl.substr_of( i->second, bhoff, bhlen );

    uint64_t pos = i->first + i->second.length();
    
    // have continuous to end?
    for (i++; i != partial.end(); i++) {
      if (pos >= end) break;
      assert(pos == i->first);

      pos = i->first + i->second.length();

      if (pos <= end) {      // this whole frag
        bl.append( i->second );
      } else {            // partial end
        unsigned bhlen = end-start-bl.length();
        bufferlist frag;
        frag.substr_of( i->second, 0, bhlen );
        bl.claim_append(frag);
        break;  // done.
      }
    }
    
    assert(pos >= end);
    assert(bl.length() == (unsigned)(end-start));
  }

  bool have_partial_range(uint64_t start, uint64_t end) {
    map<uint64_t, bufferlist>::iterator i = partial.begin();

    // skip first bits (fully to left)
    while ((i->first + i->second.length() < start) &&
           i != partial.end()) 
      i++;
    if (i == partial.end()) return false;

    // have start?
    if (i->first > start) return false;
    uint64_t pos = i->first + i->second.length();

    // have continuous to end?
    for (i++; i != partial.end(); i++) {
      assert(pos <= i->first);
      if (pos < i->first) return false;
      assert(pos == i->first);
      pos = i->first + i->second.length();
      if (pos >= end) break;  // gone far enough
    }

    if (pos >= end) return true;
    return false;
  }

  bool partial_is_complete(uint64_t size) {
    return have_partial_range( 0, MIN(size, EBOFS_BLOCK_SIZE) );
  }

  void apply_partial();
  void add_partial(uint64_t off, bufferlist& p);

  void take_read_waiters(list<Context*>& finished) {
    for (map<block_t,list<Context*> >::iterator p = waitfor_read.begin();
         p != waitfor_read.end();
         p++)
      finished.splice(finished.begin(), p->second);
    waitfor_read.clear();
  }

};

inline ostream& operator<<(ostream& out, BufferHead& bh)
{
  out << "bufferhead(" << bh.start() << "~" << bh.length();
  out << " v" << bh.get_version() << "/" << bh.get_last_flushed();
  if (bh.is_missing()) out << " missing";
  if (bh.is_dirty()) out << " dirty";
  if (bh.is_clean()) {
    out << " clean";
    if (bh.data.length() == 0)
      out << " HOLE";
  }
  if (bh.is_rx()) out << " rx";
  if (bh.is_tx()) out << " tx";
  if (bh.is_partial()) out << " partial";
  if (bh.is_corrupt()) out << " corrupt";

  // include epoch modified?
  if (bh.is_dirty() || bh.is_tx() || bh.is_partial()) 
    out << "(e" << bh.epoch_modified << ")";

  //out << " " << bh.data.length();
  out << " " << &bh;
  out << ")";
  return out;
}


class ObjectCache {
 public:
  pobject_t object_id;
  Onode *on;
  BufferCache *bc;

 private:
  map<block_t, BufferHead*>  data;
  int ref;

 public:
  version_t write_count;


 public:
  ObjectCache(pobject_t o, Onode *_on, BufferCache *b) : 
    object_id(o), on(_on), bc(b), ref(0),
    write_count(0) { }
  ~ObjectCache() {
    assert(data.empty());
    assert(ref == 0);
  }

  int get() { 
    ++ref;
    //cout << "oc.get " << object_id << " " << ref << std::endl;
    return ref; 
  }
  int put() { 
    assert(ref > 0); 
    --ref;
    //cout << "oc.put " << object_id << " " << ref << std::endl;
    return ref; 
  }
  
  pobject_t get_object_id() { return object_id; }


  /*
   * will return bh containing pos.  
   * if none, then the _next_ bh.
   * if none, then data.end().
   */
  map<block_t, BufferHead*>::iterator find_bh(block_t start, block_t len=0) {
    map<block_t, BufferHead*>::iterator p;
     
    // hack speed up common cases
    if (start == 0) {
      p = data.begin();
    } else if (len == 1 &&
	       !data.empty() &&
	       data.rbegin()->first <= start) {
      // append hack.
      p = data.end();
      p--;
      if (p->first < start) p++;
    } else {
      p = data.lower_bound(start);  
    }

    if (p != data.begin() && 
	(p == data.end() || p->first > start)) {
      p--;     // might overlap!
      if (p->first + p->second->length() <= start) 
	p++;   // doesn't overlap.
    }
    return p;
  }
  
  BufferHead *find_bh_containing(block_t b) {
    map<block_t, BufferHead*>::iterator p = find_bh(b, 1);
    if (p != data.end() &&
	p->second->start() <= b &&
	p->second->end() > b)
      return p->second;
    return 0;
  }


  void add_oc_bh(BufferHead *bh) {
    // add to my map
    assert(data.count(bh->start()) == 0);

    if (0) {  // sanity check     FIXME DEBUG
      //cout << "add_bh " << bh->start() << "~" << bh->length() << std::endl;
      map<block_t,BufferHead*>::iterator p = data.lower_bound(bh->start());
      if (p != data.end()) {
        //cout << " after " << *p->second << std::endl;
        //cout << " after starts at " << p->first << std::endl;
        assert(p->first >= bh->end());
      }
      if (p != data.begin()) {
        p--;
        //cout << " before starts at " << p->second->start() 
        //<< " and ends at " << p->second->end() << std::endl;
        //cout << " before " << *p->second << std::endl;
        assert(p->second->end() <= bh->start());
      }
    }

    data[bh->start()] = bh;
  }
  void remove_oc_bh(BufferHead *bh) {
    assert(data.count(bh->start()));
    data.erase(bh->start());
  }
  bool is_empty() { return data.empty(); }

  void try_merge_bh(BufferHead *bh);
  void try_merge_bh_left(map<block_t, BufferHead*>::iterator& p);
  void try_merge_bh_right(map<block_t, BufferHead*>::iterator& p);
  BufferHead* merge_bh_left(BufferHead *left, BufferHead *right);

  int find_tx(block_t start, block_t len,
              list<BufferHead*>& tx);

  int map_read(block_t start, block_t len, 
               map<block_t, BufferHead*>& hits,     // hits
               map<block_t, BufferHead*>& missing,  // read these from disk
               map<block_t, BufferHead*>& rx,       // wait for these to finish reading from disk
               map<block_t, BufferHead*>& partial); // (maybe) wait for these to read from disk
  int try_map_read(block_t start, block_t len);  // just tell us how many extents we're missing.

  int map_write(block_t start, block_t len,
		map<block_t, BufferHead*>& hits,
		version_t super_epoch);

  void touch_bottom(block_t bstart, block_t blast);

  BufferHead *split(BufferHead *bh, block_t off);

  /*int scan_versions(block_t start, block_t len,
                    version_t& low, version_t& high);
  */

  void rx_finish(ioh_t ioh, block_t start, block_t length, bufferlist& bl);
  void tx_finish(ioh_t ioh, block_t start, block_t length, version_t v, version_t epoch);

  void truncate(block_t blocks, version_t super_epoch);
  void discard_bh(BufferHead *bh, version_t super_epoch);
  //  void tear_down();

  void clone_to(Onode *other);

  void dump() {
    for (map<block_t,BufferHead*>::iterator i = data.begin();
         i != data.end();
         i++)
      cout << "dump: " << i->first << ": " << *i->second << std::endl;
  }

  void scrub_csums();

};



class BufferCache {
 public:
  Mutex             &ebofs_lock;          // hack: this is a ref to global ebofs_lock
  BlockDevice       &dev;

  //xlist<BufferHead*> dirty_bh;

  LRU   lru_dirty, lru_rest;

  bool poison_commit;

 private:
  Cond  stat_cond;
  Cond  flush_cond;
  int   stat_waiter;

  uint64_t stat_all;
  uint64_t stat_clean, stat_corrupt;
  uint64_t stat_dirty;
  uint64_t stat_rx;
  uint64_t stat_tx;
  uint64_t stat_partial;
  uint64_t stat_missing;
  
  int partial_reads;


#define EBOFS_BC_FLUSH_BHWRITE 0
#define EBOFS_BC_FLUSH_PARTIAL 1

  map<version_t, int> epoch_unflushed[2];
  
 public:
  BufferCache(BlockDevice& d, Mutex& el) : 
    ebofs_lock(el), dev(d), 
    stat_waiter(0),
    stat_all(0), stat_clean(0), stat_corrupt(0), stat_dirty(0), stat_rx(0), stat_tx(0), stat_partial(0), stat_missing(0),
    partial_reads(0)
    {}


  uint64_t get_size() {
    assert(stat_clean+stat_dirty+stat_rx+stat_tx+stat_partial+stat_corrupt+stat_missing == stat_all);
    return stat_all;
  }
  uint64_t get_trimmable() {
    return stat_clean+stat_corrupt;
  }


  // bh's in cache
  void add_bh(BufferHead *bh) {
    bh->get_oc()->add_oc_bh(bh);
    if (bh->is_dirty()) {
      lru_dirty.lru_insert_mid(bh);
      //dirty_bh.push_back(&bh->xlist_dirty);
    } else
      lru_rest.lru_insert_mid(bh);
    stat_add(bh);
  }
  void touch(BufferHead *bh) {
    if (bh->is_dirty()) {
      lru_dirty.lru_touch(bh);
    } else
      lru_rest.lru_touch(bh);
  }
  void touch_bottom(BufferHead *bh) {
    if (bh->is_dirty()) {
      bh->want_to_expire = true;
      lru_dirty.lru_bottouch(bh);
    } else
      lru_rest.lru_bottouch(bh);
  }
  void remove_bh(BufferHead *bh) {
    bh->get_oc()->remove_oc_bh(bh);
    stat_sub(bh);
    if (bh->is_dirty()) {
      lru_dirty.lru_remove(bh);
      //dirty_bh.push_back(&bh->xlist_dirty);
    } else
      lru_rest.lru_remove(bh);
    delete bh;
  }

  // stats
  void stat_add(BufferHead *bh) {
    assert(stat_clean+stat_dirty+stat_rx+stat_tx+stat_partial+stat_corrupt+stat_missing == stat_all);
    switch (bh->get_state()) {
    case BufferHead::STATE_MISSING: stat_missing += bh->length(); break;
    case BufferHead::STATE_CLEAN: stat_clean += bh->length(); break;
    case BufferHead::STATE_CORRUPT: stat_corrupt += bh->length(); break;
    case BufferHead::STATE_DIRTY: stat_dirty += bh->length(); break;
    case BufferHead::STATE_TX: stat_tx += bh->length(); break;
    case BufferHead::STATE_RX: stat_rx += bh->length(); break;
    case BufferHead::STATE_PARTIAL: 
      stat_partial += bh->length(); 
      inc_partial_read();
      break;
    default: assert(0);
    }
    stat_all += bh->length();
    if (stat_waiter) stat_cond.Signal();
  }
  void stat_sub(BufferHead *bh) {
    assert(stat_clean+stat_dirty+stat_rx+stat_tx+stat_partial+stat_corrupt+stat_missing == stat_all);
    switch (bh->get_state()) {
    case BufferHead::STATE_MISSING: stat_missing -= bh->length(); assert(stat_missing >= 0); break;
    case BufferHead::STATE_CLEAN: stat_clean -= bh->length(); assert(stat_clean >= 0); break;
    case BufferHead::STATE_CORRUPT: stat_corrupt -= bh->length(); assert(stat_corrupt >= 0); break;
    case BufferHead::STATE_DIRTY: stat_dirty -= bh->length(); assert(stat_dirty >= 0); break;
    case BufferHead::STATE_TX: stat_tx -= bh->length(); assert(stat_tx >= 0); break;
    case BufferHead::STATE_RX: stat_rx -= bh->length(); assert(stat_rx >= 0); break;
    case BufferHead::STATE_PARTIAL: 
      stat_partial -= bh->length(); assert(stat_partial >= 0); 
      dec_partial_read();
      break;
    default: assert(0);
    }
    stat_all -= bh->length();
  }
  uint64_t get_stat_tx() { return stat_tx; }
  uint64_t get_stat_rx() { return stat_rx; }
  uint64_t get_stat_dirty() { return stat_dirty; }
  uint64_t get_stat_clean() { return stat_clean; }
  uint64_t get_stat_partial() { return stat_partial; }

  
  map<version_t, int> &get_unflushed(int what) {
    return epoch_unflushed[what];
  }

  int get_unflushed(int what, version_t epoch) {
    return epoch_unflushed[what][epoch];
  }
  void inc_unflushed(int what, version_t epoch) {
    epoch_unflushed[what][epoch]++;
    //cout << "inc_unflushed " << epoch << " now " << epoch_unflushed[what][epoch] << std::endl;
  }
  void dec_unflushed(int what, version_t epoch) {
    epoch_unflushed[what][epoch]--;
    //cout << "dec_unflushed " << epoch << " now " << epoch_unflushed[what][epoch] << std::endl;
    if (epoch_unflushed[what][epoch] == 0) 
      flush_cond.Signal();
  }

  bool get_num_partials() {
    return partial_reads;
  }
  void inc_partial_read() {
    partial_reads++;
  }
  void dec_partial_read() {
    partial_reads--;
    if (partial_reads == 0 && stat_waiter)
      stat_cond.Signal();
  }

  void waitfor_stat() {
    stat_waiter++;
    stat_cond.Wait(ebofs_lock);
    stat_waiter--;
  }
  void waitfor_partials() {
    stat_waiter++;
    while (partial_reads > 0) 
      stat_cond.Wait(ebofs_lock);
    stat_waiter--;

  }
  void waitfor_flush() {
    flush_cond.Wait(ebofs_lock);
  }


  // bh state
  void set_state(BufferHead *bh, int s) {
    // move between lru lists?
    if (s == BufferHead::STATE_DIRTY && bh->get_state() != BufferHead::STATE_DIRTY) {
      lru_rest.lru_remove(bh);
      lru_dirty.lru_insert_top(bh);
      //dirty_bh.push_back(&bh->xlist_dirty);
    }
    if (s != BufferHead::STATE_DIRTY && bh->get_state() == BufferHead::STATE_DIRTY) {
      lru_dirty.lru_remove(bh);
      if (bh->want_to_expire)
	lru_rest.lru_insert_bot(bh);
      else
	lru_rest.lru_insert_mid(bh);
      //dirty_bh.remove(&bh->xlist_dirty);
    }

    // set state
    stat_sub(bh);
    bh->set_state(s);
    stat_add(bh);
  }      

  void copy_state(BufferHead *bh1, BufferHead *bh2) { 
    set_state(bh2, bh1->get_state());
  }
  
  void mark_missing(BufferHead *bh) { set_state(bh, BufferHead::STATE_MISSING); };
  void mark_clean(BufferHead *bh) { set_state(bh, BufferHead::STATE_CLEAN); };
  void mark_corrupt(BufferHead *bh) { set_state(bh, BufferHead::STATE_CORRUPT); };
  void mark_rx(BufferHead *bh) { set_state(bh, BufferHead::STATE_RX); };
  void mark_partial(BufferHead *bh) { set_state(bh, BufferHead::STATE_PARTIAL); };
  void mark_tx(BufferHead *bh) { set_state(bh, BufferHead::STATE_TX); };
  void mark_dirty(BufferHead *bh) { 
    set_state(bh, BufferHead::STATE_DIRTY); 
    bh->set_dirty_stamp(g_clock.now());
  };


  // io
  void bh_read(Onode *on, BufferHead *bh, block_t from=0);
  void bh_write(Onode *on, BufferHead *bh, block_t shouldbe=0);

  bool bh_cancel_read(BufferHead *bh);
  bool bh_cancel_write(BufferHead *bh, version_t cur_epoch);

  void rx_finish(ObjectCache *oc, ioh_t ioh, block_t start, block_t len, block_t diskstart, bufferlist& bl);
  void tx_finish(ObjectCache *oc, ioh_t ioh, block_t start, block_t len, version_t v, version_t e);

  friend class C_E_FlushPartial;

  // bh fun
  BufferHead *split(BufferHead *orig, block_t after);
};


class C_OC_RxFinish : public BlockDevice::callback {
  Mutex &lock;
  ObjectCache *oc;
  block_t start, length;
  block_t diskstart;
public:
  bufferlist bl;
  C_OC_RxFinish(Mutex &m, ObjectCache *o, block_t s, block_t l, block_t ds) :
    lock(m), oc(o), start(s), length(l), diskstart(ds) {}
  void finish(ioh_t ioh, int r) {
    oc->bc->rx_finish(oc, ioh, start, length, diskstart, bl);
  }
};

class C_OC_TxFinish : public BlockDevice::callback {
  Mutex &lock;
  ObjectCache *oc;
  block_t start, length;
  version_t version;
  version_t epoch;
 public:
  C_OC_TxFinish(Mutex &m, ObjectCache *o, block_t s, block_t l, version_t v, version_t e) :
    lock(m), oc(o), start(s), length(l), version(v), epoch(e) {}
  void finish(ioh_t ioh, int r) {
    oc->bc->tx_finish(oc, ioh, start, length, version, epoch);
  }  
};


#endif
