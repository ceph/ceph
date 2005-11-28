#ifndef __EBOFS_BUFFERCACHE_H
#define __EBOFS_BUFFERCACHE_H

#include "include/lru.h"

#include "types.h"
#include "AlignedBufferPool.h"


#define BH_STATE_DIRTY  1

class BufferHead : public LRUObject {
 public:
  //const static int STATUS_DNE = 0;     //     not in cache; undefined
  const static int STATE_CLEAN = 1;   // Rw  clean
  const static int STATE_DIRTY = 2;   // RW  dirty
  const static int STATE_TX = 3;      // Rw  flushing to disk
  const static int STATE_RX = 4;      //  w  reading from disk

 private:
  bufferlist bl;
  
  int        ref;
  int        state;
  version_t  version;
  
  Extent     object_loc;     // block position _in_object_
  
  list<Context*> waitfor_read;
  list<Context*> waitfor_flush;
  
 public:
  BufferHead() :
	ref(0), state(STATE_CLEAN), version(0) {}
  
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

  block_t length() { return object_loc.length; }

  void set_state(int s) {
	if (state == STATE_CLEAN && s != STATE_CLEAN) get();
	if (state != STATE_CLEAN && s == STATE_CLEAN) put();
	state = s;
  }
  int get_state() { return state; }

  bool is_dirty() { return state == STATE_DIRTY; }
  bool is_clean() { return state == STATE_CLEAN; }
  bool is_rx() { return state == STATE_RX; }
  bool is_tx() { return state == STATE_TX; }


  void substr(block_t off, block_t num, bufferlist& sub) {
	// determine offset in bufferlist
	block_t start = object_loc - off;  
	block_t len = num - start;
	if (start+len > object_loc.length)
	  len = object_loc.length - start;
	
	sub.substr_of(bl, start*EBOFS_BLOCK_SIZE, len*EBOFS_BLOCK_SIZE);
  }

};


class ObjectCache {
 private:
  object_t oid;
  class BufferCache *bc;

  map<off_t, BufferHead*>  data;

 public:
  ObjectCache(object_t o, class BufferCache *b) : oid(o), bc(b) {}
  
  int map_read(block_t start, block_t len, 
			   map<block_t, BufferHead*>& hits,     // hits
			   map<block_t, BufferHead*>& missing,  // read these from disk
			   map<block_t, BufferHead*>& rx);      // wait for these to finish reading from disk
  
  int map_write(block_t start, block_t len,
				map<block_t, BufferHead*>& hits);   // write to these.
};


class BufferCache {
 private:
  AlignedBufferPool& bufferpool;

  LRU   lru_dirty, lru_rest;
  off_t stat_clean;
  off_t stat_dirty;
  off_t stat_rx;
  off_t stat_tx;
  
 public:
  BufferCache(AlignedBufferPool& bp) : bufferpool(bp),
	stat_clean(0), stat_dirty(0),
	stat_rx(0), stat_tx(0) {}

  // bh's in cache
  void add_bh(BufferHead *bh) {
	if (bh->is_dirty())
	  lru_dirty.lru_insert_mid(bh);
	else
	  lru_rest.lru_insert_mid(bh);
	stat_add(bh);
  }
  void touch(BufferHead *bh) {
	if (bh->is_dirty())
	  lru_dirty.lru_touch(bh);
	else
	  lru_rest.lru_touch(bh);
  }
  void remove_bh(BufferHead *bh) {
	stat_sub(bh);
	if (bh->is_dirty())
	  lru_dirty.lru_remove(bh);
	else
	  lru_rest.lru_remove(bh);
  }

  // stats
  void stat_add(BufferHead *bh) {
	switch (bh->get_state()) {
	case BufferHead::STATE_CLEAN: stat_clean += bh->length(); break;
	case BufferHead::STATE_DIRTY: stat_dirty += bh->length(); break;
	case BufferHead::STATE_RX: stat_rx += bh->length(); break;
	case BufferHead::STATE_TX: stat_tx += bh->length(); break;
	}
  }
  void stat_sub(BufferHead *bh) {
	switch (bh->get_state()) {
	case BufferHead::STATE_CLEAN: stat_clean -= bh->length(); break;
	case BufferHead::STATE_DIRTY: stat_dirty -= bh->length(); break;
	case BufferHead::STATE_RX: stat_rx -= bh->length(); break;
	case BufferHead::STATE_TX: stat_tx -= bh->length(); break;
	}
  }

  // bh state
  void set_state(BufferHead *bh, int s) {
	// move between lru lists?
	if (s == BufferHead::STATE_DIRTY && bh->get_state() != BufferHead::STATE_DIRTY) {
	  lru_rest.lru_remove(bh);
	  lru_dirty.lru_insert_top(bh);
	}
	if (s != BufferHead::STATE_DIRTY && bh->get_state() == BufferHead::STATE_DIRTY) {
	  lru_dirty.lru_remove(bh);
	  lru_rest.lru_insert_mid(bh);
	}

	// set state
	stat_sub(bh);
	bh->set_state(s);
	stat_add(bh);
  }	  

  void copy_state(BufferHead *bh1, BufferHead *bh2) { 
	set_state(bh2, bh1->get_state());
  }
  
  void mark_clean(BufferHead *bh) { set_state(bh, BufferHead::STATE_CLEAN); };
  void mark_dirty(BufferHead *bh) { set_state(bh, BufferHead::STATE_DIRTY); };
  void mark_rx(BufferHead *bh) { set_state(bh, BufferHead::STATE_RX); };
  void mark_tx(BufferHead *bh) { set_state(bh, BufferHead::STATE_TX); };


  BufferHead *split(BufferHead *orig, block_t after);

};



class FlushWaiter {
  object_t oid;
  map<off_t, version_t>  version_map;  // versions i need flushed
  Context *onflush;
};


#endif
