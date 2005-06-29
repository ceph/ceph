#ifndef __Buffercache_H
#define __Buffercache_H

#include "include/buffer.h"
#include "include/bufferlist.h"
#include "include/lru.h"

// FIXME: buffer constants
#define BUFC_ALLOC_MAXSIZE 262144

// Bufferhead states
#define BUFHD_STATE_CLEAN	  1
#define BUFHD_STATE_DIRTY	  2
#define BUFHD_STATE_INFLIGHT  3

class Buffercache;

class Bufferhead : public LRUObject {
  int ref;

  int get() {
	assert(ref >= 0);
	if (ref == 0) lru_pin();
	ref++;
  }
  int put() {
	assert(ref > 0);
	ref--;
	if (ref == 0) lru_unpin();
  }

  
 public: // FIXME: make more private and write some accessors
  off_t offset;
  size_t len;
  inodeno_t ino;
  time_t last_written;
  int state; 
  bufferlist bl;
  // read_waiters: threads waiting for reads from the buffer
  // write_waiters: threads waiting for writes into the buffer
  list<Cond*> read_waiters, write_waiters;
  Buffercache *bc;
  
  // cons/destructors
  Bufferhead(inodeno_t ino, off_t off, size_t len, Buffercache *bc, int state=BUFHD_STATE_CLEAN) 
	: ref(0) {
	this->ino = ino;
    this->offset = off;
	this->len = len;
	this->state = state;
    this->bc = bc;
    last_written = time();
    // buffers are allocated later
  }
  
  ~Bufferhead() {
  }
  
  //Bufferhead(inodeno_t ino, off_t off, size_t len, int state);
  // ~Bufferhead(); FIXME: need to mesh with allocator scheme
  

  /** wait_for_(read|write) 
   * put Cond on local stack, block until woken up.
   * _caller_ pins to avoid any race weirdness
   */
  void wait_for_read(Mutex &lock) {
	Cond cond;
	get();
	read_waiters.push_back(&cond);
	cond.Wait(lock);
	put();
  }
  void wait_for_write(Mutex &lock) {
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
  
  void miss_start() {
	assert(state == BUFHD_STATE_CLEAN);
	state = BUFHD_STATE_INFLIGHT;
  }
  
  void miss_finish() {
	assert(state == BUFHD_STATE_INFLIGHT);
	state = BUFHD_STATE_CLEAN;
	wakeup_read_waiters();
	wakeup_write_waiters();
  }
  
  void mark_dirty() {
    if (state == BUFHD_STATE_CLEAN) {
      state = BUFHD_STATE_DIRTY;
      bc->dirty_size += bh->len;
      bc->clean_size -= bh->len;
      if (bc->dirty_buffers.count(offset)) {
        bc->dirty_buffers.insert(this); 
        get();
      }
      if (bc->bcache_map[ino]->dirty_buffers.count(offset)) {
        bc->bcache_map[ino]->dirty_buffers.insert(this); 
        get();
      }
    }    
  }
  
  void flush_start() {
	assert(state == BUFHD_STATE_DIRTY);
	state = BUFHD_STATE_INFLIGHT;
    bc->dirty_size -= len;
    bc->flush_size += len;
  }
  
  void flush_finish() {
	assert(state == BUFHD_STATE_INFLIGHT);
	state = BUFHD_STATE_CLEAN;
    last_written = time();
    bc->flush_size -= len;
    bc->clean_size += len;
    bc->dirty_buffers.erase(this); put();
    bc->bcache_map[ino]->dirty_buffers.erase(this); put();
	wakeup_write_waiters(); // readers never wait on flushes
  }
  
  void claim_append(Bufferhead *other) {
	bl.claim_append(other->bl);
	len += other->len;
    if (other->last_written < last_written) last_written = other->last_written;
	other->bl.clear();
	other->len = 0;
  }
};


class Filecache {
 public: 
  map<off_t, Bufferhead*> buffer_map;
  set<Bufferhead*> dirty_buffers;
  list<Cond*> waitfor_flushed;

  size_t length() {
    size_t len = 0;
    for (map<off_t, Bufferhead*>::iterator it = buffer_map.begin();
         it != buffer_map.end();
         it++) {
      len += (*it)->second->len;
    }
    return len;
  }

  map<off_t, Bufferhead*>::iterator overlap(size_t len, off_t off);
  void copy_out(size_t size, off_t offset, char *dst);    
  void map_existing(size_t len, off_t start_off, 
                    map<off_t, Bufferhead*>& hits, inflight,
                    map<off_t, size_t>& holes);
  void simplify(list<Bufferhead*>& removed);

};

class Buffercache { 
 public:
  map<inodeno_t, Filecache*> bcache_map;
  LRU lru;
  size_t dirty_size = 0, flushing_size = 0, clean_size = 0;
  set<Bufferhead*> dirty_buffers;
  list<Cond*> waitfor_flushed;
  
  // FIXME: constructor & destructor need to mesh with allocator scheme
  ~Buffercache() {
    // FIXME: make sure all buffers are cleaned  and then free them
    for (map<inodeno_t, Filecache*>::iterator it = bcache_map.begin();
         it != bcache_map.end();
         it++) {
      delete (*it)->second; 
    }
  }
  
  void insert(Bufferhead *bh);
  void dirty(inodeno_t ino, size_t size, off_t offset, char *src);
  void simplify(inodeno_t ino);
  Bufferhead *alloc_buffers(inodeno_t ino, size_t size, off_t offset, int state);
  void map_or_alloc(inodeno_t ino, size_t len, off_t off, 
                    map<off_t, Bufferhead*>& buffers, inflight);
  void free_buffers(Bufferhead *bh);
  void release_file(inodeno_t ino);       
  size_t reclaim(size_t min_size);
};

     
#endif

