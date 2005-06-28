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
  // reference counter
  int ref;  
  void get() {
	if (ref == 0) lru_pin();
	++ref;
  }
  void put() {
	--ref;
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
  Bufferhead(inodeno_t ino, off_t off, size_t len, Buffercache *bc, int state=BUFHD_STATE_CLEAN) {
	this->ref = 0;
    this->ino = ino;
    this->offset = off;
	this->len = len;
	this->state = state;
    this->bc = bc;
    last_written = time();
    // buffers are allocated later
  }
  
  ~Bufferhead() {
	// no need to delete bufferlist bufferptr's explicitly; ~list() does that (since it's list<bufferptr>, not list<bufferptr*>)
  }
  
  //Bufferhead(inodeno_t ino, off_t off, size_t len, int state);
  
  // ~Bufferhead(); FIXME: need to mesh with allocator scheme


  // -- wait for read, write: these will block
  // i think this will work okay?  and reference coutning in the waiter makes sure the wakeup fn doesn't 
  // inadvertantly unpin the bufferhead before the waiters get to go
  void wait_for_read(Mutex& lock) {
	Cond cond;  // on local stack
	get();
	read_waiters.push_back(&cond);
	cond.Wait(lock);
	put();
  }
  void wait_for_write(Mutex& lock) {
	Cond cond;  // on local stack
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
  
  void dirty() {
    if (state == BUFHD_STATE_CLEAN) {
      state = BUFHD_STATE_DIRTY;
      bc->dirty_size += bh->len;
      bc->clean_size -= bh->len;
      bc->dirty_map[last_written] = this;
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
    bc->flush_size -= len;
    bc->clean_size += len;
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
  void simplify();

};

class Buffercache { 
 public:
  map<inodeno_t, Filecache*> bcache_map;
  LRU lru;
  size_t dirty_size = 0, flushing_size = 0, clean_size = 0;
  map<time_t, Bufferhead*> dirty_map;

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

