#ifndef __BUFFERCACHE_H
#define __BUFFERCACHE_H

#include <time.h>

#include "include/types.h"
#include "include/buffer.h"
#include "include/bufferlist.h"
#include "include/lru.h"
#include "include/config.h"
#include "common/Cond.h"

// stl
#include <set>
#include <list>
#include <map>
using namespace std;

// Bufferhead states
#define BUFHD_STATE_CLEAN	  1
#define BUFHD_STATE_DIRTY	  2
#define BUFHD_STATE_INFLIGHT  3

class Buffercache;
class Filecache;

class Bufferhead : public LRUObject {
  int ref;

 public: // FIXME: make more private and write some accessors
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
  
  off_t offset;
  inodeno_t ino;
  time_t last_written;
  int state; 
  bufferlist bl;
  // read_waiters: threads waiting for reads from the buffer
  // write_waiters: threads waiting for writes into the buffer
  list<Cond*> read_waiters, write_waiters;
  Buffercache *bc;
  Filecache *fc;
  
  // cons/destructors
  Bufferhead(inodeno_t ino, off_t off, Buffercache *bc);
  ~Bufferhead(); 
  
  //Bufferhead(inodeno_t ino, off_t off, size_t len, int state);
  // ~Bufferhead(); FIXME: need to mesh with allocator scheme
  
  void alloc_buffers(size_t size);

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
  
  void dirty();
  void clean();
  void flush_start();
  void flush_finish();
  void claim_append(Bufferhead* other);
};
  
class Filecache {
 public: 
  map<off_t, Bufferhead*> buffer_map;
  set<Bufferhead*> dirty_buffers;
  list<Cond*> waitfor_flushed;
  Buffercache *bc;

  Filecache(Buffercache *bc) { 
    this->bc = bc;
    buffer_map.clear();
  }

  ~Filecache() {
    for (map<off_t, Bufferhead*>::iterator it = buffer_map.begin();
         it != buffer_map.end();
         it++) {
      delete it->second; 
    }
  }

  size_t length() {
    size_t len = 0;
    for (map<off_t, Bufferhead*>::iterator it = buffer_map.begin();
         it != buffer_map.end();
         it++) {
      len += it->second->bl.length();
    }
    return len;
  }

  void wait_for_flush(Mutex &lock) {
	Cond cond;
	waitfor_flushed.push_back(&cond);
	cond.Wait(lock);
  }

  map<off_t, Bufferhead*>::iterator overlap(size_t len, off_t off);
  int copy_out(size_t size, off_t offset, char *dst);    
  map<off_t, Bufferhead*>::iterator map_existing(size_t len, off_t start_off, 
                    map<off_t, Bufferhead*>& hits, 
		    map<off_t, Bufferhead*>& inflight,
                    map<off_t, size_t>& holes);
  void simplify();
};

class Buffercache { 
 public:
  map<inodeno_t, Filecache*> bcache_map;
  LRU lru;
  size_t dirty_size, flushing_size, clean_size;
  set<Bufferhead*> dirty_buffers;
  list<Cond*> waitfor_flushed;

  Buffercache() : dirty_size(0), flushing_size(0), clean_size(0) { }
  
  // FIXME: constructor & destructor need to mesh with allocator scheme
  ~Buffercache() {
    for (map<inodeno_t, Filecache*>::iterator it = bcache_map.begin();
         it != bcache_map.end();
         it++) {
      // FIXME: make sure all buffers are cleaned  and then free them
      delete it->second; 
    }
  }
  
  Filecache *get_fc(inodeno_t ino) {
    if (!bcache_map.count(ino)) {
      bcache_map[ino] = new Filecache(this);
    } 
    return bcache_map[ino];
  }
      
  void insert(Bufferhead *bh);

  void dirty(inodeno_t ino, size_t size, off_t offset, const char *src);
  int touch_continuous(map<off_t, Bufferhead*>& hits, size_t size, off_t offset);
  void map_or_alloc(inodeno_t ino, size_t len, off_t off, 
                    map<off_t, Bufferhead*>& buffers, 
		    map<off_t, Bufferhead*>& inflight);
  void release_file(inodeno_t ino);       
  size_t reclaim(size_t min_size);
};

     
#endif

