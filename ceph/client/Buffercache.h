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
#include <list>
#include <map>
using namespace std;

// Bufferhead states
#define BUFHD_STATE_CLEAN	  1
#define BUFHD_STATE_DIRTY	  2
#define BUFHD_STATE_INFLIGHT  3

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
	ref++;
  }
  int put() {
	assert(ref > 0);
	ref--;
	if (ref == 0) lru_unpin();
  }
  
  off_t offset;
  size_t miss_len;  // only valid during misses 
  inodeno_t ino;
  time_t dirty_since;
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
  
  void miss_start(size_t miss_len) {
	assert(state == BUFHD_STATE_CLEAN);
	state = BUFHD_STATE_INFLIGHT;
	this->miss_len = miss_len;
  }
  
  void miss_finish() {
	assert(state == BUFHD_STATE_INFLIGHT);
	state = BUFHD_STATE_CLEAN;
	//assert(bl.length() == miss_len);
	wakeup_read_waiters();
	wakeup_write_waiters();
  }
  
  void dirty();
  void leave_dirtybuffers();
  void flush_start();
  void flush_finish();
  void claim_append(Bufferhead* other);
};
  

class Dirtybuffers {
 private:
  multimap<time_t, Bufferhead*> _dbufs;

 public:
  void erase(Bufferhead* bh) {
    dout(7) << "dirtybuffer: erase bh->ino: " << bh->ino << " offset: " << bh->offset << endl;
    int osize = _dbufs.size();
    for (multimap<time_t, Bufferhead*>::iterator it = _dbufs.lower_bound(bh->dirty_since);
         it != _dbufs.upper_bound(bh->dirty_since);
	 it++) {
     if (it->second == bh) {
        _dbufs.erase(it);
        break;
      }
    }
    assert(_dbufs.size() == osize - 1);
  }

  void insert(Bufferhead* bh) {
    dout(7) << "dirtybuffer: insert bh->ino: " << bh->ino << " offset: " << bh->offset << endl;
    _dbufs.insert(pair<time_t, Bufferhead*>(bh->dirty_since, bh));
  }

  bool empty() { return _dbufs.empty(); }

  bool exist(Bufferhead* bh) {
    for (multimap<time_t, Bufferhead*>::iterator it = _dbufs.lower_bound(bh->dirty_since);
        it != _dbufs.upper_bound(bh->dirty_since);
	it++ ) {
      if (it->second == bh) {
	dout(10) << "dirtybuffer: found bh->ino: " << bh->ino << " offset: " << bh->offset << endl;
        return true;
      }
    }
    return false;
  }
  void get_expired(time_t ttl, size_t left_dirty, list<Bufferhead*>& to_flush);
};


class Filecache {
 public: 
  map<off_t, Bufferhead*> buffer_map;
  Dirtybuffers dirty_buffers;
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
 private:
  Mutex buffercache_lock;
  size_t dirty_size, flushing_size, clean_size;

 public:
  map<inodeno_t, Filecache*> bcache_map;
  LRU lru;
  Dirtybuffers dirty_buffers;
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
      
  void clean_to_dirty(size_t size) {
    clean_size -= size;
    assert(clean_size >= 0);
    dirty_size += size;
  }
  void dirty_to_flushing(size_t size) {
    dirty_size -= size;
    assert(dirty_size >= 0);
    flushing_size += size;
  }
  void flushing_to_dirty(size_t size) {
    flushing_size -= size;
    assert(flushing_size >= 0);
    dirty_size += size;
  }
  void flushing_to_clean(size_t size) {
    flushing_size -= size;
    assert(flushing_size >= 0);
    clean_size += size;
  }
  void increase_size(size_t size) {
    clean_size += size;
  }
  void decrease_size(size_t size) {
    clean_size -= size;
    assert(clean_size >= 0);
  }
  size_t get_clean_size() { return clean_size; }
  size_t get_dirty_size() { return dirty_size; }
  size_t get_flushing_size() { return flushing_size; }

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

