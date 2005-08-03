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
  size_t miss_len;  // only valid during misses 
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
  
  // cons/destructors
  Bufferhead(class Inode *inode, Buffercache *bc);
  Bufferhead(class Inode *inode, off_t off, Buffercache *bc);
  ~Bufferhead(); 
  
  //Bufferhead(inodeno_t ino, off_t off, size_t len, int state);
  // ~Bufferhead(); FIXME: need to mesh with allocator scheme
  
  void set_offset(off_t offset);

  size_t length() {
    if (state == BUFHD_STATE_RX) return miss_len;
    return bl.length();
  }

  void alloc_buffers(size_t size);

  /** wait_for_(read|write) 
   * put Cond on local stack, block until woken up.
   * _caller_ pins to avoid any race weirdness
   */
  void wait_for_read(Mutex *lock) {
	assert(state == BUFHD_STATE_RX || state == BUFHD_STATE_TX);
	Cond cond;
	get();
	read_waiters.push_back(&cond);
	cond.Wait(*lock);
	put();
  }
  void wait_for_write(Mutex *lock) {
	assert(state == BUFHD_STATE_RX || state == BUFHD_STATE_TX);
	Cond cond;
	get();
	write_waiters.push_back(&cond);
	cond.Wait(*lock);
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
  
  void miss_start(size_t miss_len);
  void miss_finish();
  void dirty();
  void dirtybuffers_erase();
  void flush_start();
  void flush_finish();
  void claim_append(Bufferhead* other);
};
  

class Dirtybuffers {
 private:
  multimap<time_t, Bufferhead*> _dbufs;
  Buffercache *bc;
  // DEBUG
  time_t former_age;

 public:
  Dirtybuffers(Buffercache *bc) { 
    former_age = 0;
    dout(5) << "Dirtybuffers() former_age: " << former_age << endl; 
    this->bc = bc;
  }
  Dirtybuffers(const Dirtybuffers& other);
  Dirtybuffers& operator=(const Dirtybuffers& other);
  void erase(Bufferhead* bh);
  void insert(Bufferhead* bh);
  bool empty() { return _dbufs.empty(); }
  bool exist(Bufferhead* bh);
  void get_expired(time_t ttl, size_t left_dirty, set<Bufferhead*>& to_flush);
  time_t get_age() { 
    time_t age;
    if (_dbufs.empty()) {
      age = 0;
    } else {
      age = time(NULL) - _dbufs.begin()->second->dirty_since;
    }
    dout(10) << "former age: " << former_age << " age: " << age << endl;
    //assert((!(former_age > 30)) || (age > 0));
    former_age = age;
    return age;
  }
};


class Filecache {
 private:
  list<Cond*> inflight_waiters;

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
    for (map<off_t, Bufferhead*>::iterator it = buffer_map.begin();
         it != buffer_map.end();
         it++) {
      delete it->second; 
    }
  }

#if 0
  size_t length() {
    size_t len = 0;
    for (map<off_t, Bufferhead*>::iterator it = buffer_map.begin();
         it != buffer_map.end();
         it++) {
      len += it->second->bl.length();
    }
    return len;
  }
#endif

  void insert(off_t offset, Bufferhead* bh);

  void splice(off_t offset, size_t size);

  void wait_for_inflight(Mutex *lock) {
	Cond cond;
	inflight_waiters.push_back(&cond);
	cond.Wait(*lock);
  }

  void wakeup_inflight_waiters() {
    for (list<Cond*>::iterator it = inflight_waiters.begin();
		 it != inflight_waiters.end();
		 it++) {
	  (*it)->Signal();
	}
    inflight_waiters.clear(); 
  }

  map<off_t, Bufferhead*>::iterator get_buf(off_t off);
  map<off_t, Bufferhead*>::iterator overlap(size_t len, off_t off);
  int copy_out(size_t size, off_t offset, char *dst);    
  map<off_t, Bufferhead*>::iterator map_existing(size_t len, off_t start_off, 
                    map<off_t, Bufferhead*>& hits, 
		    map<off_t, Bufferhead*>& rx,
		    map<off_t, Bufferhead*>& tx,
                    map<off_t, size_t>& holes);
  size_t consolidation_opp(time_t ttl, size_t clean_goal, 
                           off_t offset, list<off_t>& offlist);
  void simplify();
};

class Buffercache { 
 private:
  size_t dirty_size, rx_size, tx_size, clean_size;
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
      
  void wait_for_inflight(Mutex *lock) {
	Cond cond;
	inflight_waiters.push_back(&cond);
	cond.Wait(*lock);
  }

  void wakeup_inflight_waiters() {
    for (list<Cond*>::iterator it = inflight_waiters.begin();
		 it != inflight_waiters.end();
		 it++) {
	  (*it)->Signal();
	}
    inflight_waiters.clear(); 
  }

  void clean_to_dirty(size_t size) {
    clean_size -= size;
    assert(clean_size >= 0);
    dirty_size += size;
  }
  void dirty_to_tx(size_t size) {
    dirty_size -= size;
    assert(dirty_size >= 0);
    tx_size += size;
  }
  void tx_to_dirty(size_t size) {
    tx_size -= size;
    assert(tx_size >= 0);
    dirty_size += size;
  }
  void tx_to_clean(size_t size) {
    tx_size -= size;
    assert(tx_size >= 0);
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
  size_t get_rx_size() { return rx_size; }
  size_t get_tx_size() { return tx_size; }
  size_t get_total_size() { return clean_size + dirty_size + rx_size + tx_size; }
  void get_reclaimable(size_t min_size, list<Bufferhead*>&);

  void insert(Bufferhead *bh);
  void dirty(Inode *inode, size_t size, off_t offset, const char *src);
  size_t touch_continuous(map<off_t, Bufferhead*>& hits, size_t size, off_t offset);
  void map_or_alloc(class Inode *inode, size_t len, off_t off, 
                    map<off_t, Bufferhead*>& buffers, 
		    map<off_t, Bufferhead*>& rx,
		    map<off_t, Bufferhead*>& tx);
  void consolidate(map<Inode*, map<off_t, list<off_t> > > cons_map);
  void release_file(inodeno_t ino);       
  size_t reclaim(size_t min_size);
};

     
#endif

