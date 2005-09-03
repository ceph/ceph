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
  
  long long offset;
  unsigned long long miss_len;  // only valid during misses 
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
  Bufferhead(class Inode *inode, long long off, Buffercache *bc);
  ~Bufferhead(); 
  
  //Bufferhead(inodeno_t ino, long long off, unsigned long long len, int state);
  // ~Bufferhead(); FIXME: need to mesh with allocator scheme
  
  void set_offset(long long offset);

  unsigned long long length() {
    if (is_hole() || state == BUFHD_STATE_RX) return miss_len;
    return bl.length();
  }

  void alloc_buffers(unsigned long long size);

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
  
  void miss_start(unsigned long long miss_len);
  void miss_finish();
  void dirty();
  void dirtybuffers_erase();
  void flush_start();
  void flush_finish();
  void claim_append(Bufferhead* other);
  void splice(long long offset, unsigned long long length, Bufferhead *claim_by);

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
  void get_expired(time_t ttl, unsigned long long left_dirty, set<Bufferhead*>& to_flush);
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

 public: 
  class Inode *inode;
  map<long long, Bufferhead*> buffer_map;
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
    map<long long, Bufferhead*> to_delete = buffer_map;
    buffer_map.clear();
    for (map<long long, Bufferhead*>::iterator it = to_delete.begin();
         it != to_delete.end();
         it++) {
      delete it->second; 
    }
  }

#if 0
  unsigned long long length() {
    unsigned long long len = 0;
    for (map<long long, Bufferhead*>::iterator it = buffer_map.begin();
         it != buffer_map.end();
         it++) {
      len += it->second->bl.length();
    }
    return len;
  }
#endif

  void insert(long long offset, Bufferhead* bh);

  void splice(long long offset, unsigned long long size);

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

  map<long long, Bufferhead*>::iterator get_buf(long long off);
  map<long long, Bufferhead*>::iterator overlap(unsigned long long len, long long off);
  int copy_out(unsigned long long size, long long offset, char *dst);    
  map<long long, Bufferhead*>::iterator map_existing(unsigned long long len, long long start_off, 
                    map<long long, Bufferhead*>& hits, 
		    map<long long, Bufferhead*>& rx,
		    map<long long, Bufferhead*>& tx,
                    map<long long, unsigned long long>& holes);
  unsigned long long consolidation_opp(time_t ttl, unsigned long long clean_goal, 
                           long long offset, list<long long>& offlist);
  void get_dirty(set<Bufferhead*>& to_flush);
};

class Buffercache { 
 private:
  unsigned long long dirty_size, rx_size, tx_size, clean_size;
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

  void clean_to_dirty(unsigned long long size) {
    clean_size -= size;
    assert(clean_size >= 0);
    dirty_size += size;
  }
  void dirty_to_tx(unsigned long long size) {
    dirty_size -= size;
    assert(dirty_size >= 0);
    tx_size += size;
  }
  void tx_to_dirty(unsigned long long size) {
    tx_size -= size;
    assert(tx_size >= 0);
    dirty_size += size;
  }
  void tx_to_clean(unsigned long long size) {
    tx_size -= size;
    assert(tx_size >= 0);
    clean_size += size;
  }
  void increase_size(unsigned long long size) {
    clean_size += size;
  }
  void decrease_size(unsigned long long size) {
    clean_size -= size;
    assert(clean_size >= 0);
  }
  unsigned long long get_clean_size() { return clean_size; }
  unsigned long long get_dirty_size() { return dirty_size; }
  unsigned long long get_rx_size() { return rx_size; }
  unsigned long long get_tx_size() { return tx_size; }
  unsigned long long get_total_size() { return clean_size + dirty_size + rx_size + tx_size; }
  void get_reclaimable(unsigned long long min_size, list<Bufferhead*>&);

  void insert(Bufferhead *bh);
  void dirty(Inode *inode, unsigned long long size, long long offset, const char *src);
  unsigned long long touch_continuous(map<long long, Bufferhead*>& hits, unsigned long long size, long long offset);
  void map_or_alloc(class Inode *inode, unsigned long long len, long long off, 
                    map<long long, Bufferhead*>& buffers, 
		    map<long long, Bufferhead*>& rx,
		    map<long long, Bufferhead*>& tx);
  void consolidate(map<Inode*, map<long long, list<long long> > > cons_map);
  void release_file(inodeno_t ino);       
  unsigned long long reclaim(unsigned long long min_size);
};

     
#endif

