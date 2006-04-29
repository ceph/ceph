#ifndef __OBJECTCACHER_H_
#define __OBJECTCACHER_H_

#include "include/types.h"
#include "include/lru.h"

#include "common/Cond.h"

#include "Objecter.h"

class Objecter;
class Objecter::OSDRead;
class Objecter::OSDWrite;

class ObjectCacher {
 private:
  Objecter *objecter;
  
  // ******* Object *********
  class Object {
  public:
	
	// ******* Object::BufferHead *********
	class BufferHead : public LRUObject {
	public:
	  // states
	  static const int STATE_MISSING = 0;
	  static const int STATE_CLEAN = 1;
	  static const int STATE_DIRTY = 2;
	  static const int STATE_RX = 3;
	  static const int STATE_TX = 4;
	  
	  // my fields
	  int state;
	  int ref;

	  version_t version;      // version of object (if non-zero)
	  bufferlist  bl;

	  map<size_t, list<Context*> > waitfor_read;

	  size_t length() { return bl.length(); }

	  // states
	  void set_state(int s) {
		if (s == STATE_RX || s == STATE_TX) get();
		if (state == STATE_RX || state == STATE_TX) put();
		state = s;
	  }
	  int get_state() { return state; }
	  
	  bool is_missing() { return state == STATE_MISSING; }
	  bool is_dirty() { return state == STATE_DIRTY; }
	  bool is_clean() { return state == STATE_CLEAN; }
	  bool is_tx() { return state == STATE_TX; }
	  bool is_rx() { return state == STATE_RX; }

	  // reference counting
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

	  BufferHead() : 
		state(STATE_MISSING),
		ref(0),
		version(0) {}
	};

	// ObjectCacher::Object fields
	object_t  oid;
	inodeno_t ino;

	map<size_t, BufferHead*>     bh_map;

	Object(object_t o, inodeno_t i) : oid(o), ino(i) {}
  };

  // ObjectCacher fields
  hash_map<object_t, Object*> objects;

  set<Object::BufferHead*> dirty_bh;
  LRU   lru_dirty, lru_rest;


  // bh stats
  Cond  stat_cond;
  int   stat_waiter;

  off_t stat_clean;
  off_t stat_dirty;
  off_t stat_rx;
  off_t stat_tx;
  off_t stat_partial;
  off_t stat_missing;

  void bh_stat_add(Object::BufferHead *bh) {
	switch (bh->get_state()) {
	case Object::BufferHead::STATE_MISSING: stat_missing += bh->length(); break;
	case Object::BufferHead::STATE_CLEAN: stat_clean += bh->length(); break;
	case Object::BufferHead::STATE_DIRTY: stat_dirty += bh->length(); break;
	case Object::BufferHead::STATE_TX: stat_tx += bh->length(); break;
	case Object::BufferHead::STATE_RX: stat_rx += bh->length(); break;
	}
	if (stat_waiter) stat_cond.Signal();
  }
  void bh_stat_sub(Object::BufferHead *bh) {
	switch (bh->get_state()) {
	case Object::BufferHead::STATE_MISSING: stat_missing -= bh->length(); break;
	case Object::BufferHead::STATE_CLEAN: stat_clean -= bh->length(); break;
	case Object::BufferHead::STATE_DIRTY: stat_dirty -= bh->length(); break;
	case Object::BufferHead::STATE_TX: stat_tx -= bh->length(); break;
	case Object::BufferHead::STATE_RX: stat_rx -= bh->length(); break;
	}
  }
  off_t get_stat_tx() { return stat_tx; }
  off_t get_stat_rx() { return stat_rx; }
  off_t get_stat_dirty() { return stat_dirty; }
  off_t get_stat_clean() { return stat_clean; }
  off_t get_stat_partial() { return stat_partial; }

  // bh states
  void bh_set_state(Object::BufferHead *bh, int s) {
	// move between lru lists?
	if (s == Object::BufferHead::STATE_DIRTY && bh->get_state() != Object::BufferHead::STATE_DIRTY) {
	  lru_rest.lru_remove(bh);
	  lru_dirty.lru_insert_top(bh);
	  dirty_bh.insert(bh);
	}
	if (s != Object::BufferHead::STATE_DIRTY && bh->get_state() == Object::BufferHead::STATE_DIRTY) {
	  lru_dirty.lru_remove(bh);
	  lru_rest.lru_insert_mid(bh);
	  dirty_bh.erase(bh);
	}

	// set state
	bh_stat_sub(bh);
	bh->set_state(s);
	bh_stat_add(bh);
  }	  

  void copy_bh_state(Object::BufferHead *bh1, Object::BufferHead *bh2) { 
	bh_set_state(bh2, bh1->get_state());
  }
  
  void mark_missing(Object::BufferHead *bh) { bh_set_state(bh, Object::BufferHead::STATE_MISSING); };
  void mark_clean(Object::BufferHead *bh) { bh_set_state(bh, Object::BufferHead::STATE_CLEAN); };
  void mark_rx(Object::BufferHead *bh) { bh_set_state(bh, Object::BufferHead::STATE_RX); };
  void mark_tx(Object::BufferHead *bh) { bh_set_state(bh, Object::BufferHead::STATE_TX); };
  void mark_dirty(Object::BufferHead *bh) { 
	bh_set_state(bh, Object::BufferHead::STATE_DIRTY); 
	//bh->set_dirty_stamp(g_clock.now());
  };




  int map_read(Objecter::OSDRead *rd);
  int map_write(Objecter::OSDWrite *wr);

 public:
  // blocking.  async.
  int readx(Objecter::OSDRead *rd, inodeno_t ino, Context *onfinish);
  int writex(Objecter::OSDWrite *wr, inodeno_t ino, Context *onack, Context *oncommit);
  
  // blocking.  atomic+sync.
  int atomic_sync_readx(Objecter::OSDRead *rd, inodeno_t ino, Context *onfinish);
  int atomic_sync_writex(Objecter::OSDWrite *wr, inodeno_t ino, Context *onack, Context *oncommit);

  void flush_set(inodeno_t ino, Context *onfinish=0);
  void flush_all(Context *onfinish=0);

  void commit_set(inodeno_t ino, Context *oncommit=0);
  void commit_all(Context *oncommit=0);
};


#endif
