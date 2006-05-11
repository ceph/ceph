#ifndef __OBJECTCACHER_H_
#define __OBJECTCACHER_H_

#include "include/types.h"
#include "include/lru.h"
#include "include/Context.h"

#include "common/Cond.h"

#include "Objecter.h"

class Objecter;
class Objecter::OSDRead;
class Objecter::OSDWrite;

class ObjectCacher {
 public:
  // ******* BufferHead *********
  class BufferHead : public LRUObject {
  public:
	// states
	static const int STATE_MISSING = 0;
	static const int STATE_CLEAN = 1;
	static const int STATE_DIRTY = 2;
	static const int STATE_RX = 3;
	static const int STATE_TX = 4;
	
  private:
	// my fields
	int state;
	int ref;
	struct {
	  off_t start, length;   // bh extent in object
	} ex;
		
  public:
	bufferlist  bl;
	tid_t last_write_tid;  // version of bh (if non-zero)
	
	map< off_t, list<Context*> > waitfor_read;
	
  public:
	// cons
	BufferHead() : 
	  state(STATE_MISSING),
	  ref(0),
	  last_write_tid(0) {}
  
	// extent
	off_t start() { return ex.start; }
	void set_start(off_t s) { ex.start = s; }
	off_t length() { return ex.length; }
	void set_length(off_t l) { ex.length = l; }
	off_t end() { return ex.start + ex.length; }
	off_t last() { return end() - 1; }

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
  };
  

  // ******* Object *********
  class Object {
  private:
	// ObjectCacher::Object fields
	ObjectCacher *oc;
	object_t  oid;
	inodeno_t ino;
	
  public:
	map<off_t, BufferHead*>     data;

	tid_t last_ack_tid;    // last update acked.
	tid_t last_commit_tid; // last update commited.

	map< tid_t, list<Context*> > waitfor_ack;
	map< tid_t, list<Context*> > waitfor_commit;

	// lock
	static const int LOCK_NONE = 0;
	static const int LOCK_WRLOCK = 1;
	static const int LOCK_RDLOCK = 2;
	int lock_state;

  public:
	Object(ObjectCacher *_oc, object_t o, inodeno_t i) : 
	  oc(_oc),
	  oid(o), ino(i), 
	  last_ack_tid(0), last_commit_tid(0),
	  lock_state(LOCK_NONE) 
	  {}

	object_t get_oid() { return oid; }
	inodeno_t get_ino() { return ino; }


	// bh
	void add_bh(BufferHead *bh) {
	  // add to my map
	  assert(data.count(bh->start()) == 0);
	  
	  if (0) {  // sanity check     FIXME DEBUG
		//cout << "add_bh " << bh->start() << "~" << bh->length() << endl;
		map<off_t,BufferHead*>::iterator p = data.lower_bound(bh->start());
		if (p != data.end()) {
		  //cout << " after " << *p->second << endl;
		  //cout << " after starts at " << p->first << endl;
		  assert(p->first >= bh->end());
		}
		if (p != data.begin()) {
		  p--;
		  //cout << " before starts at " << p->second->start() 
		  //<< " and ends at " << p->second->end() << endl;
		  //cout << " before " << *p->second << endl;
		  assert(p->second->end() <= bh->start());
		}
	  }

	  data[bh->start()] = bh;
	}
	void remove_bh(BufferHead *bh) {
	  assert(data.count(bh->start()));
	  data.erase(bh->start());
	}
	bool is_empty() { return data.empty(); }

	// mid-level
	BufferHead *split(BufferHead *bh, off_t off);
	void merge(BufferHead *left, BufferHead *right);

	int map_read(Objecter::OSDRead *rd,
				 map<off_t, BufferHead*>& hits,
				 map<off_t, BufferHead*>& missing,
				 map<off_t, BufferHead*>& rx);
	BufferHead *map_write(Objecter::OSDWrite *wr);
	
  };
  
  // ******* ObjectCacher *********
  // ObjectCacher fields
 private:
  Objecter *objecter;
  Mutex& lock;
  
  hash_map<object_t, Object*> objects;
  hash_map<inodeno_t, set<Object*> > objects_by_ino;

  set<BufferHead*>    dirty_bh;
  LRU   lru_dirty, lru_rest;

  // bh stats
  Cond  stat_cond;
  int   stat_waiter;

  off_t stat_clean;
  off_t stat_dirty;
  off_t stat_rx;
  off_t stat_tx;
  off_t stat_missing;

  void bh_stat_add(BufferHead *bh) {
	switch (bh->get_state()) {
	case BufferHead::STATE_MISSING: stat_missing += bh->length(); break;
	case BufferHead::STATE_CLEAN: stat_clean += bh->length(); break;
	case BufferHead::STATE_DIRTY: stat_dirty += bh->length(); break;
	case BufferHead::STATE_TX: stat_tx += bh->length(); break;
	case BufferHead::STATE_RX: stat_rx += bh->length(); break;
	}
	if (stat_waiter) stat_cond.Signal();
  }
  void bh_stat_sub(BufferHead *bh) {
	switch (bh->get_state()) {
	case BufferHead::STATE_MISSING: stat_missing -= bh->length(); break;
	case BufferHead::STATE_CLEAN: stat_clean -= bh->length(); break;
	case BufferHead::STATE_DIRTY: stat_dirty -= bh->length(); break;
	case BufferHead::STATE_TX: stat_tx -= bh->length(); break;
	case BufferHead::STATE_RX: stat_rx -= bh->length(); break;
	}
  }
  off_t get_stat_tx() { return stat_tx; }
  off_t get_stat_rx() { return stat_rx; }
  off_t get_stat_dirty() { return stat_dirty; }
  off_t get_stat_clean() { return stat_clean; }

  void touch_bh(BufferHead *bh) {
	if (bh->is_dirty())
	  lru_dirty.lru_touch(bh);
	else
	  lru_rest.lru_touch(bh);
  }

  // bh states
  void bh_set_state(BufferHead *bh, int s) {
	// move between lru lists?
	if (s == BufferHead::STATE_DIRTY && bh->get_state() != BufferHead::STATE_DIRTY) {
	  lru_rest.lru_remove(bh);
	  lru_dirty.lru_insert_top(bh);
	  dirty_bh.insert(bh);
	}
	if (s != BufferHead::STATE_DIRTY && bh->get_state() == BufferHead::STATE_DIRTY) {
	  lru_dirty.lru_remove(bh);
	  lru_rest.lru_insert_mid(bh);
	  dirty_bh.erase(bh);
	}

	// set state
	bh_stat_sub(bh);
	bh->set_state(s);
	bh_stat_add(bh);
  }	  

  void copy_bh_state(BufferHead *bh1, BufferHead *bh2) { 
	bh_set_state(bh2, bh1->get_state());
  }
  
  void mark_missing(BufferHead *bh) { bh_set_state(bh, BufferHead::STATE_MISSING); };
  void mark_clean(BufferHead *bh) { bh_set_state(bh, BufferHead::STATE_CLEAN); };
  void mark_rx(BufferHead *bh) { bh_set_state(bh, BufferHead::STATE_RX); };
  void mark_tx(BufferHead *bh) { bh_set_state(bh, BufferHead::STATE_TX); };
  void mark_dirty(BufferHead *bh) { 
	bh_set_state(bh, BufferHead::STATE_DIRTY); 
	//bh->set_dirty_stamp(g_clock.now());
  };



  // io
  void bh_read(Object *ob, BufferHead *bh);
  void bh_write(Object *ob, BufferHead *bh);

 public:
  void bh_read_finish(object_t oid, off_t offset, size_t length, bufferlist &bl);
  void bh_write_ack(object_t oid, off_t offset, size_t length, tid_t t);
  void bh_write_commit(object_t oid, off_t offset, size_t length, tid_t t);

  class C_ReadFinish : public Context {
	ObjectCacher *oc;
	object_t oid;
	off_t start;
	size_t length;
  public:
	bufferlist bl;
	C_ReadFinish(ObjectCacher *c, object_t o, off_t s, size_t l) : oc(c), oid(o), start(s), length(l) {}
	void finish(int r) {
	  oc->bh_read_finish(oid, start, length, bl);
	}
  };

  class C_WriteAck : public Context {
	ObjectCacher *oc;
	object_t oid;
	off_t start;
	size_t length;
  public:
	tid_t tid;
	C_WriteAck(ObjectCacher *c, object_t o, off_t s, size_t l) : oc(c), oid(o), start(s), length(l) {}
	void finish(int r) {
	  oc->bh_write_ack(oid, start, length, tid);
	}
  };
  class C_WriteCommit : public Context {
	ObjectCacher *oc;
	object_t oid;
	off_t start;
	size_t length;
  public:
	tid_t tid;
	C_WriteCommit(ObjectCacher *c, object_t o, off_t s, size_t l) : oc(c), oid(o), start(s), length(l) {}
	void finish(int r) {
	  oc->bh_write_commit(oid, start, length, tid);
	}
  };



 public:
  ObjectCacher(Objecter *o, Mutex& l) : 
	objecter(o), lock(l),
	stat_waiter(0),
	stat_clean(0), stat_dirty(0), stat_rx(0), stat_tx(0), stat_missing(0)
	{}


  class C_RetryRead : public Context {
	ObjectCacher *oc;
	Objecter::OSDRead *rd;
	inodeno_t ino;
	Context *onfinish;
  public:
	C_RetryRead(ObjectCacher *_oc, Objecter::OSDRead *r, inodeno_t i, Context *c) : oc(_oc), rd(r), ino(i), onfinish(c) {}
	void finish(int) {
	  oc->readx(rd, ino, onfinish);
	}
  };

  // non-blocking.  async.
  int readx(Objecter::OSDRead *rd, inodeno_t ino, Context *onfinish);
  int writex(Objecter::OSDWrite *wr, inodeno_t ino);

  // write blocking
  void wait_for_write(size_t len, Mutex& lock);
  
  // blocking.  atomic+sync.
  int atomic_sync_readx(Objecter::OSDRead *rd, inodeno_t ino, Mutex& lock);
  int atomic_sync_writex(Objecter::OSDWrite *wr, inodeno_t ino, Mutex& lock);

  void flush_set(inodeno_t ino, Context *onfinish=0);
  void flush_all(Context *onfinish=0);

  // help.. need to figure out how to handle this wrt BufferHeads, etc..
  void commit_set(inodeno_t ino, Context *oncommit=0);
  void commit_all(Context *oncommit=0);
};


inline ostream& operator<<(ostream& out, ObjectCacher::BufferHead &bh)
{
  out << "bh["
	  << bh.start() << "~" << bh.end()
	  << " v " << bh.last_write_tid;
  if (bh.is_tx()) out << " tx";
  if (bh.is_rx()) out << " rx";
  if (bh.is_dirty()) out << " dirty";
  if (bh.is_clean()) out << " clean";
  if (bh.is_missing()) out << " missing";
  out << "]";
  return out;
}

#endif
