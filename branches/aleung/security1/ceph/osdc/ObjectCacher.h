// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#ifndef __OBJECTCACHER_H_
#define __OBJECTCACHER_H_

#include "include/types.h"
#include "include/lru.h"
#include "include/Context.h"

#include "common/Cond.h"
#include "common/Thread.h"

#include "Objecter.h"
#include "Filer.h"

class Objecter;
class Objecter::OSDRead;
class Objecter::OSDWrite;

class ObjectCacher {
 public:

  class Object;

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
    Object *ob;
    bufferlist  bl;
    tid_t last_write_tid;  // version of bh (if non-zero)
    utime_t last_write;

    // security cap per bufferhead
    // this allows any bufferhead written back
    // to have a cap
    ExtCap *bh_cap;
    
    map< off_t, list<Context*> > waitfor_read;
    
  public:
    // cons
    BufferHead(Object *o) : 
      state(STATE_MISSING),
      ref(0),
      ob(o),
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
    object_t  oid;   // this _always_ is oid.rev=0
    inodeno_t ino;
    objectrev_t rev; // last rev we're written
    
  public:
    map<off_t, BufferHead*>     data;

    tid_t last_write_tid;  // version of bh (if non-zero)
    tid_t last_ack_tid;    // last update acked.
    tid_t last_commit_tid; // last update commited.

    map< tid_t, list<Context*> > waitfor_ack;
    map< tid_t, list<Context*> > waitfor_commit;
    list<Context*> waitfor_rd;
    list<Context*> waitfor_wr;

    // lock
    static const int LOCK_NONE = 0;
    static const int LOCK_WRLOCKING = 1;
    static const int LOCK_WRLOCK = 2;
    static const int LOCK_WRUNLOCKING = 3;
    static const int LOCK_RDLOCKING = 4;
    static const int LOCK_RDLOCK = 5;
    static const int LOCK_RDUNLOCKING = 6;
    static const int LOCK_UPGRADING = 7;    // rd -> wr
    static const int LOCK_DOWNGRADING = 8;  // wr -> rd
    int lock_state;
    int wrlock_ref;  // how many ppl want or are using a WRITE lock
    int rdlock_ref;  // how many ppl want or are using a READ lock

  public:
    Object(ObjectCacher *_oc, object_t o, inodeno_t i) : 
      oc(_oc),
      oid(o), ino(i), 
      last_write_tid(0), last_ack_tid(0), last_commit_tid(0),
      lock_state(LOCK_NONE), wrlock_ref(0), rdlock_ref(0)
      {}
	~Object() {
	  assert(data.empty());
	}

    object_t get_oid() { return oid; }
    inodeno_t get_ino() { return ino; }

    bool can_close() {
      return data.empty() && lock_state == LOCK_NONE &&
        waitfor_ack.empty() && waitfor_commit.empty() &&
        waitfor_rd.empty() && waitfor_wr.empty();
    }

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
    void merge_left(BufferHead *left, BufferHead *right);
    void merge_right(BufferHead *left, BufferHead *right);

    int map_read(Objecter::OSDRead *rd,
                 map<off_t, BufferHead*>& hits,
                 map<off_t, BufferHead*>& missing,
                 map<off_t, BufferHead*>& rx);
    BufferHead *map_write(Objecter::OSDWrite *wr);
    
  };
  
  // ******* ObjectCacher *********
  // ObjectCacher fields
 public:
  Objecter *objecter;
  Filer filer;

 private:
  Mutex& lock;
  
  hash_map<object_t, Object*> objects;
  hash_map<inodeno_t, set<Object*> > objects_by_ino;

  set<BufferHead*>    dirty_bh;
  LRU   lru_dirty, lru_rest;

  Cond flusher_cond;
  bool flusher_stop;
  void flusher_entry();
  class FlusherThread : public Thread {
    ObjectCacher *oc;
  public:
    FlusherThread(ObjectCacher *o) : oc(o) {}
    void *entry() {
      oc->flusher_entry();
      return 0;
    }
  } flusher_thread;
  

  // objects
  Object *get_object(object_t oid, inodeno_t ino) {
    // have it?
    if (objects.count(oid))
      return objects[oid];

    // create it.
    Object *o = new Object(this, oid, ino);
    objects[oid] = o;
    objects_by_ino[ino].insert(o);
    return o;
  }
  void close_object(Object *ob);

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
    lru_dirty.lru_touch(bh);
    //bh->set_dirty_stamp(g_clock.now());
  };

  void bh_add(Object *ob, BufferHead *bh) {
    ob->add_bh(bh);
    if (bh->is_dirty()) {
      lru_dirty.lru_insert_top(bh);
	  dirty_bh.insert(bh);
	} else {
      lru_rest.lru_insert_top(bh);
	}
    bh_stat_add(bh);
  }
  void bh_remove(Object *ob, BufferHead *bh) {
    ob->remove_bh(bh);
    if (bh->is_dirty()) {
      lru_dirty.lru_remove(bh);
	  dirty_bh.erase(bh);
	} else {
      lru_rest.lru_remove(bh);
	}
    bh_stat_sub(bh);
  }

  // io
  void bh_read(BufferHead *bh, ExtCap* read_ext_cap);
  // doesn't need a cap (cap is in bh)
  void bh_write(BufferHead *bh);

  void trim(off_t max=-1);
  void flush(off_t amount=0);

  bool flush(Object *o);
  off_t release(Object *o);
  void purge(Object *o);

  void rdlock(Object *o);
  void rdunlock(Object *o);
  void wrlock(Object *o);
  void wrunlock(Object *o);

 public:
  void bh_read_finish(object_t oid, off_t offset, size_t length, bufferlist &bl);
  void bh_write_ack(object_t oid, off_t offset, size_t length, tid_t t);
  void bh_write_commit(object_t oid, off_t offset, size_t length, tid_t t);
  void lock_ack(list<object_t>& oids, tid_t tid);

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

  class C_LockAck : public Context {
    ObjectCacher *oc;
  public:
    list<object_t> oids;
    tid_t tid;
    C_LockAck(ObjectCacher *c, object_t o) : oc(c) {
      oids.push_back(o);
    }
    void finish(int r) {
      oc->lock_ack(oids, tid);
    }
  };



 public:
  ObjectCacher(Objecter *o, Mutex& l) : 
    objecter(o), filer(o), lock(l),
    flusher_stop(false), flusher_thread(this),
    stat_waiter(0),
    stat_clean(0), stat_dirty(0), stat_rx(0), stat_tx(0), stat_missing(0) {
    flusher_thread.create();
  }
  ~ObjectCacher() {
	// we should be empty.
	assert(objects.empty());
	assert(lru_rest.lru_get_size() == 0);
	assert(lru_dirty.lru_get_size() == 0);
	assert(dirty_bh.empty());

	assert(flusher_thread.is_started());
    lock.Lock();  // hmm.. watch out for deadlock!
    flusher_stop = true;
    flusher_cond.Signal();
    lock.Unlock();
    flusher_thread.join();
  }


  class C_RetryRead : public Context {
    ObjectCacher *oc;
    Objecter::OSDRead *rd;
    inodeno_t ino;
    Context *onfinish;
  public:
    C_RetryRead(ObjectCacher *_oc, Objecter::OSDRead *r, inodeno_t i, Context *c) : oc(_oc), rd(r), ino(i), onfinish(c) {}
    void finish(int) {
      int r = oc->readx(rd, ino, onfinish);
      if (r > 0) {
        onfinish->finish(r);
        delete onfinish;
      }
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

  bool set_is_cached(inodeno_t ino);
  bool set_is_dirty_or_committing(inodeno_t ino);

  bool flush_set(inodeno_t ino, Context *onfinish=0);
  void flush_all(Context *onfinish=0);

  bool commit_set(inodeno_t ino, Context *oncommit);
  void commit_all(Context *oncommit=0);

  void purge_set(inodeno_t ino);

  off_t release_set(inodeno_t ino);  // returns # of bytes not released (ie non-clean)

  void kick_sync_writers(inodeno_t ino);
  void kick_sync_readers(inodeno_t ino);


  // file functions

  /*** async+caching (non-blocking) file interface ***/
  int file_read(inode_t& inode,
                off_t offset, size_t len, 
                bufferlist *bl,
                Context *onfinish, ExtCap *read_ext_cap) {
    Objecter::OSDRead *rd = new Objecter::OSDRead(bl, read_ext_cap);
    filer.file_to_extents(inode, offset, len, rd->extents);
    return readx(rd, inode.ino, onfinish);
  }

  int file_write(inode_t& inode,
                 off_t offset, size_t len, 
                 bufferlist& bl, ExtCap *write_ext_cap,
				 objectrev_t rev=0) {
    // insert write_cap into write object
    // will be inserted into bufferhead later
    Objecter::OSDWrite *wr = new Objecter::OSDWrite(bl, write_ext_cap);
    filer.file_to_extents(inode, offset, len, wr->extents);
    return writex(wr, inode.ino);
  }



  /*** sync+blocking file interface ***/
  
  int file_atomic_sync_read(inode_t& inode,
                            off_t offset, size_t len, 
                            bufferlist *bl,
                            Mutex &lock, ExtCap *read_ext_cap=0) {
    Objecter::OSDRead *rd = new Objecter::OSDRead(bl, read_ext_cap);
    filer.file_to_extents(inode, offset, len, rd->extents);
    return atomic_sync_readx(rd, inode.ino, lock);
  }

  int file_atomic_sync_write(inode_t& inode,
                             off_t offset, size_t len, 
                             bufferlist& bl,
                             Mutex &lock, ExtCap *write_ext_cap=0,
							 objectrev_t rev=0) {
    // we really should always have a cap
    Objecter::OSDWrite *wr = new Objecter::OSDWrite(bl, write_ext_cap);
    filer.file_to_extents(inode, offset, len, wr->extents);
    return atomic_sync_writex(wr, inode.ino, lock);
  }

};


inline ostream& operator<<(ostream& out, ObjectCacher::BufferHead &bh)
{
  out << "bh["
      << bh.start() << "~" << bh.length()
      << " (" << bh.bl.length() << ")"
      << " v " << bh.last_write_tid;
  if (bh.is_tx()) out << " tx";
  if (bh.is_rx()) out << " rx";
  if (bh.is_dirty()) out << " dirty";
  if (bh.is_clean()) out << " clean";
  if (bh.is_missing()) out << " missing";
  out << "]";
  return out;
}

inline ostream& operator<<(ostream& out, ObjectCacher::Object &ob)
{
  out << "object["
      << hex << ob.get_oid() << " ino " << ob.get_ino() << dec
      << " wr " << ob.last_write_tid << "/" << ob.last_ack_tid << "/" << ob.last_commit_tid;

  switch (ob.lock_state) {
  case ObjectCacher::Object::LOCK_WRLOCKING: out << " wrlocking"; break;
  case ObjectCacher::Object::LOCK_WRLOCK: out << " wrlock"; break;
  case ObjectCacher::Object::LOCK_WRUNLOCKING: out << " wrunlocking"; break;
  case ObjectCacher::Object::LOCK_RDLOCKING: out << " rdlocking"; break;
  case ObjectCacher::Object::LOCK_RDLOCK: out << " rdlock"; break;
  case ObjectCacher::Object::LOCK_RDUNLOCKING: out << " rdunlocking"; break;
  }

  out << "]";
  return out;
}

#endif
