// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OBJECTCACHER_H
#define CEPH_OBJECTCACHER_H

#include "include/types.h"
#include "include/lru.h"
#include "include/Context.h"
#include "include/xlist.h"

#include "common/Cond.h"
#include "common/Thread.h"

#include "Objecter.h"
#include "Filer.h"

class Objecter;

class ObjectCacher {
 public:

  class Object;
  class ObjectSet;

  typedef void (*flush_set_callback_t) (void *p, ObjectSet *oset);

  // read scatter/gather  
  struct OSDRead {
    vector<ObjectExtent> extents;
    snapid_t snap;
    map<object_t, bufferlist*> read_data;  // bits of data as they come back
    bufferlist *bl;
    int flags;
    OSDRead(snapid_t s, bufferlist *b, int f) : snap(s), bl(b), flags(f) {}
  };

  OSDRead *prepare_read(snapid_t snap, bufferlist *b, int f) {
    return new OSDRead(snap, b, f);
  }
  
  // write scatter/gather  
  struct OSDWrite {
    vector<ObjectExtent> extents;
    SnapContext snapc;
    bufferlist bl;
    utime_t mtime;
    int flags;
    OSDWrite(const SnapContext& sc, bufferlist& b, utime_t mt, int f) : snapc(sc), bl(b), mtime(mt), flags(f) {}
  };

  OSDWrite *prepare_write(const SnapContext& sc, bufferlist &b, utime_t mt, int f) { 
    return new OSDWrite(sc, b, mt, f); 
  }



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
      loff_t start, length;   // bh extent in object
    } ex;
        
  public:
    Object *ob;
    bufferlist  bl;
    tid_t last_write_tid;  // version of bh (if non-zero)
    utime_t last_write;
    SnapContext snapc;
    
    map< loff_t, list<Context*> > waitfor_read;
    
  public:
    // cons
    BufferHead(Object *o) : 
      state(STATE_MISSING),
      ref(0),
      ob(o),
      last_write_tid(0) {}
  
    // extent
    loff_t start() const { return ex.start; }
    void set_start(loff_t s) { ex.start = s; }
    loff_t length() const { return ex.length; }
    void set_length(loff_t l) { ex.length = l; }
    loff_t end() const { return ex.start + ex.length; }
    loff_t last() const { return end() - 1; }

    // states
    void set_state(int s) {
      if (s == STATE_RX || s == STATE_TX) get();
      if (state == STATE_RX || state == STATE_TX) put();
      state = s;
    }
    int get_state() const { return state; }
    
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
    sobject_t oid;
    friend class ObjectSet;

  public:
    ObjectSet *oset;
    xlist<Object*>::item set_item;
    object_locator_t oloc;
    

  public:
    map<loff_t, BufferHead*>     data;

    tid_t last_write_tid;  // version of bh (if non-zero)
    tid_t last_ack_tid;    // last update acked.
    tid_t last_commit_tid; // last update commited.

    map< tid_t, list<Context*> > waitfor_ack;
    map< tid_t, list<Context*> > waitfor_commit;
    list<Context*> waitfor_rd;
    list<Context*> waitfor_wr;

    xlist<Object*>::item uncommitted_item;

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
    Object(ObjectCacher *_oc, sobject_t o, ObjectSet *os, object_locator_t& l) : 
      oc(_oc),
      oid(o), oset(os), set_item(this), oloc(l),
      last_write_tid(0), last_ack_tid(0), last_commit_tid(0),
      uncommitted_item(this),
      lock_state(LOCK_NONE), wrlock_ref(0), rdlock_ref(0) {
      // add to set
      os->objects.push_back(&set_item);
    }
    ~Object() {
      assert(data.empty());
      set_item.remove_myself();
    }

    sobject_t get_soid() { return oid; }
    object_t get_oid() { return oid.oid; }
    snapid_t get_snap() { return oid.snap; }
    ObjectSet *get_object_set() { return oset; }
    
    object_locator_t& get_oloc() { return oloc; }
    void set_object_locator(object_locator_t& l) { oloc = l; }

    bool can_close() {
      return data.empty() && lock_state == LOCK_NONE &&
        waitfor_ack.empty() && waitfor_commit.empty() &&
        waitfor_rd.empty() && waitfor_wr.empty() &&
	!uncommitted_item.is_on_list();
    }

    // bh
    void add_bh(BufferHead *bh) {
      // add to my map
      assert(data.count(bh->start()) == 0);
      
      if (0) {  // sanity check     FIXME DEBUG
        //cout << "add_bh " << bh->start() << "~" << bh->length() << endl;
        map<loff_t,BufferHead*>::iterator p = data.lower_bound(bh->start());
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
    BufferHead *split(BufferHead *bh, loff_t off);
    void merge_left(BufferHead *left, BufferHead *right);
    void try_merge_bh(BufferHead *bh);

    bool is_cached(loff_t off, loff_t len);
    int map_read(OSDRead *rd,
                 map<loff_t, BufferHead*>& hits,
                 map<loff_t, BufferHead*>& missing,
                 map<loff_t, BufferHead*>& rx);
    BufferHead *map_write(OSDWrite *wr);
    
    void truncate(loff_t s);

  };
  

  struct ObjectSet {
    void *parent;

    inodeno_t ino;
    uint64_t truncate_seq, truncate_size;

    int poolid;
    xlist<Object*> objects;
    xlist<Object*> uncommitted;

    int dirty_tx;

    ObjectSet(void *p, int _poolid, inodeno_t i) : parent(p), ino(i), truncate_seq(0),
                                      truncate_size(0), poolid(_poolid), dirty_tx(0) {}
  };


  // ******* ObjectCacher *********
  // ObjectCacher fields
 public:
  Objecter *objecter;
  Filer filer;

 private:
  Mutex& lock;
  
  flush_set_callback_t flush_set_callback, commit_set_callback;
  void *flush_set_callback_arg;

  vector<hash_map<sobject_t, Object*> > objects; // indexed by pool_id

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
  Object *get_object_maybe(sobject_t oid, object_locator_t &l) {
    // have it?
    assert(l.pool >= 0);
    if ((l.pool < objects.size()) &&
        (objects[l.pool].count(oid)))
      return objects[l.pool][oid];
    return NULL;
  }

  Object *get_object(sobject_t oid, ObjectSet *oset, object_locator_t &l) {
    // have it?
    assert(l.pool >= 0);
    if (l.pool < objects.size()) {
      if (objects[l.pool].count(oid))
        return objects[l.pool][oid];
    } else {
      objects.resize(l.pool+1);
    }

    // create it.
    Object *o = new Object(this, oid, oset, l);
    objects[l.pool][oid] = o;
    return o;
  }
  void close_object(Object *ob);

  // bh stats
  Cond  stat_cond;
  int   stat_waiter;

  loff_t stat_clean;
  loff_t stat_dirty;
  loff_t stat_rx;
  loff_t stat_tx;
  loff_t stat_missing;

  void verify_stats() const;

  void bh_stat_add(BufferHead *bh) {
    switch (bh->get_state()) {
    case BufferHead::STATE_MISSING:
      stat_missing += bh->length();
      break;
    case BufferHead::STATE_CLEAN:
      stat_clean += bh->length();
      break;
    case BufferHead::STATE_DIRTY: 
      stat_dirty += bh->length(); 
      bh->ob->oset->dirty_tx += bh->length();
      break;
    case BufferHead::STATE_TX: 
      stat_tx += bh->length(); 
      bh->ob->oset->dirty_tx += bh->length();
      break;
    case BufferHead::STATE_RX:
      stat_rx += bh->length();
      break;
    }
    if (stat_waiter) stat_cond.Signal();
  }
  void bh_stat_sub(BufferHead *bh) {
    switch (bh->get_state()) {
    case BufferHead::STATE_MISSING:
      stat_missing -= bh->length();
      break;
    case BufferHead::STATE_CLEAN: 
      stat_clean -= bh->length();
      break;
    case BufferHead::STATE_DIRTY: 
      stat_dirty -= bh->length(); 
      bh->ob->oset->dirty_tx -= bh->length();
      break;
    case BufferHead::STATE_TX: 
      stat_tx -= bh->length(); 
      bh->ob->oset->dirty_tx -= bh->length();
      break;
    case BufferHead::STATE_RX:
      stat_rx -= bh->length();
      break;
    }
  }
  loff_t get_stat_tx() { return stat_tx; }
  loff_t get_stat_rx() { return stat_rx; }
  loff_t get_stat_dirty() { return stat_dirty; }
  loff_t get_stat_clean() { return stat_clean; }

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
      lru_rest.lru_insert_top(bh);
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
  void bh_read(BufferHead *bh);
  void bh_write(BufferHead *bh);

  void trim(loff_t max=-1);
  void flush(loff_t amount=0);

  bool flush(Object *o);
  loff_t release(Object *o);
  void purge(Object *o);

  void rdlock(Object *o);
  void rdunlock(Object *o);
  void wrlock(Object *o);
  void wrunlock(Object *o);

 public:
  void bh_read_finish(int poolid, sobject_t oid, loff_t offset, uint64_t length, bufferlist &bl);
  void bh_write_ack(int poolid, sobject_t oid, loff_t offset, uint64_t length, tid_t t);
  void bh_write_commit(int poolid, sobject_t oid, loff_t offset, uint64_t length, tid_t t);
  void lock_ack(int poolid, list<sobject_t>& oids, tid_t tid);

  class C_ReadFinish : public Context {
    ObjectCacher *oc;
    int poolid;
    sobject_t oid;
    loff_t start;
    uint64_t length;
  public:
    bufferlist bl;
    C_ReadFinish(ObjectCacher *c, int _poolid, sobject_t o, loff_t s, uint64_t l) :
      oc(c), poolid(_poolid), oid(o), start(s), length(l) {}
    void finish(int r) {
      oc->bh_read_finish(poolid, oid, start, length, bl);
    }
  };

  class C_WriteAck : public Context {
    ObjectCacher *oc;
    int poolid;
    sobject_t oid;
    loff_t start;
    uint64_t length;
  public:
    tid_t tid;
    C_WriteAck(ObjectCacher *c, int _poolid, sobject_t o, loff_t s, uint64_t l) :
      oc(c), poolid(_poolid), oid(o), start(s), length(l) {}
    void finish(int r) {
      oc->bh_write_ack(poolid, oid, start, length, tid);
    }
  };
  class C_WriteCommit : public Context {
    ObjectCacher *oc;
    int poolid;
    sobject_t oid;
    loff_t start;
    uint64_t length;
  public:
    tid_t tid;
    C_WriteCommit(ObjectCacher *c, int _poolid, sobject_t o, loff_t s, uint64_t l) :
      oc(c), poolid(_poolid), oid(o), start(s), length(l) {}
    void finish(int r) {
      oc->bh_write_commit(poolid, oid, start, length, tid);
    }
  };

  class C_LockAck : public Context {
    ObjectCacher *oc;
  public:
    int poolid;
    list<sobject_t> oids;
    tid_t tid;
    C_LockAck(ObjectCacher *c, int _poolid, sobject_t o) : oc(c), poolid(_poolid) {
      oids.push_back(o);
    }
    void finish(int r) {
      oc->lock_ack(poolid, oids, tid);
    }
  };



 public:
  ObjectCacher(Objecter *o, Mutex& l,
	       flush_set_callback_t flush_callback,
	       flush_set_callback_t commit_callback,
	       void *callback_arg) : 
    objecter(o), filer(o), lock(l),
    flush_set_callback(flush_callback), commit_set_callback(commit_callback), flush_set_callback_arg(callback_arg),
    flusher_stop(false), flusher_thread(this),
    stat_waiter(0),
    stat_clean(0), stat_dirty(0), stat_rx(0), stat_tx(0), stat_missing(0) {
  }
  ~ObjectCacher() {
    // we should be empty.
    for (vector<hash_map<sobject_t, Object *> >::iterator i = objects.begin();
        i != objects.end();
        ++i)
      assert(!i->size());
    assert(lru_rest.lru_get_size() == 0);
    assert(lru_dirty.lru_get_size() == 0);
    assert(dirty_bh.empty());
  }

  void start() {
    flusher_thread.create();
  }
  void stop() {
    assert(flusher_thread.is_started());
    lock.Lock();  // hmm.. watch out for deadlock!
    flusher_stop = true;
    flusher_cond.Signal();
    lock.Unlock();
    flusher_thread.join();
  }


  class C_RetryRead : public Context {
    ObjectCacher *oc;
    OSDRead *rd;
    ObjectSet *oset;
    Context *onfinish;
  public:
    C_RetryRead(ObjectCacher *_oc, OSDRead *r, ObjectSet *os, Context *c) : oc(_oc), rd(r), oset(os), onfinish(c) {}
    void finish(int) {
      int r = oc->readx(rd, oset, onfinish);
      if (r > 0 && onfinish) {
        onfinish->finish(r);
        delete onfinish;
      }
    }
  };



  // non-blocking.  async.
  int readx(OSDRead *rd, ObjectSet *oset, Context *onfinish);
  int writex(OSDWrite *wr, ObjectSet *oset);
  bool is_cached(ObjectSet *oset, vector<ObjectExtent>& extents, snapid_t snapid);

  // write blocking
  bool wait_for_write(uint64_t len, Mutex& lock);
  
  // blocking.  atomic+sync.
  int atomic_sync_readx(OSDRead *rd, ObjectSet *oset, Mutex& lock);
  int atomic_sync_writex(OSDWrite *wr, ObjectSet *oset, Mutex& lock);

  bool set_is_cached(ObjectSet *oset);
  bool set_is_dirty_or_committing(ObjectSet *oset);

  bool flush_set(ObjectSet *oset, Context *onfinish=0);
  void flush_all(Context *onfinish=0);

  bool commit_set(ObjectSet *oset, Context *oncommit);
  void commit_all(Context *oncommit=0);

  void purge_set(ObjectSet *oset);

  loff_t release_set(ObjectSet *oset);  // returns # of bytes not released (ie non-clean)
  uint64_t release_all();

  void truncate_set(ObjectSet *oset, vector<ObjectExtent>& ex);

  void kick_sync_writers(ObjectSet *oset);
  void kick_sync_readers(ObjectSet *oset);


  // file functions

  /*** async+caching (non-blocking) file interface ***/
  int file_is_cached(ObjectSet *oset, ceph_file_layout *layout, snapid_t snapid,
		     loff_t offset, uint64_t len) {
    vector<ObjectExtent> extents;
    filer.file_to_extents(oset->ino, layout, offset, len, extents);
    return is_cached(oset, extents, snapid);
  }

  int file_read(ObjectSet *oset, ceph_file_layout *layout, snapid_t snapid,
                loff_t offset, uint64_t len, 
                bufferlist *bl,
		int flags,
                Context *onfinish) {
    OSDRead *rd = prepare_read(snapid, bl, flags);
    filer.file_to_extents(oset->ino, layout, offset, len, rd->extents);
    return readx(rd, oset, onfinish);
  }

  int file_write(ObjectSet *oset, ceph_file_layout *layout, const SnapContext& snapc,
                 loff_t offset, uint64_t len, 
                 bufferlist& bl, utime_t mtime, int flags) {
    OSDWrite *wr = prepare_write(snapc, bl, mtime, flags);
    filer.file_to_extents(oset->ino, layout, offset, len, wr->extents);
    return writex(wr, oset);
  }



  /*** sync+blocking file interface ***/

  int file_atomic_sync_read(ObjectSet *oset, ceph_file_layout *layout, 
			    snapid_t snapid,
                            loff_t offset, uint64_t len, 
                            bufferlist *bl, int flags,
                            Mutex &lock) {
    OSDRead *rd = prepare_read(snapid, bl, flags);
    filer.file_to_extents(oset->ino, layout, offset, len, rd->extents);
    return atomic_sync_readx(rd, oset, lock);
  }

  int file_atomic_sync_write(ObjectSet *oset, ceph_file_layout *layout, 
			     const SnapContext& snapc,
                             loff_t offset, uint64_t len, 
                             bufferlist& bl, utime_t mtime, int flags,
                             Mutex &lock) {
    OSDWrite *wr = prepare_write(snapc, bl, mtime, flags);
    filer.file_to_extents(oset->ino, layout, offset, len, wr->extents);
    return atomic_sync_writex(wr, oset, lock);
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
  if (bh.bl.length() > 0) out << " firstbyte=" << (int)bh.bl[0];
  out << "]";
  return out;
}

inline ostream& operator<<(ostream& out, ObjectCacher::Object &ob)
{
  out << "object["
      << ob.get_soid() << " oset " << ob.oset << dec
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
