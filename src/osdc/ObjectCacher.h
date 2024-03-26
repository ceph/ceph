// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OBJECTCACHER_H
#define CEPH_OBJECTCACHER_H

#include "include/types.h"
#include "include/lru.h"
#include "include/Context.h"
#include "include/xlist.h"
#include "include/common_fwd.h"

#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/Thread.h"
#include "common/zipkin_trace.h"

#include "Objecter.h"
#include "Striper.h"

class WritebackHandler;

enum {
  l_objectcacher_first = 25000,

  l_objectcacher_cache_ops_hit, // ops we satisfy completely from cache
  l_objectcacher_cache_ops_miss, // ops we don't satisfy completely from cache

  l_objectcacher_cache_bytes_hit, // bytes read directly from cache

  l_objectcacher_cache_bytes_miss, // bytes we couldn't read directly

				   // from cache

  l_objectcacher_data_read, // total bytes read out
  l_objectcacher_data_written, // bytes written to cache
  l_objectcacher_data_flushed, // bytes flushed to WritebackHandler
  l_objectcacher_overwritten_in_flush, // bytes overwritten while
				       // flushing is in progress

  l_objectcacher_write_ops_blocked, // total write ops we delayed due
				    // to dirty limits
  l_objectcacher_write_bytes_blocked, // total number of write bytes
				      // we delayed due to dirty
				      // limits
  l_objectcacher_write_time_blocked, // total time in seconds spent
				     // blocking a write due to dirty
				     // limits

  l_objectcacher_last,
};

class ObjectCacher {
  PerfCounters *perfcounter;
 public:
  CephContext *cct;
  class Object;
  struct ObjectSet;
  class C_ReadFinish;

  typedef void (*flush_set_callback_t) (void *p, ObjectSet *oset);

  // read scatter/gather
  struct OSDRead {
    std::vector<ObjectExtent> extents;
    snapid_t snap;
    ceph::buffer::list *bl;
    int fadvise_flags;
    OSDRead(snapid_t s, ceph::buffer::list *b, int f)
      : snap(s), bl(b), fadvise_flags(f) {}
  };

  OSDRead *prepare_read(snapid_t snap, ceph::buffer::list *b, int f) const {
    return new OSDRead(snap, b, f);
  }

  // write scatter/gather
  struct OSDWrite {
    std::vector<ObjectExtent> extents;
    SnapContext snapc;
    ceph::buffer::list bl;
    ceph::real_time mtime;
    int fadvise_flags;
    ceph_tid_t journal_tid;
    OSDWrite(const SnapContext& sc, const ceph::buffer::list& b, ceph::real_time mt,
	     int f, ceph_tid_t _journal_tid)
      : snapc(sc), bl(b), mtime(mt), fadvise_flags(f),
	journal_tid(_journal_tid) {}
  };

  OSDWrite *prepare_write(const SnapContext& sc,
			  const ceph::buffer::list &b,
			  ceph::real_time mt,
			  int f,
			  ceph_tid_t journal_tid) const {
    return new OSDWrite(sc, b, mt, f, journal_tid);
  }



  // ******* BufferHead *********
  class BufferHead : public LRUObject {
  public:
    // states
    static const int STATE_MISSING = 0;
    static const int STATE_CLEAN = 1;
    static const int STATE_ZERO = 2;   // NOTE: these are *clean* zeros
    static const int STATE_DIRTY = 3;
    static const int STATE_RX = 4;
    static const int STATE_TX = 5;
    static const int STATE_ERROR = 6; // a read error occurred

  private:
    // my fields
    int state;
    int ref;
    struct {
      loff_t start, length;   // bh extent in object
    } ex;
    bool dontneed; //indicate bh don't need by anyone
    bool nocache; //indicate bh don't need by this caller

  public:
    Object *ob;
    ceph::buffer::list  bl;
    ceph_tid_t last_write_tid;  // version of bh (if non-zero)
    ceph_tid_t last_read_tid;   // tid of last read op (if any)
    ceph::real_time last_write;
    SnapContext snapc;
    ceph_tid_t journal_tid;
    int error; // holds return value for failed reads

    std::map<loff_t, std::list<Context*> > waitfor_read;

    // cons
    explicit BufferHead(Object *o) :
      state(STATE_MISSING),
      ref(0),
      dontneed(false),
      nocache(false),
      ob(o),
      last_write_tid(0),
      last_read_tid(0),
      journal_tid(0),
      error(0) {
      ex.start = ex.length = 0;
    }

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

    inline int get_error() const {
      return error;
    }
    inline void set_error(int _error) {
      error = _error;
    }

    inline ceph_tid_t get_journal_tid() const {
      return journal_tid;
    }
    inline void set_journal_tid(ceph_tid_t _journal_tid) {
      journal_tid = _journal_tid;
    }

    bool is_missing() const { return state == STATE_MISSING; }
    bool is_dirty() const { return state == STATE_DIRTY; }
    bool is_clean() const { return state == STATE_CLEAN; }
    bool is_zero() const { return state == STATE_ZERO; }
    bool is_tx() const { return state == STATE_TX; }
    bool is_rx() const { return state == STATE_RX; }
    bool is_error() const { return state == STATE_ERROR; }

    // reference counting
    int get() {
      ceph_assert(ref >= 0);
      if (ref == 0) lru_pin();
      return ++ref;
    }
    int put() {
      ceph_assert(ref > 0);
      if (ref == 1) lru_unpin();
      --ref;
      return ref;
    }

    void set_dontneed(bool v) {
      dontneed = v;
    }
    bool get_dontneed() const {
      return dontneed;
    }

    void set_nocache(bool v) {
      nocache = v;
    }
    bool get_nocache() const {
      return nocache;
    }

    inline bool can_merge_journal(BufferHead *bh) const {
      return (get_journal_tid() == bh->get_journal_tid());
    }

    struct ptr_lt {
      bool operator()(const BufferHead* l, const BufferHead* r) const {
	const Object *lob = l->ob;
	const Object *rob = r->ob;
	const ObjectSet *loset = lob->oset;
	const ObjectSet *roset = rob->oset;
	if (loset != roset)
	  return loset < roset;
	if (lob != rob)
	  return lob < rob;
	if (l->start() != r->start())
	  return l->start() < r->start();
	return l < r;
      }
    };
  };

  // ******* Object *********
  class Object : public LRUObject {
  private:
    // ObjectCacher::Object fields
    int ref;
    ObjectCacher *oc;
    sobject_t oid;
    friend struct ObjectSet;

  public:
    uint64_t object_no;
    ObjectSet *oset;
    xlist<Object*>::item set_item;
    object_locator_t oloc;
    uint64_t truncate_size, truncate_seq;

    bool complete;
    bool exists;

    std::map<loff_t, BufferHead*>     data;

    ceph_tid_t last_write_tid;  // version of bh (if non-zero)
    ceph_tid_t last_commit_tid; // last update committed.

    int dirty_or_tx;

    std::map< ceph_tid_t, std::list<Context*> > waitfor_commit;
    xlist<C_ReadFinish*> reads;

    Object(const Object&) = delete;
    Object& operator=(const Object&) = delete;

    Object(ObjectCacher *_oc, sobject_t o, uint64_t ono, ObjectSet *os,
	   object_locator_t& l, uint64_t ts, uint64_t tq) :
      ref(0),
      oc(_oc),
      oid(o), object_no(ono), oset(os), set_item(this), oloc(l),
      truncate_size(ts), truncate_seq(tq),
      complete(false), exists(true),
      last_write_tid(0), last_commit_tid(0),
      dirty_or_tx(0) {
      // add to set
      os->objects.push_back(&set_item);
    }
    ~Object() {
      reads.clear();
      ceph_assert(ref == 0);
      ceph_assert(data.empty());
      ceph_assert(dirty_or_tx == 0);
      set_item.remove_myself();
    }

    sobject_t get_soid() const { return oid; }
    object_t get_oid() { return oid.oid; }
    snapid_t get_snap() { return oid.snap; }
    ObjectSet *get_object_set() const { return oset; }
    std::string get_namespace() { return oloc.nspace; }
    uint64_t get_object_number() const { return object_no; }

    const object_locator_t& get_oloc() const { return oloc; }
    void set_object_locator(object_locator_t& l) { oloc = l; }

    bool can_close() const {
      if (lru_is_expireable()) {
	ceph_assert(data.empty());
	ceph_assert(waitfor_commit.empty());
	return true;
      }
      return false;
    }

    /**
     * Check buffers and waiters for consistency
     * - no overlapping buffers
     * - index in map matches BH
     * - waiters fall within BH
     */
    void audit_buffers();

    /**
     * find first buffer that includes or follows an offset
     *
     * @param offset object byte offset
     * @return iterator pointing to buffer, or data.end()
     */
    std::map<loff_t,BufferHead*>::const_iterator data_lower_bound(loff_t offset) const {
      auto p = data.lower_bound(offset);
      if (p != data.begin() &&
	  (p == data.end() || p->first > offset)) {
	--p;     // might overlap!
	if (p->first + p->second->length() <= offset)
	  ++p;   // doesn't overlap.
      }
      return p;
    }

    // bh
    // add to my map
    void add_bh(BufferHead *bh) {
      if (data.empty())
	get();
      ceph_assert(data.count(bh->start()) == 0);
      data[bh->start()] = bh;
    }
    void remove_bh(BufferHead *bh) {
      ceph_assert(data.count(bh->start()));
      data.erase(bh->start());
      if (data.empty())
	put();
    }

    bool is_empty() const { return data.empty(); }

    // mid-level
    BufferHead *split(BufferHead *bh, loff_t off);
    void merge_left(BufferHead *left, BufferHead *right);
    bool can_merge_bh(BufferHead *left, BufferHead *right);
    void try_merge_bh(BufferHead *bh);
    void maybe_rebuild_buffer(BufferHead *bh);

    bool is_cached(loff_t off, loff_t len) const;
    bool include_all_cached_data(loff_t off, loff_t len);
    int map_read(ObjectExtent &ex,
                 std::map<loff_t, BufferHead*>& hits,
                 std::map<loff_t, BufferHead*>& missing,
                 std::map<loff_t, BufferHead*>& rx,
		 std::map<loff_t, BufferHead*>& errors);
    BufferHead *map_write(ObjectExtent &ex, ceph_tid_t tid);

    void replace_journal_tid(BufferHead *bh, ceph_tid_t tid);
    void truncate(loff_t s);
    void discard(loff_t off, loff_t len, C_GatherBuilder* commit_gather);

    // reference counting
    int get() {
      ceph_assert(ref >= 0);
      if (ref == 0) lru_pin();
      return ++ref;
    }
    int put() {
      ceph_assert(ref > 0);
      if (ref == 1) lru_unpin();
      --ref;
      return ref;
    }
  };


  struct ObjectSet {
    void *parent;

    inodeno_t ino;
    uint64_t truncate_seq, truncate_size;

    int64_t poolid;
    xlist<Object*> objects;

    int dirty_or_tx;
    bool return_enoent;

    ObjectSet(void *p, int64_t _poolid, inodeno_t i)
      : parent(p), ino(i), truncate_seq(0),
	truncate_size(0), poolid(_poolid), dirty_or_tx(0),
	return_enoent(false) {}

  };


  // ******* ObjectCacher *********
  // ObjectCacher fields
 private:
  WritebackHandler& writeback_handler;
  bool scattered_write;

  std::string name;
  ceph::mutex& lock;

  uint64_t max_dirty, target_dirty, max_size, max_objects;
  ceph::timespan max_dirty_age;
  bool cfg_block_writes_upfront;

  ZTracer::Endpoint trace_endpoint;

  flush_set_callback_t flush_set_callback;
  void *flush_set_callback_arg;

  // indexed by pool_id
  std::vector<ceph::unordered_map<sobject_t, Object*> > objects;

  std::list<Context*> waitfor_read;

  ceph_tid_t last_read_tid;

  std::set<BufferHead*, BufferHead::ptr_lt> dirty_or_tx_bh;
  LRU   bh_lru_dirty, bh_lru_rest;
  LRU   ob_lru;

  ceph::condition_variable flusher_cond;
  bool flusher_stop;
  void flusher_entry();
  class FlusherThread : public Thread {
    ObjectCacher *oc;
  public:
    explicit FlusherThread(ObjectCacher *o) : oc(o) {}
    void *entry() override {
      oc->flusher_entry();
      return 0;
    }
  } flusher_thread;

  Finisher finisher;

  // objects
  Object *get_object_maybe(sobject_t oid, object_locator_t &l) {
    // have it?
    if (((uint32_t)l.pool < objects.size()) &&
	(objects[l.pool].count(oid)))
      return objects[l.pool][oid];
    return NULL;
  }

  Object *get_object(sobject_t oid, uint64_t object_no, ObjectSet *oset,
		     object_locator_t &l, uint64_t truncate_size,
		     uint64_t truncate_seq);
  void close_object(Object *ob);

  // bh stats
  ceph::condition_variable  stat_cond;

  loff_t stat_clean;
  loff_t stat_zero;
  loff_t stat_dirty;
  loff_t stat_rx;
  loff_t stat_tx;
  loff_t stat_missing;
  loff_t stat_error;
  loff_t stat_dirty_waiting;   // bytes that writers are waiting on to write

  size_t stat_nr_dirty_waiters;

  void verify_stats() const;

  void bh_stat_add(BufferHead *bh);
  void bh_stat_sub(BufferHead *bh);
  loff_t get_stat_tx() const { return stat_tx; }
  loff_t get_stat_rx() const { return stat_rx; }
  loff_t get_stat_dirty() const { return stat_dirty; }
  loff_t get_stat_clean() const { return stat_clean; }
  loff_t get_stat_zero() const { return stat_zero; }
  loff_t get_stat_dirty_waiting() const { return stat_dirty_waiting; }
  size_t get_stat_nr_dirty_waiters() const { return stat_nr_dirty_waiters; }

  void touch_bh(BufferHead *bh) {
    if (bh->is_dirty())
      bh_lru_dirty.lru_touch(bh);
    else
      bh_lru_rest.lru_touch(bh);

    bh->set_dontneed(false);
    bh->set_nocache(false);
    touch_ob(bh->ob);
  }
  void touch_ob(Object *ob) {
    ob_lru.lru_touch(ob);
  }
  void bottouch_ob(Object *ob) {
    ob_lru.lru_bottouch(ob);
  }

  // bh states
  void bh_set_state(BufferHead *bh, int s);
  void copy_bh_state(BufferHead *bh1, BufferHead *bh2) {
    bh_set_state(bh2, bh1->get_state());
  }

  void mark_missing(BufferHead *bh) {
    bh_set_state(bh,BufferHead::STATE_MISSING);
  }
  void mark_clean(BufferHead *bh) {
    bh_set_state(bh, BufferHead::STATE_CLEAN);
  }
  void mark_zero(BufferHead *bh) {
    bh_set_state(bh, BufferHead::STATE_ZERO);
  }
  void mark_rx(BufferHead *bh) {
    bh_set_state(bh, BufferHead::STATE_RX);
  }
  void mark_tx(BufferHead *bh) {
    bh_set_state(bh, BufferHead::STATE_TX); }
  void mark_error(BufferHead *bh) {
    bh_set_state(bh, BufferHead::STATE_ERROR);
  }
  void mark_dirty(BufferHead *bh) {
    bh_set_state(bh, BufferHead::STATE_DIRTY);
    bh_lru_dirty.lru_touch(bh);
    //bh->set_dirty_stamp(ceph_clock_now());
  }

  void bh_add(Object *ob, BufferHead *bh);
  void bh_remove(Object *ob, BufferHead *bh);

  // io
  void bh_read(BufferHead *bh, int op_flags,
               const ZTracer::Trace &parent_trace);
  void bh_write(BufferHead *bh, const ZTracer::Trace &parent_trace);
  void bh_write_scattered(std::list<BufferHead*>& blist);
  void bh_write_adjacencies(BufferHead *bh, ceph::real_time cutoff,
			    int64_t *amount, int *max_count);

  void trim();
  void flush(ZTracer::Trace *trace, loff_t amount=0);

  /**
   * flush a range of buffers
   *
   * Flush any buffers that intersect the specified extent.  If len==0,
   * flush *all* buffers for the object.
   *
   * @param o object
   * @param off start offset
   * @param len extent length, or 0 for entire object
   * @return true if object was already clean/flushed.
   */
  bool flush(Object *o, loff_t off, loff_t len,
             ZTracer::Trace *trace);
  loff_t release(Object *o);
  void purge(Object *o);

  int64_t reads_outstanding;
  ceph::condition_variable read_cond;

  int _readx(OSDRead *rd, ObjectSet *oset, Context *onfinish,
	     bool external_call, ZTracer::Trace *trace);
  void retry_waiting_reads();

 public:
  bool CFG_block_writes_upfront(void) { return cfg_block_writes_upfront; }
  void bh_read_finish(int64_t poolid, sobject_t oid, ceph_tid_t tid,
		      loff_t offset, uint64_t length,
		      ceph::buffer::list &bl, int r,
		      bool trust_enoent);
  void bh_write_commit(int64_t poolid, sobject_t oid,
		       std::vector<std::pair<loff_t, uint64_t> >& ranges,
		       ceph_tid_t t, int r);

  class C_WriteCommit;
  class C_WaitForWrite;

  void perf_start();
  void perf_stop();



  ObjectCacher(CephContext *cct_, std::string name, WritebackHandler& wb, ceph::mutex& l,
	       flush_set_callback_t flush_callback,
	       void *flush_callback_arg,
	       uint64_t max_bytes, uint64_t max_objects,
	       uint64_t max_dirty, uint64_t target_dirty, double max_age,
	       bool block_writes_upfront);
  ~ObjectCacher();

  void start() {
    flusher_thread.create("flusher");
  }
  void stop() {
    ceph_assert(flusher_thread.is_started());
    lock.lock();  // hmm.. watch out for deadlock!
    flusher_stop = true;
    flusher_cond.notify_all();
    lock.unlock();
    flusher_thread.join();
  }


  class C_RetryRead;


  // non-blocking.  async.

  /**
   * @note total read size must be <= INT_MAX, since
   * the return value is total bytes read
   */
  int readx(OSDRead *rd, ObjectSet *oset, Context *onfinish,
	    ZTracer::Trace *parent_trace = nullptr);
  int writex(OSDWrite *wr, ObjectSet *oset, Context *onfreespace,
	     ZTracer::Trace *parent_trace,
	     bool block_writes_upfront);
  int writex(OSDWrite *wr, ObjectSet *oset, Context *onfreespace,
	     ZTracer::Trace *parent_trace = nullptr) {
    return writex(wr, oset, onfreespace, parent_trace, cfg_block_writes_upfront);
  }
  bool is_cached(ObjectSet *oset, std::vector<ObjectExtent>& extents,
		 snapid_t snapid);

private:
  // write blocking
  int _wait_for_write(OSDWrite *wr, uint64_t len, ObjectSet *oset,
                      ZTracer::Trace *trace, Context *onfreespace,
                      bool block_writes_upfront);
  void _maybe_wait_for_writeback(uint64_t len, ZTracer::Trace *trace);
  bool _flush_set_finish(C_GatherBuilder *gather, Context *onfinish);

  void _discard(ObjectSet *oset, const std::vector<ObjectExtent>& exls,
                C_GatherBuilder* gather);
  void _discard_finish(ObjectSet *oset, bool was_dirty, Context* on_finish);

public:
  bool set_is_empty(ObjectSet *oset);
  bool set_is_cached(ObjectSet *oset);
  bool set_is_dirty_or_committing(ObjectSet *oset);

  bool flush_set(ObjectSet *oset, Context *onfinish=0);
  bool flush_set(ObjectSet *oset, std::vector<ObjectExtent>& ex,
                 ZTracer::Trace *trace, Context *onfinish = 0);
  bool flush_all(Context *onfinish = 0);

  void purge_set(ObjectSet *oset);

  // returns # of bytes not released (ie non-clean)
  loff_t release_set(ObjectSet *oset);
  uint64_t release_all();

  void discard_set(ObjectSet *oset, const std::vector<ObjectExtent>& ex);
  void discard_writeback(ObjectSet *oset, const std::vector<ObjectExtent>& ex,
                         Context* on_finish);

  /**
   * Retry any in-flight reads that get -ENOENT instead of marking
   * them zero, and get rid of any cached -ENOENTs.
   * After this is called and the cache's lock is unlocked,
   * any new requests will treat -ENOENT normally.
   */
  void clear_nonexistence(ObjectSet *oset);


  // cache sizes
  void set_max_dirty(uint64_t v) {
    max_dirty = v;
  }
  void set_target_dirty(int64_t v) {
    target_dirty = v;
  }
  void set_max_size(int64_t v) {
    max_size = v;
  }
  void set_max_dirty_age(double a) {
    max_dirty_age = ceph::make_timespan(a);
  }
  void set_max_objects(int64_t v) {
    max_objects = v;
  }


  // file functions

  /*** async+caching (non-blocking) file interface ***/
  int file_is_cached(ObjectSet *oset, file_layout_t *layout,
		     snapid_t snapid, loff_t offset, uint64_t len) {
    std::vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, oset->ino, layout, offset, len,
			     oset->truncate_size, extents);
    return is_cached(oset, extents, snapid);
  }

  int file_read(ObjectSet *oset, file_layout_t *layout, snapid_t snapid,
		loff_t offset, uint64_t len, ceph::buffer::list *bl, int flags,
		Context *onfinish) {
    OSDRead *rd = prepare_read(snapid, bl, flags);
    Striper::file_to_extents(cct, oset->ino, layout, offset, len,
			     oset->truncate_size, rd->extents);
    return readx(rd, oset, onfinish);
  }

  int file_write(ObjectSet *oset, file_layout_t *layout,
		 const SnapContext& snapc, loff_t offset, uint64_t len,
		 ceph::buffer::list& bl, ceph::real_time mtime, int flags,
		 Context *onfreespace, bool block_writes_upfront) {
    OSDWrite *wr = prepare_write(snapc, bl, mtime, flags, 0);
    Striper::file_to_extents(cct, oset->ino, layout, offset, len,
			     oset->truncate_size, wr->extents);
    return writex(wr, oset, onfreespace, nullptr, block_writes_upfront);
  }

  bool file_flush(ObjectSet *oset, file_layout_t *layout,
		  const SnapContext& snapc, loff_t offset, uint64_t len,
		  Context *onfinish) {
    std::vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, oset->ino, layout, offset, len,
			     oset->truncate_size, extents);
    ZTracer::Trace trace;
    return flush_set(oset, extents, &trace, onfinish);
  }
};


inline std::ostream& operator<<(std::ostream &out,
				const ObjectCacher::BufferHead &bh)
{
  out << "bh[ " << &bh << " "
      << bh.start() << "~" << bh.length()
      << " " << bh.ob
      << " (" << bh.bl.length() << ")"
      << " v " << bh.last_write_tid;
  if (bh.get_journal_tid() != 0) {
    out << " j " << bh.get_journal_tid();
  }
  if (bh.is_tx()) out << " tx";
  if (bh.is_rx()) out << " rx";
  if (bh.is_dirty()) out << " dirty";
  if (bh.is_clean()) out << " clean";
  if (bh.is_zero()) out << " zero";
  if (bh.is_missing()) out << " missing";
  if (bh.bl.length() > 0) out << " firstbyte=" << (int)bh.bl[0];
  if (bh.error) out << " error=" << bh.error;
  out << "]";
  out << " waiters = {";
  for (auto it = bh.waitfor_read.begin(); it != bh.waitfor_read.end(); ++it) {
    out << " " << it->first << "->[";
    for (auto lit = it->second.begin();
	 lit != it->second.end(); ++lit) {
	 out << *lit << ", ";
    }
    out << "]";
  }
  out << "}";
  return out;
}

inline std::ostream& operator<<(std::ostream &out,
				const ObjectCacher::ObjectSet &os)
{
  return out << "objectset[" << os.ino
	     << " ts " << os.truncate_seq << "/" << os.truncate_size
	     << " objects " << os.objects.size()
	     << " dirty_or_tx " << os.dirty_or_tx
	     << "]";
}

inline std::ostream& operator<<(std::ostream &out,
				const ObjectCacher::Object &ob)
{
  out << "object["
      << ob.get_soid() << " oset " << ob.oset << std::dec
      << " wr " << ob.last_write_tid << "/" << ob.last_commit_tid;

  if (ob.complete)
    out << " COMPLETE";
  if (!ob.exists)
    out << " !EXISTS";

  out << "]";
  return out;
}

#endif
