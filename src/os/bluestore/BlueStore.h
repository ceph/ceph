// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_BLUESTORE_H
#define CEPH_OSD_BLUESTORE_H

#include "acconfig.h"

#include <unistd.h>

#include <atomic>
#include <mutex>
#include <condition_variable>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/functional/hash.hpp>

#include "include/assert.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/Finisher.h"
#include "os/ObjectStore.h"

#include "bluestore_types.h"
#include "BlockDevice.h"
class Allocator;
class FreelistManager;
class BlueFS;

enum {
  l_bluestore_first = 732430,
  l_bluestore_state_prepare_lat,
  l_bluestore_state_aio_wait_lat,
  l_bluestore_state_io_done_lat,
  l_bluestore_state_kv_queued_lat,
  l_bluestore_state_kv_committing_lat,
  l_bluestore_state_kv_done_lat,
  l_bluestore_state_wal_queued_lat,
  l_bluestore_state_wal_applying_lat,
  l_bluestore_state_wal_aio_wait_lat,
  l_bluestore_state_wal_cleanup_lat,
  l_bluestore_state_wal_done_lat,
  l_bluestore_state_finishing_lat,
  l_bluestore_state_done_lat,
  l_bluestore_last
};

class BlueStore : public ObjectStore {
  // -----------------------------------------------------
  // types
public:

  class TransContext;

  /// an in-memory extent-map, shared by a group of objects (w/ same hash value)
  struct EnodeSet;

  struct Enode : public boost::intrusive::unordered_set_base_hook<> {
    std::atomic_int nref;        ///< reference count
    uint32_t hash;
    string key;           ///< key under PREFIX_OBJ where we are stored
    EnodeSet *enode_set;  ///< reference to the containing set

    bluestore_extent_ref_map_t ref_map;

    Enode(uint32_t h, const string& k, EnodeSet *s)
      : nref(0),
	hash(h),
	key(k),
	enode_set(s) {}

    void get() {
      ++nref;
    }
    void put();

    friend void intrusive_ptr_add_ref(Enode *e) { e->get(); }
    friend void intrusive_ptr_release(Enode *e) { e->put(); }

    friend bool operator==(const Enode &l, const Enode &r) {
      return l.hash == r.hash;
    }
    friend std::size_t hash_value(const Enode &e) {
      return e.hash;
    }
  };
  typedef boost::intrusive_ptr<Enode> EnodeRef;

  /// hash of Enodes, by (object) hash value
  struct EnodeSet {
    typedef boost::intrusive::unordered_set<Enode>::bucket_type bucket_type;
    typedef boost::intrusive::unordered_set<Enode>::bucket_traits bucket_traits;

    unsigned num_buckets;
    vector<bucket_type> buckets;

    boost::intrusive::unordered_set<Enode> uset;

    explicit EnodeSet(unsigned n)
      : num_buckets(n),
	buckets(n),
	uset(bucket_traits(buckets.data(), num_buckets)) {
      assert(n > 0);
    }
    ~EnodeSet() {
      assert(uset.empty());
    }
  };

  /// an in-memory object
  struct Onode {
    std::atomic_int nref;  ///< reference count

    ghobject_t oid;
    string key;     ///< key under PREFIX_OBJ where we are stored
    boost::intrusive::list_member_hook<> lru_item;

    EnodeRef enode;  ///< ref to Enode [optional]

    bluestore_onode_t onode;  ///< metadata stored as value in kv store
    bool exists;

    std::mutex flush_lock;  ///< protect flush_txns
    std::condition_variable flush_cond;   ///< wait here for unapplied txns
    set<TransContext*> flush_txns;   ///< committing or wal txns

    uint64_t tail_offset;
    bufferlist tail_bl;

    Onode(const ghobject_t& o, const string& k)
      : nref(0),
	oid(o),
	key(k),
	exists(false) {
    }

    void flush();
    void get() {
      ++nref;
    }
    void put() {
      if (--nref == 0)
	delete this;
    }

    void clear_tail() {
      tail_offset = 0;
      tail_bl.clear();
    }
  };
  typedef boost::intrusive_ptr<Onode> OnodeRef;

  struct OnodeHashLRU {
    typedef boost::intrusive::list<
      Onode,
      boost::intrusive::member_hook<
        Onode,
	boost::intrusive::list_member_hook<>,
	&Onode::lru_item> > lru_list_t;

    std::mutex lock;
    ceph::unordered_map<ghobject_t,OnodeRef> onode_map;  ///< forward lookups
    lru_list_t lru;                                      ///< lru

    OnodeHashLRU() {}

    void add(const ghobject_t& oid, OnodeRef o);
    void _touch(OnodeRef o);
    OnodeRef lookup(const ghobject_t& o);
    void rename(const ghobject_t& old_oid, const ghobject_t& new_oid);
    void clear();
    bool get_next(const ghobject_t& after, pair<ghobject_t,OnodeRef> *next);
    int trim(int max=-1);
  };

  struct Collection : public CollectionImpl {
    BlueStore *store;
    coll_t cid;
    bluestore_cnode_t cnode;
    RWLock lock;

    bool exists;

    // cache onodes on a per-collection basis to avoid lock
    // contention.
    OnodeHashLRU onode_map;

    EnodeSet enode_set;      ///< open Enodes

    OnodeRef get_onode(const ghobject_t& oid, bool create);
    EnodeRef get_enode(uint32_t hash);

    const coll_t &get_cid() override {
      return cid;
    }

    bool contains(const ghobject_t& oid) {
      if (cid.is_meta())
	return oid.hobj.pool == -1;
      spg_t spgid;
      if (cid.is_pg(&spgid))
	return
	  spgid.pgid.contains(cnode.bits, oid) &&
	  oid.shard_id == spgid.shard;
      return false;
    }

    Collection(BlueStore *ns, coll_t c);
  };
  typedef boost::intrusive_ptr<Collection> CollectionRef;

  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    CollectionRef c;
    OnodeRef o;
    KeyValueDB::Iterator it;
    string head, tail;
  public:
    OmapIteratorImpl(CollectionRef c, OnodeRef o, KeyValueDB::Iterator it);
    int seek_to_first();
    int upper_bound(const string &after);
    int lower_bound(const string &to);
    bool valid();
    int next(bool validate=true);
    string key();
    bufferlist value();
    int status() {
      return 0;
    }
  };

  class OpSequencer;
  typedef boost::intrusive_ptr<OpSequencer> OpSequencerRef;

  struct TransContext {
    typedef enum {
      STATE_PREPARE,
      STATE_AIO_WAIT,
      STATE_IO_DONE,
      STATE_KV_QUEUED,
      STATE_KV_COMMITTING,
      STATE_KV_DONE,
      STATE_WAL_QUEUED,
      STATE_WAL_APPLYING,
      STATE_WAL_AIO_WAIT,
      STATE_WAL_CLEANUP,   // remove wal kv record
      STATE_WAL_DONE,
      STATE_FINISHING,
      STATE_DONE,
    } state_t;

    state_t state;

    const char *get_state_name() {
      switch (state) {
      case STATE_PREPARE: return "prepare";
      case STATE_AIO_WAIT: return "aio_wait";
      case STATE_IO_DONE: return "io_done";
      case STATE_KV_QUEUED: return "kv_queued";
      case STATE_KV_COMMITTING: return "kv_committing";
      case STATE_KV_DONE: return "kv_done";
      case STATE_WAL_QUEUED: return "wal_queued";
      case STATE_WAL_APPLYING: return "wal_applying";
      case STATE_WAL_AIO_WAIT: return "wal_aio_wait";
      case STATE_WAL_CLEANUP: return "wal_cleanup";
      case STATE_WAL_DONE: return "wal_done";
      case STATE_FINISHING: return "finishing";
      case STATE_DONE: return "done";
      }
      return "???";
    }

    void log_state_latency(PerfCounters *logger, int state) {
      utime_t lat, now = ceph_clock_now(g_ceph_context);
      lat = now - start;
      logger->tinc(state, lat);
      start = now;
    }

    OpSequencerRef osr;
    boost::intrusive::list_member_hook<> sequencer_item;

    uint64_t ops, bytes;

    set<OnodeRef> onodes;     ///< these onodes need to be updated/written
    set<EnodeRef> enodes;     ///< these enodes need to be updated/written
    KeyValueDB::Transaction t; ///< then we will commit this
    Context *oncommit;         ///< signal on commit
    Context *onreadable;         ///< signal on readable
    Context *onreadable_sync;         ///< signal on readable
    list<Context*> oncommits;  ///< more commit completions
    list<CollectionRef> removed_collections; ///< colls we removed

    boost::intrusive::list_member_hook<> wal_queue_item;
    bluestore_wal_transaction_t *wal_txn; ///< wal transaction (if any)
    vector<OnodeRef> wal_op_onodes;

    interval_set<uint64_t> allocated, released;

    IOContext ioc;

    CollectionRef first_collection;  ///< first referenced collection

    utime_t start;

    explicit TransContext(OpSequencer *o)
      : state(STATE_PREPARE),
	osr(o),
	ops(0),
	bytes(0),
	oncommit(NULL),
	onreadable(NULL),
	onreadable_sync(NULL),
	wal_txn(NULL),
	ioc(this),
	start(ceph_clock_now(g_ceph_context)) {
      //cout << "txc new " << this << std::endl;
    }
    ~TransContext() {
      delete wal_txn;
      //cout << "txc del " << this << std::endl;
    }

    void write_onode(OnodeRef &o) {
      onodes.insert(o);
    }
    void write_enode(EnodeRef &e) {
      enodes.insert(e);
    }
  };

  class OpSequencer : public Sequencer_impl {
  public:
    std::mutex qlock;
    std::condition_variable qcond;
    typedef boost::intrusive::list<
      TransContext,
      boost::intrusive::member_hook<
        TransContext,
	boost::intrusive::list_member_hook<>,
	&TransContext::sequencer_item> > q_list_t;
    q_list_t q;  ///< transactions

    typedef boost::intrusive::list<
      TransContext,
      boost::intrusive::member_hook<
	TransContext,
	boost::intrusive::list_member_hook<>,
	&TransContext::wal_queue_item> > wal_queue_t;
    wal_queue_t wal_q; ///< transactions

    boost::intrusive::list_member_hook<> wal_osr_queue_item;

    Sequencer *parent;

    std::mutex wal_apply_mutex;
    std::unique_lock<std::mutex> wal_apply_lock;

    OpSequencer()
	//set the qlock to to PTHREAD_MUTEX_RECURSIVE mode
      : parent(NULL),
	wal_apply_lock(wal_apply_mutex, std::defer_lock) {
    }
    ~OpSequencer() {
      assert(q.empty());
    }

    void queue_new(TransContext *txc) {
      std::lock_guard<std::mutex> l(qlock);
      q.push_back(*txc);
    }

    void flush() {
      std::unique_lock<std::mutex> l(qlock);
      while (!q.empty())
	qcond.wait(l);
    }

    bool flush_commit(Context *c) {
      std::lock_guard<std::mutex> l(qlock);
      if (q.empty()) {
	return true;
      }
      TransContext *txc = &q.back();
      if (txc->state >= TransContext::STATE_KV_DONE) {
	return true;
      }
      assert(txc->state < TransContext::STATE_KV_DONE);
      txc->oncommits.push_back(c);
      return false;
    }
  };

  class WALWQ : public ThreadPool::WorkQueue<TransContext> {
    // We need to order WAL items within each Sequencer.  To do that,
    // queue each txc under osr, and queue the osr's here.  When we
    // dequeue an txc, requeue the osr if there are more pending, and
    // do it at the end of the list so that the next thread does not
    // get a conflicted txc.  Hold an osr mutex while doing the wal to
    // preserve the ordering.
  public:
    typedef boost::intrusive::list<
      OpSequencer,
      boost::intrusive::member_hook<
	OpSequencer,
	boost::intrusive::list_member_hook<>,
	&OpSequencer::wal_osr_queue_item> > wal_osr_queue_t;

  private:
    BlueStore *store;
    wal_osr_queue_t wal_queue;

  public:
    WALWQ(BlueStore *s, time_t ti, time_t sti, ThreadPool *tp)
      : ThreadPool::WorkQueue<TransContext>("BlueStore::WALWQ", ti, sti, tp),
	store(s) {
    }
    bool _empty() {
      return wal_queue.empty();
    }
    bool _enqueue(TransContext *i) {
      if (i->osr->wal_q.empty()) {
	wal_queue.push_back(*i->osr);
      }
      i->osr->wal_q.push_back(*i);
      return true;
    }
    void _dequeue(TransContext *p) {
      assert(0 == "not needed, not implemented");
    }
    TransContext *_dequeue() {
      if (wal_queue.empty())
	return NULL;
      OpSequencer *osr = &wal_queue.front();
      TransContext *i = &osr->wal_q.front();
      osr->wal_q.pop_front();
      wal_queue.pop_front();
      if (!osr->wal_q.empty()) {
	// requeue at the end to minimize contention
	wal_queue.push_back(*i->osr);
      }

      // preserve wal ordering for this sequencer by taking the lock
      // while still holding the queue lock
      i->osr->wal_apply_lock.lock();
      return i;
    }
    void _process(TransContext *i, ThreadPool::TPHandle &) override {
      store->_wal_apply(i);
      i->osr->wal_apply_lock.unlock();
    }
    void _clear() {
      assert(wal_queue.empty());
    }

    void flush() {
      lock();
      while (!wal_queue.empty()) {
	_wait();
      }
      unlock();
      drain();
    }
  };

  struct KVSyncThread : public Thread {
    BlueStore *store;
    explicit KVSyncThread(BlueStore *s) : store(s) {}
    void *entry() {
      store->_kv_sync_thread();
      return NULL;
    }
  };

  // --------------------------------------------------------
  // members
private:
  CephContext *cct;
  BlueFS *bluefs;
  unsigned bluefs_shared_bdev;  ///< which bluefs bdev we are sharing
  KeyValueDB *db;
  BlockDevice *bdev;
  FreelistManager *fm;
  Allocator *alloc;
  uuid_d fsid;
  int path_fd;  ///< open handle to $path
  int fsid_fd;  ///< open handle (locked) to $path/fsid
  bool mounted;

  RWLock coll_lock;    ///< rwlock to protect coll_map
  ceph::unordered_map<coll_t, CollectionRef> coll_map;

  std::mutex nid_lock;
  uint64_t nid_last;
  uint64_t nid_max;

  Throttle throttle_ops, throttle_bytes;          ///< submit to commit
  Throttle throttle_wal_ops, throttle_wal_bytes;  ///< submit to wal complete

  interval_set<uint64_t> bluefs_extents;  ///< block extents owned by bluefs

  std::mutex wal_lock;
  atomic64_t wal_seq;
  ThreadPool wal_tp;
  WALWQ wal_wq;

  Finisher finisher;

  KVSyncThread kv_sync_thread;
  std::mutex kv_lock;
  std::condition_variable kv_cond, kv_sync_cond;
  bool kv_stop;
  deque<TransContext*> kv_queue, kv_committing;
  deque<TransContext*> wal_cleanup_queue, wal_cleaning;

  PerfCounters *logger;

  std::mutex reap_lock;
  list<CollectionRef> removed_collections;


  // --------------------------------------------------------
  // private methods

  void _init_logger();
  void _shutdown_logger();

  int _open_path();
  void _close_path();
  int _open_fsid(bool create);
  int _lock_fsid();
  int _read_fsid(uuid_d *f);
  int _write_fsid();
  void _close_fsid();
  int _open_bdev(bool create);
  void _close_bdev();
  int _open_db(bool create);
  void _close_db();
  int _open_alloc();
  void _close_alloc();
  int _open_collections(int *errors=0);
  void _close_collections();

  int _setup_block_symlink_or_file(string name, string path, uint64_t size,
				   bool create);

  int _write_bdev_label(string path, bluestore_bdev_label_t label);
  static int _read_bdev_label(string path, bluestore_bdev_label_t *label);
  int _check_or_set_bdev_label(string path, uint64_t size, string desc,
			       bool create);

  int _open_super_meta();

  int _reconcile_bluefs_freespace();
  int _balance_bluefs_freespace(vector<bluestore_extent_t> *extents,
				KeyValueDB::Transaction t);
  void _commit_bluefs_freespace(const vector<bluestore_extent_t>& extents);

  CollectionRef _get_collection(const coll_t& cid);
  void _queue_reap_collection(CollectionRef& c);
  void _reap_collections();

  void _assign_nid(TransContext *txc, OnodeRef o);

  void _dump_onode(OnodeRef o, int log_leverl=30);

  TransContext *_txc_create(OpSequencer *osr);
  void _txc_release(TransContext *txc, CollectionRef& c, OnodeRef& onode,
		    uint64_t offset, uint64_t length,
		    bool shared);
  void _txc_add_transaction(TransContext *txc, Transaction *t);
  int _txc_finalize(OpSequencer *osr, TransContext *txc);
  void _txc_state_proc(TransContext *txc);
  void _txc_aio_submit(TransContext *txc);
  void _txc_update_fm(TransContext *txc);
public:
  void _txc_aio_finish(void *p) {
    _txc_state_proc(static_cast<TransContext*>(p));
  }
private:
  void _txc_finish_io(TransContext *txc);
  void _txc_finish_kv(TransContext *txc);
  void _txc_finish(TransContext *txc);

  void _osr_reap_done(OpSequencer *osr);

  void _kv_sync_thread();
  void _kv_stop() {
    {
      std::lock_guard<std::mutex> l(kv_lock);
      kv_stop = true;
      kv_cond.notify_all();
    }
    kv_sync_thread.join();
    kv_stop = false;
  }

  bluestore_wal_op_t *_get_wal_op(TransContext *txc, OnodeRef o);
  int _wal_apply(TransContext *txc);
  int _wal_finish(TransContext *txc);
  int _do_wal_op(bluestore_wal_op_t& wo, IOContext *ioc);
  int _wal_replay();

  // for fsck
  int _verify_enode_shared(EnodeRef enode, vector<bluestore_extent_t>& v);

public:
  BlueStore(CephContext *cct, const string& path);
  ~BlueStore();

  string get_type() override {
    return "bluestore";
  }

  bool needs_journal() override { return false; };
  bool wants_journal() override { return false; };
  bool allows_journal() override { return false; };

  static int get_block_device_fsid(const string& path, uuid_d *fsid);

  bool test_mount_in_use() override;

  int mount() override;
  int umount() override;
  void _sync();

  int fsck() override;

  unsigned get_max_object_name_length() override {
    return 4096;
  }
  unsigned get_max_attr_name_length() override {
    return 256;  // arbitrary; there is no real limit internally
  }

  int mkfs() override;
  int mkjournal() override {
    return 0;
  }

public:
  int statfs(struct statfs *buf) override;

  bool exists(const coll_t& cid, const ghobject_t& oid) override;
  bool exists(CollectionHandle &c, const ghobject_t& oid) override;
  int stat(
    const coll_t& cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false) override;
  int stat(
    CollectionHandle &c,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false) override;
  int read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false) override;
  int read(
    CollectionHandle &c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false) override;
  int _do_read(
    OnodeRef o,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0);

  int fiemap(const coll_t& cid, const ghobject_t& oid,
	     uint64_t offset, size_t len, bufferlist& bl) override;
  int fiemap(CollectionHandle &c, const ghobject_t& oid,
	     uint64_t offset, size_t len, bufferlist& bl) override;

  int getattr(const coll_t& cid, const ghobject_t& oid, const char *name,
	      bufferptr& value) override;
  int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name,
	      bufferptr& value) override;

  int getattrs(const coll_t& cid, const ghobject_t& oid,
	       map<string,bufferptr>& aset) override;
  int getattrs(CollectionHandle &c, const ghobject_t& oid,
	       map<string,bufferptr>& aset) override;

  int list_collections(vector<coll_t>& ls) override;

  CollectionHandle open_collection(const coll_t &c) override;

  bool collection_exists(const coll_t& c) override;
  bool collection_empty(const coll_t& c) override;
  int collection_bits(const coll_t& c) override;

  int collection_list(const coll_t& cid, ghobject_t start, ghobject_t end,
		      bool sort_bitwise, int max,
		      vector<ghobject_t> *ls, ghobject_t *next) override;
  int collection_list(CollectionHandle &c, ghobject_t start, ghobject_t end,
		      bool sort_bitwise, int max,
		      vector<ghobject_t> *ls, ghobject_t *next) override;

  int omap_get(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    ) override;
  int omap_get(
    CollectionHandle &c,     ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    ) override;

  /// Get omap header
  int omap_get_header(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) override;
  int omap_get_header(
    CollectionHandle &c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) override;

  /// Get keys defined on oid
  int omap_get_keys(
    const coll_t& cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    ) override;
  int omap_get_keys(
    CollectionHandle &c,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    ) override;

  /// Get key values
  int omap_get_values(
    const coll_t& cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    ) override;
  int omap_get_values(
    CollectionHandle &c,         ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    ) override;

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    ) override;
  int omap_check_keys(
    CollectionHandle &c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    ) override;

  ObjectMap::ObjectMapIterator get_omap_iterator(
    const coll_t& cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    ) override;
  ObjectMap::ObjectMapIterator get_omap_iterator(
    CollectionHandle &c,   ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    ) override;

  void set_fsid(uuid_d u) override {
    fsid = u;
  }
  uuid_d get_fsid() override {
    return fsid;
  }

  objectstore_perf_stat_t get_cur_stats() override {
    return objectstore_perf_stat_t();
  }

  int queue_transactions(
    Sequencer *osr,
    vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL) override;

private:
  // --------------------------------------------------------
  // write ops

  int _do_transaction(Transaction *t,
		      TransContext *txc,
		      ThreadPool::TPHandle *handle);

  int _write(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& o,
	     uint64_t offset, size_t len,
	     bufferlist& bl,
	     uint32_t fadvise_flags);
  bool _can_overlay_write(OnodeRef o, uint64_t length);
  int _do_overlay_trim(TransContext *txc,
		       OnodeRef o,
		       uint64_t offset,
		       uint64_t length);
  int _do_overlay_write(TransContext *txc,
			OnodeRef o,
			uint64_t offset,
			uint64_t length,
			const bufferlist& bl);
  int _do_write_overlays(TransContext *txc, CollectionRef& c, OnodeRef o,
			 uint64_t offset, uint64_t length);
  void _do_read_all_overlays(bluestore_wal_op_t& wo);
  void _pad_zeros(OnodeRef o, bufferlist *bl, uint64_t *offset, uint64_t *length,
		  uint64_t block_size);
  void _pad_zeros_head(OnodeRef o, bufferlist *bl,
		       uint64_t *offset, uint64_t *length,
		       uint64_t block_size);
  void _pad_zeros_tail(OnodeRef o, bufferlist *bl,
		       uint64_t offset, uint64_t *length,
		       uint64_t block_size);
  int _do_allocate(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef o,
		   uint64_t offset, uint64_t length,
		   uint32_t fadvise_flags,
		   bool allow_overlay,
		   uint64_t *rmw_cow_head,
		   uint64_t *rmw_cow_tail);
  int _do_write(TransContext *txc,
		CollectionRef &c,
		OnodeRef o,
		uint64_t offset, uint64_t length,
		bufferlist& bl,
		uint32_t fadvise_flags);
  int _touch(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& o);
  int _do_write_zero(TransContext *txc,
		     CollectionRef &c,
		     OnodeRef o,
		     uint64_t offset, uint64_t length);
  int _zero(TransContext *txc,
	    CollectionRef& c,
	    OnodeRef& o,
	    uint64_t offset, size_t len);
  int _do_truncate(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef o,
		   uint64_t offset);
  int _truncate(TransContext *txc,
		CollectionRef& c,
		OnodeRef& o,
		uint64_t offset);
  int _remove(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& o);
  int _do_remove(TransContext *txc,
		 CollectionRef& c,
		 OnodeRef o);
  int _setattr(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o,
	       const string& name,
	       bufferptr& val);
  int _setattrs(TransContext *txc,
		CollectionRef& c,
		OnodeRef& o,
		const map<string,bufferptr>& aset);
  int _rmattr(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& o,
	      const string& name);
  int _rmattrs(TransContext *txc,
	       CollectionRef& c,
	       OnodeRef& o);
  void _do_omap_clear(TransContext *txc, uint64_t id);
  int _omap_clear(TransContext *txc,
		  CollectionRef& c,
		  OnodeRef& o);
  int _omap_setkeys(TransContext *txc,
		    CollectionRef& c,
		    OnodeRef& o,
		    bufferlist& bl);
  int _omap_setheader(TransContext *txc,
		      CollectionRef& c,
		      OnodeRef& o,
		      bufferlist& header);
  int _omap_rmkeys(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef& o,
		   bufferlist& bl);
  int _omap_rmkey_range(TransContext *txc,
			CollectionRef& c,
			OnodeRef& o,
			const string& first, const string& last);
  int _setallochint(TransContext *txc,
		    CollectionRef& c,
		    OnodeRef& o,
		    uint64_t expected_object_size,
		    uint64_t expected_write_size);
  int _clone(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& oldo,
	     OnodeRef& newo);
  int _clone_range(TransContext *txc,
		   CollectionRef& c,
		   OnodeRef& oldo,
		   OnodeRef& newo,
		   uint64_t srcoff, uint64_t length, uint64_t dstoff);
  int _rename(TransContext *txc,
	      CollectionRef& c,
	      OnodeRef& oldo,
	      OnodeRef& newo,
	      const ghobject_t& new_oid);
  int _create_collection(TransContext *txc, coll_t cid, unsigned bits,
			 CollectionRef *c);
  int _remove_collection(TransContext *txc, coll_t cid, CollectionRef *c);
  int _split_collection(TransContext *txc,
			CollectionRef& c,
			CollectionRef& d,
			unsigned bits, int rem);

};

inline ostream& operator<<(ostream& out, const BlueStore::OpSequencer& s) {
  return out << *s.parent;
}

static inline void intrusive_ptr_add_ref(BlueStore::Onode *o) {
  o->get();
}
static inline void intrusive_ptr_release(BlueStore::Onode *o) {
  o->put();
}

static inline void intrusive_ptr_add_ref(BlueStore::OpSequencer *o) {
  o->get();
}
static inline void intrusive_ptr_release(BlueStore::OpSequencer *o) {
  o->put();
}

#endif
