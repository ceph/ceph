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

#ifndef CEPH_OSD_KSTORE_H
#define CEPH_OSD_KSTORE_H

#include "acconfig.h"

#include <unistd.h>

#include <atomic>
#include <mutex>
#include <condition_variable>

#include "include/assert.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/Finisher.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "os/ObjectStore.h"
#include "common/perf_counters.h"
#include "os/fs/FS.h"
#include "kv/KeyValueDB.h"

#include "kstore_types.h"

#include "boost/intrusive/list.hpp"

enum {  
  l_kstore_first = 832430,
  l_kstore_state_prepare_lat,
  l_kstore_state_kv_queued_lat,
  l_kstore_state_kv_done_lat,
  l_kstore_state_finishing_lat,
  l_kstore_state_done_lat,
  l_kstore_last
};

class KStore : public ObjectStore {
  // -----------------------------------------------------
  // types
public:

  class TransContext;

  /// an in-memory object
  struct Onode {
    std::atomic_int nref;  ///< reference count

    ghobject_t oid;
    string key;     ///< key under PREFIX_OBJ where we are stored
    boost::intrusive::list_member_hook<> lru_item;

    kstore_onode_t onode;  ///< metadata stored as value in kv store
    bool dirty;     // ???
    bool exists;

    std::mutex flush_lock;  ///< protect flush_txns
    std::condition_variable flush_cond;   ///< wait here for unapplied txns
    set<TransContext*> flush_txns;   ///< committing txns

    uint64_t tail_offset;
    bufferlist tail_bl;

    map<uint64_t,bufferlist> pending_stripes;  ///< unwritten stripes

    Onode(const ghobject_t& o, const string& k)
      : nref(0),
	oid(o),
	key(k),
	dirty(false),
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
    void clear_pending_stripes() {
      pending_stripes.clear();
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

  struct Collection {
    KStore *store;
    coll_t cid;
    kstore_cnode_t cnode;
    RWLock lock;

    // cache onodes on a per-collection basis to avoid lock
    // contention.
    OnodeHashLRU onode_map;

    OnodeRef get_onode(const ghobject_t& oid, bool create);

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

    Collection(KStore *ns, coll_t c);
  };
  typedef ceph::shared_ptr<Collection> CollectionRef;

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
    KeyValueDB::Transaction t; ///< then we will commit this
    Context *oncommit;         ///< signal on commit
    Context *onreadable;         ///< signal on readable
    Context *onreadable_sync;         ///< signal on readable
    list<Context*> oncommits;  ///< more commit completions
    list<CollectionRef> removed_collections; ///< colls we removed

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
        start(ceph_clock_now(g_ceph_context)){
      //cout << "txc new " << this << std::endl;
    }
    ~TransContext() {
      //cout << "txc del " << this << std::endl;
    }

    void write_onode(OnodeRef &o) {
      onodes.insert(o);
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

    Sequencer *parent;

    OpSequencer()
	//set the qlock to PTHREAD_MUTEX_RECURSIVE mode
      : parent(NULL) {
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

  struct KVSyncThread : public Thread {
    KStore *store;
    explicit KVSyncThread(KStore *s) : store(s) {}
    void *entry() {
      store->_kv_sync_thread();
      return NULL;
    }
  };

  // --------------------------------------------------------
  // members
private:
  CephContext *cct;
  KeyValueDB *db;
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

  Finisher finisher;

  KVSyncThread kv_sync_thread;
  std::mutex kv_lock;
  std::condition_variable kv_cond, kv_sync_cond;
  bool kv_stop;
  deque<TransContext*> kv_queue, kv_committing;

  //Logger *logger;
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
  int _open_db(bool create);
  void _close_db();
  int _open_collections(int *errors=0);
  void _close_collections();

  int _open_super_meta();

  CollectionRef _get_collection(coll_t cid);
  void _queue_reap_collection(CollectionRef& c);
  void _reap_collections();

  void _assign_nid(TransContext *txc, OnodeRef o);

  void _dump_onode(OnodeRef o);

  TransContext *_txc_create(OpSequencer *osr);
  void _txc_release(TransContext *txc, uint64_t offset, uint64_t length);
  void _txc_add_transaction(TransContext *txc, Transaction *t);
  void _txc_finalize(OpSequencer *osr, TransContext *txc);
  void _txc_state_proc(TransContext *txc);
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

  void _do_read_stripe(OnodeRef o, uint64_t offset, bufferlist *pbl);
  void _do_write_stripe(TransContext *txc, OnodeRef o,
			uint64_t offset, bufferlist& bl);
  void _do_remove_stripe(TransContext *txc, OnodeRef o, uint64_t offset);

public:
  KStore(CephContext *cct, const string& path);
  ~KStore();

  string get_type() {
    return "kstore";
  }

  bool needs_journal() { return false; };
  bool wants_journal() { return false; };
  bool allows_journal() { return false; };

  static int get_block_device_fsid(const string& path, uuid_d *fsid);

  bool test_mount_in_use();

  int mount();
  int umount();
  void _sync();

  int fsck();


  int validate_hobject_key(const hobject_t &obj) const override {
    return 0;
  }
  unsigned get_max_attr_name_length() {
    return 256;  // arbitrary; there is no real limit internally
  }

  int mkfs();
  int mkjournal() {
    return 0;
  }

  int statfs(struct statfs *buf);

  using ObjectStore::exists;
  bool exists(const coll_t& cid, const ghobject_t& oid);
  using ObjectStore::stat;
  int stat(
    const coll_t& cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false); // struct stat?
  using ObjectStore::read;
  int read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false);
  int _do_read(
    OnodeRef o,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0);

  using ObjectStore::fiemap;
  int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
  using ObjectStore::getattr;
  int getattr(const coll_t& cid, const ghobject_t& oid, const char *name, bufferptr& value);
  using ObjectStore::getattrs;
  int getattrs(const coll_t& cid, const ghobject_t& oid, map<string,bufferptr>& aset);

  int list_collections(vector<coll_t>& ls);
  bool collection_exists(const coll_t& c);
  bool collection_empty(const coll_t& c);

  using ObjectStore::collection_list;
  int collection_list(const coll_t& cid, ghobject_t start, ghobject_t end,
		      bool sort_bitwise, int max,
		      vector<ghobject_t> *ls, ghobject_t *next);

  using ObjectStore::omap_get;
  int omap_get(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    );

  using ObjectStore::omap_get_header;
  /// Get omap header
  int omap_get_header(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    );

  using ObjectStore::omap_get_keys;
  /// Get keys defined on oid
  int omap_get_keys(
    const coll_t& cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    );

  using ObjectStore::omap_get_values;
  /// Get key values
  int omap_get_values(
    const coll_t& cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    );

  using ObjectStore::omap_check_keys;
  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    );

  using ObjectStore::get_omap_iterator;
  ObjectMap::ObjectMapIterator get_omap_iterator(
    const coll_t& cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    );

  void set_fsid(uuid_d u) {
    fsid = u;
  }
  uuid_d get_fsid() {
    return fsid;
  }

  uint64_t estimate_objects_overhead(uint64_t num_objects) override {
    return num_objects * 300; //assuming per-object overhead is 300 bytes
  }

  objectstore_perf_stat_t get_cur_stats() {
    return objectstore_perf_stat_t();
  }

  int queue_transactions(
    Sequencer *osr,
    vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL);

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
  int _do_write(TransContext *txc,
		OnodeRef o,
		uint64_t offset, uint64_t length,
		bufferlist& bl,
		uint32_t fadvise_flags);
  int _touch(TransContext *txc,
	     CollectionRef& c,
	     OnodeRef& o);
  int _zero(TransContext *txc,
	    CollectionRef& c,
	    OnodeRef& o,
	    uint64_t offset, size_t len);
  int _do_truncate(TransContext *txc,
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

inline ostream& operator<<(ostream& out, const KStore::OpSequencer& s) {
  return out << *s.parent;
}

static inline void intrusive_ptr_add_ref(KStore::Onode *o) {
  o->get();
}
static inline void intrusive_ptr_release(KStore::Onode *o) {
  o->put();
}

static inline void intrusive_ptr_add_ref(KStore::OpSequencer *o) {
  o->get();
}
static inline void intrusive_ptr_release(KStore::OpSequencer *o) {
  o->put();
}

#endif
