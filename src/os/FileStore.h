// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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


#ifndef CEPH_FILESTORE_H
#define CEPH_FILESTORE_H

#include "ObjectStore.h"
#include "JournalingObjectStore.h"

#include "common/WorkQueue.h"

#include "common/Mutex.h"

#include "Fake.h"
//#include "FakeStoreBDBCollections.h"

#include <map>
#include <deque>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

// fake attributes in memory, if we need to.

class FileStore : public JournalingObjectStore {
  string basedir, journalpath;
  char current_fn[PATH_MAX];
  char current_op_seq_fn[PATH_MAX];
  uint64_t fsid;
  
  bool btrfs;
  bool btrfs_trans_start_end;
  bool btrfs_clone_range;
  bool btrfs_snap_create;
  bool btrfs_snap_destroy;
  bool btrfs_snap_create_v2;
  bool btrfs_wait_sync;
  bool ioctl_fiemap;
  int fsid_fd, op_fd;

  int basedir_fd, current_fd;
  deque<uint64_t> snaps;

  // fake attrs?
  FakeAttrs attrs;
  bool fake_attrs;

  // fake collections?
  FakeCollections collections;
  bool fake_collections;
  
  Finisher ondisk_finisher;

  // helper fns
  void append_oname(const sobject_t &oid, char *s, int len);
  //void get_oname(sobject_t oid, char *s);
  void get_cdir(coll_t cid, char *s, int len);
  void get_coname(coll_t cid, const sobject_t& oid, char *s, int len);
  bool parse_object(char *s, sobject_t& o);
  
  int lock_fsid();

  // sync thread
  Mutex lock;
  bool force_sync;
  Cond sync_cond;
  uint64_t sync_epoch;
  list<Context*> sync_waiters;
  bool stop;
  void sync_entry();
  struct SyncThread : public Thread {
    FileStore *fs;
    SyncThread(FileStore *f) : fs(f) {}
    void *entry() {
      fs->sync_entry();
      return 0;
    }
  } sync_thread;

  void sync_fs(); // actuall sync underlying fs

  // -- op workqueue --
  struct Op {
    uint64_t op;
    list<Transaction*> tls;
    Context *onreadable, *onreadable_sync;
    uint64_t ops, bytes;
  };
  class OpSequencer : public Sequencer_impl {
    Mutex qlock; // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    list<Op*> q;
    list<uint64_t> jq;
    Cond cond;
  public:
    Sequencer *parent;
    Mutex apply_lock;  // for apply mutual exclusion
    
    void queue_journal(uint64_t s) {
      Mutex::Locker l(qlock);
      jq.push_back(s);
    }
    void dequeue_journal() {
      Mutex::Locker l(qlock);
      jq.pop_front();
      cond.Signal();
    }
    void queue(Op *o) {
      Mutex::Locker l(qlock);
      q.push_back(o);
    }
    Op *peek_queue() {
      assert(apply_lock.is_locked());
      return q.front();
    }
    Op *dequeue() {
      assert(apply_lock.is_locked());
      Mutex::Locker l(qlock);
      Op *o = q.front();
      q.pop_front();
      cond.Signal();
      return o;
    }
    void flush() {
      Mutex::Locker l(qlock);

      // get max for journal _or_ op queues
      uint64_t seq = 0;
      if (!q.empty())
	seq = q.back()->op;
      if (!jq.empty() && jq.back() > seq)
	seq = jq.back();

      if (seq) {
	// everything prior to our watermark to drain through either/both queues
	while ((!q.empty() && q.front()->op <= seq) ||
	       (!jq.empty() && jq.front() <= seq))
	  cond.Wait(qlock);
      }
    }

    OpSequencer() : qlock("FileStore::OpSequencer::qlock", false, false),
		    apply_lock("FileStore::OpSequencer::apply_lock", false, false) {}
    ~OpSequencer() {
      assert(q.empty());
    }
  };
  Sequencer default_osr;
  deque<OpSequencer*> op_queue;
  uint64_t op_queue_len, op_queue_bytes;
  Cond op_throttle_cond;
  Finisher op_finisher;
  uint64_t next_finish;
  map<uint64_t, pair<Context*,Context*> > finish_queue;

  ThreadPool op_tp;
  struct OpWQ : public ThreadPool::WorkQueue<OpSequencer> {
    FileStore *store;
    OpWQ(FileStore *fs, ThreadPool *tp) : ThreadPool::WorkQueue<OpSequencer>("FileStore::OpWQ", tp), store(fs) {}

    bool _enqueue(OpSequencer *osr) {
      store->op_queue.push_back(osr);
      return true;
    }
    void _dequeue(OpSequencer *o) {
      assert(0);
    }
    bool _empty() {
      return store->op_queue.empty();
    }
    OpSequencer *_dequeue() {
      if (store->op_queue.empty())
	return NULL;
      OpSequencer *osr = store->op_queue.front();
      store->op_queue.pop_front();
      return osr;
    }
    void _process(OpSequencer *osr) {
      store->_do_op(osr);
    }
    void _process_finish(OpSequencer *osr) {
      store->_finish_op(osr);
    }
    void _clear() {
      assert(store->op_queue.empty());
    }
  } op_wq;

  void _do_op(OpSequencer *o);
  void _finish_op(OpSequencer *o);
  void queue_op(OpSequencer *osr, uint64_t op, list<Transaction*>& tls, Context *onreadable, Context *onreadable_sync);
  void op_queue_throttle();
  void _journaled_ahead(OpSequencer *osr, uint64_t op, list<Transaction*> &tls,
			Context *onreadable, Context *ondisk, Context *onreadable_sync);
  friend class C_JournaledAhead;

  // flusher thread
  Cond flusher_cond;
  list<uint64_t> flusher_queue;
  int flusher_queue_len;
  void flusher_entry();
  struct FlusherThread : public Thread {
    FileStore *fs;
    FlusherThread(FileStore *f) : fs(f) {}
    void *entry() {
      fs->flusher_entry();
      return 0;
    }
  } flusher_thread;
  bool queue_flusher(int fd, uint64_t off, uint64_t len);

  int open_journal();

 public:
  FileStore(const char *base, const char *jdev = 0) : 
    basedir(base), journalpath(jdev ? jdev:""),
    btrfs(false), btrfs_trans_start_end(false), btrfs_clone_range(false),
    btrfs_snap_create(false),
    btrfs_snap_destroy(false),
    btrfs_snap_create_v2(false),
    btrfs_wait_sync(false),
    ioctl_fiemap(false),
    fsid_fd(-1), op_fd(-1),
    attrs(this), fake_attrs(false), 
    collections(this), fake_collections(false),
    lock("FileStore::lock"),
    force_sync(false), sync_epoch(0), stop(false), sync_thread(this),
    op_queue_len(0), op_queue_bytes(0), next_finish(0),
    op_tp("FileStore::op_tp", g_conf.filestore_op_threads), op_wq(this, &op_tp),
    flusher_queue_len(0), flusher_thread(this) {
    // init current_fn
    snprintf(current_fn, sizeof(current_fn), "%s/current", basedir.c_str());
    snprintf(current_op_seq_fn, sizeof(current_op_seq_fn), "%s/current/commit_op_seq", basedir.c_str());
  }

  int _detect_fs();
  int _sanity_check_fs();
  
  bool test_mount_in_use();
  int read_op_seq(const char *fn, uint64_t *seq);
  int write_op_seq(int, uint64_t seq);
  int mount();
  int umount();
  int wipe_subvol(const char *s);
  int mkfs();
  int mkjournal();

  int statfs(struct statfs *buf);

  int do_transactions(list<Transaction*> &tls, uint64_t op_seq);
  unsigned apply_transaction(Transaction& t, Context *ondisk=0);
  unsigned apply_transactions(list<Transaction*>& tls, Context *ondisk=0);
  int _transaction_start(uint64_t bytes, uint64_t ops);
  void _transaction_finish(int id);
  unsigned _do_transaction(Transaction& t);

  int queue_transaction(Sequencer *osr, Transaction* t);
  int queue_transactions(Sequencer *osr, list<Transaction*>& tls, Context *onreadable, Context *ondisk=0,
			 Context *onreadable_sync=0);

  // ------------------
  // objects
  int pick_object_revision_lt(sobject_t& oid) {
    return 0;
  }
  bool exists(coll_t cid, const sobject_t& oid);
  int stat(coll_t cid, const sobject_t& oid, struct stat *st);
  int read(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
  int fiemap(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);

  int _touch(coll_t cid, const sobject_t& oid);
  int _write(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len, const bufferlist& bl);
  int _zero(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len);
  int _truncate(coll_t cid, const sobject_t& oid, uint64_t size);
  int _clone(coll_t cid, const sobject_t& oldoid, const sobject_t& newoid);
  int _clone_range(coll_t cid, const sobject_t& oldoid, const sobject_t& newoid, uint64_t off, uint64_t len);
  int _do_clone_range(int from, int to, uint64_t off, uint64_t len);
  int _remove(coll_t cid, const sobject_t& oid);

  void _start_sync();

  void start_sync();
  void start_sync(Context *onsafe);
  void sync();
  void _flush_op_queue();
  void flush();
  void sync_and_flush();

  // attrs
  int getattr(coll_t cid, const sobject_t& oid, const char *name, void *value, size_t size);
  int getattr(coll_t cid, const sobject_t& oid, const char *name, bufferptr &bp);
  int getattrs(coll_t cid, const sobject_t& oid, map<string,bufferptr>& aset, bool user_only = false);

  int _getattr(const char *fn, const char *name, bufferptr& bp);
  int _getattrs(const char *fn, map<string,bufferptr>& aset, bool user_only = false);

  int _setattr(coll_t cid, const sobject_t& oid, const char *name, const void *value, size_t size);
  int _setattrs(coll_t cid, const sobject_t& oid, map<string,bufferptr>& aset);
  int _rmattr(coll_t cid, const sobject_t& oid, const char *name);
  int _rmattrs(coll_t cid, const sobject_t& oid);

  int collection_getattr(coll_t c, const char *name, void *value, size_t size);
  int collection_getattr(coll_t c, const char *name, bufferlist& bl);
  int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);

  int _collection_setattr(coll_t c, const char *name, const void *value, size_t size);
  int _collection_rmattr(coll_t c, const char *name);
  int _collection_setattrs(coll_t cid, map<string,bufferptr> &aset);
  int _collection_rename(const coll_t &cid, const coll_t &ncid);

  // collections
  int list_collections(vector<coll_t>& ls);
  int collection_stat(coll_t c, struct stat *st);
  bool collection_exists(coll_t c);
  bool collection_empty(coll_t c);
  int collection_list_partial(coll_t c, snapid_t seq, vector<sobject_t>& o, int count, collection_list_handle_t *handle);
  int collection_list(coll_t c, vector<sobject_t>& o);

  int _create_collection(coll_t c);
  int _destroy_collection(coll_t c);
  int _collection_add(coll_t c, coll_t ocid, const sobject_t& o);
  int _collection_remove(coll_t c, const sobject_t& o);

  void trim_from_cache(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len) {}
  int is_cached(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len) { return -1; }
};

#endif
