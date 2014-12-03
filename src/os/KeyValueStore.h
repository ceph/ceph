// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_KEYVALUESTORE_H
#define CEPH_KEYVALUESTORE_H

#include "include/types.h"

#include <map>
#include <deque>
#include <boost/scoped_ptr.hpp>
#include <fstream>
using namespace std;

#include "include/assert.h"

#include "ObjectStore.h"

#include "common/WorkQueue.h"
#include "common/Finisher.h"
#include "common/fd.h"

#include "common/Mutex.h"
#include "GenericObjectMap.h"
#include "KeyValueDB.h"
#include "common/random_cache.hpp"

#include "include/uuid.h"

static uint64_t default_strip_size = 1024;

class StripObjectMap: public GenericObjectMap {
 public:

  struct StripExtent {
    uint64_t no;
    uint64_t offset;    // in key
    uint64_t len;    // in key
    StripExtent(uint64_t n, uint64_t off, size_t len):
      no(n), offset(off), len(len) {}
  };

  // -- strip object --
  struct StripObjectHeader {
    // Persistent state
    uint64_t strip_size;
    uint64_t max_size;
    vector<char> bits;

    // soft state
    Header header; // FIXME: Hold lock to avoid concurrent operations, it will
                   // also block read operation which not should be permitted.
    coll_t cid;
    ghobject_t oid;
    bool updated;
    bool deleted;

    StripObjectHeader(): strip_size(default_strip_size), max_size(0), updated(false), deleted(false) {}

    void encode(bufferlist &bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(strip_size, bl);
      ::encode(max_size, bl);
      ::encode(bits, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &bl) {
      DECODE_START(1, bl);
      ::decode(strip_size, bl);
      ::decode(max_size, bl);
      ::decode(bits, bl);
      DECODE_FINISH(bl);
    }
  };
  typedef ceph::shared_ptr<StripObjectHeader> StripObjectHeaderRef;

  static int file_to_extents(uint64_t offset, size_t len, uint64_t strip_size,
                             vector<StripExtent> &extents);
  int lookup_strip_header(const coll_t & cid, const ghobject_t &oid,
                          StripObjectHeaderRef *header);
  int save_strip_header(StripObjectHeaderRef header, KeyValueDB::Transaction t);
  int create_strip_header(const coll_t &cid, const ghobject_t &oid,
                          StripObjectHeaderRef *strip_header,
                          KeyValueDB::Transaction t);
  void clone_wrap(StripObjectHeaderRef old_header,
                  const coll_t &cid, const ghobject_t &oid,
                  KeyValueDB::Transaction t,
                  StripObjectHeaderRef *target_header);
  void rename_wrap(StripObjectHeaderRef old_header, const coll_t &cid, const ghobject_t &oid,
                   KeyValueDB::Transaction t,
                   StripObjectHeaderRef *new_header);
  // Already hold header to avoid lock header seq again
  int get_with_header(
    const StripObjectHeaderRef header,
    const string &prefix,
    map<string, bufferlist> *out
    );

  int get_values_with_header(
    const StripObjectHeaderRef header,
    const string &prefix,
    const set<string> &keys,
    map<string, bufferlist> *out
    );
  int get_keys_with_header(
    const StripObjectHeaderRef header,
    const string &prefix,
    set<string> *keys
    );

  Mutex lock;
  void invalidate_cache(const coll_t &c, const ghobject_t &oid) {
    Mutex::Locker l(lock);
    caches.clear(oid);
  }

  RandomCache<ghobject_t, pair<coll_t, StripObjectHeaderRef> > caches;
  StripObjectMap(KeyValueDB *db): GenericObjectMap(db),
                                  lock("StripObjectMap::lock"),
                                  caches(g_conf->keyvaluestore_header_cache_size)
  {}
};


class KVSuperblock {
public:
  CompatSet compat_features;
  string backend;

  KVSuperblock() { }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<KVSuperblock*>& o);
};
WRITE_CLASS_ENCODER(KVSuperblock)


inline ostream& operator<<(ostream& out, const KVSuperblock& sb)
{
  return out << "sb(" << sb.compat_features << " " << sb.backend << ")";
}


class KeyValueStore : public ObjectStore,
                      public md_config_obs_t {
 public:
  struct KVPerfTracker {
    PerfCounters::avg_tracker<uint64_t> os_commit_latency;
    PerfCounters::avg_tracker<uint64_t> os_apply_latency;

    objectstore_perf_stat_t get_cur_stats() const {
      objectstore_perf_stat_t ret;
      ret.filestore_commit_latency = os_commit_latency.avg();
      ret.filestore_apply_latency = os_apply_latency.avg();
      return ret;
    }

    void update_from_perfcounters(PerfCounters &logger) {
      os_commit_latency.consume_next(
        logger.get_tavg_ms(
          l_os_commit_lat));
      os_apply_latency.consume_next(
        logger.get_tavg_ms(
          l_os_apply_lat));
    }

  } perf_tracker;

  objectstore_perf_stat_t get_cur_stats() {
    perf_tracker.update_from_perfcounters(*perf_logger);
    return perf_tracker.get_cur_stats();
  }

  static const uint32_t target_version = 1;

 private:
  string internal_name; // internal name, used to name the perfcounter instance
  string basedir;
  std::string current_fn;
  std::string current_op_seq_fn;
  uuid_d fsid;

  int fsid_fd, current_fd;

  deque<uint64_t> snaps;

  // ObjectMap
  boost::scoped_ptr<StripObjectMap> backend;

  Finisher ondisk_finisher;

  Mutex lock;

  int _create_current();

  /// read a uuid from fd
  int read_fsid(int fd, uuid_d *uuid);

  /// lock fsid_fd
  int lock_fsid();

  string strip_object_key(uint64_t no) {
    char n[100];
    snprintf(n, 100, "%lld", (long long)no);
    return string(n);
  }

  // A special coll used by store collection info, each obj in this coll
  // represent a coll_t
  static bool is_coll_obj(coll_t c) {
    return c == coll_t("COLLECTIONS");
  }
  static coll_t get_coll_for_coll() {
    return coll_t("COLLECTIONS");
  }
  static ghobject_t make_ghobject_for_coll(const coll_t &col) {
    return ghobject_t(hobject_t(sobject_t(col.to_str(), CEPH_NOSNAP)));
  }

  // Each transaction has side effect which may influent the following
  // operations, we need to make it visible for the following within
  // transaction by caching middle result.
  // Side effects contains:
  // 1. Creating/Deleting collection
  // 2. Creating/Deleting object
  // 3. Object modify(including omap, xattr)
  // 4. Clone or rename
  struct BufferTransaction {
    typedef pair<coll_t, ghobject_t> uniq_id;
    typedef map<uniq_id, StripObjectMap::StripObjectHeaderRef> StripHeaderMap;

    //Dirty records
    StripHeaderMap strip_headers;
    map< uniq_id, map<pair<string, string>, bufferlist> > buffers;  // pair(prefix, key),to buffer updated data in one transaction

    list<Context*> finishes;

    KeyValueStore *store;

    KeyValueDB::Transaction t;

    int lookup_cached_header(const coll_t &cid, const ghobject_t &oid,
                             StripObjectMap::StripObjectHeaderRef *strip_header,
                             bool create_if_missing);
    int get_buffer_keys(StripObjectMap::StripObjectHeaderRef strip_header,
                        const string &prefix, const set<string> &keys,
                        map<string, bufferlist> *out);
    void set_buffer_keys(StripObjectMap::StripObjectHeaderRef strip_header,
                         const string &prefix, map<string, bufferlist> &bl);
    int remove_buffer_keys(StripObjectMap::StripObjectHeaderRef strip_header,
                           const string &prefix, const set<string> &keys);
    void clear_buffer_keys(StripObjectMap::StripObjectHeaderRef strip_header,
                           const string &prefix);
    int clear_buffer(StripObjectMap::StripObjectHeaderRef strip_header);
    void clone_buffer(StripObjectMap::StripObjectHeaderRef old_header,
                      const coll_t &cid, const ghobject_t &oid);
    void rename_buffer(StripObjectMap::StripObjectHeaderRef old_header,
                       const coll_t &cid, const ghobject_t &oid);
    int submit_transaction();

    BufferTransaction(KeyValueStore *store): store(store) {
      t = store->backend->get_transaction();
    }

    struct InvalidateCacheContext : public Context {
      KeyValueStore *store;
      const coll_t cid;
      const ghobject_t oid;
      InvalidateCacheContext(KeyValueStore *s, const coll_t &c, const ghobject_t &oid): store(s), cid(c), oid(oid) {}
      void finish(int r) {
      if (r == 0)
        store->backend->invalidate_cache(cid, oid);
      }
    };
  };

  // -- op workqueue --
  struct Op {
    utime_t start;
    uint64_t op;
    list<Transaction*> tls;
    Context *ondisk, *onreadable, *onreadable_sync;
    uint64_t ops, bytes;
    TrackedOpRef osd_op;
  };
  class OpSequencer : public Sequencer_impl {
    Mutex qlock; // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    list<Op*> q;
    Cond cond;
    list<pair<uint64_t, Context*> > flush_commit_waiters;
    uint64_t op; // used by flush() to know the sequence of op
   public:
    Sequencer *parent;
    Mutex apply_lock;  // for apply mutual exclusion
    
    /// get_max_uncompleted
    bool _get_max_uncompleted(
      uint64_t *seq ///< [out] max uncompleted seq
      ) {
      assert(qlock.is_locked());
      assert(seq);
      *seq = 0;
      if (q.empty()) {
	return true;
      } else {
	*seq = q.back()->op;
	return false;
      }
    } /// @returns true if the queue is empty

    /// get_min_uncompleted
    bool _get_min_uncompleted(
      uint64_t *seq ///< [out] min uncompleted seq
      ) {
      assert(qlock.is_locked());
      assert(seq);
      *seq = 0;
      if (q.empty()) {
	return true;
      } else {
	*seq = q.front()->op;
	return false;
      }
    } /// @returns true if both queues are empty

    void _wake_flush_waiters(list<Context*> *to_queue) {
      uint64_t seq;
      if (_get_min_uncompleted(&seq))
	seq = -1;

      for (list<pair<uint64_t, Context*> >::iterator i =
	     flush_commit_waiters.begin();
	   i != flush_commit_waiters.end() && i->first < seq;
	   flush_commit_waiters.erase(i++)) {
	to_queue->push_back(i->second);
      }
    }

    void queue(Op *o) {
      Mutex::Locker l(qlock);
      q.push_back(o);
      op++;
      o->op = op;
    }
    Op *peek_queue() {
      assert(apply_lock.is_locked());
      return q.front();
    }

    Op *dequeue(list<Context*> *to_queue) {
      assert(to_queue);
      assert(apply_lock.is_locked());
      Mutex::Locker l(qlock);
      Op *o = q.front();
      q.pop_front();
      cond.Signal();

      _wake_flush_waiters(to_queue);
      return o;
    }

    void flush() {
      Mutex::Locker l(qlock);

      // get max for journal _or_ op queues
      uint64_t seq = 0;
      if (!q.empty())
        seq = q.back()->op;

      if (seq) {
        // everything prior to our watermark to drain through either/both
        // queues
        while (!q.empty() && q.front()->op <= seq)
          cond.Wait(qlock);
      }
    }
    bool flush_commit(Context *c) {
      Mutex::Locker l(qlock);
      uint64_t seq = 0;
      if (_get_max_uncompleted(&seq)) {
	delete c;
	return true;
      } else {
	flush_commit_waiters.push_back(make_pair(seq, c));
	return false;
      }
    }

    OpSequencer()
      : qlock("KeyValueStore::OpSequencer::qlock", false, false),
        op(0), parent(0),
	apply_lock("KeyValueStore::OpSequencer::apply_lock", false, false) {}
    ~OpSequencer() {
      assert(q.empty());
    }

    const string& get_name() const {
      return parent->get_name();
    }
  };

  friend ostream& operator<<(ostream& out, const OpSequencer& s);

  Sequencer default_osr;
  deque<OpSequencer*> op_queue;
  uint64_t op_queue_len, op_queue_bytes;
  Cond op_throttle_cond;
  Mutex op_throttle_lock;
  Finisher op_finisher;

  ThreadPool op_tp;
  struct OpWQ : public ThreadPool::WorkQueue<OpSequencer> {
    KeyValueStore *store;
    OpWQ(KeyValueStore *fs, time_t timeout, time_t suicide_timeout,
         ThreadPool *tp) :
      ThreadPool::WorkQueue<OpSequencer>("KeyValueStore::OpWQ",
                                         timeout, suicide_timeout, tp),
      store(fs) {}

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
    void _process(OpSequencer *osr, ThreadPool::TPHandle &handle) {
      store->_do_op(osr, handle);
    }
    void _process_finish(OpSequencer *osr) {
      store->_finish_op(osr);
    }
    void _clear() {
      assert(store->op_queue.empty());
    }
  } op_wq;

  Op *build_op(list<Transaction*>& tls, Context *ondisk, Context *onreadable,
               Context *onreadable_sync, TrackedOpRef osd_op);
  void queue_op(OpSequencer *osr, Op *o);
  void op_queue_reserve_throttle(Op *o, ThreadPool::TPHandle *handle = NULL);
  void _do_op(OpSequencer *osr, ThreadPool::TPHandle &handle);
  void op_queue_release_throttle(Op *o);
  void _finish_op(OpSequencer *osr);

  PerfCounters *perf_logger;

 public:

  KeyValueStore(const std::string &base,
                const char *internal_name = "keyvaluestore-dev",
                bool update_to=false);
  ~KeyValueStore();

  bool test_mount_in_use();
  int version_stamp_is_valid(uint32_t *version);
  int update_version_stamp();
  uint32_t get_target_version() {
    return target_version;
  }
  bool need_journal() { return false; };
  int peek_journal_fsid(uuid_d *id) {
    *id = fsid;
    return 0;
  }

  int write_version_stamp();
  int mount();
  int umount();
  unsigned get_max_object_name_length() {
    return 4096;  // no real limit for leveldb
  }
  unsigned get_max_attr_name_length() {
    return 256;  // arbitrary; there is no real limit internally
  }
  int mkfs();
  int mkjournal() {return 0;}

  /**
   ** set_allow_sharded_objects()
   **
   ** Before sharded ghobject_t can be specified this function must be called
   **/
  void set_allow_sharded_objects() {}

  /**
   ** get_allow_sharded_objects()
   **
   ** return value: true if set_allow_sharded_objects() called, otherwise false
   **/
  bool get_allow_sharded_objects() {return false;}

  int statfs(struct statfs *buf);

  int _do_transactions(
    list<Transaction*> &tls, uint64_t op_seq,
    ThreadPool::TPHandle *handle);
  int do_transactions(list<Transaction*> &tls, uint64_t op_seq) {
    return _do_transactions(tls, op_seq, 0);
  }
  unsigned _do_transaction(Transaction& transaction,
                           BufferTransaction &bt,
                           ThreadPool::TPHandle *handle);

  int queue_transactions(Sequencer *osr, list<Transaction*>& tls,
                         TrackedOpRef op = TrackedOpRef(),
                         ThreadPool::TPHandle *handle = NULL);


  // ------------------
  // objects

  int _generic_read(StripObjectMap::StripObjectHeaderRef header,
                    uint64_t offset, size_t len, bufferlist& bl,
                    bool allow_eio = false, BufferTransaction *bt = 0);
  int _generic_write(StripObjectMap::StripObjectHeaderRef header,
                     uint64_t offset, size_t len, const bufferlist& bl,
                     BufferTransaction &t, bool replica = false);

  bool exists(coll_t cid, const ghobject_t& oid);
  int stat(coll_t cid, const ghobject_t& oid, struct stat *st,
           bool allow_eio = false);
  int read(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len,
           bufferlist& bl, bool allow_eio = false);
  int fiemap(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len,
             bufferlist& bl);

  int _touch(coll_t cid, const ghobject_t& oid, BufferTransaction &t);
  int _write(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len,
             const bufferlist& bl, BufferTransaction &t, bool replica = false);
  int _zero(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len,
            BufferTransaction &t);
  int _truncate(coll_t cid, const ghobject_t& oid, uint64_t size,
                BufferTransaction &t);
  int _clone(coll_t cid, const ghobject_t& oldoid, const ghobject_t& newoid,
             BufferTransaction &t);
  int _clone_range(coll_t cid, const ghobject_t& oldoid,
                   const ghobject_t& newoid, uint64_t srcoff,
                   uint64_t len, uint64_t dstoff, BufferTransaction &t);
  int _remove(coll_t cid, const ghobject_t& oid, BufferTransaction &t);
  int _set_alloc_hint(coll_t cid, const ghobject_t& oid,
                      uint64_t expected_object_size,
                      uint64_t expected_write_size,
                      BufferTransaction &t);

  void start_sync() {}
  void sync() {}
  void flush() {}
  void sync_and_flush() {}

  void set_fsid(uuid_d u) { fsid = u; }
  uuid_d get_fsid() { return fsid; }

  // attrs
  int getattr(coll_t cid, const ghobject_t& oid, const char *name,
              bufferptr &bp);
  int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);

  int _setattrs(coll_t cid, const ghobject_t& oid,
                map<string, bufferptr>& aset, BufferTransaction &t);
  int _rmattr(coll_t cid, const ghobject_t& oid, const char *name,
              BufferTransaction &t);
  int _rmattrs(coll_t cid, const ghobject_t& oid, BufferTransaction &t);

  int collection_getattr(coll_t c, const char *name, void *value, size_t size);
  int collection_getattr(coll_t c, const char *name, bufferlist& bl);
  int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);

  int _collection_setattr(coll_t c, const char *name, const void *value,
                          size_t size, BufferTransaction &t);
  int _collection_rmattr(coll_t c, const char *name, BufferTransaction &t);
  int _collection_setattrs(coll_t cid, map<string,bufferptr> &aset,
                           BufferTransaction &t);

  // collections
  int _collection_hint_expected_num_objs(coll_t cid, uint32_t pg_num,
      uint64_t num_objs) const { return 0; }
  int _create_collection(coll_t c, BufferTransaction &t);
  int _destroy_collection(coll_t c, BufferTransaction &t);
  int _collection_add(coll_t c, coll_t ocid, const ghobject_t& oid,
                      BufferTransaction &t);
  int _collection_move_rename(coll_t oldcid, const ghobject_t& oldoid,
                              coll_t c, const ghobject_t& o,
                              BufferTransaction &t);
  int _collection_remove_recursive(const coll_t &cid,
                                   BufferTransaction &t);
  int list_collections(vector<coll_t>& ls);
  bool collection_exists(coll_t c);
  bool collection_empty(coll_t c);
  int collection_list(coll_t c, vector<ghobject_t>& oid);
  int collection_list_partial(coll_t c, ghobject_t start,
                              int min, int max, snapid_t snap,
                              vector<ghobject_t> *ls, ghobject_t *next);
  int collection_list_range(coll_t c, ghobject_t start, ghobject_t end,
                            snapid_t seq, vector<ghobject_t> *ls);
  int collection_version_current(coll_t c, uint32_t *version);

  // omap (see ObjectStore.h for documentation)
  int omap_get(coll_t c, const ghobject_t &oid, bufferlist *header,
               map<string, bufferlist> *out);
  int omap_get_header(
    coll_t c,
    const ghobject_t &oid,
    bufferlist *out,
    bool allow_eio = false);
  int omap_get_keys(coll_t c, const ghobject_t &oid, set<string> *keys);
  int omap_get_values(coll_t c, const ghobject_t &oid, const set<string> &keys,
                      map<string, bufferlist> *out);
  int omap_check_keys(coll_t c, const ghobject_t &oid, const set<string> &keys,
                      set<string> *out);
  ObjectMap::ObjectMapIterator get_omap_iterator(coll_t c,
                                                 const ghobject_t &oid);

  void dump_transactions(list<ObjectStore::Transaction*>& ls, uint64_t seq,
                         OpSequencer *osr);

 private:
  void _inject_failure() {}

  // omap
  int _omap_clear(coll_t cid, const ghobject_t &oid,
                  BufferTransaction &t);
  int _omap_setkeys(coll_t cid, const ghobject_t &oid,
                    map<string, bufferlist> &aset,
                    BufferTransaction &t);
  int _omap_rmkeys(coll_t cid, const ghobject_t &oid, const set<string> &keys,
                   BufferTransaction &t);
  int _omap_rmkeyrange(coll_t cid, const ghobject_t &oid,
                       const string& first, const string& last,
                       BufferTransaction &t);
  int _omap_setheader(coll_t cid, const ghobject_t &oid, const bufferlist &bl,
                      BufferTransaction &t);
  int _split_collection(coll_t cid, uint32_t bits, uint32_t rem, coll_t dest,
                        BufferTransaction &t);
  int _split_collection_create(coll_t cid, uint32_t bits, uint32_t rem,
                               coll_t dest, BufferTransaction &t){
    return 0;
  }

  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
                                  const std::set <std::string> &changed);

  std::string m_osd_rollback_to_cluster_snap;
  int m_keyvaluestore_queue_max_ops;
  int m_keyvaluestore_queue_max_bytes;
  int m_keyvaluestore_strip_size;
  uint64_t m_keyvaluestore_max_expected_write_size;
  int do_update;

  static const string OBJECT_STRIP_PREFIX;
  static const string OBJECT_XATTR;
  static const string OBJECT_OMAP;
  static const string OBJECT_OMAP_HEADER;
  static const string OBJECT_OMAP_HEADER_KEY;
  static const string COLLECTION;
  static const string COLLECTION_ATTR;
  static const uint32_t COLLECTION_VERSION = 1;

  KVSuperblock superblock;
  /**
   * write_superblock()
   *
   * Write superblock to persisent storage
   *
   * return value: 0 on success, otherwise negative errno
   */
  int write_superblock();

  /**
   * read_superblock()
   *
   * Fill in KeyValueStore::superblock by reading persistent storage
   *
   * return value: 0 on success, otherwise negative errno
   */
  int read_superblock();
};

WRITE_CLASS_ENCODER(StripObjectMap::StripObjectHeader)

#endif
