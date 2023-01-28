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

#include "include/types.h"

#include <map>
#include <deque>
#include <atomic>
#include <fstream>


#include <boost/scoped_ptr.hpp>

#include "include/unordered_map.h"

#include "include/ceph_assert.h"

#include "os/ObjectStore.h"
#include "JournalingObjectStore.h"

#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/perf_counters.h"
#include "common/zipkin_trace.h"

#include "common/ceph_mutex.h"
#include "HashIndex.h"
#include "IndexManager.h"
#include "os/ObjectMap.h"
#include "SequencerPosition.h"
#include "FDCache.h"
#include "WBThrottle.h"

#include "include/uuid.h"
#include "extblkdev/ExtBlkDevPlugin.h"

#if defined(__linux__)
# ifndef BTRFS_SUPER_MAGIC
#define BTRFS_SUPER_MAGIC 0x9123683EUL
# endif
# ifndef XFS_SUPER_MAGIC
#define XFS_SUPER_MAGIC 0x58465342UL
# endif
# ifndef ZFS_SUPER_MAGIC
#define ZFS_SUPER_MAGIC 0x2fc12fc1UL
# endif
#endif


class FileStoreBackend;

#define CEPH_FS_FEATURE_INCOMPAT_SHARDS CompatSet::Feature(1, "sharded objects")

enum {
  l_filestore_first = 84000,
  l_filestore_journal_queue_ops,
  l_filestore_journal_queue_bytes,
  l_filestore_journal_ops,
  l_filestore_journal_bytes,
  l_filestore_journal_latency,
  l_filestore_journal_wr,
  l_filestore_journal_wr_bytes,
  l_filestore_journal_full,
  l_filestore_committing,
  l_filestore_commitcycle,
  l_filestore_commitcycle_interval,
  l_filestore_commitcycle_latency,
  l_filestore_op_queue_max_ops,
  l_filestore_op_queue_ops,
  l_filestore_ops,
  l_filestore_op_queue_max_bytes,
  l_filestore_op_queue_bytes,
  l_filestore_bytes,
  l_filestore_apply_latency,
  l_filestore_queue_transaction_latency_avg,
  l_filestore_sync_pause_max_lat,
  l_filestore_last,
};

class FSSuperblock {
public:
  CompatSet compat_features;
  std::string omap_backend;

  FSSuperblock() { }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<FSSuperblock*>& o);
};
WRITE_CLASS_ENCODER(FSSuperblock)

inline std::ostream& operator<<(std::ostream& out, const FSSuperblock& sb)
{
  return out << "sb(" << sb.compat_features << "): "
             << sb.omap_backend;
}

class FileStore : public JournalingObjectStore,
                  public md_config_obs_t
{
  static const uint32_t target_version = 4;
public:
  uint32_t get_target_version() {
    return target_version;
  }

  static int get_block_device_fsid(CephContext* cct, const std::string& path,
				   uuid_d *fsid);
  struct FSPerfTracker {
    PerfCounters::avg_tracker<uint64_t> os_commit_latency_ns;
    PerfCounters::avg_tracker<uint64_t> os_apply_latency_ns;

    objectstore_perf_stat_t get_cur_stats() const {
      objectstore_perf_stat_t ret;
      ret.os_commit_latency_ns = os_commit_latency_ns.current_avg();
      ret.os_apply_latency_ns = os_apply_latency_ns.current_avg();
      return ret;
    }

    void update_from_perfcounters(PerfCounters &logger);
  } perf_tracker;
  objectstore_perf_stat_t get_cur_stats() override {
    perf_tracker.update_from_perfcounters(*logger);
    return perf_tracker.get_cur_stats();
  }
  const PerfCounters* get_perf_counters() const override {
    return logger;
  }

private:
  std::string internal_name;         ///< internal name, used to name the perfcounter instance
  std::string basedir, journalpath;
  osflagbits_t generic_flags;
  std::string current_fn;
  std::string current_op_seq_fn;
  std::string omap_dir;
  uuid_d fsid;

  size_t blk_size;            ///< fs block size

  int fsid_fd, op_fd, basedir_fd, current_fd;

  FileStoreBackend *backend;

  void create_backend(unsigned long f_type);

  std::string devname;

  ExtBlkDevInterfaceRef ebd_impl;  // structure for retrieving compression state from extended block device

  deque<uint64_t> snaps;

  // Indexed Collections
  IndexManager index_manager;
  int get_index(const coll_t& c, Index *index);
  int init_index(const coll_t& c);

  bool _need_temp_object_collection(const coll_t& cid, const ghobject_t& oid) {
    // - normal temp case: cid is pg, object is temp (pool < -1)
    // - hammer temp case: cid is pg (or already temp), object pool is -1
    return cid.is_pg() && oid.hobj.pool <= -1;
  }
  void init_temp_collections();

  void handle_eio();

  // ObjectMap
  boost::scoped_ptr<ObjectMap> object_map;

  // helper fns
  int get_cdir(const coll_t& cid, char *s, int len);

  /// read a uuid from fd
  int read_fsid(int fd, uuid_d *uuid);

  /// lock fsid_fd
  int lock_fsid();

  // sync thread
  ceph::mutex lock = ceph::make_mutex("FileStore::lock");
  bool force_sync;
  ceph::condition_variable sync_cond;

  ceph::mutex sync_entry_timeo_lock = ceph::make_mutex("FileStore::sync_entry_timeo_lock");
  SafeTimer timer;

  std::list<Context*> sync_waiters;
  bool stop;
  void sync_entry();
  struct SyncThread : public Thread {
    FileStore *fs;
    explicit SyncThread(FileStore *f) : fs(f) {}
    void *entry() override {
      fs->sync_entry();
      return 0;
    }
  } sync_thread;

  // -- op workqueue --
  struct Op {
    utime_t start;
    uint64_t op;
    std::vector<Transaction> tls;
    Context *onreadable, *onreadable_sync;
    uint64_t ops, bytes;
    TrackedOpRef osd_op;
    ZTracer::Trace trace;
    bool registered_apply = false;
  };
  class OpSequencer : public CollectionImpl {
    CephContext *cct;
    // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    ceph::mutex qlock =
      ceph::make_mutex("FileStore::OpSequencer::qlock", false);
    std::list<Op*> q;
    std::list<uint64_t> jq;
    std::list<std::pair<uint64_t, Context*> > flush_commit_waiters;
    ceph::condition_variable cond;
    std::string osr_name_str;
    /// hash of pointers to ghobject_t's for in-flight writes
    std::unordered_multimap<uint32_t,const ghobject_t*> applying;
  public:
    // for apply mutual exclusion
    ceph::mutex apply_lock =
      ceph::make_mutex("FileStore::OpSequencer::apply_lock", false);
    int id;
    const char *osr_name;

    /// get_max_uncompleted
    bool _get_max_uncompleted(
      uint64_t *seq ///< [out] max uncompleted seq
      ) {
      ceph_assert(seq);
      *seq = 0;
      if (q.empty() && jq.empty())
	return true;

      if (!q.empty())
	*seq = q.back()->op;
      if (!jq.empty() && jq.back() > *seq)
	*seq = jq.back();

      return false;
    } /// @returns true if both queues are empty

    /// get_min_uncompleted
    bool _get_min_uncompleted(
      uint64_t *seq ///< [out] min uncompleted seq
      ) {
      ceph_assert(seq);
      *seq = 0;
      if (q.empty() && jq.empty())
	return true;

      if (!q.empty())
	*seq = q.front()->op;
      if (!jq.empty() && jq.front() < *seq)
	*seq = jq.front();

      return false;
    } /// @returns true if both queues are empty

    void _wake_flush_waiters(std::list<Context*> *to_queue) {
      uint64_t seq;
      if (_get_min_uncompleted(&seq))
	seq = -1;

      for (auto i = flush_commit_waiters.begin();
	   i != flush_commit_waiters.end() && i->first < seq;
	   flush_commit_waiters.erase(i++)) {
	to_queue->push_back(i->second);
      }
    }

    void queue_journal(Op *o) {
      std::lock_guard l{qlock};
      jq.push_back(o->op);
      _register_apply(o);
    }
    void dequeue_journal(std::list<Context*> *to_queue) {
      std::lock_guard l{qlock};
      jq.pop_front();
      cond.notify_all();
      _wake_flush_waiters(to_queue);
    }
    void queue(Op *o) {
      std::lock_guard l{qlock};
      q.push_back(o);
      _register_apply(o);
      o->trace.keyval("queue depth", q.size());
    }
    void _register_apply(Op *o);
    void _unregister_apply(Op *o);
    void wait_for_apply(const ghobject_t& oid);
    Op *peek_queue() {
      std::lock_guard l{qlock};
      ceph_assert(ceph_mutex_is_locked(apply_lock));
      return q.front();
    }

    Op *dequeue(std::list<Context*> *to_queue) {
      ceph_assert(to_queue);
      ceph_assert(ceph_mutex_is_locked(apply_lock));
      std::lock_guard l{qlock};
      Op *o = q.front();
      q.pop_front();
      cond.notify_all();
      _unregister_apply(o);
      _wake_flush_waiters(to_queue);
      return o;
    }

    void flush() override {
      std::unique_lock l{qlock};
      // wait forever
      cond.wait(l, [this] { return !cct->_conf->filestore_blackhole; });

      // get max for journal _or_ op queues
      uint64_t seq = 0;
      if (!q.empty())
	seq = q.back()->op;
      if (!jq.empty() && jq.back() > seq)
	seq = jq.back();

      if (seq) {
	// everything prior to our watermark to drain through either/both queues
	cond.wait(l, [seq, this] {
          return ((q.empty() || q.front()->op > seq) &&
		  (jq.empty() || jq.front() > seq));
        });
      }
    }
    bool flush_commit(Context *c) override {
      std::lock_guard l{qlock};
      uint64_t seq = 0;
      if (_get_max_uncompleted(&seq)) {
	return true;
      } else {
	flush_commit_waiters.push_back(std::make_pair(seq, c));
	return false;
      }
    }

  private:
    FRIEND_MAKE_REF(OpSequencer);
    OpSequencer(CephContext* cct, int i, coll_t cid)
      : CollectionImpl(cct, cid),
	cct(cct),
	osr_name_str(stringify(cid)),
        id(i),
	osr_name(osr_name_str.c_str()) {}
    ~OpSequencer() override {
      ceph_assert(q.empty());
    }
  };
  typedef boost::intrusive_ptr<OpSequencer> OpSequencerRef;

  ceph::mutex coll_lock = ceph::make_mutex("FileStore::coll_lock");
  std::map<coll_t,OpSequencerRef> coll_map;

  friend std::ostream& operator<<(std::ostream& out, const OpSequencer& s);

  FDCache fdcache;
  WBThrottle wbthrottle;

  std::atomic<int64_t> next_osr_id = { 0 };
  bool m_disable_wbthrottle;
  deque<OpSequencer*> op_queue;
  BackoffThrottle throttle_ops, throttle_bytes;
  const int m_ondisk_finisher_num;
  const int m_apply_finisher_num;
  std::vector<Finisher*> ondisk_finishers;
  std::vector<Finisher*> apply_finishers;

  ThreadPool op_tp;
  struct OpWQ : public ThreadPool::WorkQueue<OpSequencer> {
    FileStore *store;
    OpWQ(FileStore *fs,
	 ceph::timespan timeout,
	 ceph::timespan suicide_timeout,
	 ThreadPool *tp)
      : ThreadPool::WorkQueue<OpSequencer>("FileStore::OpWQ",
					   timeout, suicide_timeout, tp),
	store(fs) {}

    bool _enqueue(OpSequencer *osr) override {
      store->op_queue.push_back(osr);
      return true;
    }
    void _dequeue(OpSequencer *o) override {
      ceph_abort();
    }
    bool _empty() override {
      return store->op_queue.empty();
    }
    OpSequencer *_dequeue() override {
      if (store->op_queue.empty())
	return nullptr;
      OpSequencer *osr = store->op_queue.front();
      store->op_queue.pop_front();
      return osr;
    }
    void _process(OpSequencer *osr, ThreadPool::TPHandle &handle) override {
      store->_do_op(osr, handle);
    }
    void _process_finish(OpSequencer *osr) override {
      store->_finish_op(osr);
    }
    void _clear() override {
      ceph_assert(store->op_queue.empty());
    }
  } op_wq;

  void _do_op(OpSequencer *o, ThreadPool::TPHandle &handle);
  void _finish_op(OpSequencer *o);
  Op *build_op(std::vector<Transaction>& tls,
	       Context *onreadable, Context *onreadable_sync,
	       TrackedOpRef osd_op);
  void queue_op(OpSequencer *osr, Op *o);
  void op_queue_reserve_throttle(Op *o);
  void op_queue_release_throttle(Op *o);
  void _journaled_ahead(OpSequencer *osr, Op *o, Context *ondisk);
  friend struct C_JournaledAhead;

  void new_journal();

  PerfCounters *logger;

  ZTracer::Endpoint trace_endpoint;

public:
  int lfn_find(const ghobject_t& oid, const Index& index,
                                  IndexedPath *path = nullptr);
  int lfn_truncate(const coll_t& cid, const ghobject_t& oid, off_t length);
  int lfn_stat(const coll_t& cid, const ghobject_t& oid, struct stat *buf);
  int lfn_open(
    const coll_t& cid,
    const ghobject_t& oid,
    bool create,
    FDRef *outfd,
    Index *index = nullptr);

  void lfn_close(FDRef fd);
  int lfn_link(const coll_t& c, const coll_t& newcid, const ghobject_t& o, const ghobject_t& newoid) ;
  int lfn_unlink(const coll_t& cid, const ghobject_t& o, const SequencerPosition &spos,
		 bool force_clear_omap=false);

public:
  FileStore(CephContext* cct, const std::string &base, const std::string &jdev,
	    osflagbits_t flags = 0,
    const char *internal_name = "filestore", bool update_to=false);
  ~FileStore() override;

  std::string get_type() override {
    return "filestore";
  }

  int _detect_fs();
  int _sanity_check_fs();

  bool test_mount_in_use() override;
  int read_op_seq(uint64_t *seq);
  int write_op_seq(int, uint64_t seq);
  int mount() override;
  int umount() override;

  int validate_hobject_key(const hobject_t &obj) const override;

  unsigned get_max_attr_name_length() override {
    // xattr limit is 128; leave room for our prefixes (user.ceph._),
    // some margin, and cap at 100
    return 100;
  }
  int mkfs() override;
  int mkjournal() override;
  bool wants_journal() override {
    return true;
  }
  bool allows_journal() override {
    return true;
  }
  bool needs_journal() override {
    return false;
  }

  bool is_sync_onreadable() const override {
    return false;
  }

  bool is_rotational() override;
  bool is_journal_rotational() override;

  void dump_perf_counters(ceph::Formatter *f) override {
    f->open_object_section("perf_counters");
    logger->dump_formatted(f, false);
    f->close_section();
  }

  int flush_cache(std::ostream *os = NULL) override;
  int write_version_stamp();
  int version_stamp_is_valid(uint32_t *version);
  int update_version_stamp();
  int upgrade() override;

  bool can_sort_nibblewise() override {
    return true;    // i support legacy sort order
  }

  void collect_metadata(std::map<std::string,std::string> *pm) override;
  int get_devices(std::set<std::string> *ls) override;

  int statfs(struct store_statfs_t *buf,
             osd_alert_list_t* alerts = nullptr) override;
  int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
		  bool *per_pool_omap) override;

  int _do_transactions(
    std::vector<Transaction> &tls, uint64_t op_seq,
    ThreadPool::TPHandle *handle,
    const char *osr_name);
  int do_transactions(std::vector<Transaction> &tls, uint64_t op_seq) override {
    return _do_transactions(tls, op_seq, nullptr, "replay");
  }
  void _do_transaction(
    Transaction& t, uint64_t op_seq, int trans_num,
    ThreadPool::TPHandle *handle, const char *osr_name);

  CollectionHandle open_collection(const coll_t& c) override;
  CollectionHandle create_new_collection(const coll_t& c) override;
  void set_collection_commit_queue(const coll_t& cid,
				   ContextQueue *commit_queue) override {
  }

  int queue_transactions(CollectionHandle& ch, std::vector<Transaction>& tls,
			 TrackedOpRef op = TrackedOpRef(),
			 ThreadPool::TPHandle *handle = nullptr) override;

  /**
   * set replay guard xattr on given file
   *
   * This will ensure that we will not replay this (or any previous) operation
   * against this particular inode/object.
   *
   * @param fd open file descriptor for the file/object
   * @param spos sequencer position of the last operation we should not replay
   */
  void _set_replay_guard(int fd,
			 const SequencerPosition& spos,
			 const ghobject_t *oid=0,
			 bool in_progress=false);
  void _set_replay_guard(const coll_t& cid,
                         const SequencerPosition& spos,
                         bool in_progress);
  void _set_global_replay_guard(const coll_t& cid,
				const SequencerPosition &spos);

  /// close a replay guard opened with in_progress=true
  void _close_replay_guard(int fd, const SequencerPosition& spos,
			   const ghobject_t *oid=0);
  void _close_replay_guard(const coll_t& cid, const SequencerPosition& spos);

  /**
   * check replay guard xattr on given file
   *
   * Check the current position against any marker on the file that
   * indicates which operations have already been applied.  If the
   * current or a newer operation has been marked as applied, we
   * should not replay the current operation again.
   *
   * If we are not replaying the journal, we already return true.  It
   * is only on replay that we might return false, indicated that the
   * operation should not be performed (again).
   *
   * @param fd open fd on the file/object in question
   * @param spos sequencerposition for an operation we could apply/replay
   * @return 1 if we can apply (maybe replay) this operation, -1 if spos has already been applied, 0 if it was in progress
   */
  int _check_replay_guard(int fd, const SequencerPosition& spos);
  int _check_replay_guard(const coll_t& cid, const SequencerPosition& spos);
  int _check_replay_guard(const coll_t& cid, const ghobject_t &oid, const SequencerPosition& pos);
  int _check_global_replay_guard(const coll_t& cid, const SequencerPosition& spos);

  // ------------------
  // objects
  int pick_object_revision_lt(ghobject_t& oid) {
    return 0;
  }
  using ObjectStore::exists;
  bool exists(CollectionHandle& c, const ghobject_t& oid) override;
  using ObjectStore::stat;
  int stat(
    CollectionHandle& c,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false) override;
  using ObjectStore::set_collection_opts;
  int set_collection_opts(
    CollectionHandle& c,
    const pool_opts_t& opts) override;
  using ObjectStore::read;
  int read(
    CollectionHandle& c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    ceph::buffer::list& bl,
    uint32_t op_flags = 0) override;
  int _do_fiemap(int fd, uint64_t offset, size_t len,
                 std::map<uint64_t, uint64_t> *m);
  int _do_seek_hole_data(int fd, uint64_t offset, size_t len,
                         std::map<uint64_t, uint64_t> *m);
  using ObjectStore::fiemap;
  int fiemap(CollectionHandle& c, const ghobject_t& oid, uint64_t offset, size_t len, ceph::buffer::list& bl) override;
  int fiemap(CollectionHandle& c, const ghobject_t& oid, uint64_t offset, size_t len, std::map<uint64_t, uint64_t>& destmap) override;

  int _touch(const coll_t& cid, const ghobject_t& oid);
  int _write(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len,
	      const ceph::buffer::list& bl, uint32_t fadvise_flags = 0);
  int _zero(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len);
  int _truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size);
  int _clone(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid,
	     const SequencerPosition& spos);
  int _clone_range(const coll_t& oldcid, const ghobject_t& oldoid, const coll_t& newcid, const ghobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff,
		   const SequencerPosition& spos);
  int _do_clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _do_sparse_copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _do_copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff, bool skip_sloppycrc=false);
  int _remove(const coll_t& cid, const ghobject_t& oid, const SequencerPosition &spos);

  int _fgetattr(int fd, const char *name, ceph::bufferptr& bp);
  int _fgetattrs(int fd, std::map<std::string, ceph::bufferptr,std::less<>>& aset);
  int _fsetattrs(int fd, std::map<std::string, ceph::bufferptr,std::less<>>& aset);

  void do_force_sync();
  void start_sync(Context *onsafe);
  void sync();
  void _flush_op_queue();
  void flush();
  void sync_and_flush();

  int flush_journal() override;
  int dump_journal(std::ostream& out) override;

  void set_fsid(uuid_d u) override {
    fsid = u;
  }
  uuid_d get_fsid() override { return fsid; }
  
  uint64_t estimate_objects_overhead(uint64_t num_objects) override;

  // DEBUG read error injection, an object is removed from both on delete()
  ceph::mutex read_error_lock = ceph::make_mutex("FileStore::read_error_lock");
  std::set<ghobject_t> data_error_set; // read() will return -EIO
  std::set<ghobject_t> mdata_error_set; // getattr(),stat() will return -EIO
  void inject_data_error(const ghobject_t &oid) override;
  void inject_mdata_error(const ghobject_t &oid) override;

  void compact() override {
    ceph_assert(object_map);
    object_map->compact();
  }

  bool has_builtin_csum() const override {
    return false;
  }

  void debug_obj_on_delete(const ghobject_t &oid);
  bool debug_data_eio(const ghobject_t &oid);
  bool debug_mdata_eio(const ghobject_t &oid);

  int snapshot(const std::string& name) override;

  // attrs
  using ObjectStore::getattr;
  using ObjectStore::getattrs;
  int getattr(CollectionHandle& c, const ghobject_t& oid, const char *name, ceph::bufferptr &bp) override;
  int getattrs(CollectionHandle& c, const ghobject_t& oid, std::map<std::string,ceph::bufferptr,std::less<>>& aset) override;

  int _setattrs(const coll_t& cid, const ghobject_t& oid, std::map<std::string,ceph::bufferptr>& aset,
		const SequencerPosition &spos);
  int _rmattr(const coll_t& cid, const ghobject_t& oid, const char *name,
	      const SequencerPosition &spos);
  int _rmattrs(const coll_t& cid, const ghobject_t& oid,
	       const SequencerPosition &spos);

  int _collection_remove_recursive(const coll_t &cid,
				   const SequencerPosition &spos);

  int _collection_set_bits(const coll_t& cid, int bits);

  // collections
  using ObjectStore::collection_list;
  int collection_bits(CollectionHandle& c) override;
  int collection_list(CollectionHandle& c,
		      const ghobject_t& start, const ghobject_t& end, int max,
		      std::vector<ghobject_t> *ls, ghobject_t *next) override {
    c->flush();
    return collection_list(c->cid, start, end, max, ls, next);
  }
  int collection_list(const coll_t& cid,
		      const ghobject_t& start, const ghobject_t& end, int max,
		      std::vector<ghobject_t> *ls, ghobject_t *next);
  int list_collections(std::vector<coll_t>& ls) override;
  int list_collections(std::vector<coll_t>& ls, bool include_temp);
  int collection_stat(const coll_t& c, struct stat *st);
  bool collection_exists(const coll_t& c) override;
  int collection_empty(CollectionHandle& c, bool *empty) override {
    c->flush();
    return collection_empty(c->cid, empty);
  }
  int collection_empty(const coll_t& cid, bool *empty);

  // omap (see ObjectStore.h for documentation)
  using ObjectStore::omap_get;
  int omap_get(CollectionHandle& c, const ghobject_t &oid, ceph::buffer::list *header,
	       std::map<std::string, ceph::buffer::list> *out) override;
  using ObjectStore::omap_get_header;
  int omap_get_header(
    CollectionHandle& c,
    const ghobject_t &oid,
    ceph::buffer::list *out,
    bool allow_eio = false) override;
  using ObjectStore::omap_get_keys;
  int omap_get_keys(CollectionHandle& c, const ghobject_t &oid, std::set<std::string> *keys) override;
  using ObjectStore::omap_get_values;
  int omap_get_values(CollectionHandle& c, const ghobject_t &oid, const std::set<std::string> &keys,
		      std::map<std::string, ceph::buffer::list> *out) override;
  using ObjectStore::omap_check_keys;
  int omap_check_keys(CollectionHandle& c, const ghobject_t &oid, const std::set<std::string> &keys,
		      std::set<std::string> *out) override;
  using ObjectStore::get_omap_iterator;
  ObjectMap::ObjectMapIterator get_omap_iterator(CollectionHandle& c, const ghobject_t &oid) override;
  ObjectMap::ObjectMapIterator get_omap_iterator(const coll_t& cid, const ghobject_t &oid);

  int _create_collection(const coll_t& c, int bits,
			 const SequencerPosition &spos);
  int _destroy_collection(const coll_t& c);
  /**
   * Give an expected number of objects hint to the collection.
   *
   * @param c                 - collection id.
   * @param pg_num            - pg number of the pool this collection belongs to
   * @param expected_num_objs - expected number of objects in this collection
   * @param spos              - sequence position
   *
   * @return 0 on success, an error code otherwise
   */
  int _collection_hint_expected_num_objs(const coll_t& c, uint32_t pg_num,
      uint64_t expected_num_objs,
      const SequencerPosition &spos);
  int _collection_add(const coll_t& c, const coll_t& ocid, const ghobject_t& oid,
		      const SequencerPosition& spos);
  int _collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid,
			      coll_t c, const ghobject_t& o,
			      const SequencerPosition& spos,
			      bool ignore_enoent = false);

  int _set_alloc_hint(const coll_t& cid, const ghobject_t& oid,
                      uint64_t expected_object_size,
                      uint64_t expected_write_size);

  void dump_start(const std::string& file);
  void dump_stop();
  void dump_transactions(std::vector<Transaction>& ls, uint64_t seq, OpSequencer *osr);

  virtual int apply_layout_settings(const coll_t &cid, int target_level);

  void get_db_statistics(ceph::Formatter* f) override;

private:
  void _inject_failure();

  // omap
  int _omap_clear(const coll_t& cid, const ghobject_t &oid,
		  const SequencerPosition &spos);
  int _omap_setkeys(const coll_t& cid, const ghobject_t &oid,
		    const std::map<std::string, ceph::buffer::list> &aset,
		    const SequencerPosition &spos);
  int _omap_rmkeys(const coll_t& cid, const ghobject_t &oid, const std::set<std::string> &keys,
		   const SequencerPosition &spos);
  int _omap_rmkeyrange(const coll_t& cid, const ghobject_t &oid,
		       const std::string& first, const std::string& last,
		       const SequencerPosition &spos);
  int _omap_setheader(const coll_t& cid, const ghobject_t &oid, const ceph::buffer::list &bl,
		      const SequencerPosition &spos);
  int _split_collection(const coll_t& cid, uint32_t bits, uint32_t rem, coll_t dest,
                        const SequencerPosition &spos);
  int _merge_collection(const coll_t& cid, uint32_t bits, coll_t dest,
                        const SequencerPosition &spos);

  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override;
  int set_throttle_params();
  float m_filestore_commit_timeout;
  bool m_filestore_journal_parallel;
  bool m_filestore_journal_trailing;
  bool m_filestore_journal_writeahead;
  int m_filestore_fiemap_threshold;
  double m_filestore_max_sync_interval;
  double m_filestore_min_sync_interval;
  bool m_filestore_fail_eio;
  bool m_filestore_fadvise;
  int do_update;
  bool m_journal_dio, m_journal_aio, m_journal_force_aio;
  std::string m_osd_rollback_to_cluster_snap;
  bool m_osd_use_stale_snap;
  bool m_filestore_do_dump;
  std::ofstream m_filestore_dump;
  ceph::JSONFormatter m_filestore_dump_fmt;
  std::atomic<int64_t> m_filestore_kill_at = { 0 };
  bool m_filestore_sloppy_crc;
  int m_filestore_sloppy_crc_block_size;
  uint64_t m_filestore_max_alloc_hint_size;
  unsigned long m_fs_type;

  //Determined xattr handling based on fs type
  void set_xattr_limits_via_conf();
  uint32_t m_filestore_max_inline_xattr_size;
  uint32_t m_filestore_max_inline_xattrs;
  uint32_t m_filestore_max_xattr_value_size;

  FSSuperblock superblock;

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
   * Fill in FileStore::superblock by reading persistent storage
   *
   * return value: 0 on success, otherwise negative errno
   */
  int read_superblock();

  friend class FileStoreBackend;
  friend class TestFileStore;
};

std::ostream& operator<<(std::ostream& out, const FileStore::OpSequencer& s);

struct fiemap;

class FileStoreBackend {
private:
  FileStore *filestore;
protected:
  int get_basedir_fd() {
    return filestore->basedir_fd;
  }
  int get_current_fd() {
    return filestore->current_fd;
  }
  int get_op_fd() {
    return filestore->op_fd;
  }
  size_t get_blksize() {
    return filestore->blk_size;
  }
  const std::string& get_basedir_path() {
    return filestore->basedir;
  }
  const std::string& get_journal_path() {
    return filestore->journalpath;
  }
  const std::string& get_current_path() {
    return filestore->current_fn;
  }
  int _copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) {
    if (has_fiemap() || has_seek_data_hole()) {
      return filestore->_do_sparse_copy_range(from, to, srcoff, len, dstoff);
    } else {
      return filestore->_do_copy_range(from, to, srcoff, len, dstoff);
    }
  }
  int get_crc_block_size() {
    return filestore->m_filestore_sloppy_crc_block_size;
  }

public:
  explicit FileStoreBackend(FileStore *fs) : filestore(fs) {}
  virtual ~FileStoreBackend() {}

  CephContext* cct() const {
    return filestore->cct;
  }

  static FileStoreBackend *create(unsigned long f_type, FileStore *fs);

  virtual const char *get_name() = 0;
  virtual int detect_features() = 0;
  virtual int create_current() = 0;
  virtual bool can_checkpoint() = 0;
  virtual int list_checkpoints(std::list<std::string>& ls) = 0;
  virtual int create_checkpoint(const std::string& name, uint64_t *cid) = 0;
  virtual int sync_checkpoint(uint64_t id) = 0;
  virtual int rollback_to(const std::string& name) = 0;
  virtual int destroy_checkpoint(const std::string& name) = 0;
  virtual int syncfs() = 0;
  virtual bool has_fiemap() = 0;
  virtual bool has_seek_data_hole() = 0;
  virtual bool is_rotational() = 0;
  virtual bool is_journal_rotational() = 0;
  virtual int do_fiemap(int fd, off_t start, size_t len, struct fiemap **pfiemap) = 0;
  virtual int clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) = 0;
  virtual int set_alloc_hint(int fd, uint64_t hint) = 0;
  virtual bool has_splice() const = 0;

  // hooks for (sloppy) crc tracking
  virtual int _crc_update_write(int fd, loff_t off, size_t len, const ceph::buffer::list& bl) = 0;
  virtual int _crc_update_truncate(int fd, loff_t off) = 0;
  virtual int _crc_update_zero(int fd, loff_t off, size_t len) = 0;
  virtual int _crc_update_clone_range(int srcfd, int destfd,
				      loff_t srcoff, size_t len, loff_t dstoff) = 0;
  virtual int _crc_verify_read(int fd, loff_t off, size_t len, const ceph::buffer::list& bl,
			       std::ostream *out) = 0;
};

#endif
