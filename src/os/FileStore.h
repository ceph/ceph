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
#include <boost/scoped_ptr.hpp>
#include <fstream>
using namespace std;

#include "include/unordered_map.h"

#include "include/assert.h"

#include "ObjectStore.h"
#include "JournalingObjectStore.h"

#include "common/Timer.h"
#include "common/WorkQueue.h"

#include "common/Mutex.h"
#include "HashIndex.h"
#include "IndexManager.h"
#include "ObjectMap.h"
#include "SequencerPosition.h"
#include "FDCache.h"
#include "WBThrottle.h"

#include "include/uuid.h"


// from include/linux/falloc.h:
#ifndef FALLOC_FL_PUNCH_HOLE
# define FALLOC_FL_PUNCH_HOLE 0x2
#endif

#if defined(__linux__)
# ifndef BTRFS_SUPER_MAGIC
static const __SWORD_TYPE BTRFS_SUPER_MAGIC(0x9123683E);
# endif
# ifndef XFS_SUPER_MAGIC
static const __SWORD_TYPE XFS_SUPER_MAGIC(0x58465342);
# endif
#ifndef ZFS_SUPER_MAGIC
static const __SWORD_TYPE ZFS_SUPER_MAGIC(0x2fc12fc1);
#endif
#endif


class FileStoreBackend;

#define CEPH_FS_FEATURE_INCOMPAT_SHARDS CompatSet::Feature(1, "sharded objects")

class FSSuperblock {
public:
  CompatSet compat_features;
  string omap_backend;

  FSSuperblock() { }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<FSSuperblock*>& o);
};
WRITE_CLASS_ENCODER(FSSuperblock)

inline ostream& operator<<(ostream& out, const FSSuperblock& sb)
{
  return out << "sb(" << sb.compat_features << "): "
             << sb.omap_backend;
}

class FileStore : public JournalingObjectStore,
                  public md_config_obs_t
{
  static const uint32_t target_version = 3;
public:
  uint32_t get_target_version() {
    return target_version;
  }

  bool need_journal() { return true; }
  int peek_journal_fsid(uuid_d *fsid);

  struct FSPerfTracker {
    PerfCounters::avg_tracker<uint64_t> os_commit_latency;
    PerfCounters::avg_tracker<uint64_t> os_apply_latency;

    objectstore_perf_stat_t get_cur_stats() const {
      objectstore_perf_stat_t ret;
      ret.filestore_commit_latency = os_commit_latency.avg();
      ret.filestore_apply_latency = os_apply_latency.avg();
      return ret;
    }

    void update_from_perfcounters(PerfCounters &logger);
  } perf_tracker;
  objectstore_perf_stat_t get_cur_stats() {
    perf_tracker.update_from_perfcounters(*logger);
    return perf_tracker.get_cur_stats();
  }

private:
  string internal_name;         ///< internal name, used to name the perfcounter instance
  string basedir, journalpath;
  osflagbits_t generic_flags;
  std::string current_fn;
  std::string current_op_seq_fn;
  std::string omap_dir;
  uuid_d fsid;
  
  size_t blk_size;            ///< fs block size

  int fsid_fd, op_fd, basedir_fd, current_fd;

  FileStoreBackend *backend;

  void create_backend(long f_type);

  deque<uint64_t> snaps;

  // Indexed Collections
  IndexManager index_manager;
  int get_index(coll_t c, Index *index);
  int init_index(coll_t c);

  // ObjectMap
  boost::scoped_ptr<ObjectMap> object_map;
  
  Finisher ondisk_finisher;

  // helper fns
  int get_cdir(coll_t cid, char *s, int len);
  
  /// read a uuid from fd
  int read_fsid(int fd, uuid_d *uuid);

  /// lock fsid_fd
  int lock_fsid();

  // sync thread
  Mutex lock;
  bool force_sync;
  Cond sync_cond;
  uint64_t sync_epoch;

  Mutex sync_entry_timeo_lock;
  SafeTimer timer;

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

  // -- op workqueue --
  struct Op {
    utime_t start;
    uint64_t op;
    list<Transaction*> tls;
    Context *onreadable, *onreadable_sync;
    uint64_t ops, bytes;
    TrackedOpRef osd_op;
  };
  class OpSequencer : public Sequencer_impl {
    Mutex qlock; // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    list<Op*> q;
    list<uint64_t> jq;
    list<pair<uint64_t, Context*> > flush_commit_waiters;
    Cond cond;
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
      assert(qlock.is_locked());
      assert(seq);
      *seq = 0;
      if (q.empty() && jq.empty())
	return true;

      if (!q.empty())
	*seq = q.front()->op;
      if (!jq.empty() && jq.front() < *seq)
	*seq = jq.front();

      return false;
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

    void queue_journal(uint64_t s) {
      Mutex::Locker l(qlock);
      jq.push_back(s);
    }
    void dequeue_journal(list<Context*> *to_queue) {
      Mutex::Locker l(qlock);
      jq.pop_front();
      cond.Signal();
      _wake_flush_waiters(to_queue);
    }
    void queue(Op *o) {
      Mutex::Locker l(qlock);
      q.push_back(o);
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

      while (g_conf->filestore_blackhole)
	cond.Wait(qlock);  // wait forever


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
      : qlock("FileStore::OpSequencer::qlock", false, false),
	parent(0),
	apply_lock("FileStore::OpSequencer::apply_lock", false, false) {}
    ~OpSequencer() {
      assert(q.empty());
    }

    const string& get_name() const {
      return parent->get_name();
    }
  };

  friend ostream& operator<<(ostream& out, const OpSequencer& s);

  FDCache fdcache;
  WBThrottle wbthrottle;

  Sequencer default_osr;
  deque<OpSequencer*> op_queue;
  uint64_t op_queue_len, op_queue_bytes;
  Cond op_throttle_cond;
  Mutex op_throttle_lock;
  Finisher op_finisher;

  ThreadPool op_tp;
  struct OpWQ : public ThreadPool::WorkQueue<OpSequencer> {
    FileStore *store;
    OpWQ(FileStore *fs, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<OpSequencer>("FileStore::OpWQ", timeout, suicide_timeout, tp), store(fs) {}

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

  void _do_op(OpSequencer *o, ThreadPool::TPHandle &handle);
  void _finish_op(OpSequencer *o);
  Op *build_op(list<Transaction*>& tls,
	       Context *onreadable, Context *onreadable_sync,
	       TrackedOpRef osd_op);
  void queue_op(OpSequencer *osr, Op *o);
  void op_queue_reserve_throttle(Op *o, ThreadPool::TPHandle *handle = NULL);
  void op_queue_release_throttle(Op *o);
  void _journaled_ahead(OpSequencer *osr, Op *o, Context *ondisk);
  friend struct C_JournaledAhead;

  int open_journal();

  PerfCounters *logger;

public:
  int lfn_find(const ghobject_t& oid, const Index& index, 
                                  IndexedPath *path = NULL);
  int lfn_truncate(coll_t cid, const ghobject_t& oid, off_t length);
  int lfn_stat(coll_t cid, const ghobject_t& oid, struct stat *buf);
  int lfn_open(
    coll_t cid,
    const ghobject_t& oid,
    bool create,
    FDRef *outfd,
    Index *index = 0);

  void lfn_close(FDRef fd);
  int lfn_link(coll_t c, coll_t newcid, const ghobject_t& o, const ghobject_t& newoid) ;
  int lfn_unlink(coll_t cid, const ghobject_t& o, const SequencerPosition &spos,
		 bool force_clear_omap=false);

public:
  FileStore(const std::string &base, const std::string &jdev,
    osflagbits_t flags = 0,
    const char *internal_name = "filestore", bool update_to=false);
  ~FileStore();

  int _detect_fs();
  int _sanity_check_fs();
  
  bool test_mount_in_use();
  int read_op_seq(uint64_t *seq);
  int write_op_seq(int, uint64_t seq);
  int mount();
  int umount();
  unsigned get_max_object_name_length() {
    // not safe for all file systems, btw!  use the tunable to limit this.
    return 4096;
  }
  unsigned get_max_attr_name_length() {
    // xattr limit is 128; leave room for our prefixes (user.ceph._),
    // some margin, and cap at 100
    return 100;
  }
  int mkfs();
  int mkjournal();

  int write_version_stamp();
  int version_stamp_is_valid(uint32_t *version);
  int update_version_stamp();
  int upgrade();

  /**
   * set_allow_sharded_objects()
   *
   * Before sharded ghobject_t can be specified this function must be called
   *
   * Once this function is called the FileStore is not mountable by prior releases
   */
  void set_allow_sharded_objects();

  /**
   * get_allow_sharded_objects()
   *
   * return value: true if set_allow_sharded_objects() called, otherwise false
   */
  bool get_allow_sharded_objects();

  void collect_metadata(map<string,string> *pm);

  int statfs(struct statfs *buf);

  int _do_transactions(
    list<Transaction*> &tls, uint64_t op_seq,
    ThreadPool::TPHandle *handle);
  int do_transactions(list<Transaction*> &tls, uint64_t op_seq) {
    return _do_transactions(tls, op_seq, 0);
  }
  unsigned _do_transaction(
    Transaction& t, uint64_t op_seq, int trans_num,
    ThreadPool::TPHandle *handle);

  int queue_transactions(Sequencer *osr, list<Transaction*>& tls,
			 TrackedOpRef op = TrackedOpRef(),
			 ThreadPool::TPHandle *handle = NULL);

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
  void _set_replay_guard(coll_t cid,
                         const SequencerPosition& spos,
                         bool in_progress);
  void _set_global_replay_guard(coll_t cid,
				const SequencerPosition &spos);

  /// close a replay guard opened with in_progress=true
  void _close_replay_guard(int fd, const SequencerPosition& spos);
  void _close_replay_guard(coll_t cid, const SequencerPosition& spos);

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
  int _check_replay_guard(coll_t cid, const SequencerPosition& spos);
  int _check_replay_guard(coll_t cid, ghobject_t oid, const SequencerPosition& pos);
  int _check_global_replay_guard(coll_t cid, const SequencerPosition& spos);

  // ------------------
  // objects
  int pick_object_revision_lt(ghobject_t& oid) {
    return 0;
  }
  bool exists(coll_t cid, const ghobject_t& oid);
  int stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false);
  int read(
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio = false);
  int fiemap(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);

  int _touch(coll_t cid, const ghobject_t& oid);
  int _write(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, const bufferlist& bl,
      bool replica = false);
  int _zero(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len);
  int _truncate(coll_t cid, const ghobject_t& oid, uint64_t size);
  int _clone(coll_t cid, const ghobject_t& oldoid, const ghobject_t& newoid,
	     const SequencerPosition& spos);
  int _clone_range(coll_t cid, const ghobject_t& oldoid, const ghobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff,
		   const SequencerPosition& spos);
  int _do_clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _do_sparse_copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _do_copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _remove(coll_t cid, const ghobject_t& oid, const SequencerPosition &spos);

  int _fgetattr(int fd, const char *name, bufferptr& bp);
  int _fgetattrs(int fd, map<string,bufferptr>& aset);
  int _fsetattrs(int fd, map<string, bufferptr> &aset);

  void _start_sync();

  void do_force_sync();
  void start_sync(Context *onsafe);
  void sync();
  void _flush_op_queue();
  void flush();
  void sync_and_flush();

  int dump_journal(ostream& out);

  void set_fsid(uuid_d u) {
    fsid = u;
  }
  uuid_d get_fsid() { return fsid; }

  // DEBUG read error injection, an object is removed from both on delete()
  Mutex read_error_lock;
  set<ghobject_t> data_error_set; // read() will return -EIO
  set<ghobject_t> mdata_error_set; // getattr(),stat() will return -EIO
  void inject_data_error(const ghobject_t &oid);
  void inject_mdata_error(const ghobject_t &oid);
  void debug_obj_on_delete(const ghobject_t &oid);
  bool debug_data_eio(const ghobject_t &oid);
  bool debug_mdata_eio(const ghobject_t &oid);

  int snapshot(const string& name);

  // attrs
  int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr &bp);
  int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);

  int _setattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset,
		const SequencerPosition &spos);
  int _rmattr(coll_t cid, const ghobject_t& oid, const char *name,
	      const SequencerPosition &spos);
  int _rmattrs(coll_t cid, const ghobject_t& oid,
	       const SequencerPosition &spos);

  int collection_getattr(coll_t c, const char *name, void *value, size_t size);
  int collection_getattr(coll_t c, const char *name, bufferlist& bl);
  int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);

  int _collection_setattr(coll_t c, const char *name, const void *value, size_t size);
  int _collection_rmattr(coll_t c, const char *name);
  int _collection_setattrs(coll_t cid, map<string,bufferptr> &aset);
  int _collection_remove_recursive(const coll_t &cid,
				   const SequencerPosition &spos);

  // collections
  int list_collections(vector<coll_t>& ls);
  int collection_version_current(coll_t c, uint32_t *version);
  int collection_stat(coll_t c, struct stat *st);
  bool collection_exists(coll_t c);
  bool collection_empty(coll_t c);
  int collection_list(coll_t c, vector<ghobject_t>& oid);
  int collection_list_partial(coll_t c, ghobject_t start,
			      int min, int max, snapid_t snap,
			      vector<ghobject_t> *ls, ghobject_t *next);
  int collection_list_range(coll_t c, ghobject_t start, ghobject_t end,
                            snapid_t seq, vector<ghobject_t> *ls);

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
  ObjectMap::ObjectMapIterator get_omap_iterator(coll_t c, const ghobject_t &oid);

  int _create_collection(coll_t c);
  int _create_collection(coll_t c, const SequencerPosition &spos);
  int _destroy_collection(coll_t c);
  /**
   * Give an expected number of objects hint to the collection.
   *
   * @param c                 - collection id.
   * @param pg_num            - pg number of the pool this collection belongs to
   * @param expected_num_objs - expected number of objects in this collection
   * @param spos              - sequence position
   *
   * @Return 0 on success, an error code otherwise
   */
  int _collection_hint_expected_num_objs(coll_t c, uint32_t pg_num,
      uint64_t expected_num_objs,
      const SequencerPosition &spos);
  int _collection_add(coll_t c, coll_t ocid, const ghobject_t& oid,
		      const SequencerPosition& spos);
  int _collection_move_rename(coll_t oldcid, const ghobject_t& oldoid,
			      coll_t c, const ghobject_t& o,
			      const SequencerPosition& spos);

  int _set_alloc_hint(coll_t cid, const ghobject_t& oid,
                      uint64_t expected_object_size,
                      uint64_t expected_write_size);

  void dump_start(const std::string& file);
  void dump_stop();
  void dump_transactions(list<ObjectStore::Transaction*>& ls, uint64_t seq, OpSequencer *osr);

private:
  void _inject_failure();

  // omap
  int _omap_clear(coll_t cid, const ghobject_t &oid,
		  const SequencerPosition &spos);
  int _omap_setkeys(coll_t cid, const ghobject_t &oid,
		    const map<string, bufferlist> &aset,
		    const SequencerPosition &spos);
  int _omap_rmkeys(coll_t cid, const ghobject_t &oid, const set<string> &keys,
		   const SequencerPosition &spos);
  int _omap_rmkeyrange(coll_t cid, const ghobject_t &oid,
		       const string& first, const string& last,
		       const SequencerPosition &spos);
  int _omap_setheader(coll_t cid, const ghobject_t &oid, const bufferlist &bl,
		      const SequencerPosition &spos);
  int _split_collection(coll_t cid, uint32_t bits, uint32_t rem, coll_t dest,
                        const SequencerPosition &spos);
  int _split_collection_create(coll_t cid, uint32_t bits, uint32_t rem,
			       coll_t dest,
			       const SequencerPosition &spos);

  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed);
  float m_filestore_commit_timeout;
  bool m_filestore_journal_parallel;
  bool m_filestore_journal_trailing;
  bool m_filestore_journal_writeahead;
  int m_filestore_fiemap_threshold;
  double m_filestore_max_sync_interval;
  double m_filestore_min_sync_interval;
  bool m_filestore_fail_eio;
  bool m_filestore_replica_fadvise;
  int do_update;
  bool m_journal_dio, m_journal_aio, m_journal_force_aio;
  std::string m_osd_rollback_to_cluster_snap;
  bool m_osd_use_stale_snap;
  int m_filestore_queue_max_ops;
  int m_filestore_queue_max_bytes;
  int m_filestore_queue_committing_max_ops;
  int m_filestore_queue_committing_max_bytes;
  bool m_filestore_do_dump;
  std::ofstream m_filestore_dump;
  JSONFormatter m_filestore_dump_fmt;
  atomic_t m_filestore_kill_at;
  bool m_filestore_sloppy_crc;
  int m_filestore_sloppy_crc_block_size;
  uint64_t m_filestore_max_alloc_hint_size;
  long m_fs_type;

  //Determined xattr handling based on fs type
  void set_xattr_limits_via_conf();
  uint32_t m_filestore_max_inline_xattr_size;
  uint32_t m_filestore_max_inline_xattrs;

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

ostream& operator<<(ostream& out, const FileStore::OpSequencer& s);

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
  const string& get_basedir_path() {
    return filestore->basedir;
  }
  const string& get_current_path() {
    return filestore->current_fn;
  }
  int _copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) {
    if (has_fiemap()) {
      return filestore->_do_sparse_copy_range(from, to, srcoff, len, dstoff);
    } else {
      return filestore->_do_copy_range(from, to, srcoff, len, dstoff);
    }
  }
  int get_crc_block_size() {
    return filestore->m_filestore_sloppy_crc_block_size;
  }

public:
  FileStoreBackend(FileStore *fs) : filestore(fs) {}
  virtual ~FileStoreBackend() {}

  static FileStoreBackend *create(long f_type, FileStore *fs);

  virtual const char *get_name() = 0;
  virtual int detect_features() = 0;
  virtual int create_current() = 0;
  virtual bool can_checkpoint() = 0;
  virtual int list_checkpoints(list<string>& ls) = 0;
  virtual int create_checkpoint(const string& name, uint64_t *cid) = 0;
  virtual int sync_checkpoint(uint64_t id) = 0;
  virtual int rollback_to(const string& name) = 0;
  virtual int destroy_checkpoint(const string& name) = 0;
  virtual int syncfs() = 0;
  virtual bool has_fiemap() = 0;
  virtual int do_fiemap(int fd, off_t start, size_t len, struct fiemap **pfiemap) = 0;
  virtual int clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) = 0;
  virtual int set_alloc_hint(int fd, uint64_t hint) = 0;

  // hooks for (sloppy) crc tracking
  virtual int _crc_update_write(int fd, loff_t off, size_t len, const bufferlist& bl) = 0;
  virtual int _crc_update_truncate(int fd, loff_t off) = 0;
  virtual int _crc_update_zero(int fd, loff_t off, size_t len) = 0;
  virtual int _crc_update_clone_range(int srcfd, int destfd,
				      loff_t srcoff, size_t len, loff_t dstoff) = 0;
  virtual int _crc_verify_read(int fd, loff_t off, size_t len, const bufferlist& bl,
			       ostream *out) = 0;
};

#endif
