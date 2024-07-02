#pragma once

#include "mds/MDSRank.h"
#include "mds/events/ESubtreeMap.h"
#include "MemoryJournaler.h"
#include "MemoryJournalPointerStore.h"

#include "MDSTestMap.h"
#include <memory>

using std::unique_ptr;
struct TestLogProxy: MDCacheLogProxy
{
  // MDCacheLogProxy
  void oft_trim_destroyed_inos(uint64_t seq) { };
  bool oft_try_to_commit(MDSContext *c, uint64_t log_seq, int op_prio) {
    if (last_committed >= log_seq) {
      return false;
    }
    last_committed = log_seq;
    if (c) {
      c->complete(0);
    }
    return true;
  };
  uint64_t oft_get_committed_log_seq() { return last_committed; }
  void advance_stray() { }
  ESubtreeMap * create_subtree_map() { return new ESubtreeMap(); }
  void standby_trim_segment(LogSegment *ls) { }
  std::pair<bool, uint64_t> trim(uint64_t count=0) { return std::pair(false, 0); }
  bool is_readonly() { return readonly; }
  file_layout_t const * get_default_log_layout() { return &default_log_layout; }

  TestLogProxy()
  {
    default_log_layout = file_layout_t::get_default();
  }

  uint64_t last_committed = 0;
  bool readonly = false;
  file_layout_t default_log_layout;
};

struct TestRank: public MDSRankBase
{
  mutable ceph::fair_mutex lock;
  unique_ptr<TestLogProxy> log_proxy;
  unique_ptr<Finisher> finisher;
  unique_ptr<MemoryJournalPointerStore> jp_store;
  unique_ptr<MDSTestMap> mds_map;
  LogChannelRef log_ref;

  MDLog*  md_log;
  MDSMap::mds_info_t* my_info;
  mds_rank_t my_rank;

  TestRank(mds_rank_t rank = 0)
      : lock("test_rank")
      , md_log(nullptr)
      , my_rank(rank)
  { 
    finisher = std::make_unique<Finisher>(g_ceph_context);
    finisher->start();
    log_proxy = std::make_unique<TestLogProxy>();
    mds_map = std::make_unique<MDSTestMap>();
    jp_store = std::make_unique<MemoryJournalPointerStore>();
    my_info = mds_map->add_rank(my_rank);
  }

  MDSMap* get_mds_map() const { return mds_map.get(); }
  const LogChannelRef& get_clog_ref() const { return log_ref; }
  MDCache* get_cache() const { return nullptr; }
  MDCacheLogProxy* get_cache_log_proxy() const { return log_proxy.get(); }
  MDLog* get_log() const { return md_log; }
  MDBalancer* get_balancer() const { return nullptr; }
  MDSTableClient *get_table_client(int t) const { return nullptr; }
  MDSTableServer *get_table_server(int t) const { return nullptr; }
  JournalPointerStore* get_journal_pointer_store() { return jp_store.get(); }
  Finisher* get_finisher() const { return finisher.get(); }

  Journaler *make_journaler(
    const char * name, 
    inodeno_t ino, 
    const char * magic, 
    PerfCounters *latency_logger = 0, 
    int latency_logger_key = 0) const {
      return new MemoryJournaler(get_finisher()); 
    }

  inline ceph::fair_mutex& get_lock() const { return lock; }
  mds_rank_t get_nodeid() const { return my_rank; }
  int64_t get_metadata_pool() const { return 0; }
  mono_time get_starttime() const { return mono_time(); }

  bool is_daemon_stopping() const { return is_stopping(); }

  MDSMap::DaemonState get_state() const { return mds_map->get_state(get_nodeid()); }
  MDSMap::DaemonState get_want_state() const { return get_state(); }
  int get_incarnation() const { return 1; }

  bool is_stopped() const { return get_state() == MDSMap::DaemonState::STATE_STOPPED; }
  bool is_cluster_degraded() const { return false; }
  bool allows_multimds_snaps() const { return true; }

  inline bool is_cache_trimmable() const
  {
    return is_standby_replay() || is_clientreplay() || is_active() || is_stopping();
  }

  void handle_write_error(int err) { }
  inline void handle_write_error_unlocked(int err)
  {
    std::lock_guard l{ get_lock() };
    handle_write_error(err);
  }

  void update_mlogger() { }

  void queue_waiter(MDSContext* c)
  {
    MDSContext::vec v { c };
    queue_waiters(v);
  }
  void queue_waiter_front(MDSContext* c)
  {
    MDSContext::vec v { c };
    queue_waiters_front(v);
  }

  void queue_waiters(MDSContext::vec& ls) { }
  void queue_waiters_front(MDSContext::vec& ls) { }

  // Daemon lifetime functions: these guys break the abstraction
  // and call up into the parent MDSDaemon instance.  It's kind
  // of unavoidable: if we want any depth into our calls
  // to be able to e.g. tear down the whole process, we have to
  // have a reference going all the way down.
  // >>>
  void suicide() { }
  void respawn() { }
  // <<<

  /**
   * Call this periodically if inside a potentially long running piece
   * of code while holding the mds_lock
   */
  void heartbeat_reset() { }
  int heartbeat_reset_grace(int count = 1) { return 0; }

  /**
   * Report state DAMAGED to the mon, and then pass on to respawn().  Call
   * this when an unrecoverable error is encountered while attempting
   * to load an MDS rank's data structures.  This is *not* for use with
   * errors affecting normal dirfrag/inode objects -- they should be handled
   * through cleaner scrub/repair mechanisms.
   *
   * Callers must already hold mds_lock.
   */
  void damaged() { }

  /**
   * Wrapper around `damaged` for users who are not
   * already holding mds_lock.
   *
   * Callers must not already hold mds_lock.
   */
  inline void damaged_unlocked()
  {
    std::lock_guard l{ get_lock() };
    damaged();
  }
};