// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef MDS_RANK_H_
#define MDS_RANK_H_

#include <string_view>

#include <boost/asio/io_context.hpp>

#include "common/DecayCounter.h"
#include "common/LogClient.h"
#include "common/Timer.h"
#include "common/fair_mutex.h"
#include "common/TrackedOp.h"
#include "common/ceph_mutex.h"

#include "include/common_fwd.h"

#include "messages/MClientRequest.h"
#include "messages/MCommand.h"
#include "messages/MMDSMap.h"

#include "Beacon.h"
#include "DamageTable.h"
#include "MDSMap.h"
#include "SessionMap.h"
#include "MDCache.h"
#include "MDLog.h"
#include "MDSContext.h"
#include "PurgeQueue.h"
#include "Server.h"
#include "MetricsHandler.h"
#include "osdc/Journaler.h"

// Full .h import instead of forward declaration for PerfCounter, for the
// benefit of those including this header and using MDSRank::logger
#include "common/perf_counters.h"

enum {
  l_mds_first = 2000,
  l_mds_request,
  l_mds_reply,
  l_mds_reply_latency,
  l_mds_slow_reply,
  l_mds_forward,
  l_mds_dir_fetch_complete,
  l_mds_dir_fetch_keys,
  l_mds_dir_commit,
  l_mds_dir_split,
  l_mds_dir_merge,
  l_mds_inodes,
  l_mds_inodes_top,
  l_mds_inodes_bottom,
  l_mds_inodes_pin_tail,
  l_mds_inodes_pinned,
  l_mds_inodes_expired,
  l_mds_inodes_with_caps,
  l_mds_caps,
  l_mds_subtrees,
  l_mds_traverse,
  l_mds_traverse_hit,
  l_mds_traverse_forward,
  l_mds_traverse_discover,
  l_mds_traverse_dir_fetch,
  l_mds_traverse_remote_ino,
  l_mds_traverse_lock,
  l_mds_load_cent,
  l_mds_dispatch_queue_len,
  l_mds_exported,
  l_mds_exported_inodes,
  l_mds_imported,
  l_mds_imported_inodes,
  l_mds_openino_dir_fetch,
  l_mds_openino_backtrace_fetch,
  l_mds_openino_peer_discover,
  l_mds_root_rfiles,
  l_mds_root_rbytes,
  l_mds_root_rsnaps,
  l_mds_scrub_backtrace_fetch,
  l_mds_scrub_set_tag,
  l_mds_scrub_backtrace_repaired,
  l_mds_scrub_inotable_repaired,
  l_mds_scrub_dir_inodes,
  l_mds_scrub_dir_base_inodes,
  l_mds_scrub_dirfrag_rstats,
  l_mds_scrub_file_inodes,
  l_mdss_handle_inode_file_caps,
  l_mdss_ceph_cap_op_revoke,
  l_mdss_ceph_cap_op_grant,
  l_mdss_ceph_cap_op_trunc,
  l_mdss_ceph_cap_op_flushsnap_ack,
  l_mdss_ceph_cap_op_flush_ack,
  l_mdss_handle_client_caps,
  l_mdss_handle_client_caps_dirty,
  l_mdss_handle_client_cap_release,
  l_mdss_process_request_cap_release,
  l_mds_last,
};

// memory utilization
enum {
  l_mdm_first = 2500,
  l_mdm_ino,
  l_mdm_inoa,
  l_mdm_inos,
  l_mdm_dir,
  l_mdm_dira,
  l_mdm_dirs,
  l_mdm_dn,
  l_mdm_dna,
  l_mdm_dns,
  l_mdm_cap,
  l_mdm_capa,
  l_mdm_caps,
  l_mdm_rss,
  l_mdm_heap,
  l_mdm_last,
};

namespace ceph {
  struct heartbeat_handle_d;
}

class Locker;
class MDCache;
class MDLog;
class MDBalancer;
class InoTable;
class SnapServer;
class SnapClient;
class MDSTableServer;
class MDSTableClient;
class Messenger;
class MetricAggregator;
class Objecter;
class MonClient;
class MgrClient;
class Finisher;
class ScrubStack;
class C_ExecAndReply;

/**
 * The public part of this class's interface is what's exposed to all
 * the various subsystems (server, mdcache, etc), such as pointers
 * to the other subsystems, and message-sending calls.
 */
class MDSRank {
  public:
    friend class C_Flush_Journal;
    friend class C_Drop_Cache;
    friend class C_CacheDropExecAndReply;
    friend class C_ScrubExecAndReply;
    friend class C_ScrubControlExecAndReply;

    CephContext *cct;

    MDSRank(
        mds_rank_t whoami_,
        ceph::fair_mutex &mds_lock_,
        LogChannelRef &clog_,
        CommonSafeTimer<ceph::fair_mutex> &timer_,
        Beacon &beacon_,
        std::unique_ptr<MDSMap> & mdsmap_,
        Messenger *msgr,
        MonClient *monc_,
        MgrClient *mgrc,
        Context *respawn_hook_,
        Context *suicide_hook_,
	boost::asio::io_context& ioc);

    mds_rank_t get_nodeid() const { return whoami; }
    int64_t get_metadata_pool() const
    {
        return metadata_pool;
    }

    mono_time get_starttime() const {
      return starttime;
    }
    std::chrono::duration<double> get_uptime() const {
      mono_time now = mono_clock::now();
      return std::chrono::duration<double>(now-starttime);
    }

    bool is_daemon_stopping() const;

    MDSTableClient *get_table_client(int t);
    MDSTableServer *get_table_server(int t);

    Session *get_session(client_t client) {
      return sessionmap.get_session(entity_name_t::CLIENT(client.v));
    }
    Session *get_session(const cref_t<Message> &m);

    MDSMap::DaemonState get_state() const { return state; } 
    MDSMap::DaemonState get_want_state() const { return beacon.get_want_state(); } 

    bool is_creating() const { return state == MDSMap::STATE_CREATING; }
    bool is_starting() const { return state == MDSMap::STATE_STARTING; }
    bool is_standby() const { return state == MDSMap::STATE_STANDBY; }
    bool is_replay() const { return state == MDSMap::STATE_REPLAY; }
    bool is_standby_replay() const { return state == MDSMap::STATE_STANDBY_REPLAY; }
    bool is_resolve() const { return state == MDSMap::STATE_RESOLVE; }
    bool is_reconnect() const { return state == MDSMap::STATE_RECONNECT; }
    bool is_rejoin() const { return state == MDSMap::STATE_REJOIN; }
    bool is_clientreplay() const { return state == MDSMap::STATE_CLIENTREPLAY; }
    bool is_active() const { return state == MDSMap::STATE_ACTIVE; }
    bool is_stopping() const { return state == MDSMap::STATE_STOPPING; }
    bool is_any_replay() const { return (is_replay() || is_standby_replay()); }
    bool is_stopped() const { return mdsmap->is_stopped(whoami); }
    bool is_cluster_degraded() const { return cluster_degraded; }
    bool allows_multimds_snaps() const { return mdsmap->allows_multimds_snaps(); }

    bool is_cache_trimmable() const {
      return is_standby_replay() || is_clientreplay() || is_active() || is_stopping();
    }

    void handle_write_error(int err);
    void handle_write_error_with_lock(int err);

    void update_mlogger();

    void queue_waiter(MDSContext *c) {
      finished_queue.push_back(c);
      progress_thread.signal();
    }
    void queue_waiter_front(MDSContext *c) {
      finished_queue.push_front(c);
      progress_thread.signal();
    }
    void queue_waiters(MDSContext::vec& ls) {
      MDSContext::vec v;
      v.swap(ls);
      std::copy(v.begin(), v.end(), std::back_inserter(finished_queue));
      progress_thread.signal();
    }
    void queue_waiters_front(MDSContext::vec& ls) {
      MDSContext::vec v;
      v.swap(ls);
      std::copy(v.rbegin(), v.rend(), std::front_inserter(finished_queue));
      progress_thread.signal();
    }

    // Daemon lifetime functions: these guys break the abstraction
    // and call up into the parent MDSDaemon instance.  It's kind
    // of unavoidable: if we want any depth into our calls 
    // to be able to e.g. tear down the whole process, we have to
    // have a reference going all the way down.
    // >>>
    void suicide();
    void respawn();
    // <<<

    /**
     * Call this periodically if inside a potentially long running piece
     * of code while holding the mds_lock
     */
    void heartbeat_reset();
    int heartbeat_reset_grace(int count=1) {
      return count * _heartbeat_reset_grace;
    }

    /**
     * Abort the MDS and flush any clog messages.
     *
     * Callers must already hold mds_lock.
     */
    void abort(std::string_view msg);

    /**
     * Report state DAMAGED to the mon, and then pass on to respawn().  Call
     * this when an unrecoverable error is encountered while attempting
     * to load an MDS rank's data structures.  This is *not* for use with
     * errors affecting normal dirfrag/inode objects -- they should be handled
     * through cleaner scrub/repair mechanisms.
     *
     * Callers must already hold mds_lock.
     */
    void damaged();

    /**
     * Wrapper around `damaged` for users who are not
     * already holding mds_lock.
     *
     * Callers must not already hold mds_lock.
     */
    void damaged_unlocked();

    double last_cleared_laggy() const {
      return beacon.last_cleared_laggy();
    }

    double get_dispatch_queue_max_age(utime_t now) const;

    void send_message_mds(const ref_t<Message>& m, mds_rank_t mds);
    void send_message_mds(const ref_t<Message>& m, const entity_addrvec_t &addr);
    void forward_message_mds(const MDRequestRef& mdr, mds_rank_t mds);
    void send_message_client_counted(const ref_t<Message>& m, client_t client);
    void send_message_client_counted(const ref_t<Message>& m, Session* session);
    void send_message_client_counted(const ref_t<Message>& m, const ConnectionRef& connection);
    void send_message_client(const ref_t<Message>& m, Session* session);
    void send_message(const ref_t<Message>& m, const ConnectionRef& c);

    void wait_for_bootstrapped_peer(mds_rank_t who, MDSContext *c) {
      waiting_for_bootstrapping_peer[who].push_back(c);
    }
    void wait_for_active_peer(mds_rank_t who, MDSContext *c) { 
      waiting_for_active_peer[who].push_back(c);
    }
    void wait_for_cluster_recovered(MDSContext *c) {
      ceph_assert(cluster_degraded);
      waiting_for_active_peer[MDS_RANK_NONE].push_back(c);
    }

    void wait_for_any_client_connection(MDSContext *c) {
      waiting_for_any_client_connection.push_back(c);
    }
    void kick_waiters_for_any_client_connection(void) {
      finish_contexts(g_ceph_context, waiting_for_any_client_connection);
    }
    void wait_for_active(MDSContext *c) {
      waiting_for_active.push_back(c);
    }
    void wait_for_replay(MDSContext *c) { 
      waiting_for_replay.push_back(c); 
    }
    void wait_for_rejoin(MDSContext *c) {
      waiting_for_rejoin.push_back(c);
    }
    void wait_for_reconnect(MDSContext *c) {
      waiting_for_reconnect.push_back(c);
    }
    void wait_for_resolve(MDSContext *c) {
      waiting_for_resolve.push_back(c);
    }
    void wait_for_mdsmap(epoch_t e, MDSContext *c) {
      waiting_for_mdsmap[e].push_back(c);
    }
    void enqueue_replay(MDSContext *c) {
      replay_queue.push_back(c);
    }

    bool queue_one_replay();
    void maybe_clientreplay_done();

    void set_osd_epoch_barrier(epoch_t e);
    epoch_t get_osd_epoch_barrier() const {return osd_epoch_barrier;}
    epoch_t get_osd_epoch() const;

    ceph_tid_t issue_tid() { return ++last_tid; }

    MDSMap *get_mds_map() { return mdsmap.get(); }

    uint64_t get_num_requests() const { return logger->get(l_mds_request); }
  
    int get_mds_slow_req_count() const { return mds_slow_req_count; }

    void dump_status(Formatter *f) const;

    void hit_export_target(mds_rank_t rank, double amount=-1.0);
    bool is_export_target(mds_rank_t rank) {
      const std::set<mds_rank_t>& map_targets = mdsmap->get_mds_info(get_nodeid()).export_targets;
      return map_targets.count(rank);
    }

    bool evict_client(int64_t session_id, bool wait, bool blocklist,
                      std::ostream& ss, Context *on_killed=nullptr);
    int config_client(int64_t session_id, bool remove,
		      const std::string& option, const std::string& value,
		      std::ostream& ss);
    void schedule_inmemory_logger();

    double get_inject_journal_corrupt_dentry_first() const {
      return inject_journal_corrupt_dentry_first;
    }

    // Reference to global MDS::mds_lock, so that users of MDSRank don't
    // carry around references to the outer MDS, and we can substitute
    // a separate lock here in future potentially.
    ceph::fair_mutex &mds_lock;

    // Reference to global cluster log client, just to avoid initialising
    // a separate one here.
    LogChannelRef &clog;

    // Reference to global timer utility, because MDSRank and MDSDaemon
    // currently both use the same mds_lock, so it makes sense for them
    // to share a timer.
    CommonSafeTimer<ceph::fair_mutex> &timer;

    std::unique_ptr<MDSMap> &mdsmap; /* MDSDaemon::mdsmap */

    Objecter *objecter;

    // sub systems
    Server *server = nullptr;
    MDCache *mdcache = nullptr;
    Locker *locker = nullptr;
    MDLog *mdlog = nullptr;
    MDBalancer *balancer = nullptr;
    ScrubStack *scrubstack = nullptr;
    DamageTable damage_table;

    InoTable *inotable = nullptr;

    SnapServer *snapserver = nullptr;
    SnapClient *snapclient = nullptr;

    SessionMap sessionmap;

    PerfCounters *logger = nullptr, *mlogger = nullptr;
    OpTracker op_tracker;

    // The last different state I held before current
    MDSMap::DaemonState last_state = MDSMap::STATE_BOOT;
    // The state assigned to me by the MDSMap
    MDSMap::DaemonState state = MDSMap::STATE_STANDBY;

    bool cluster_degraded = false;

    Finisher *finisher;
  protected:
    typedef enum {
      // The MDSMap is available, configure default layouts and structures
      MDS_BOOT_INITIAL = 0,
      // We are ready to open some inodes
      MDS_BOOT_OPEN_ROOT,
      // We are ready to do a replay if needed
      MDS_BOOT_PREPARE_LOG,
      // Replay is complete
      MDS_BOOT_REPLAY_DONE
    } BootStep;

    class ProgressThread : public Thread {
      public:
      explicit ProgressThread(MDSRank *mds_) : mds(mds_) {}
      void * entry() override;
      void shutdown();
      void signal() {cond.notify_all();}
      private:
      MDSRank *mds;
      std::condition_variable_any cond;
    } progress_thread;

    class C_MDS_StandbyReplayRestart;
    class C_MDS_StandbyReplayRestartFinish;
    // Friended to access retry_dispatch
    friend class C_MDS_RetryMessage;
    friend class C_MDS_BootStart;
    friend class C_MDS_InternalBootStart;
    friend class C_MDS_MonCommand;

    const mds_rank_t whoami;

    ~MDSRank();

    void inc_dispatch_depth() { ++dispatch_depth; }
    void dec_dispatch_depth() { --dispatch_depth; }
    void retry_dispatch(const cref_t<Message> &m);
    bool is_valid_message(const cref_t<Message> &m);
    void handle_message(const cref_t<Message> &m);
    void _advance_queues();
    bool _dispatch(const cref_t<Message> &m, bool new_msg);
    bool is_stale_message(const cref_t<Message> &m) const;

    /**
     * Emit clog warnings for any ops reported as warnings by optracker
     */
    void check_ops_in_flight();

     /**
     * Share MDSMap with clients
     */
    void create_logger();

    void dump_clientreplay_status(Formatter *f) const;
    void command_scrub_start(Formatter *f,
                             std::string_view path, std::string_view tag,
                             const std::vector<std::string>& scrubop_vec, Context *on_finish);
    void command_tag_path(Formatter *f, std::string_view path,
                          std::string_view tag);
    // scrub control commands
    void command_scrub_abort(Formatter *f, Context *on_finish);
    void command_scrub_pause(Formatter *f, Context *on_finish);
    void command_scrub_resume(Formatter *f);
    void command_scrub_status(Formatter *f);

    void command_flush_path(Formatter *f, std::string_view path);
    void command_flush_journal(Formatter *f);
    void command_get_subtrees(Formatter *f);
    void command_export_dir(Formatter *f,
        std::string_view path, mds_rank_t dest);
    bool command_dirfrag_split(
        cmdmap_t cmdmap,
        std::ostream &ss);
    bool command_dirfrag_merge(
        cmdmap_t cmdmap,
        std::ostream &ss);
    bool command_dirfrag_ls(
        cmdmap_t cmdmap,
        std::ostream &ss,
        Formatter *f);
    int _command_export_dir(std::string_view path, mds_rank_t dest);
    CDir *_command_dirfrag_get(
        const cmdmap_t &cmdmap,
        std::ostream &ss);
    void command_openfiles_ls(Formatter *f);
    void command_dump_tree(const cmdmap_t &cmdmap, std::ostream &ss, Formatter *f);
    int command_quiesce_path(Formatter *f, const cmdmap_t &cmdmap, std::ostream &ss);
    void command_dump_inode(Formatter *f, const cmdmap_t &cmdmap, std::ostream &ss);
    void command_cache_drop(uint64_t timeout, Formatter *f, Context *on_finish);

    // FIXME the state machine logic should be separable from the dispatch
    // logic that calls it.
    // >>>
    void calc_recovery_set();
    void request_state(MDSMap::DaemonState s);

    void boot_create();             // i am new mds.
    void boot_start(BootStep step=MDS_BOOT_INITIAL, int r=0);    // starting|replay

    void replay_start();
    void creating_done();
    void starting_done();
    void replay_done();
    void standby_replay_restart();
    void _standby_replay_restart_finish(int r, uint64_t old_read_pos);

    void reopen_log();

    void resolve_start();
    void resolve_done();
    void reconnect_start();
    void reconnect_done();
    void rejoin_joint_start();
    void rejoin_start();
    void rejoin_done();
    void recovery_done(int oldstate);
    void clientreplay_start();
    void clientreplay_done();
    void active_start();
    void stopping_start();
    void stopping_done();

    void validate_sessions();

    void handle_mds_recovery(mds_rank_t who);
    void handle_mds_failure(mds_rank_t who);

    /* Update MDSMap export_targets for this rank. Called on ::tick(). */
    void update_targets();

    void _mon_command_finish(int r, std::string_view cmd, std::string_view outs);
    void set_mdsmap_multimds_snaps_allowed();

    Context *create_async_exec_context(C_ExecAndReply *ctx);

    // blocklist the provided addrs and set OSD epoch barrier
    // with the provided epoch.
    void apply_blocklist(const std::set<entity_addr_t> &addrs, epoch_t epoch);

    void reset_event_flags();

    // Incarnation as seen in MDSMap at the point where a rank is
    // assigned.
    int incarnation = 0;

    // Flag to indicate we entered shutdown: anyone seeing this to be true
    // after taking mds_lock must drop out.
    bool stopping = false;

    // PurgeQueue is only used by StrayManager, but it is owned by MDSRank
    // because its init/shutdown happens at the top level.
    PurgeQueue purge_queue;

    MetricsHandler metrics_handler;
    std::unique_ptr<MetricAggregator> metric_aggregator;

    std::list<cref_t<Message>> waiting_for_nolaggy;
    MDSContext::que finished_queue;
    // Dispatch, retry, queues
    int dispatch_depth = 0;

    ceph::heartbeat_handle_d *hb = nullptr;  // Heartbeat for threads using mds_lock
    double heartbeat_grace;
    int _heartbeat_reset_grace;

    std::map<mds_rank_t, version_t> peer_mdsmap_epoch;

    ceph_tid_t last_tid = 0;    // for mds-initiated requests (e.g. stray rename)

    MDSContext::vec waiting_for_active, waiting_for_replay, waiting_for_rejoin,
				waiting_for_reconnect, waiting_for_resolve;
    MDSContext::vec waiting_for_any_client_connection;
    MDSContext::que replay_queue;
    bool replaying_requests_done = false;

    std::map<mds_rank_t, MDSContext::vec> waiting_for_active_peer;
    std::map<mds_rank_t, MDSContext::vec> waiting_for_bootstrapping_peer;
    std::map<epoch_t, MDSContext::vec> waiting_for_mdsmap;

    epoch_t osd_epoch_barrier = 0;

    // Const reference to the beacon so that we can behave differently
    // when it's laggy.
    Beacon &beacon;

    int mds_slow_req_count = 0;

    std::map<mds_rank_t,DecayCounter> export_targets; /* targets this MDS is exporting to or wants/tries to */

    Messenger *messenger;
    MonClient *monc;
    MgrClient *mgrc;

    Context *respawn_hook;
    Context *suicide_hook;

    bool standby_replaying = false;  // true if current replay pass is in standby-replay mode
    uint64_t extraordinary_events_dump_interval = 0;
    double inject_journal_corrupt_dentry_first = 0.0;
private:
    bool send_status = true;

    // The metadata pool won't change in the whole life time of the fs,
    // with this we can get rid of the mds_lock in many places too.
    int64_t metadata_pool = -1;

    // "task" string that gets displayed in ceph status
    inline static const std::string SCRUB_STATUS_KEY = "scrub status";

    bool client_eviction_dump = false;

    void get_task_status(std::map<std::string, std::string> *status);
    void schedule_update_timer_task();
    void send_task_status();

    void inmemory_logger();
    bool is_rank0() const {
      return whoami == (mds_rank_t)0;
    }

    mono_time starttime = mono_clock::zero();
    boost::asio::io_context& ioc;
};

class C_MDS_RetryMessage : public MDSInternalContext {
public:
  C_MDS_RetryMessage(MDSRank *mds, const cref_t<Message> &m)
    : MDSInternalContext(mds), m(m) {}
  void finish(int r) override {
    get_mds()->retry_dispatch(m);
  }
protected:
  cref_t<Message> m;
};

class CF_MDS_RetryMessageFactory : public MDSContextFactory {
public:
  CF_MDS_RetryMessageFactory(MDSRank *mds, const cref_t<Message> &m)
    : mds(mds), m(m) {}

  MDSContext *build() {
    return new C_MDS_RetryMessage(mds, m);
  }
private:
  MDSRank *mds;
  cref_t<Message> m;
};

/**
 * The aspect of MDSRank exposed to MDSDaemon but not subsystems: i.e.
 * the service/dispatcher stuff like init/shutdown that subsystems should
 * never touch.
 */
class MDSRankDispatcher : public MDSRank, public md_config_obs_t
{
public:
  MDSRankDispatcher(
      mds_rank_t whoami_,
      ceph::fair_mutex &mds_lock_,
      LogChannelRef &clog_,
      CommonSafeTimer<ceph::fair_mutex> &timer_,
      Beacon &beacon_,
      std::unique_ptr<MDSMap> &mdsmap_,
      Messenger *msgr,
      MonClient *monc_,
      MgrClient *mgrc,
      Context *respawn_hook_,
      Context *suicide_hook_,
      boost::asio::io_context& ioc);

  void init();
  void tick();
  void shutdown();
  void handle_asok_command(
    std::string_view command,
    const cmdmap_t& cmdmap,
    Formatter *f,
    const bufferlist &inbl,
    std::function<void(int,const std::string&,bufferlist&)> on_finish);
  void handle_mds_map(const cref_t<MMDSMap> &m, const MDSMap &oldmap);
  void handle_osd_map();
  void update_log_config();

  const char** get_tracked_conf_keys() const override final;
  void handle_conf_change(const ConfigProxy& conf, const std::set<std::string>& changed) override;

  void dump_sessions(const SessionFilter &filter, Formatter *f, bool cap_dump=false) const;
  void evict_clients(const SessionFilter &filter,
		     std::function<void(int,const std::string&,bufferlist&)> on_finish);

  // Call into me from MDS::ms_dispatch
  bool ms_dispatch(const cref_t<Message> &m);
};

#endif // MDS_RANK_H_
