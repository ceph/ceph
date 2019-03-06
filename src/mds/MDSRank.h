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

#include "common/DecayCounter.h"
#include "common/LogClient.h"
#include "common/Timer.h"
#include "common/TrackedOp.h"

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
#include "osdc/Journaler.h"

// Full .h import instead of forward declaration for PerfCounter, for the
// benefit of those including this header and using MDSRank::logger
#include "common/perf_counters.h"

enum {
  l_mds_first = 2000,
  l_mds_request,
  l_mds_reply,
  l_mds_reply_latency,
  l_mds_forward,
  l_mds_dir_fetch,
  l_mds_dir_commit,
  l_mds_dir_split,
  l_mds_dir_merge,
  l_mds_inode_max,
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
class Objecter;
class MonClient;
class Finisher;
class ScrubStack;
class C_MDS_Send_Command_Reply;
class C_ExecAndReply;

/**
 * The public part of this class's interface is what's exposed to all
 * the various subsystems (server, mdcache, etc), such as pointers
 * to the other subsystems, and message-sending calls.
 */
class MDSRank {
  protected:
    const mds_rank_t whoami;

    // Incarnation as seen in MDSMap at the point where a rank is
    // assigned.
    int incarnation;

  public:

    friend class C_Flush_Journal;
    friend class C_Drop_Cache;

    friend class C_CacheDropExecAndReply;
    friend class C_ScrubExecAndReply;
    friend class C_ScrubControlExecAndReply;

    mds_rank_t get_nodeid() const { return whoami; }
    int64_t get_metadata_pool();

    // Reference to global MDS::mds_lock, so that users of MDSRank don't
    // carry around references to the outer MDS, and we can substitute
    // a separate lock here in future potentially.
    Mutex &mds_lock;

    mono_time get_starttime() const {
      return starttime;
    }
    chrono::duration<double> get_uptime() const {
      mono_time now = mono_clock::now();
      return chrono::duration<double>(now-starttime);
    }

    class CephContext *cct;

    bool is_daemon_stopping() const;

    // Reference to global cluster log client, just to avoid initialising
    // a separate one here.
    LogChannelRef &clog;

    // Reference to global timer utility, because MDSRank and MDSDaemon
    // currently both use the same mds_lock, so it makes sense for them
    // to share a timer.
    SafeTimer &timer;

    std::unique_ptr<MDSMap> &mdsmap; /* MDSDaemon::mdsmap */

    Objecter     *objecter;

    // sub systems
    Server       *server;
    MDCache      *mdcache;
    Locker       *locker;
    MDLog        *mdlog;
    MDBalancer   *balancer;
    ScrubStack   *scrubstack;
    DamageTable  damage_table;


    InoTable     *inotable;

    SnapServer   *snapserver;
    SnapClient   *snapclient;

    MDSTableClient *get_table_client(int t);
    MDSTableServer *get_table_server(int t);

    SessionMap   sessionmap;
    Session *get_session(client_t client) {
      return sessionmap.get_session(entity_name_t::CLIENT(client.v));
    }
    Session *get_session(const Message::const_ref &m);

    PerfCounters       *logger, *mlogger;
    OpTracker    op_tracker;

    // The last different state I held before current
    MDSMap::DaemonState last_state;
    // The state assigned to me by the MDSMap
    MDSMap::DaemonState state;

    bool cluster_degraded;

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

    void handle_write_error(int err);

    void handle_conf_change(const ConfigProxy& conf,
                            const std::set <std::string> &changed)
    {
      sessionmap.handle_conf_change(conf, changed);
      server->handle_conf_change(conf, changed);
      mdcache->handle_conf_change(conf, changed, *mdsmap);
      purge_queue.handle_conf_change(conf, changed, *mdsmap);
    }

    void update_mlogger();
  protected:
    // Flag to indicate we entered shutdown: anyone seeing this to be true
    // after taking mds_lock must drop out.
    bool stopping;

    // PurgeQueue is only used by StrayManager, but it is owned by MDSRank
    // because its init/shutdown happens at the top level.
    PurgeQueue   purge_queue;

    class ProgressThread : public Thread {
      MDSRank *mds;
      Cond cond;
      public:
      explicit ProgressThread(MDSRank *mds_) : mds(mds_) {}
      void * entry() override;
      void shutdown();
      void signal() {cond.Signal();}
    } progress_thread;

    list<Message::const_ref> waiting_for_nolaggy;
    MDSContext::que finished_queue;
    // Dispatch, retry, queues
    int dispatch_depth;
    void inc_dispatch_depth() { ++dispatch_depth; }
    void dec_dispatch_depth() { --dispatch_depth; }
    void retry_dispatch(const Message::const_ref &m);
    bool handle_deferrable_message(const Message::const_ref &m);
    void _advance_queues();
    bool _dispatch(const Message::const_ref &m, bool new_msg);

    ceph::heartbeat_handle_d *hb;  // Heartbeat for threads using mds_lock

    bool is_stale_message(const Message::const_ref &m) const;

    map<mds_rank_t, version_t> peer_mdsmap_epoch;

    ceph_tid_t last_tid;    // for mds-initiated requests (e.g. stray rename)

    MDSContext::vec waiting_for_active, waiting_for_replay, waiting_for_rejoin,
				waiting_for_reconnect, waiting_for_resolve;
    MDSContext::vec waiting_for_any_client_connection;
    MDSContext::que replay_queue;
    bool replaying_requests_done = false;

    map<mds_rank_t, MDSContext::vec > waiting_for_active_peer;
    map<epoch_t, MDSContext::vec > waiting_for_mdsmap;

    epoch_t osd_epoch_barrier;

    // Const reference to the beacon so that we can behave differently
    // when it's laggy.
    Beacon &beacon;

    /**
     * Emit clog warnings for any ops reported as warnings by optracker
     */
    void check_ops_in_flight();
  
    int mds_slow_req_count;

    /**
     * Share MDSMap with clients
     */
    void bcast_mds_map();  // to mounted clients
    epoch_t      last_client_mdsmap_bcast;

    map<mds_rank_t,DecayCounter> export_targets; /* targets this MDS is exporting to or wants/tries to */

    void create_logger();
  public:

    void queue_waiter(MDSContext *c) {
      finished_queue.push_back(c);
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

    MDSRank(
        mds_rank_t whoami_,
        Mutex &mds_lock_,
        LogChannelRef &clog_,
        SafeTimer &timer_,
        Beacon &beacon_,
        std::unique_ptr<MDSMap> & mdsmap_,
        Messenger *msgr,
        MonClient *monc_,
        Context *respawn_hook_,
        Context *suicide_hook_);

  protected:
    ~MDSRank();

  public:

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

    void send_message_mds(const Message::ref& m, mds_rank_t mds);
    void forward_message_mds(const MClientRequest::const_ref& req, mds_rank_t mds);
    void send_message_client_counted(const Message::ref& m, client_t client);
    void send_message_client_counted(const Message::ref& m, Session* session);
    void send_message_client_counted(const Message::ref& m, const ConnectionRef& connection);
    void send_message_client(const Message::ref& m, Session* session);
    void send_message(const Message::ref& m, const ConnectionRef& c);

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

    Finisher     *finisher;

    MDSMap *get_mds_map() { return mdsmap.get(); }

    uint64_t get_num_requests() const { return logger->get(l_mds_request); }
  
    int get_mds_slow_req_count() const { return mds_slow_req_count; }

    void dump_status(Formatter *f) const;

    void hit_export_target(mds_rank_t rank, double amount=-1.0);
    bool is_export_target(mds_rank_t rank) {
      const set<mds_rank_t>& map_targets = mdsmap->get_mds_info(get_nodeid()).export_targets;
      return map_targets.count(rank);
    }

    bool evict_client(int64_t session_id, bool wait, bool blacklist,
                      std::ostream& ss, Context *on_killed=nullptr);

    void mark_base_recursively_scrubbed(inodeno_t ino);

  protected:
    void dump_clientreplay_status(Formatter *f) const;
    void command_scrub_start(Formatter *f,
                             std::string_view path, std::string_view tag,
                             const vector<string>& scrubop_vec, Context *on_finish);
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
    void command_dump_inode(Formatter *f, const cmdmap_t &cmdmap, std::ostream &ss);
    void command_cache_drop(uint64_t timeout, Formatter *f, Context *on_finish);

  protected:
    Messenger    *messenger;
    MonClient    *monc;

    Context *respawn_hook;
    Context *suicide_hook;

    // Friended to access retry_dispatch
    friend class C_MDS_RetryMessage;

    // FIXME the state machine logic should be separable from the dispatch
    // logic that calls it.
    // >>>
    void calc_recovery_set();
    void request_state(MDSMap::DaemonState s);

    bool standby_replaying;  // true if current replay pass is in standby-replay mode

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
    friend class C_MDS_BootStart;
    friend class C_MDS_InternalBootStart;
    void boot_create();             // i am new mds.
    void boot_start(BootStep step=MDS_BOOT_INITIAL, int r=0);    // starting|replay

    void replay_start();
    void creating_done();
    void starting_done();
    void replay_done();
    void standby_replay_restart();
    void _standby_replay_restart_finish(int r, uint64_t old_read_pos);
    class C_MDS_StandbyReplayRestart;
    class C_MDS_StandbyReplayRestartFinish;

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
    // <<<
    
    // >>>
    void handle_mds_recovery(mds_rank_t who);
    void handle_mds_failure(mds_rank_t who);
    // <<<

    /* Update MDSMap export_targets for this rank. Called on ::tick(). */
    void update_targets();

    friend class C_MDS_MonCommand;
    void _mon_command_finish(int r, std::string_view cmd, std::string_view outs);
    void set_mdsmap_multimds_snaps_allowed();
private:
    mono_time starttime = mono_clock::zero();

protected:
  Context *create_async_exec_context(C_ExecAndReply *ctx);
};

/* This expects to be given a reference which it is responsible for.
 * The finish function calls functions which
 * will put the Message exactly once.*/
class C_MDS_RetryMessage : public MDSInternalContext {
public:
  C_MDS_RetryMessage(MDSRank *mds, const Message::const_ref &m)
    : MDSInternalContext(mds), m(m) {}
  void finish(int r) override {
    get_mds()->retry_dispatch(m);
  }
protected:
  Message::const_ref m;
};

class CF_MDS_RetryMessageFactory : public MDSContextFactory {
public:
  CF_MDS_RetryMessageFactory(MDSRank *mds, const Message::const_ref &m)
    : mds(mds), m(m) {}

  MDSContext *build() {
    return new C_MDS_RetryMessage(mds, m);
  }

private:
  MDSRank *mds;
  Message::const_ref m;
};

/**
 * The aspect of MDSRank exposed to MDSDaemon but not subsystems: i.e.
 * the service/dispatcher stuff like init/shutdown that subsystems should
 * never touch.
 */
class MDSRankDispatcher : public MDSRank
{
public:
  void init();
  void tick();
  void shutdown();
  bool handle_asok_command(std::string_view command, const cmdmap_t& cmdmap,
                           Formatter *f, std::ostream& ss);
  void handle_mds_map(const MMDSMap::const_ref &m, const MDSMap &oldmap);
  void handle_osd_map();
  void update_log_config();

  bool handle_command(
    const cmdmap_t &cmdmap,
    const MCommand::const_ref &m,
    int *r,
    std::stringstream *ds,
    std::stringstream *ss,
    Context **run_later,
    bool *need_reply);

  void dump_sessions(const SessionFilter &filter, Formatter *f) const;
  void evict_clients(const SessionFilter &filter, const MCommand::const_ref &m);

  // Call into me from MDS::ms_dispatch
  bool ms_dispatch(const Message::const_ref &m);

  MDSRankDispatcher(
      mds_rank_t whoami_,
      Mutex &mds_lock_,
      LogChannelRef &clog_,
      SafeTimer &timer_,
      Beacon &beacon_,
      std::unique_ptr<MDSMap> &mdsmap_,
      Messenger *msgr,
      MonClient *monc_,
      Context *respawn_hook_,
      Context *suicide_hook_);
};

// This utility for MDS and MDSRank dispatchers.
#define ALLOW_MESSAGES_FROM(peers) \
do { \
  if (m->get_connection() && (m->get_connection()->get_peer_type() & (peers)) == 0) { \
    dout(0) << __FILE__ << "." << __LINE__ << ": filtered out request, peer=" << m->get_connection()->get_peer_type() \
           << " allowing=" << #peers << " message=" << *m << dendl; \
    return true; \
  } \
} while (0)

#endif // MDS_RANK_H_

