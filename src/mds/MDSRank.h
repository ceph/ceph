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

#include "common/TrackedOp.h"
#include "common/LogClient.h"
#include "common/Timer.h"

#include "Beacon.h"
#include "DamageTable.h"
#include "MDSMap.h"
#include "SessionMap.h"
#include "MDCache.h"
#include "Migrator.h"
#include "MDLog.h"
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
  l_mdm_malloc,
  l_mdm_buf,
  l_mdm_last,
};

namespace ceph {
  struct heartbeat_handle_d;
}

class Server;
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
class MMDSMap;
class ScrubStack;

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
    mds_rank_t get_nodeid() const { return whoami; }
    uint64_t get_metadata_pool();

    // Reference to global MDS::mds_lock, so that users of MDSRank don't
    // carry around references to the outer MDS, and we can substitute
    // a separate lock here in future potentially.
    Mutex &mds_lock;

    bool is_daemon_stopping() const;

    // Reference to global cluster log client, just to avoid initialising
    // a separate one here.
    LogChannelRef &clog;

    // Reference to global timer utility, because MDSRank and MDSDaemon
    // currently both use the same mds_lock, so it makes sense for them
    // to share a timer.
    SafeTimer &timer;

    MDSMap *&mdsmap;

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

    PerfCounters       *logger, *mlogger;
    OpTracker    op_tracker;

    // The last different state I held before current
    MDSMap::DaemonState last_state;
    // The state assigned to me by the MDSMap
    MDSMap::DaemonState state;

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
    bool is_oneshot_replay() const { return state == MDSMap::STATE_ONESHOT_REPLAY; }
    bool is_any_replay() const { return (is_replay() || is_standby_replay() ||
        is_oneshot_replay()); }
    bool is_stopped() const { return mdsmap->is_stopped(whoami); }

    void handle_write_error(int err);

  protected:
    // Flag to indicate we entered shutdown: anyone seeing this to be true
    // after taking mds_lock must drop out.
    bool stopping;

    class ProgressThread : public Thread {
      MDSRank *mds;
      Cond cond;
      public:
      explicit ProgressThread(MDSRank *mds_) : mds(mds_) {}
      void * entry(); 
      void shutdown();
      void signal() {cond.Signal();}
    } progress_thread;

    list<Message*> waiting_for_nolaggy;
    list<MDSInternalContextBase*> finished_queue;
    // Dispatch, retry, queues
    int dispatch_depth;
    void inc_dispatch_depth() { ++dispatch_depth; }
    void dec_dispatch_depth() { --dispatch_depth; }
    void retry_dispatch(Message *m);
    bool handle_deferrable_message(Message *m);
    void _advance_queues();
    bool _dispatch(Message *m, bool new_msg);

    ceph::heartbeat_handle_d *hb;  // Heartbeat for threads using mds_lock
    void heartbeat_reset();

    bool is_stale_message(Message *m);

    map<mds_rank_t, version_t> peer_mdsmap_epoch;

    ceph_tid_t last_tid;    // for mds-initiated requests (e.g. stray rename)

    list<MDSInternalContextBase*> waiting_for_active, waiting_for_replay, waiting_for_reconnect, waiting_for_resolve;
    list<MDSInternalContextBase*> replay_queue;
    map<mds_rank_t, list<MDSInternalContextBase*> > waiting_for_active_peer;
    map<epoch_t, list<MDSInternalContextBase*> > waiting_for_mdsmap;

    epoch_t osd_epoch_barrier;

    // Const reference to the beacon so that we can behave differently
    // when it's laggy.
    Beacon &beacon;

    /**
     * Emit clog warnings for any ops reported as warnings by optracker
     */
    void check_ops_in_flight();

    /**
     * Share MDSMap with clients
     */
    void bcast_mds_map();  // to mounted clients
    epoch_t      last_client_mdsmap_bcast;

    void create_logger();
  public:

    void queue_waiter(MDSInternalContextBase *c) {
      finished_queue.push_back(c);
      progress_thread.signal();
    }
    void queue_waiters(list<MDSInternalContextBase*>& ls) {
      finished_queue.splice( finished_queue.end(), ls );
      progress_thread.signal();
    }

    MDSRank(
        mds_rank_t whoami_,
        Mutex &mds_lock_,
        LogChannelRef &clog_,
        SafeTimer &timer_,
        Beacon &beacon_,
        MDSMap *& mdsmap_,
        Messenger *msgr,
        MonClient *monc_,
        Objecter *objecter_,
        Context *respawn_hook_,
        Context *suicide_hook_);
    ~MDSRank();

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

    utime_t get_laggy_until() const;

    void send_message_mds(Message *m, mds_rank_t mds);
    void forward_message_mds(Message *req, mds_rank_t mds);

    void send_message_client_counted(Message *m, client_t client);
    void send_message_client_counted(Message *m, Session *session);
    void send_message_client_counted(Message *m, Connection *connection);
    void send_message_client_counted(Message *m, const ConnectionRef& con) {
      send_message_client_counted(m, con.get());
    }
    void send_message_client(Message *m, Session *session);
    void send_message(Message *m, Connection *c);
    void send_message(Message *m, const ConnectionRef& c) {
      send_message(m, c.get());
    }

    void wait_for_active(MDSInternalContextBase *c) { 
      waiting_for_active.push_back(c); 
    }
    void wait_for_active_peer(mds_rank_t who, MDSInternalContextBase *c) { 
      waiting_for_active_peer[who].push_back(c);
    }
    void wait_for_replay(MDSInternalContextBase *c) { 
      waiting_for_replay.push_back(c); 
    }
    void wait_for_reconnect(MDSInternalContextBase *c) {
      waiting_for_reconnect.push_back(c);
    }
    void wait_for_resolve(MDSInternalContextBase *c) {
      waiting_for_resolve.push_back(c);
    }
    void wait_for_mdsmap(epoch_t e, MDSInternalContextBase *c) {
      waiting_for_mdsmap[e].push_back(c);
    }
    void enqueue_replay(MDSInternalContextBase *c) {
      replay_queue.push_back(c);
    }

    bool queue_one_replay();

    void set_osd_epoch_barrier(epoch_t e);
    epoch_t get_osd_epoch_barrier() const {return osd_epoch_barrier;}

    ceph_tid_t issue_tid() { return ++last_tid; }

    Finisher     *finisher;

    MDSMap *get_mds_map() { return mdsmap; }

    int get_req_rate() { return logger->get(l_mds_request); }

    void dump_status(Formatter *f) const;

  protected:
    void dump_clientreplay_status(Formatter *f) const;
    void command_scrub_path(Formatter *f, const string& path, vector<string>& scrubop_vec);
    void command_tag_path(Formatter *f, const string& path,
                          const string &tag);
    void command_flush_path(Formatter *f, const string& path);
    void command_flush_journal(Formatter *f);
    void command_get_subtrees(Formatter *f);
    void command_export_dir(Formatter *f,
        const std::string &path, mds_rank_t dest);
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
    int _command_export_dir(const std::string &path, mds_rank_t dest);
    int _command_flush_journal(std::stringstream *ss);
    CDir *_command_dirfrag_get(
        const cmdmap_t &cmdmap,
        std::ostream &ss);

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
    // <<<
    
    // >>>
    void handle_mds_recovery(mds_rank_t who);
    void handle_mds_failure(mds_rank_t who);
    // <<<
};

/* This expects to be given a reference which it is responsible for.
 * The finish function calls functions which
 * will put the Message exactly once.*/
class C_MDS_RetryMessage : public MDSInternalContext {
protected:
  Message *m;
public:
  C_MDS_RetryMessage(MDSRank *mds, Message *m)
    : MDSInternalContext(mds)
  {
    assert(m);
    this->m = m;
  }
  virtual void finish(int r) {
    mds->retry_dispatch(m);
  }
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
  bool handle_asok_command(std::string command, cmdmap_t& cmdmap,
                           Formatter *f, std::ostream& ss);
  void handle_mds_map(MMDSMap *m, MDSMap *oldmap);
  void handle_osd_map();
  bool kill_session(int64_t session_id);
  void update_log_config();
  bool handle_command_legacy(std::vector<std::string> args);

  bool handle_command(
    const cmdmap_t &cmdmap,
    bufferlist const &inbl,
    int *r,
    std::stringstream *ds,
    std::stringstream *ss);

  void dump_sessions(
      const SessionFilter &filter, Formatter *f) const;
  std::vector<entity_name_t> evict_sessions(
      const SessionFilter &filter);

  // Call into me from MDS::ms_dispatch
  bool ms_dispatch(Message *m);

  MDSRankDispatcher(
      mds_rank_t whoami_,
      Mutex &mds_lock_,
      LogChannelRef &clog_,
      SafeTimer &timer_,
      Beacon &beacon_,
      MDSMap *& mdsmap_,
      Messenger *msgr,
      MonClient *monc_,
      Objecter *objecter_,
      Context *respawn_hook_,
      Context *suicide_hook_);
};

// This utility for MDS and MDSRank dispatchers.
#define ALLOW_MESSAGES_FROM(peers) \
do { \
  if (m->get_connection() && (m->get_connection()->get_peer_type() & (peers)) == 0) { \
    dout(0) << __FILE__ << "." << __LINE__ << ": filtered out request, peer=" << m->get_connection()->get_peer_type() \
           << " allowing=" << #peers << " message=" << *m << dendl; \
    m->put();							    \
    return true; \
  } \
} while (0)

#endif // MDS_RANK_H_

