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
#include "MDSMap.h"
#include "SessionMap.h"
#include "MDCache.h"
#include "Migrator.h"

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
class MDS;
class Messenger;
class Objecter;
class MonClient;
class Finisher;

/**
 * The public part of this class's interface is what's exposed to all
 * the various subsystems (server, mdcache, etc)
 */
class MDSRank {
  public:
    mds_rank_t whoami;

    // FIXME: incarnation is logically a daemon property, not a rank property,
    // but used in log msgs and set on objecter (objecter *is* logically
    // a rank thing rather than daemon thing)
    int incarnation;

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

    MDSMap::DaemonState last_state;
    MDSMap::DaemonState want_state;    // the state i want
    MDSMap::DaemonState state;         // my confirmed state
    MDSMap::DaemonState get_state() { return state; } 
    MDSMap::DaemonState get_want_state() { return want_state; } 


    bool is_creating() { return state == MDSMap::STATE_CREATING; }
    bool is_starting() { return state == MDSMap::STATE_STARTING; }
    bool is_standby()  { return state == MDSMap::STATE_STANDBY; }
    bool is_replay()   { return state == MDSMap::STATE_REPLAY; }
    bool is_standby_replay() { return state == MDSMap::STATE_STANDBY_REPLAY; }
    bool is_resolve()  { return state == MDSMap::STATE_RESOLVE; }
    bool is_reconnect() { return state == MDSMap::STATE_RECONNECT; }
    bool is_rejoin()   { return state == MDSMap::STATE_REJOIN; }
    bool is_clientreplay()   { return state == MDSMap::STATE_CLIENTREPLAY; }
    bool is_active()   { return state == MDSMap::STATE_ACTIVE; }
    bool is_stopping() { return state == MDSMap::STATE_STOPPING; }
    bool is_oneshot_replay()   { return state == MDSMap::STATE_ONESHOT_REPLAY; }
    bool is_any_replay() { return (is_replay() || is_standby_replay() ||
        is_oneshot_replay()); }
    bool is_stopped()  { return mdsmap->is_stopped(whoami); }

    void handle_write_error(int err);

  protected:
    class ProgressThread : public Thread {
      MDSRank *mds;
      Cond cond;
      public:
      ProgressThread(MDSRank *mds_) : mds(mds_) {}
      void * entry(); 
      void shutdown();
      void signal() {cond.Signal();}
    } progress_thread;

    list<Message*> waiting_for_nolaggy;
    list<MDSInternalContextBase*> finished_queue;

    bool handle_deferrable_message(Message *m);
    void _advance_queues();

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
        Mutex &mds_lock_,
        LogChannelRef &clog_,
        SafeTimer &timer_,
        Beacon &beacon_,
        MDSMap *& mdsmap_,
        Finisher *finisher_,
        MDS *mds_daemon_,
        Messenger *msgr,
        MonClient *monc_);
    ~MDSRank();

    // Daemon functions: these guys break the abstraction
    // and call up into the parent MDSDaemon instance.  It's kind
    // of unavoidable: if we want any depth into our calls 
    // to be able to e.g. tear down the whole process, we have to
    // have a reference going all the way down.
    //
    // But for others, like dispatch, these can go away once the
    // logical separate between MDSDaemon messages and MDSRank messages
    // is put in place.
    // >>>
    void suicide(bool fast = false);
    void respawn();
    void damaged();
    void damaged_unlocked();
    void dispatch(Message *m);
    utime_t get_laggy_until() const;
    // <<<

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

    bool queue_one_replay() {
      if (replay_queue.empty())
        return false;
      queue_waiter(replay_queue.front());
      replay_queue.pop_front();
      return true;
    }

    void set_osd_epoch_barrier(epoch_t e);
    epoch_t get_osd_epoch_barrier() const {return osd_epoch_barrier;}

    ceph_tid_t issue_tid() { return ++last_tid; }

    Finisher     *finisher;

    MDSMap *get_mds_map() { return mdsmap; }

    // Access to monc functionality needed by balancer and snapserver
    uint64_t get_global_id() const;
    void send_mon_message(Message *m);

    int get_req_rate() { return logger->get(l_mds_request); }

  protected:
    // FIXME: reference cycle: MDSRank should not carry a reference up to MDSDaemon
    MDS          *mds_daemon;
    Messenger    *messenger;
    MonClient    *monc;
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
    mds->dispatch(m);
  }
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

