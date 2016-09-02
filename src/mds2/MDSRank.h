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

#include "messages/MCommand.h"

#include "Beacon.h"
#include "MDSMap.h"
#include "MDSContext.h"

#include "common/WorkQueue.h"
#include "common/PrioritizedQueue.h"

namespace ceph {
  struct heartbeat_handle_d;
}

class Server;
class Locker;
class MDCache;
class Messenger;
class MonClient;
class Finisher;
class MMDSMap;
class Objecter;
class SessionMap;
class Session;
class InoTable;
class MDLog;

struct MDRequestImpl;
typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;

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
  int64_t get_metadata_pool();

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
  Locker       *locker;
  MDCache      *mdcache;

  MDLog        *mdlog;
  SessionMap   *sessionmap;
  InoTable     *inotable;

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
  bool is_any_replay() const { return (is_replay() || is_standby_replay()); }
  bool is_stopped() const { return mdsmap->is_stopped(whoami); }

  void handle_write_error(int err) { assert(0); };
  void damaged() { assert(0); }
  void damaged_unlocked() { assert(0); }

protected:
  // Flag to indicate we entered shutdown: anyone seeing this to be true
  // after taking mds_lock must drop out.
  bool stopping;

  // Dispatch, retry, queues
  bool is_deferrable_message(Message *m);
  void handle_deferrable_message(Message *m);

  ceph::heartbeat_handle_d *hb;  // Heartbeat for threads using mds_lock
  void heartbeat_reset();

  map<mds_rank_t, version_t> peer_mdsmap_epoch;

  // Const reference to the beacon so that we can behave differently
  // when it's laggy.
  Beacon &beacon;

  /**
   * Share MDSMap with clients
   */
  epoch_t      last_client_mdsmap_bcast;

  void create_logger();
public:
  MDSRank(
      mds_rank_t whoami_,
      Mutex &mds_lock_,
      LogChannelRef &clog_,
      SafeTimer &timer_,
      Beacon &beacon_,
      MDSMap *& mdsmap_,
      Messenger *msgr,
      MonClient *monc_,
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

  utime_t get_laggy_until() const;

  void send_message_mds(Message *m, mds_rank_t mds);
  void forward_message_mds(Message *req, mds_rank_t mds);

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

  void bcast_mds_map();  // to mounted clients

public:
  Session *get_session(Message *m);

  MDSMap *get_mds_map() { return mdsmap; }

  void dump_status(Formatter *f) const;

protected:
  Messenger    *messenger;
  MonClient    *monc;

  Context *respawn_hook;
  Context *suicide_hook;

  void request_state(MDSMap::DaemonState s);

  typedef enum {
    // The MDSMap is available, configure default layouts and structures
    MDS_BOOT_INITIAL = 0,
    // We are ready to open some inodes
    // MDS_BOOT_OPEN_ROOT,
    // We are ready to do a replay if needed
    MDS_BOOT_PREPARE_LOG,
    // Replay is complete
    MDS_BOOT_REPLAY_DONE
  } BootStep;
  friend class C_MDS_BootStart;
  void boot_start(BootStep step=MDS_BOOT_INITIAL, int r=0);    // starting|replay
  void boot_create();             // i am new mds.

  void starting_done();
  void creating_done();
  void replay_start();
  void replay_done();
  void resolve_start();
  void resolve_done();
  void reconnect_start();
  void reconnect_done();
  void rejoin_start();
  void rejoin_done();
  void recovery_done(int oldstate);
  void clientreplay_start();
  void active_start();
  void stopping_start();
  // <<<
  
  // >>>
  void handle_mds_recovery(mds_rank_t who);
  void handle_mds_failure(mds_rank_t who);
  // <<<

protected:
  ThreadPool op_tp;
  ThreadPool msg_tp;
  // finish log contexts in same order of log entires
  Finisher *log_finisher;
public:
  Finisher *get_log_finisher() { return log_finisher; }

  class CtxWQ: public ThreadPool::WorkQueueVal<pair<MDSContextBase*,int> > {
  public:
    CtxWQ(MDSRank *m, time_t ti, ThreadPool *tp);
    void queue(MDSContextBase *ctx, int result = 0) {
      ThreadPool::WorkQueueVal<pair<MDSContextBase*,int> >::queue(make_pair(ctx, result));
    }
  protected:
    MDSRank *mds;
    list<pair<MDSContextBase*,int> > context_queue;
    bool _empty() override {
      return context_queue.empty();
    }
    void _enqueue(pair<MDSContextBase*,int> item) override {
      context_queue.push_back(item);
    }
    void _enqueue_front(pair<MDSContextBase*,int> item) override {
      context_queue.push_front(item);
    }
    pair<MDSContextBase*,int> _dequeue() override {
      auto item = context_queue.front();
      context_queue.pop_front();
      return item;
    }
    void _process(pair<MDSContextBase*,int> item, ThreadPool::TPHandle &) override {
      item.first->complete(item.second);
    }
    void _clear() override {
      assert(context_queue.empty());
    }
  } ctx_wq;

  void queue_context(MDSContextBase *c, int r=0) {
    ctx_wq.queue(c, r);
  }
  void queue_contexts(std::list<MDSContextBase*>& ls, int r=0) {
    for (auto c : ls)
      ctx_wq.queue(c, r);
  }

  class ReqWQ: public ThreadPool::WorkQueueVal<const MDRequestRef&, entity_name_t> {
  public:
    ReqWQ(MDSRank *m, time_t ti, ThreadPool *tp);
  protected:
    MDSRank *mds;
    Mutex qlock;
    std::map<entity_name_t, std::list<MDRequestRef> > req_for_processing;
    PrioritizedQueue<MDRequestRef, entity_name_t> pqueue;

    void _enqueue_front(const MDRequestRef&);
    void _enqueue(const MDRequestRef&);
    entity_name_t _dequeue();

    bool _empty() {
      return pqueue.empty();
    }
    void _process(entity_name_t, ThreadPool::TPHandle &handle);
    void _clear() override {
      assert(pqueue.empty() && req_for_processing.empty());
    }
  } req_wq;

  class MsgWQ: public ThreadPool::WorkQueueVal<Message*, entity_inst_t> {
  public:
    MsgWQ(MDSRank *m, time_t ti, ThreadPool *tp);
  protected:
    MDSRank *mds;
    Mutex qlock;
    std::map<entity_inst_t, pair<bool, std::list<Message*> > > msg_for_processing;
    std::list<entity_inst_t> more_to_process;
    PrioritizedQueue<Message*, entity_inst_t> pqueue;

    void _enqueue_front(Message*);
    void _enqueue(Message*);
    entity_inst_t _dequeue();

    bool _empty() {
      if (!pqueue.empty())
        return false;
      Mutex::Locker l(qlock);
      return more_to_process.empty();
    }
    void _process(entity_inst_t, ThreadPool::TPHandle &handle);
    void _clear() override {
      assert(pqueue.empty() && msg_for_processing.empty());
    }
  } msg_wq;

  void retry_dispatch(const MDRequestRef &mdr);
};

class C_MDS_RetryMessage : public MDSContext {
  protected:
    Message *msg;
  public:
    C_MDS_RetryMessage(MDSRank *mds, Message *m)
      : MDSContext(mds) , msg(m) {}
    virtual void finish(int r) {
//      mds->retry_dispatch(m);
    }
};

class C_MDS_RetryRequest : public MDSContext {
  MDRequestRef mdr;
public:
  C_MDS_RetryRequest(MDSRank *mds, const MDRequestRef& r)
    : MDSContext(mds), mdr(r) {}
  bool is_async() const { return true; }
  virtual void finish(int r) {
    get_mds()->retry_dispatch(mdr);
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
    MCommand *m, int *r,
    std::stringstream *ds,
    std::stringstream *ss,
    bool *need_reply);

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
    return; \
  } \
} while (0)

#endif // MDS_RANK_H_

