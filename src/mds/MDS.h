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



#ifndef CEPH_MDS_H
#define CEPH_MDS_H

#include "mdstypes.h"

#include "msg/Dispatcher.h"
#include "include/CompatSet.h"
#include "include/types.h"
#include "include/Context.h"
#include "common/DecayCounter.h"
#include "common/perf_counters.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Timer.h"
#include "common/LogClient.h"
#include "common/TrackedOp.h"
#include "common/Finisher.h"
#include "common/cmdparse.h"

#include "MDSMap.h"

#include "SessionMap.h"
#include "Beacon.h"


#define CEPH_MDS_PROTOCOL    24 /* cluster internal */

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

enum {
  l_mdc_first = 3000,
  l_mdc_last,
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
class filepath;

class MonClient;

class Objecter;
class Filer;

class Server;
class Locker;
class MDCache;
class MDLog;
class MDBalancer;
class MDSInternalContextBase;

class CInode;
class CDir;
class CDentry;

class Messenger;
class Message;

class MClientRequest;
class MClientReply;

class MMDSBeacon;

class InoTable;
class SnapServer;
class SnapClient;

class MDSTableServer;
class MDSTableClient;

class AuthAuthorizeHandlerRegistry;

class MDS : public Dispatcher, public md_config_obs_t {
 public:
  Mutex        mds_lock;
  SafeTimer    timer;

 private:
  ceph::heartbeat_handle_d *hb;  // Heartbeat for threads using mds_lock
  void heartbeat_reset();
  Beacon  beacon;
  void set_want_state(MDSMap::DaemonState newstate);
 public:
  utime_t get_laggy_until() {return beacon.get_laggy_until();}

  AuthAuthorizeHandlerRegistry *authorize_handler_cluster_registry;
  AuthAuthorizeHandlerRegistry *authorize_handler_service_registry;

  string name;
  mds_rank_t whoami;
  int incarnation;

  mds_rank_t standby_for_rank;
  MDSMap::DaemonState standby_type;  // one of STANDBY_REPLAY, ONESHOT_REPLAY
  string standby_for_name;
  bool standby_replaying;  // true if current replay pass is in standby-replay mode

  Messenger    *messenger;
  MonClient    *monc;
  MDSMap       *mdsmap;
  Objecter     *objecter;
  Filer        *filer;       // for reading/writing to/from osds
  LogClient    log_client;
  LogChannelRef clog;

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

  PerfCounters       *logger, *mlogger;
  OpTracker    op_tracker;

  Finisher finisher;

  int orig_argc;
  const char **orig_argv;

 protected:
  // -- MDS state --
  MDSMap::DaemonState last_state;
  MDSMap::DaemonState state;         // my confirmed state
  MDSMap::DaemonState want_state;    // the state i want

  list<MDSInternalContextBase*> waiting_for_active, waiting_for_replay, waiting_for_reconnect, waiting_for_resolve;
  list<MDSInternalContextBase*> replay_queue;
  map<mds_rank_t, list<MDSInternalContextBase*> > waiting_for_active_peer;
  list<Message*> waiting_for_nolaggy;
  map<epoch_t, list<MDSInternalContextBase*> > waiting_for_mdsmap;

  map<mds_rank_t, version_t> peer_mdsmap_epoch;

  ceph_tid_t last_tid;    // for mds-initiated requests (e.g. stray rename)

 public:
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

  void request_state(MDSMap::DaemonState s);

  ceph_tid_t issue_tid() { return ++last_tid; }
    

  // -- waiters --
private:
  list<MDSInternalContextBase*> finished_queue;
  void _advance_queues();
public:

  void queue_waiter(MDSInternalContextBase *c) {
    finished_queue.push_back(c);
    progress_thread.signal();
  }
  void queue_waiters(list<MDSInternalContextBase*>& ls) {
    finished_queue.splice( finished_queue.end(), ls );
    progress_thread.signal();
  }
  bool queue_one_replay() {
    if (replay_queue.empty())
      return false;
    queue_waiter(replay_queue.front());
    replay_queue.pop_front();
    return true;
  }
  
  // tick and other timer fun
  class C_MDS_Tick : public MDSInternalContext {
  public:
    C_MDS_Tick(MDS *m) : MDSInternalContext(m) {}
    void finish(int r) {
      mds->tick_event = 0;
      mds->tick();
    }
  } *tick_event;
  void     reset_tick();

  // -- client map --
  SessionMap   sessionmap;
  epoch_t      last_client_mdsmap_bcast;
  //void log_clientmap(Context *c);


  // shutdown crap
  int req_rate;

  // ino's and fh's
 public:

  int get_req_rate() { return req_rate; }
  Session *get_session(client_t client) {
    return sessionmap.get_session(entity_name_t::CLIENT(client.v));
  }

 private:
  int dispatch_depth;
  bool ms_dispatch(Message *m);
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  bool ms_verify_authorizer(Connection *con, int peer_type,
			       int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			       bool& isvalid, CryptoKey& session_key);
  void ms_handle_accept(Connection *con);
  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);

private:
  class ProgressThread : public Thread {
    MDS *mds;
    bool stopping;
    Cond cond;
  public:
    ProgressThread(MDS *mds_) : mds(mds_), stopping(false) {}
    void * entry(); 
    void shutdown();
    void signal() {cond.Signal();}
  } progress_thread;
  void _progress_thread();

 public:
  MDS(const std::string &n, Messenger *m, MonClient *mc);
  ~MDS();

  // handle a signal (e.g., SIGTERM)
  void handle_signal(int signum);

  // who am i etc
  mds_rank_t get_nodeid() const { return whoami; }
  uint64_t get_metadata_pool() { return mdsmap->get_metadata_pool(); }
  MDSMap *get_mds_map() { return mdsmap; }

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

  // start up, shutdown
  int init(MDSMap::DaemonState wanted_state=MDSMap::STATE_BOOT);

  // admin socket handling
  friend class MDSSocketHook;
  class MDSSocketHook *asok_hook;
  bool asok_command(string command, cmdmap_t& cmdmap, string format,
		    ostream& ss);
  void set_up_admin_socket();
  void clean_up_admin_socket();
  void check_ops_in_flight(); // send off any slow ops to monitor
  void command_scrub_path(Formatter *f, const string& path);
  void command_flush_path(Formatter *f, const string& path);
  void command_flush_journal(Formatter *f);
 private:
  int _command_flush_journal(std::stringstream *ss);
 public:
    // config observer bits
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
				  const std::set <std::string> &changed);
  void create_logger();
  void update_log_config();

  void bcast_mds_map();  // to mounted clients

  void boot_create();             // i am new mds.

 private:
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
  void boot_start(BootStep step=MDS_BOOT_INITIAL, int r=0);    // starting|replay
  void calc_recovery_set();
 public:

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

  void handle_mds_recovery(mds_rank_t who);
  void handle_mds_failure(mds_rank_t who);

  void suicide();
  void respawn();

  void tick();
  

  void inc_dispatch_depth() { ++dispatch_depth; }
  void dec_dispatch_depth() { --dispatch_depth; }

  // messages
  bool _dispatch(Message *m);

  protected:
  bool is_stale_message(Message *m);

  bool handle_core_message(Message *m);
  bool handle_deferrable_message(Message *m);
  
  // special message types
  int _handle_command_legacy(std::vector<std::string> args);
  int _handle_command(
      const cmdmap_t &cmdmap,
      bufferlist const &inbl,
      bufferlist *outbl,
      std::string *outs,
      Context **run_later);
  void handle_command(class MMonCommand *m);
  void handle_command(class MCommand *m);
  void handle_mds_map(class MMDSMap *m);
};


/* This expects to be given a reference which it is responsible for.
 * The finish function calls functions which
 * will put the Message exactly once.*/
class C_MDS_RetryMessage : public MDSInternalContext {
  Message *m;
public:
  C_MDS_RetryMessage(MDS *mds, Message *m) : MDSInternalContext(mds) {
    assert(m);
    this->m = m;
  }
  virtual void finish(int r) {
    mds->inc_dispatch_depth();
    mds->_dispatch(m);
    mds->dec_dispatch_depth();
  }
};

#endif
