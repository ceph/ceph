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

#include "MDSRank.h"
#include "MDSMap.h"

#include "Beacon.h"


#define CEPH_MDS_PROTOCOL    27 /* cluster internal */

class filepath;

class MonClient;

class Objecter;
class Filer;

class Server;
class Locker;
class MDCache;
class MDBalancer;
class MDSInternalContextBase;

class CInode;
class CDir;
class CDentry;

class Messenger;
class Message;

class MMDSBeacon;

class InoTable;
class SnapServer;
class SnapClient;

class MDSTableServer;
class MDSTableClient;

class AuthAuthorizeHandlerRegistry;

class MDS : public MDSRank, public Dispatcher, public md_config_obs_t {
 public:

  /* Global MDS lock: every time someone takes this, they must
   * also check the `stopping` flag.  If stopping is true, you
   * must either do nothing and immediately drop the lock, or
   * never drop the lock again (i.e. call respawn()) */
  Mutex        mds_lock;
  bool         stopping;

  SafeTimer    timer;

 public:
  // XXX fixme beacon only public for the benefit of MDSRank who wants
  // to see if it's laggy
  Beacon  beacon;
 private:
  void set_want_state(MDSMap::DaemonState newstate);
 public:

  AuthAuthorizeHandlerRegistry *authorize_handler_cluster_registry;
  AuthAuthorizeHandlerRegistry *authorize_handler_service_registry;

  string name;

  mds_rank_t standby_for_rank;
  MDSMap::DaemonState standby_type;  // one of STANDBY_REPLAY, ONESHOT_REPLAY
  string standby_for_name;
  bool standby_replaying;  // true if current replay pass is in standby-replay mode

  Messenger    *messenger;
  MonClient    *monc;
  MDSMap       *mdsmap;
  Objecter     *objecter;
  LogClient    log_client;
  LogChannelRef clog;

  //OpTracker    op_tracker;

  Finisher finisher;

  int orig_argc;
  const char **orig_argv;

public:
  void request_state(MDSMap::DaemonState s);

  // tick and other timer fun
  class C_MDS_Tick : public MDSInternalContext {
    protected:
      MDS *mds_daemon;
  public:
    C_MDS_Tick(MDS *m) : MDSInternalContext(m), mds_daemon(m) {}
    void finish(int r) {
      mds_daemon->tick_event = 0;
      mds_daemon->tick();
    }
  } *tick_event;
  void     reset_tick();

  // -- client map --
  epoch_t      last_client_mdsmap_bcast;

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

 public:
  MDS(const std::string &n, Messenger *m, MonClient *mc);
  ~MDS();

  // handle a signal (e.g., SIGTERM)
  void handle_signal(int signum);

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
 private:
  int _command_export_dir(const std::string &path, mds_rank_t dest);
  int _command_flush_journal(std::stringstream *ss);
  CDir *_command_dirfrag_get(
      const cmdmap_t &cmdmap,
      std::ostream &ss);
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

 protected:
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
 public:

  void handle_mds_recovery(mds_rank_t who);
  void handle_mds_failure(mds_rank_t who);

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
  void damaged_unlocked()
  {
    Mutex::Locker l(mds_lock);
    damaged();
  }

  /**
   * Terminate this daemon process.
   *
   * This function will return, but once it does so the calling thread
   * must do no more work as all subsystems will have been shut down.
   *
   * @param fast: if true, do not send a message to the mon before shutting
   *              down
   */
  void suicide(bool fast = false);

  /**
   * Start a new daemon process with the same command line parameters that
   * this process was run with, then terminate this process
   */
  void respawn();

  void tick();
  
  void inc_dispatch_depth() { ++dispatch_depth; }
  void dec_dispatch_depth() { --dispatch_depth; }

  // messages
  bool _dispatch(Message *m, bool new_msg);

  protected:
  bool handle_core_message(Message *m);
  
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


#endif
