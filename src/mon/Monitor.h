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

/* 
 * This is the top level monitor. It runs on each machine in the Monitor   
 * Cluster. The election of a leader for the paxos algorithm only happens 
 * once per machine via the elector. There is a separate paxos instance (state) 
 * kept for each of the system components: Object Store Device (OSD) Monitor, 
 * Placement Group (PG) Monitor, Metadata Server (MDS) Monitor, and Client Monitor.
 */

#ifndef CEPH_MONITOR_H
#define CEPH_MONITOR_H

#include "include/types.h"
#include "msg/Messenger.h"

#include "common/Timer.h"

#include "MonMap.h"
#include "Elector.h"
#include "Paxos.h"
#include "Session.h"

#include "osd/OSDMap.h"

#include "common/LogClient.h"
#include "common/SimpleRNG.h"
#include "common/cmdparse.h"

#include "auth/cephx/CephxKeyServer.h"
#include "auth/AuthMethodList.h"
#include "auth/KeyRing.h"

#include "perfglue/heap_profiler.h"

#include "messages/MMonCommand.h"
#include "messages/MPing.h"
#include "mon/MonitorDBStore.h"

#include <memory>
#include "include/memory.h"
#include "include/str_map.h"
#include <errno.h>
#include <cmath>

#include "common/TrackedOp.h"
#include "mon/MonOpRequest.h"


#define CEPH_MON_PROTOCOL     13 /* cluster internal */


enum {
  l_cluster_first = 555000,
  l_cluster_num_mon,
  l_cluster_num_mon_quorum,
  l_cluster_num_osd,
  l_cluster_num_osd_up,
  l_cluster_num_osd_in,
  l_cluster_osd_epoch,
  l_cluster_osd_bytes,
  l_cluster_osd_bytes_used,
  l_cluster_osd_bytes_avail,
  l_cluster_num_pool,
  l_cluster_num_pg,
  l_cluster_num_pg_active_clean,
  l_cluster_num_pg_active,
  l_cluster_num_pg_peering,
  l_cluster_num_object,
  l_cluster_num_object_degraded,
  l_cluster_num_object_misplaced,
  l_cluster_num_object_unfound,
  l_cluster_num_bytes,
  l_cluster_num_mds_up,
  l_cluster_num_mds_in,
  l_cluster_num_mds_failed,
  l_cluster_mds_epoch,
  l_cluster_last,
};

enum {
  l_mon_first = 456000,
  l_mon_num_sessions,
  l_mon_session_add,
  l_mon_session_rm,
  l_mon_session_trim,
  l_mon_num_elections,
  l_mon_election_call,
  l_mon_election_win,
  l_mon_election_lose,
  l_mon_last,
};

class QuorumService;
class PaxosService;

class PerfCounters;
class AdminSocketHook;

class MMonGetMap;
class MMonGetVersion;
class MMonMetadata;
class MMonSync;
class MMonScrub;
class MMonProbe;
struct MMonSubscribe;
class MAuthRotating;
struct MRoute;
struct MForward;
struct MTimeCheck;
struct MMonHealth;
struct MonCommand;

#define COMPAT_SET_LOC "feature_set"

class Monitor : public Dispatcher,
                public md_config_obs_t {
public:
  // me
  string name;
  int rank;
  Messenger *messenger;
  ConnectionRef con_self;
  Mutex lock;
  SafeTimer timer;
  
  /// true if we have ever joined a quorum.  if false, we are either a
  /// new cluster, a newly joining monitor, or a just-upgraded
  /// monitor.
  bool has_ever_joined;

  PerfCounters *logger, *cluster_logger;
  bool cluster_logger_registered;

  void register_cluster_logger();
  void unregister_cluster_logger();

  MonMap *monmap;
  uuid_d fingerprint;

  set<entity_addr_t> extra_probe_peers;

  LogClient log_client;
  LogChannelRef clog;
  LogChannelRef audit_clog;
  KeyRing keyring;
  KeyServer key_server;

  AuthMethodList auth_cluster_required;
  AuthMethodList auth_service_required;

  CompatSet features;

  const MonCommand *leader_supported_mon_commands;
  int leader_supported_mon_commands_size;

private:
  void new_tick();
  friend class C_Mon_Tick;

  // -- local storage --
public:
  MonitorDBStore *store;
  static const string MONITOR_NAME;
  static const string MONITOR_STORE_PREFIX;

  // -- monitor state --
private:
  enum {
    STATE_PROBING = 1,
    STATE_SYNCHRONIZING,
    STATE_ELECTING,
    STATE_LEADER,
    STATE_PEON,
    STATE_SHUTDOWN
  };
  int state;

public:
  static const char *get_state_name(int s) {
    switch (s) {
    case STATE_PROBING: return "probing";
    case STATE_SYNCHRONIZING: return "synchronizing";
    case STATE_ELECTING: return "electing";
    case STATE_LEADER: return "leader";
    case STATE_PEON: return "peon";
    case STATE_SHUTDOWN: return "shutdown";
    default: return "???";
    }
  }
  const char *get_state_name() const {
    return get_state_name(state);
  }

  bool is_shutdown() const { return state == STATE_SHUTDOWN; }
  bool is_probing() const { return state == STATE_PROBING; }
  bool is_synchronizing() const { return state == STATE_SYNCHRONIZING; }
  bool is_electing() const { return state == STATE_ELECTING; }
  bool is_leader() const { return state == STATE_LEADER; }
  bool is_peon() const { return state == STATE_PEON; }

  const utime_t &get_leader_since() const;

  void prepare_new_fingerprint(MonitorDBStore::TransactionRef t);

  // -- elector --
private:
  Paxos *paxos;
  Elector elector;
  friend class Elector;

  /// features we require of peers (based on on-disk compatset)
  uint64_t required_features;
  
  int leader;            // current leader (to best of knowledge)
  set<int> quorum;       // current active set of monitors (if !starting)
  utime_t leader_since;  // when this monitor became the leader, if it is the leader
  utime_t exited_quorum; // time detected as not in quorum; 0 if in
  /**
   * Intersection of quorum member's connection feature bits.
   */
  uint64_t quorum_con_features;
  /**
   * Intersection of quorum members mon-specific feature bits
   */
  mon_feature_t quorum_mon_features;
  bufferlist supported_commands_bl; // encoded MonCommands we support
  bufferlist classic_commands_bl; // encoded MonCommands supported by Dumpling
  set<int> classic_mons; // set of "classic" monitors; only valid on leader

  set<string> outside_quorum;

  /**
   * @defgroup Monitor_h_scrub
   * @{
   */
  version_t scrub_version;            ///< paxos version we are scrubbing
  map<int,ScrubResult> scrub_result;  ///< results so far

  /**
   * trigger a cross-mon scrub
   *
   * Verify all mons are storing identical content
   */
  int scrub_start();
  int scrub();
  void handle_scrub(MonOpRequestRef op);
  bool _scrub(ScrubResult *r,
              pair<string,string> *start,
              int *num_keys);
  void scrub_check_results();
  void scrub_timeout();
  void scrub_finish();
  void scrub_reset();
  void scrub_update_interval(int secs);

  struct C_Scrub : public Context {
    Monitor *mon;
    explicit C_Scrub(Monitor *m) : mon(m) { }
    void finish(int r) {
      mon->scrub_start();
    }
  };
  struct C_ScrubTimeout : public Context {
    Monitor *mon;
    explicit C_ScrubTimeout(Monitor *m) : mon(m) { }
    void finish(int r) {
      mon->scrub_timeout();
    }
  };
  Context *scrub_event;       ///< periodic event to trigger scrub (leader)
  Context *scrub_timeout_event;  ///< scrub round timeout (leader)
  void scrub_event_start();
  void scrub_event_cancel();
  void scrub_reset_timeout();
  void scrub_cancel_timeout();

  struct ScrubState {
    pair<string,string> last_key; ///< last scrubbed key
    bool finished;

    ScrubState() : finished(false) { }
    virtual ~ScrubState() { }
  };
  ceph::shared_ptr<ScrubState> scrub_state; ///< keeps track of current scrub

  /**
   * @defgroup Monitor_h_sync Synchronization
   * @{
   */
  /**
   * @} // provider state
   */
  struct SyncProvider {
    entity_inst_t entity;  ///< who
    uint64_t cookie;       ///< unique cookie for this sync attempt
    utime_t timeout;       ///< when we give up and expire this attempt
    version_t last_committed; ///< last paxos version on peer
    pair<string,string> last_key; ///< last key sent to (or on) peer
    bool full;             ///< full scan?
    MonitorDBStore::Synchronizer synchronizer;   ///< iterator

    SyncProvider() : cookie(0), last_committed(0), full(false) {}

    void reset_timeout(CephContext *cct, int grace) {
      timeout = ceph_clock_now(cct);
      timeout += grace;
    }
  };

  map<uint64_t, SyncProvider> sync_providers;  ///< cookie -> SyncProvider for those syncing from us
  uint64_t sync_provider_count;   ///< counter for issued cookies to keep them unique

  /**
   * @} // requester state
   */
  entity_inst_t sync_provider;   ///< who we are syncing from
  uint64_t sync_cookie;          ///< 0 if we are starting, non-zero otherwise
  bool sync_full;                ///< true if we are a full sync, false for recent catch-up
  version_t sync_start_version;  ///< last_committed at sync start
  Context *sync_timeout_event;   ///< timeout event

  /**
   * floor for sync source
   *
   * When we sync we forget about our old last_committed value which
   * can be dangerous.  For example, if we have a cluster of:
   *
   *   mon.a: lc 100
   *   mon.b: lc 80
   *   mon.c: lc 100 (us)
   *
   * If something forces us to sync (say, corruption, or manual
   * intervention, or bug), we forget last_committed, and might abort.
   * If mon.a happens to be down when we come back, we will see:
   *
   *   mon.b: lc 80
   *   mon.c: lc 0 (us)
   *
   * and sync from mon.b, at which point a+b will both have lc 80 and
   * come online with a majority holding out of date commits.
   *
   * Avoid this by preserving our old last_committed value prior to
   * sync and never going backwards.
   */
  version_t sync_last_committed_floor;

  struct C_SyncTimeout : public Context {
    Monitor *mon;
    explicit C_SyncTimeout(Monitor *m) : mon(m) {}
    void finish(int r) {
      mon->sync_timeout();
    }
  };

  /**
   * Obtain the synchronization target prefixes in set form.
   *
   * We consider a target prefix all those that are relevant when
   * synchronizing two stores. That is, all those that hold paxos service's
   * versions, as well as paxos versions, or any control keys such as the
   * first or last committed version.
   *
   * Given the current design, this function should return the name of all and
   * any available paxos service, plus the paxos name.
   *
   * @returns a set of strings referring to the prefixes being synchronized
   */
  set<string> get_sync_targets_names();

  /**
   * Reset the monitor's sync-related data structures for syncing *from* a peer
   */
  void sync_reset_requester();

  /**
   * Reset sync state related to allowing others to sync from us
   */
  void sync_reset_provider();

  /**
   * Caled when a sync attempt times out (requester-side)
   */
  void sync_timeout();

  /**
   * Get the latest monmap for backup purposes during sync
   */
  void sync_obtain_latest_monmap(bufferlist &bl);

  /**
   * Start sync process
   *
   * Start pulling committed state from another monitor.
   *
   * @param entity where to pull committed state from
   * @param full whether to do a full sync or just catch up on recent paxos
   */
  void sync_start(entity_inst_t &entity, bool full);

public:
  /**
   * force a sync on next mon restart
   */
  void sync_force(Formatter *f, ostream& ss);

private:
  /**
   * store critical state for safekeeping during sync
   *
   * We store a few things on the side that we don't want to get clobbered by sync.  This
   * includes the latest monmap and a lower bound on last_committed.
   */
  void sync_stash_critical_state(MonitorDBStore::TransactionRef tx);

  /**
   * reset the sync timeout
   *
   * This is used on the client to restart if things aren't progressing
   */
  void sync_reset_timeout();

  /**
   * trim stale sync provider state
   *
   * If someone is syncing from us and hasn't talked to us recently, expire their state.
   */
  void sync_trim_providers();

  /**
   * Complete a sync
   *
   * Finish up a sync after we've gotten all of the chunks.
   *
   * @param last_committed final last_committed value from provider
   */
  void sync_finish(version_t last_committed);

  /**
   * request the next chunk from the provider
   */
  void sync_get_next_chunk();

  /**
   * handle sync message
   *
   * @param m Sync message with operation type MMonSync::OP_START_CHUNKS
   */
  void handle_sync(MonOpRequestRef op);

  void _sync_reply_no_cookie(MonOpRequestRef op);

  void handle_sync_get_cookie(MonOpRequestRef op);
  void handle_sync_get_chunk(MonOpRequestRef op);
  void handle_sync_finish(MonOpRequestRef op);

  void handle_sync_cookie(MonOpRequestRef op);
  void handle_sync_forward(MonOpRequestRef op);
  void handle_sync_chunk(MonOpRequestRef op);
  void handle_sync_no_cookie(MonOpRequestRef op);

  /**
   * @} // Synchronization
   */

  list<Context*> waitfor_quorum;
  list<Context*> maybe_wait_for_quorum;

  /**
   * @defgroup Monitor_h_TimeCheck Monitor Clock Drift Early Warning System
   * @{
   *
   * We use time checks to keep track of any clock drifting going on in the
   * cluster. This is accomplished by periodically ping each monitor in the
   * quorum and register its response time on a map, assessing how much its
   * clock has drifted. We also take this opportunity to assess the latency
   * on response.
   *
   * This mechanism works as follows:
   *
   *  - Leader sends out a 'PING' message to each other monitor in the quorum.
   *    The message is timestamped with the leader's current time. The leader's
   *    current time is recorded in a map, associated with each peon's
   *    instance.
   *  - The peon replies to the leader with a timestamped 'PONG' message.
   *  - The leader calculates a delta between the peon's timestamp and its
   *    current time and stashes it.
   *  - The leader also calculates the time it took to receive the 'PONG'
   *    since the 'PING' was sent, and stashes an approximate latency estimate.
   *  - Once all the quorum members have pong'ed, the leader will share the
   *    clock skew and latency maps with all the monitors in the quorum.
   */
  map<entity_inst_t, utime_t> timecheck_waiting;
  map<entity_inst_t, double> timecheck_skews;
  map<entity_inst_t, double> timecheck_latencies;
  // odd value means we are mid-round; even value means the round has
  // finished.
  version_t timecheck_round;
  unsigned int timecheck_acks;
  utime_t timecheck_round_start;
  /* When we hit a skew we will start a new round based off of
   * 'mon_timecheck_skew_interval'. Each new round will be backed off
   * until we hit 'mon_timecheck_interval' -- which is the typical
   * interval when not in the presence of a skew.
   *
   * This variable tracks the number of rounds with skews since last clean
   * so that we can report to the user and properly adjust the backoff.
   */
  uint64_t timecheck_rounds_since_clean;
  /**
   * Time Check event.
   */
  Context *timecheck_event;

  struct C_TimeCheck : public Context {
    Monitor *mon;
    explicit C_TimeCheck(Monitor *m) : mon(m) { }
    void finish(int r) {
      mon->timecheck_start_round();
    }
  };

  void timecheck_start();
  void timecheck_finish();
  void timecheck_start_round();
  void timecheck_finish_round(bool success = true);
  void timecheck_cancel_round();
  void timecheck_cleanup();
  void timecheck_reset_event();
  void timecheck_check_skews();
  void timecheck_report();
  void timecheck();
  health_status_t timecheck_status(ostringstream &ss,
                                   const double skew_bound,
                                   const double latency);
  void handle_timecheck_leader(MonOpRequestRef op);
  void handle_timecheck_peon(MonOpRequestRef op);
  void handle_timecheck(MonOpRequestRef op);

  /**
   * Returns 'true' if this is considered to be a skew; 'false' otherwise.
   */
  bool timecheck_has_skew(const double skew_bound, double *abs) const {
    double abs_skew = std::fabs(skew_bound);
    if (abs)
      *abs = abs_skew;
    return (abs_skew > g_conf->mon_clock_drift_allowed);
  }
  /**
   * @}
   */
  /**
   * @defgroup Monitor_h_stats Keep track of monitor statistics
   * @{
   */
  struct MonStatsEntry {
    // data dir
    uint64_t kb_total;
    uint64_t kb_used;
    uint64_t kb_avail;
    unsigned int latest_avail_ratio;
    utime_t last_update;
  };

  struct MonStats {
    MonStatsEntry ours;
    map<entity_inst_t,MonStatsEntry> others;
  };

  MonStats stats;

  void stats_update();
  /**
   * @}
   */
  /**
   * Handle ping messages from others.
   */
  void handle_ping(MonOpRequestRef op);

  Context *probe_timeout_event;  // for probing

  struct C_ProbeTimeout : public Context {
    Monitor *mon;
    explicit C_ProbeTimeout(Monitor *m) : mon(m) {}
    void finish(int r) {
      mon->probe_timeout(r);
    }
  };

  void reset_probe_timeout();
  void cancel_probe_timeout();
  void probe_timeout(int r);

public:
  epoch_t get_epoch();
  int get_leader() { return leader; }
  const set<int>& get_quorum() { return quorum; }
  list<string> get_quorum_names() {
    list<string> q;
    for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
      q.push_back(monmap->get_name(*p));
    return q;
  }
  uint64_t get_quorum_con_features() const {
    return quorum_con_features;
  }
  uint64_t get_required_features() const {
    return required_features;
  }
  mon_feature_t get_required_mon_features() const {
    return monmap->get_required_features();
  }
  void apply_quorum_to_compatset_features();
  void apply_compatset_features_to_quorum_requirements();

private:
  void _reset();   ///< called from bootstrap, start_, or join_election
  void wait_for_paxos_write();
public:
  void bootstrap();
  void join_election();
  void start_election();
  void win_standalone_election();
  // end election (called by Elector)
  void win_election(epoch_t epoch, set<int>& q,
		    uint64_t features,
                    const mon_feature_t& mon_features,
		    const MonCommand *cmdset, int cmdsize,
		    const set<int> *classic_monitors);
  void lose_election(epoch_t epoch, set<int>& q, int l,
		     uint64_t features,
                     const mon_feature_t& mon_features);
  // end election (called by Elector)
  void finish_election();

  const bufferlist& get_supported_commands_bl() {
    return supported_commands_bl;
  }
  const bufferlist& get_classic_commands_bl() {
    return classic_commands_bl;
  }
  const set<int>& get_classic_mons() {
    return classic_mons;
  }

  void update_logger();

  /**
   * Vector holding the Services serviced by this Monitor.
   */
  vector<PaxosService*> paxos_service;

  PaxosService *get_paxos_service_by_name(const string& name);

  class PGMonitor *pgmon() {
    return (class PGMonitor *)paxos_service[PAXOS_PGMAP];
  }

  class MDSMonitor *mdsmon() {
    return (class MDSMonitor *)paxos_service[PAXOS_MDSMAP];
  }

  class MonmapMonitor *monmon() {
    return (class MonmapMonitor *)paxos_service[PAXOS_MONMAP];
  }

  class OSDMonitor *osdmon() {
    return (class OSDMonitor *)paxos_service[PAXOS_OSDMAP];
  }

  class AuthMonitor *authmon() {
    return (class AuthMonitor *)paxos_service[PAXOS_AUTH];
  }

  class LogMonitor *logmon() {
    return (class LogMonitor*) paxos_service[PAXOS_LOG];
  }

  friend class Paxos;
  friend class OSDMonitor;
  friend class MDSMonitor;
  friend class MonmapMonitor;
  friend class PGMonitor;
  friend class LogMonitor;

  QuorumService *health_monitor;
  QuorumService *config_key_service;

  // -- sessions --
  MonSessionMap session_map;
  AdminSocketHook *admin_hook;

  void check_subs();
  void check_sub(Subscription *sub);

  void send_latest_monmap(Connection *con);

  // messages
  void handle_get_version(MonOpRequestRef op);
  void handle_subscribe(MonOpRequestRef op);
  void handle_mon_get_map(MonOpRequestRef op);

  static void _generate_command_map(map<string,cmd_vartype>& cmdmap,
                                    map<string,string> &param_str_map);
  static const MonCommand *_get_moncommand(const string &cmd_prefix,
                                           MonCommand *cmds, int cmds_size);
  bool _allowed_command(MonSession *s, string &module, string &prefix,
                        const map<string,cmd_vartype>& cmdmap,
                        const map<string,string>& param_str_map,
                        const MonCommand *this_cmd);
  void get_mon_status(Formatter *f, ostream& ss);
  void _quorum_status(Formatter *f, ostream& ss);
  bool _add_bootstrap_peer_hint(string cmd, cmdmap_t& cmdmap, ostream& ss);
  void handle_command(MonOpRequestRef op);
  void handle_route(MonOpRequestRef op);

  void handle_mon_metadata(MonOpRequestRef op);
  int get_mon_metadata(int mon, Formatter *f, ostream& err);
  int print_nodes(Formatter *f, ostream& err);
  map<int, Metadata> metadata;

  /**
   *
   */
  struct health_cache_t {
    health_status_t overall;
    string summary;

    void reset() {
      // health_status_t doesn't really have a NONE value and we're not
      // okay with setting something else (say, HEALTH_ERR).  so just
      // leave it be.
      summary.clear();
    }
  } health_status_cache;

  struct C_HealthToClogTick : public Context {
    Monitor *mon;
    explicit C_HealthToClogTick(Monitor *m) : mon(m) { }
    void finish(int r) {
      if (r < 0)
        return;
      mon->do_health_to_clog();
      mon->health_tick_start();
    }
  };

  struct C_HealthToClogInterval : public Context {
    Monitor *mon;
    explicit C_HealthToClogInterval(Monitor *m) : mon(m) { }
    void finish(int r) {
      if (r < 0)
        return;
      mon->do_health_to_clog_interval();
    }
  };

  Context *health_tick_event;
  Context *health_interval_event;

  void health_tick_start();
  void health_tick_stop();
  utime_t health_interval_calc_next_update();
  void health_interval_start();
  void health_interval_stop();
  void health_events_cleanup();

  void health_to_clog_update_conf(const std::set<std::string> &changed);

  void do_health_to_clog_interval();
  void do_health_to_clog(bool force = false);

  /**
   * Generate health report
   *
   * @param status one-line status summary
   * @param detailbl optional bufferlist* to fill with a detailed report
   * @returns health status
   */
  health_status_t get_health(list<string>& status, bufferlist *detailbl,
                             Formatter *f);
  void get_cluster_status(stringstream &ss, Formatter *f);

  void reply_command(MonOpRequestRef op, int rc, const string &rs, version_t version);
  void reply_command(MonOpRequestRef op, int rc, const string &rs, bufferlist& rdata, version_t version);


  void handle_probe(MonOpRequestRef op);
  /**
   * Handle a Probe Operation, replying with our name, quorum and known versions.
   *
   * We use the MMonProbe message class for anything and everything related with
   * Monitor probing. One of the operations relates directly with the probing
   * itself, in which we receive a probe request and to which we reply with
   * our name, our quorum and the known versions for each Paxos service. Thus the
   * redundant function name. This reply will obviously be sent to the one
   * probing/requesting these infos.
   *
   * @todo Add @pre and @post
   *
   * @param m A Probe message, with an operation of type Probe.
   */
  void handle_probe_probe(MonOpRequestRef op);
  void handle_probe_reply(MonOpRequestRef op);

  // request routing
  struct RoutedRequest {
    uint64_t tid;
    bufferlist request_bl;
    MonSession *session;
    ConnectionRef con;
    uint64_t con_features;
    entity_inst_t client_inst;
    MonOpRequestRef op;

    RoutedRequest() : tid(0), session(NULL), con_features(0) {}
    ~RoutedRequest() {
      if (session)
	session->put();
    }
  };
  uint64_t routed_request_tid;
  map<uint64_t, RoutedRequest*> routed_requests;
  
  void forward_request_leader(MonOpRequestRef op);
  void handle_forward(MonOpRequestRef op);
  void try_send_message(Message *m, const entity_inst_t& to);
  void send_reply(MonOpRequestRef op, Message *reply);
  void no_reply(MonOpRequestRef op);
  void resend_routed_requests();
  void remove_session(MonSession *s);
  void remove_all_sessions();
  void waitlist_or_zap_client(MonOpRequestRef op);

  void send_command(const entity_inst_t& inst,
		    const vector<string>& com);

public:
  struct C_Command : public C_MonOp {
    Monitor *mon;
    int rc;
    string rs;
    bufferlist rdata;
    version_t version;
    C_Command(Monitor *_mm, MonOpRequestRef _op, int r, string s, version_t v) :
      C_MonOp(_op), mon(_mm), rc(r), rs(s), version(v){}
    C_Command(Monitor *_mm, MonOpRequestRef _op, int r, string s, bufferlist rd, version_t v) :
      C_MonOp(_op), mon(_mm), rc(r), rs(s), rdata(rd), version(v){}

    virtual void _finish(int r) {
      MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
      if (r >= 0) {
        ostringstream ss;
        if (!op->get_req()->get_connection()) {
          ss << "connection dropped for command ";
        } else {
          MonSession *s = op->get_session();

          // if client drops we may not have a session to draw information from.
          if (s) {
            ss << "from='" << s->inst << "' "
              << "entity='" << s->entity_name << "' ";
          } else {
            ss << "session dropped for command ";
          }
        }
        ss << "cmd='" << m->cmd << "': finished";

        mon->audit_clog->info() << ss.str();
	mon->reply_command(op, rc, rs, rdata, version);
      }
      else if (r == -ECANCELED)
        return;
      else if (r == -EAGAIN)
	mon->dispatch_op(op);
      else
	assert(0 == "bad C_Command return value");
    }
  };

 private:
  class C_RetryMessage : public C_MonOp {
    Monitor *mon;
  public:
    C_RetryMessage(Monitor *m, MonOpRequestRef op) :
      C_MonOp(op), mon(m) { }

    virtual void _finish(int r) {
      if (r == -EAGAIN || r >= 0)
        mon->dispatch_op(op);
      else if (r == -ECANCELED)
        return;
      else
	assert(0 == "bad C_RetryMessage return value");
    }
  };

  //ms_dispatch handles a lot of logic and we want to reuse it
  //on forwarded messages, so we create a non-locking version for this class
  void _ms_dispatch(Message *m);
  bool ms_dispatch(Message *m) {
    lock.Lock();
    _ms_dispatch(m);
    lock.Unlock();
    return true;
  }
  void dispatch_op(MonOpRequestRef op);
  //mon_caps is used for un-connected messages from monitors
  MonCap * mon_caps;
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  bool ms_verify_authorizer(Connection *con, int peer_type,
			    int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			    bool& isvalid, CryptoKey& session_key);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

  int write_default_keyring(bufferlist& bl);
  void extract_save_mon_key(KeyRing& keyring);

  void update_mon_metadata(int from, const Metadata& m);
  int load_metadata(map<int, Metadata>& m);

  // features
  static CompatSet get_initial_supported_features();
  static CompatSet get_supported_features();
  static CompatSet get_legacy_features();
  /// read the ondisk features into the CompatSet pointed to by read_features
  static void read_features_off_disk(MonitorDBStore *store, CompatSet *read_features);
  void read_features();
  void write_features(MonitorDBStore::TransactionRef t);

  OpTracker op_tracker;

 public:
  Monitor(CephContext *cct_, string nm, MonitorDBStore *s,
	  Messenger *m, MonMap *map);
  ~Monitor();

  static int check_features(MonitorDBStore *store);

  // config observer
  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
                                  const std::set<std::string> &changed);

  void update_log_clients();
  int sanitize_options();
  int preinit();
  int init();
  void init_paxos();
  void refresh_from_paxos(bool *need_bootstrap);
  void shutdown();
  void tick();

  void handle_signal(int sig);

  int mkfs(bufferlist& osdmapbl);

  /**
   * check cluster_fsid file
   *
   * @return EEXIST if file exists and doesn't match, 0 on match, or negative error code
   */
  int check_fsid();

  /**
   * write cluster_fsid file
   *
   * @return 0 on success, or negative error code
   */
  int write_fsid();
  int write_fsid(MonitorDBStore::TransactionRef t);

  void do_admin_command(std::string command, cmdmap_t& cmdmap,
			std::string format, ostream& ss);

private:
  // don't allow copying
  Monitor(const Monitor& rhs);
  Monitor& operator=(const Monitor &rhs);

public:
  static void format_command_descriptions(const MonCommand *commands,
					  unsigned commands_size,
					  Formatter *f,
					  bufferlist *rdata);
  void get_locally_supported_monitor_commands(const MonCommand **cmds, int *count);
  void get_classic_monitor_commands(const MonCommand **cmds, int *count);
  /// the Monitor owns this pointer once you pass it in
  void set_leader_supported_commands(const MonCommand *cmds, int size);
  static bool is_keyring_required();
};

#define CEPH_MON_FEATURE_INCOMPAT_BASE CompatSet::Feature (1, "initial feature set (~v.18)")
#define CEPH_MON_FEATURE_INCOMPAT_GV CompatSet::Feature (2, "global version sequencing (v0.52)")
#define CEPH_MON_FEATURE_INCOMPAT_SINGLE_PAXOS CompatSet::Feature (3, "single paxos with k/v store (v0.\?)")
#define CEPH_MON_FEATURE_INCOMPAT_OSD_ERASURE_CODES CompatSet::Feature(4, "support erasure code pools")
#define CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC CompatSet::Feature(5, "new-style osdmap encoding")
#define CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V2 CompatSet::Feature(6, "support isa/lrc erasure code")
#define CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V3 CompatSet::Feature(7, "support shec erasure code")
// make sure you add your feature to Monitor::get_supported_features

long parse_pos_long(const char *s, ostream *pss = NULL);

struct MonCommand {
  string cmdstring;
  string helpstring;
  string module;
  string req_perms;
  string availability;
  uint64_t flags;

  // MonCommand flags
  static const uint64_t FLAG_NONE       = 0;
  static const uint64_t FLAG_NOFORWARD  = 1 << 0;
  static const uint64_t FLAG_OBSOLETE   = 1 << 1;
  static const uint64_t FLAG_DEPRECATED = 1 << 2;
  
  bool has_flag(uint64_t flag) const { return (flags & flag) != 0; }
  void set_flag(uint64_t flag) { flags |= flag; }
  void unset_flag(uint64_t flag) { flags &= ~flag; }

  void encode(bufferlist &bl) const {
    /*
     * very naughty: deliberately unversioned because individual commands
     * shouldn't be encoded standalone, only as a full set (which we do
     * version, see encode_array() below).
     */
    ::encode(cmdstring, bl);
    ::encode(helpstring, bl);
    ::encode(module, bl);
    ::encode(req_perms, bl);
    ::encode(availability, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(cmdstring, bl);
    ::decode(helpstring, bl);
    ::decode(module, bl);
    ::decode(req_perms, bl);
    ::decode(availability, bl);
  }
  bool is_compat(const MonCommand* o) const {
    return cmdstring == o->cmdstring &&
	module == o->module && req_perms == o->req_perms &&
	availability == o->availability;
  }

  bool is_noforward() const {
    return has_flag(MonCommand::FLAG_NOFORWARD);
  }

  bool is_obsolete() const {
    return has_flag(MonCommand::FLAG_OBSOLETE);
  }

  bool is_deprecated() const {
    return has_flag(MonCommand::FLAG_DEPRECATED);
  }

  static void encode_array(const MonCommand *cmds, int size, bufferlist &bl) {
    ENCODE_START(2, 1, bl);
    uint16_t s = size;
    ::encode(s, bl);
    ::encode_array_nohead(cmds, size, bl);
    for (int i = 0; i < size; i++)
      ::encode(cmds[i].flags, bl);
    ENCODE_FINISH(bl);
  }
  static void decode_array(MonCommand **cmds, int *size,
                           bufferlist::iterator &bl) {
    DECODE_START(2, bl);
    uint16_t s = 0;
    ::decode(s, bl);
    *size = s;
    *cmds = new MonCommand[*size];
    ::decode_array_nohead(*cmds, *size, bl);
    if (struct_v >= 2) {
      for (int i = 0; i < *size; i++)
	::decode((*cmds)[i].flags, bl);
    } else {
      for (int i = 0; i < *size; i++)
	(*cmds)[i].flags = 0;
    }
    DECODE_FINISH(bl);
  }

  bool requires_perm(char p) const {
    return (req_perms.find(p) != string::npos); 
  }
};
WRITE_CLASS_ENCODER(MonCommand)

#endif
