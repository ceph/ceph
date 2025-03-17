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

#include <errno.h>
#include <cmath>
#include <string>
#include <array>

#include "include/types.h"
#include "include/health.h"
#include "msg/Messenger.h"

#include "common/Timer.h"

#include "health_check.h"
#include "MonMap.h"
#include "Elector.h"
#include "Paxos.h"
#include "Session.h"
#include "MonCommand.h"


#include "common/config_obs.h"
#include "common/LogClient.h"
#include "auth/AuthClient.h"
#include "auth/AuthServer.h"
#include "auth/cephx/CephxKeyServer.h"
#include "auth/AuthMethodList.h"
#include "auth/KeyRing.h"
#include "include/common_fwd.h"
#include "messages/MMonCommand.h"
#include "mon/MonitorDBStore.h"
#include "mgr/MgrClient.h"

#include "mon/MonOpRequest.h"
#include "common/WorkQueue.h"

using namespace TOPNSPC::common;

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

class PaxosService;

class AdminSocketHook;

#define COMPAT_SET_LOC "feature_set"

class Monitor : public Dispatcher,
		public AuthClient,
		public AuthServer,
                public md_config_obs_t {
public:
  int orig_argc = 0;
  const char **orig_argv = nullptr;

  // me
  std::string name;
  int rank;
  Messenger *messenger;
  ConnectionRef con_self;
  ceph::mutex lock = ceph::make_mutex("Monitor::lock");
  SafeTimer timer;
  Finisher finisher;
  ThreadPool cpu_tp;  ///< threadpool for CPU intensive work

  ceph::mutex auth_lock = ceph::make_mutex("Monitor::auth_lock");

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

  std::set<entity_addrvec_t> extra_probe_peers;

  LogClient log_client;
  LogChannelRef clog;
  LogChannelRef audit_clog;
  KeyRing keyring;
  KeyServer key_server;

  AuthMethodList auth_cluster_required;
  AuthMethodList auth_service_required;

  CompatSet features;

  std::vector<MonCommand> leader_mon_commands; // quorum leader's commands
  std::vector<MonCommand> local_mon_commands;  // commands i support
  ceph::buffer::list local_mon_commands_bl;       // encoded version of above

  std::vector<MonCommand> prenautilus_local_mon_commands;
  ceph::buffer::list prenautilus_local_mon_commands_bl;

  Messenger *mgr_messenger;
  MgrClient mgr_client;
  uint64_t mgr_proxy_bytes = 0;  // in-flight proxied mgr command message bytes
  std::string gss_ktfile_client{};

private:
  void new_tick();

  // -- local storage --
public:
  MonitorDBStore *store;
  static const std::string MONITOR_NAME;
  static const std::string MONITOR_STORE_PREFIX;

  // -- monitor state --
private:
  enum {
    STATE_INIT = 1,
    STATE_PROBING,
    STATE_SYNCHRONIZING,
    STATE_ELECTING,
    STATE_LEADER,
    STATE_PEON,
    STATE_SHUTDOWN
  };
  int state = STATE_INIT;

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

  bool is_init() const { return state == STATE_INIT; }
  bool is_shutdown() const { return state == STATE_SHUTDOWN; }
  bool is_probing() const { return state == STATE_PROBING; }
  bool is_synchronizing() const { return state == STATE_SYNCHRONIZING; }
  bool is_electing() const { return state == STATE_ELECTING; }
  bool is_leader() const { return state == STATE_LEADER; }
  bool is_peon() const { return state == STATE_PEON; }

  const utime_t &get_leader_since() const;

  void prepare_new_fingerprint(MonitorDBStore::TransactionRef t);

  std::vector<DaemonHealthMetric> get_health_metrics();

  int quorum_age() const {
    auto age = std::chrono::duration_cast<std::chrono::seconds>(
      ceph::mono_clock::now() - quorum_since);
    return age.count();
  }

  bool is_mon_down() const {
    int max = monmap->size();
    int actual = get_quorum().size();
    auto now = ceph::real_clock::now();
    return actual < max && now > monmap->created.to_real_time();
  }

  // -- elector --
private:
  std::unique_ptr<Paxos> paxos;
  Elector elector;
  friend class Elector;

  /// features we require of peers (based on on-disk compatset)
  uint64_t required_features;
  
  int leader;            // current leader (to best of knowledge)
  std::set<int> quorum;       // current active set of monitors (if !starting)
  ceph::mono_clock::time_point quorum_since;  // when quorum formed
  utime_t leader_since;  // when this monitor became the leader, if it is the leader
  utime_t exited_quorum; // time detected as not in quorum; 0 if in

  // map of counts of connected clients, by type and features, for
  // each quorum mon
  std::map<int,FeatureMap> quorum_feature_map;

  /**
   * Intersection of quorum member's connection feature bits.
   */
  uint64_t quorum_con_features;
  /**
   * Intersection of quorum members mon-specific feature bits
   */
  mon_feature_t quorum_mon_features;

  ceph_release_t quorum_min_mon_release{ceph_release_t::unknown};

  std::set<std::string> outside_quorum;

  bool stretch_mode_engaged{false};
  bool degraded_stretch_mode{false};
  bool recovering_stretch_mode{false};
  std::string stretch_bucket_divider;
  std::map<std::string, std::set<std::string>> dead_mon_buckets; // bucket->mon ranks, locations with no live mons
  std::set<std::string> up_mon_buckets; // locations with a live mon
  void do_stretch_mode_election_work();

  bool session_stretch_allowed(MonSession *s, MonOpRequestRef& op);
  void disconnect_disallowed_stretch_sessions();
  void set_elector_disallowed_leaders(bool allow_election);

  std::map<std::string,std::string> crush_loc;
  bool need_set_crush_loc{false};
public:
  bool is_stretch_mode() { return stretch_mode_engaged; }
  bool is_degraded_stretch_mode() { return degraded_stretch_mode; }
  bool is_recovering_stretch_mode() { return recovering_stretch_mode; }

  /**
   * This set of functions maintains the in-memory stretch state
   * and sets up transitions of the map states by calling in to
   * MonmapMonitor and OSDMonitor.
   *
   * The [maybe_]go_* functions are called on the leader to
   * decide if transitions should happen; the trigger_* functions
   * set up the map transitions; and the set_* functions actually
   * change the memory state -- but these are only called
   * via OSDMonitor::update_from_paxos, to guarantee consistent
   * updates across the entire cluster.
   */
  void try_engage_stretch_mode();
  void try_disable_stretch_mode();
  void maybe_go_degraded_stretch_mode();
  void trigger_degraded_stretch_mode(const std::set<std::string>& dead_mons,
				     const std::set<int>& dead_buckets);
  void set_degraded_stretch_mode();
  void go_recovery_stretch_mode();
  void set_recovery_stretch_mode();
  void trigger_healthy_stretch_mode();
  void set_healthy_stretch_mode();
  void enable_stretch_mode();
  void set_mon_crush_location(const std::string& loc);

  
private:

  /**
   * @defgroup Monitor_h_scrub
   * @{
   */
  version_t scrub_version;            ///< paxos version we are scrubbing
  std::map<int,ScrubResult> scrub_result;  ///< results so far

  /**
   * trigger a cross-mon scrub
   *
   * Verify all mons are storing identical content
   */
  int scrub_start();
  int scrub();
  void handle_scrub(MonOpRequestRef op);
  bool _scrub(ScrubResult *r,
              std::pair<std::string,std::string> *start,
              int *num_keys);
  void scrub_check_results();
  void scrub_timeout();
  void scrub_finish();
  void scrub_reset();
  void scrub_update_interval(ceph::timespan interval);

  Context *scrub_event;       ///< periodic event to trigger scrub (leader)
  Context *scrub_timeout_event;  ///< scrub round timeout (leader)
  void scrub_event_start();
  void scrub_event_cancel();
  void scrub_reset_timeout();
  void scrub_cancel_timeout();

  struct ScrubState {
    std::pair<std::string,std::string> last_key; ///< last scrubbed key
    bool finished;

    ScrubState() : finished(false) { }
    virtual ~ScrubState() { }
  };
  std::shared_ptr<ScrubState> scrub_state; ///< keeps track of current scrub

  /**
   * @defgroup Monitor_h_sync Synchronization
   * @{
   */
  /**
   * @} // provider state
   */
  struct SyncProvider {
    entity_addrvec_t addrs;
    uint64_t cookie;       ///< unique cookie for this sync attempt
    utime_t timeout;       ///< when we give up and expire this attempt
    version_t last_committed; ///< last paxos version on peer
    std::pair<std::string,std::string> last_key; ///< last key sent to (or on) peer
    bool full;             ///< full scan?
    MonitorDBStore::Synchronizer synchronizer;   ///< iterator

    SyncProvider() : cookie(0), last_committed(0), full(false) {}

    void reset_timeout(CephContext *cct, int grace) {
      timeout = ceph_clock_now();
      timeout += grace;
    }
  };

  std::map<std::uint64_t, SyncProvider> sync_providers;  ///< cookie -> SyncProvider for those syncing from us
  uint64_t sync_provider_count;   ///< counter for issued cookies to keep them unique

  /**
   * @} // requester state
   */
  entity_addrvec_t sync_provider;  ///< who we are syncing from
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
  std::set<std::string> get_sync_targets_names();

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
  void sync_obtain_latest_monmap(ceph::buffer::list &bl);

  /**
   * Start sync process
   *
   * Start pulling committed state from another monitor.
   *
   * @param entity where to pull committed state from
   * @param full whether to do a full sync or just catch up on recent paxos
   */
  void sync_start(entity_addrvec_t &addrs, bool full);

public:
  /**
   * force a sync on next mon restart
   */
  void sync_force(ceph::Formatter *f);

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

  std::list<Context*> waitfor_quorum;
  std::list<Context*> maybe_wait_for_quorum;

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
  std::map<int, utime_t> timecheck_waiting;
  std::map<int, double> timecheck_skews;
  std::map<int, double> timecheck_latencies;
  // odd value means we are mid-round; even value means the round has
  // finished.
  version_t timecheck_round;
  unsigned int timecheck_acks;
  utime_t timecheck_round_start;
  friend class HealthMonitor;
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
  health_status_t timecheck_status(std::ostringstream &ss,
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
    return (abs_skew > g_conf()->mon_clock_drift_allowed);
  }

  /**
   * @}
   */
  /**
   * Handle ping messages from others.
   */
  void handle_ping(MonOpRequestRef op);

  Context *probe_timeout_event = nullptr;  // for probing

  void reset_probe_timeout();
  void cancel_probe_timeout();
  void probe_timeout(int r);

  void _apply_compatset_features(CompatSet &new_features);

public:
  epoch_t get_epoch();
  int get_leader() const { return leader; }
  std::string get_leader_name() {
    return quorum.empty() ? std::string() : monmap->get_name(leader);
  }
  const std::set<int>& get_quorum() const { return quorum; }
  std::list<std::string> get_quorum_names() {
    std::list<std::string> q;
    for (auto p = quorum.begin(); p != quorum.end(); ++p)
      q.push_back(monmap->get_name(*p));
    return q;
  }
  uint64_t get_quorum_con_features() const {
    return quorum_con_features;
  }
  mon_feature_t get_quorum_mon_features() const {
    return quorum_mon_features;
  }
  uint64_t get_required_features() const {
    return required_features;
  }
  mon_feature_t get_required_mon_features() const {
    return monmap->get_required_features();
  }
  void apply_quorum_to_compatset_features();
  void apply_monmap_to_compatset_features();
  void calc_quorum_requirements();

  void get_combined_feature_map(FeatureMap *fm);

private:
  void _reset();   ///< called from bootstrap, start_, or join_election
  void wait_for_paxos_write();
  void _finish_svc_election(); ///< called by {win,lose}_election
  void respawn();
public:
  void bootstrap();
  void join_election();
  void start_election();
  void win_standalone_election();
  // end election (called by Elector)
  void win_election(epoch_t epoch, const std::set<int>& q,
		    uint64_t features,
                    const mon_feature_t& mon_features,
		    ceph_release_t min_mon_release,
		    const std::map<int,Metadata>& metadata);
  void lose_election(epoch_t epoch, std::set<int>& q, int l,
		     uint64_t features,
                     const mon_feature_t& mon_features,
		     ceph_release_t min_mon_release);
  // end election (called by Elector)
  void finish_election();

  void update_logger();

  /**
   * Vector holding the Services serviced by this Monitor.
   */
  std::array<std::unique_ptr<PaxosService>, PAXOS_NUM> paxos_service;

  class MDSMonitor *mdsmon() {
    return (class MDSMonitor *)paxos_service[PAXOS_MDSMAP].get();
  }

  class MonmapMonitor *monmon() {
    return (class MonmapMonitor *)paxos_service[PAXOS_MONMAP].get();
  }

  class OSDMonitor *osdmon() {
    return (class OSDMonitor *)paxos_service[PAXOS_OSDMAP].get();
  }

  class AuthMonitor *authmon() {
    return (class AuthMonitor *)paxos_service[PAXOS_AUTH].get();
  }

  class LogMonitor *logmon() {
    return (class LogMonitor*) paxos_service[PAXOS_LOG].get();
  }

  class MgrMonitor *mgrmon() {
    return (class MgrMonitor*) paxos_service[PAXOS_MGR].get();
  }

  class MgrStatMonitor *mgrstatmon() {
    return (class MgrStatMonitor*) paxos_service[PAXOS_MGRSTAT].get();
  }

  class HealthMonitor *healthmon() {
    return (class HealthMonitor*) paxos_service[PAXOS_HEALTH].get();
  }

  class ConfigMonitor *configmon() {
    return (class ConfigMonitor*) paxos_service[PAXOS_CONFIG].get();
  }

  class KVMonitor *kvmon() {
    return (class KVMonitor*) paxos_service[PAXOS_KV].get();
  }

  class NVMeofGwMon *nvmegwmon() {
      return (class NVMeofGwMon*) paxos_service[PAXOS_NVMEGW].get();
  }


  friend class Paxos;
  friend class OSDMonitor;
  friend class MDSMonitor;
  friend class MonmapMonitor;
  friend class LogMonitor;
  friend class KVMonitor;

  // -- sessions --
  MonSessionMap session_map;
  ceph::mutex session_map_lock = ceph::make_mutex("Monitor::session_map_lock");
  AdminSocketHook *admin_hook;

  template<typename Func, typename...Args>
  void with_session_map(Func&& func) {
    std::lock_guard l(session_map_lock);
    std::forward<Func>(func)(session_map);
  }
  void send_latest_monmap(Connection *con);

  // messages
  void handle_get_version(MonOpRequestRef op);
  void handle_subscribe(MonOpRequestRef op);
  void handle_mon_get_map(MonOpRequestRef op);

  static void _generate_command_map(cmdmap_t& cmdmap,
                                    std::map<std::string,std::string> &param_str_map);
  static const MonCommand *_get_moncommand(
    const std::string &cmd_prefix,
    const std::vector<MonCommand>& cmds);
  bool _allowed_command(MonSession *s, const std::string& module,
			const std::string& prefix,
                        const cmdmap_t& cmdmap,
                        const std::map<std::string,std::string>& param_str_map,
                        const MonCommand *this_cmd);
  void get_mon_status(ceph::Formatter *f);
  void _quorum_status(ceph::Formatter *f, std::ostream& ss);
  bool _add_bootstrap_peer_hint(std::string_view cmd, const cmdmap_t& cmdmap,
				std::ostream& ss);
  void handle_tell_command(MonOpRequestRef op);
  void handle_command(MonOpRequestRef op);
  void handle_route(MonOpRequestRef op);

  int get_mon_metadata(int mon, ceph::Formatter *f, std::ostream& err);
  int print_nodes(ceph::Formatter *f, std::ostream& err);

  // track metadata reported by win_election()
  std::map<int, Metadata> mon_metadata;
  std::map<int, Metadata> pending_metadata;

  /**
   *
   */
  struct health_cache_t {
    health_status_t overall;
    std::string summary;

    void reset() {
      // health_status_t doesn't really have a NONE value and we're not
      // okay with setting something else (say, HEALTH_ERR).  so just
      // leave it be.
      summary.clear();
    }
  } health_status_cache;

  Context *health_tick_event = nullptr;
  Context *health_interval_event = nullptr;

  void health_tick_start();
  void health_tick_stop();
  ceph::real_clock::time_point health_interval_calc_next_update();
  void health_interval_start();
  void health_interval_stop();
  void health_events_cleanup();

  void health_to_clog_update_conf(const std::set<std::string> &changed);

  void do_health_to_clog_interval();
  void do_health_to_clog(bool force = false);

  void log_health(
    const health_check_map_t& updated,
    const health_check_map_t& previous,
    MonitorDBStore::TransactionRef t);

  void update_pending_metadata();

protected:

  class HealthCheckLogStatus {
    public:
    health_status_t severity;
    std::string last_message;
    utime_t updated_at = 0;
    HealthCheckLogStatus(health_status_t severity_,
                         const std::string &last_message_,
                         utime_t updated_at_)
      : severity(severity_),
        last_message(last_message_),
        updated_at(updated_at_)
    {}
  };
  std::map<std::string, HealthCheckLogStatus> health_check_log_times;

public:

  void get_cluster_status(std::stringstream &ss, ceph::Formatter *f,
			  MonSession *session);

  void reply_command(MonOpRequestRef op, int rc, const std::string &rs, version_t version);
  void reply_command(MonOpRequestRef op, int rc, const std::string &rs, ceph::buffer::list& rdata, version_t version);

  void reply_tell_command(MonOpRequestRef op, int rc, const std::string &rs);



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
    ceph::buffer::list request_bl;
    MonSession *session;
    ConnectionRef con;
    uint64_t con_features;
    MonOpRequestRef op;

    RoutedRequest() : tid(0), session(NULL), con_features(0) {}
    ~RoutedRequest() {
      if (session)
	session->put();
    }
  };
  uint64_t routed_request_tid;
  std::map<uint64_t, RoutedRequest*> routed_requests;

  void forward_request_leader(MonOpRequestRef op);
  void handle_forward(MonOpRequestRef op);
  void send_reply(MonOpRequestRef op, Message *reply);
  void no_reply(MonOpRequestRef op);
  void resend_routed_requests();
  void remove_session(MonSession *s);
  void remove_all_sessions();
  void waitlist_or_zap_client(MonOpRequestRef op);

  void send_mon_message(Message *m, int rank);
  /** can_change_external_state if we can do things like
   *  call elections as a result of the new map.
   */
  void notify_new_monmap(bool can_change_external_state=false, bool remove_rank_elector=true);

public:
  struct C_Command : public C_MonOp {
    Monitor &mon;
    int rc;
    std::string rs;
    ceph::buffer::list rdata;
    version_t version;
    C_Command(Monitor &_mm, MonOpRequestRef _op, int r, std::string s, version_t v) :
      C_MonOp(_op), mon(_mm), rc(r), rs(s), version(v){}
    C_Command(Monitor &_mm, MonOpRequestRef _op, int r, std::string s, ceph::buffer::list rd, version_t v) :
      C_MonOp(_op), mon(_mm), rc(r), rs(s), rdata(rd), version(v){}

    void _finish(int r) override {
      auto m = op->get_req<MMonCommand>();
      if (r >= 0) {
	std::ostringstream ss;
        if (!op->get_req()->get_connection()) {
          ss << "connection dropped for command ";
        } else {
          MonSession *s = op->get_session();

          // if client drops we may not have a session to draw information from.
          if (s) {
            ss << "from='" << s->name << " " << s->addrs << "' "
              << "entity='" << s->entity_name << "' ";
          } else {
            ss << "session dropped for command ";
          }
        }
        cmdmap_t cmdmap;
        std::ostringstream ds;
        std::string prefix;
        cmdmap_from_json(m->cmd, &cmdmap, ds);
        cmd_getval(cmdmap, "prefix", prefix);
        if (prefix != "config set" && prefix != "config-key set")
          ss << "cmd='" << m->cmd << "': finished";

        mon.audit_clog->info() << ss.str();
        mon.reply_command(op, rc, rs, rdata, version);
      }
      else if (r == -ECANCELED)
        return;
      else if (r == -EAGAIN)
        mon.dispatch_op(op);
      else
	ceph_abort_msg("bad C_Command return value");
    }
  };

 private:
  class C_RetryMessage : public C_MonOp {
    Monitor *mon;
  public:
    C_RetryMessage(Monitor *m, MonOpRequestRef op) :
      C_MonOp(op), mon(m) { }

    void _finish(int r) override {
      if (r == -EAGAIN || r >= 0)
        mon->dispatch_op(op);
      else if (r == -ECANCELED)
        return;
      else
	ceph_abort_msg("bad C_RetryMessage return value");
    }
  };

  //ms_dispatch handles a lot of logic and we want to reuse it
  //on forwarded messages, so we create a non-locking version for this class
  void _ms_dispatch(Message *m);
  bool ms_dispatch(Message *m) override {
    std::lock_guard l{lock};
    _ms_dispatch(m);
    return true;
  }
  void dispatch_op(MonOpRequestRef op);
  //mon_caps is used for un-connected messages from monitors
  MonCap mon_caps;
  bool get_authorizer(int dest_type, AuthAuthorizer **authorizer);
public: // for AuthMonitor msgr1:
  bool ms_handle_fast_authentication(Connection *con) override;
private:
  void ms_handle_accept(Connection *con) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;

  // AuthClient
  int get_auth_request(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint32_t *method,
    std::vector<uint32_t> *preferred_modes,
    ceph::buffer::list *out) override;
  int handle_auth_reply_more(
    Connection *con,
    AuthConnectionMeta *auth_meta,
   const ceph::buffer::list& bl,
    ceph::buffer::list *reply) override;
  int handle_auth_done(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint64_t global_id,
    uint32_t con_mode,
    const ceph::buffer::list& bl,
    CryptoKey *session_key,
    std::string *connection_secret) override;
  int handle_auth_bad_method(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint32_t old_auth_method,
    int result,
    const std::vector<uint32_t>& allowed_methods,
    const std::vector<uint32_t>& allowed_modes) override;
  // /AuthClient
  // AuthServer
  int handle_auth_request(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    bool more,
    uint32_t auth_method,
    const ceph::buffer::list& bl,
    ceph::buffer::list *reply) override;
  // /AuthServer

  int write_default_keyring(ceph::buffer::list& bl);
  void extract_save_mon_key(KeyRing& keyring);

  void collect_metadata(Metadata *m);
  int load_metadata();
  void count_metadata(const std::string& field, ceph::Formatter *f);
  void count_metadata(const std::string& field, std::map<std::string,int> *out);
  // get_all_versions() gathers version information from daemons for health check
  void get_all_versions(std::map<std::string, std::list<std::string>> &versions);
  void get_versions(std::map<std::string, std::list<std::string>> &versions);

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
  Monitor(CephContext *cct_, std::string nm, MonitorDBStore *s,
	  Messenger *m, Messenger *mgr_m, MonMap *map);
  ~Monitor() override;

  static int check_features(MonitorDBStore *store);

  // config observer
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string> &changed) override;

  void update_log_clients();
  int sanitize_options();
  int preinit();
  int init();
  void init_paxos();
  void refresh_from_paxos(bool *need_bootstrap);
  void shutdown();
  void tick();

  void handle_signal(int sig);

  int mkfs(ceph::buffer::list& osdmapbl);

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

  int do_admin_command(std::string_view command, const cmdmap_t& cmdmap,
		       ceph::Formatter *f,
		       std::ostream& err,
		       std::ostream& out);

private:
  // don't allow copying
  Monitor(const Monitor& rhs);
  Monitor& operator=(const Monitor &rhs);

public:
  static void format_command_descriptions(const std::vector<MonCommand> &commands,
					  ceph::Formatter *f,
					  uint64_t features,
					  ceph::buffer::list *rdata);

  const std::vector<MonCommand> &get_local_commands(mon_feature_t f) {
    if (f.contains_all(ceph::features::mon::FEATURE_NAUTILUS)) {
      return local_mon_commands;
    } else {
      return prenautilus_local_mon_commands;
    }
  }
  const ceph::buffer::list& get_local_commands_bl(mon_feature_t f) {
    if (f.contains_all(ceph::features::mon::FEATURE_NAUTILUS)) {
      return local_mon_commands_bl;
    } else {
      return prenautilus_local_mon_commands_bl;
    }
  }
  void set_leader_commands(const std::vector<MonCommand>& cmds) {
    leader_mon_commands = cmds;
  }

  bool is_keyring_required();

public:
  ceph::coarse_mono_time get_starttime() const {
    return starttime;
  }
  std::chrono::milliseconds get_uptime() const {
    auto now = ceph::coarse_mono_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now-starttime);
  }

private:
  ceph::coarse_mono_time const starttime = coarse_mono_clock::now();
};

#define CEPH_MON_FEATURE_INCOMPAT_BASE CompatSet::Feature (1, "initial feature set (~v.18)")
#define CEPH_MON_FEATURE_INCOMPAT_GV CompatSet::Feature (2, "global version sequencing (v0.52)")
#define CEPH_MON_FEATURE_INCOMPAT_SINGLE_PAXOS CompatSet::Feature (3, "single paxos with k/v store (v0.\?)")
#define CEPH_MON_FEATURE_INCOMPAT_OSD_ERASURE_CODES CompatSet::Feature(4, "support erasure code pools")
#define CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC CompatSet::Feature(5, "new-style osdmap encoding")
#define CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V2 CompatSet::Feature(6, "support isa/lrc erasure code")
#define CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V3 CompatSet::Feature(7, "support shec erasure code")
#define CEPH_MON_FEATURE_INCOMPAT_KRAKEN CompatSet::Feature(8, "support monmap features")
#define CEPH_MON_FEATURE_INCOMPAT_LUMINOUS CompatSet::Feature(9, "luminous ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_MIMIC CompatSet::Feature(10, "mimic ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_NAUTILUS CompatSet::Feature(11, "nautilus ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_OCTOPUS CompatSet::Feature(12, "octopus ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_PACIFIC CompatSet::Feature(13, "pacific ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_QUINCY CompatSet::Feature(14, "quincy ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_REEF CompatSet::Feature(15, "reef ondisk layout")
#define CEPH_MON_FEATURE_INCOMPAT_SQUID CompatSet::Feature(16, "squid ondisk layout")
// make sure you add your feature to Monitor::get_supported_features


/* Callers use:
 *
 *      new C_MonContext{...}
 *
 * instead of
 *
 *      new C_MonContext(...)
 *
 * because of gcc bug [1].
 *
 * [1] https://gcc.gnu.org/bugzilla/show_bug.cgi?id=85883
 */
template<typename T>
class C_MonContext : public LambdaContext<T> {
public:
  C_MonContext(const Monitor* m, T&& f) :
      LambdaContext<T>(std::forward<T>(f)),
      mon(m)
  {}
  void finish(int r) override {
    if (mon->is_shutdown())
      return;
    LambdaContext<T>::finish(r);
  }
private:
  const Monitor* mon;
};

#endif
