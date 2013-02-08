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

#include "auth/cephx/CephxKeyServer.h"
#include "auth/AuthMethodList.h"
#include "auth/KeyRing.h"

#include "perfglue/heap_profiler.h"

#include "messages/MMonCommand.h"

#include <memory>
#include <errno.h>


#define CEPH_MON_PROTOCOL     9 /* cluster internal */


enum {
  l_cluster_first = 555000,
  l_cluster_num_mon,
  l_cluster_num_mon_quorum,
  l_cluster_num_osd,
  l_cluster_num_osd_up,
  l_cluster_num_osd_in,
  l_cluster_osd_epoch,
  l_cluster_osd_kb,
  l_cluster_osd_kb_used,
  l_cluster_osd_kb_avail,
  l_cluster_num_pool,
  l_cluster_num_pg,
  l_cluster_num_pg_active_clean,
  l_cluster_num_pg_active,
  l_cluster_num_pg_peering,
  l_cluster_num_object,
  l_cluster_num_object_degraded,
  l_cluster_num_object_unfound,
  l_cluster_num_bytes,
  l_cluster_num_mds_up,
  l_cluster_num_mds_in,
  l_cluster_num_mds_failed,
  l_cluster_mds_epoch,
  l_cluster_last,
};

class MonitorStore;

class PaxosService;

class PerfCounters;
class AdminSocketHook;

class MMonGetMap;
class MMonGetVersion;
class MMonProbe;
class MMonSubscribe;
class MAuthRotating;
class MRoute;
class MForward;
class MTimeCheck;

#define COMPAT_SET_LOC "feature_set"

class Monitor : public Dispatcher {
public:
  // me
  string name;
  int rank;
  Messenger *messenger;
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

  set<entity_addr_t> extra_probe_peers;

  LogClient clog;
  KeyRing keyring;
  KeyServer key_server;

  AuthMethodList auth_cluster_required;
  AuthMethodList auth_service_required;

  CompatSet features;

private:
  void new_tick();
  friend class C_Mon_Tick;

  // -- local storage --
public:
  MonitorStore *store;

  // -- monitor state --
private:
  enum {
    STATE_PROBING = 1,
    STATE_SLURPING,
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
    case STATE_SLURPING: return "slurping";
    case STATE_ELECTING: return "electing";
    case STATE_LEADER: return "leader";
    case STATE_PEON: return "peon";
    default: return "???";
    }
  }
  const char *get_state_name() const {
    return get_state_name(state);
  }

  bool is_probing() const { return state == STATE_PROBING; }
  bool is_slurping() const { return state == STATE_SLURPING; }
  bool is_electing() const { return state == STATE_ELECTING; }
  bool is_leader() const { return state == STATE_LEADER; }
  bool is_peon() const { return state == STATE_PEON; }

  const utime_t &get_leader_since() const;

  // -- elector --
private:
  Elector elector;
  friend class Elector;
  
  int leader;            // current leader (to best of knowledge)
  set<int> quorum;       // current active set of monitors (if !starting)
  utime_t leader_since;  // when this monitor became the leader, if it is the leader
  utime_t exited_quorum; // time detected as not in quorum; 0 if in
  uint64_t quorum_features;  ///< intersection of quorum member feature bits

  set<string> outside_quorum;
  entity_inst_t slurp_source;
  map<string,version_t> slurp_versions;

  list<Context*> waitfor_quorum;
  list<Context*> maybe_wait_for_quorum;

  // multi-paxos global version sequencing kludge-o-rama
  set<int> paxos_recovered;     ///< num paxos machines fully recovered during this election epoch
  version_t global_version;

  void require_gv_ondisk();
  void require_gv_onwire();

public:
  void recovered_leader(int id);
  void recovered_peon(int id);
  version_t get_global_paxos_version();
  bool is_all_paxos_recovered() {
    return paxos_recovered.size() == paxos.size();
  }

private:
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
  /**
   * Time Check event.
   */
  Context *timecheck_event;

  struct C_TimeCheck : public Context {
    Monitor *mon;
    C_TimeCheck(Monitor *m) : mon(m) { }
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
  void timecheck_report();
  void timecheck();
  health_status_t timecheck_status(ostringstream &ss,
                                   const double skew_bound,
                                   const double latency);
  void handle_timecheck_leader(MTimeCheck *m);
  void handle_timecheck_peon(MTimeCheck *m);
  void handle_timecheck(MTimeCheck *m);
  /**
   * @}
   */


  Context *probe_timeout_event;  // for probing and slurping states

  struct C_ProbeTimeout : public Context {
    Monitor *mon;
    C_ProbeTimeout(Monitor *m) : mon(m) {}
    void finish(int r) {
      mon->probe_timeout(r);
    }
  };

  void reset_probe_timeout();
  void cancel_probe_timeout();
  void probe_timeout(int r);

  void slurp();

 
public:
  epoch_t get_epoch();
  int get_leader() { return leader; }
  const set<int>& get_quorum() { return quorum; }
  set<string> get_quorum_names() {
    set<string> q;
    for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
      q.insert(monmap->get_name(*p));
    return q;
  }
  uint64_t get_quorum_features() const {
    return quorum_features;
  }

  void bootstrap();
  void reset();
  void start_election();
  void win_standalone_election();
  void win_election(epoch_t epoch, set<int>& q,
		    uint64_t features);         // end election (called by Elector)
  void lose_election(epoch_t epoch, set<int>& q, int l,
		     uint64_t features); // end election (called by Elector)
  void finish_election();

  void update_logger();

  // -- paxos -- These vector indices are matched
  list<Paxos*> paxos;
  vector<PaxosService*> paxos_service;

  Paxos *add_paxos(int type);
  Paxos *get_paxos_by_name(const string& name);
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


  // -- sessions --
  MonSessionMap session_map;
  AdminSocketHook *admin_hook;

  void check_subs();
  void check_sub(Subscription *sub);

  void send_latest_monmap(Connection *con);

  // messages
  void handle_get_version(MMonGetVersion *m);
  void handle_subscribe(MMonSubscribe *m);
  void handle_mon_get_map(MMonGetMap *m);
  bool _allowed_command(MonSession *s, const vector<std::string>& cmd);
  void _mon_status(ostream& ss);
  void _quorum_status(ostream& ss);
  void _add_bootstrap_peer_hint(string cmd, string args, ostream& ss);
  void handle_command(class MMonCommand *m);
  void handle_route(MRoute *m);

  /**
   * Generate health report
   *
   * @param status one-line status summary
   * @param detailbl optional bufferlist* to fill with a detailed report
   */
  void get_health(string& status, bufferlist *detailbl, Formatter *f);
  void get_status(stringstream &ss, Formatter *f);

  void reply_command(MMonCommand *m, int rc, const string &rs, version_t version);
  void reply_command(MMonCommand *m, int rc, const string &rs, bufferlist& rdata, version_t version);

  void handle_probe(MMonProbe *m);
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
  void handle_probe_probe(MMonProbe *m);
  void handle_probe_reply(MMonProbe *m);
  void handle_probe_slurp(MMonProbe *m);
  void handle_probe_slurp_latest(MMonProbe *m);
  void handle_probe_data(MMonProbe *m);
  /**
   * Given an MMonProbe and associated Paxos machine, create a reply,
   * fill it with the missing Paxos states and current commit pointers
   *
   * @param m The incoming MMonProbe. We use this to determine the range
   * of paxos states to include in the reply.
   * @param pax The Paxos state machine which m is associated with.
   *
   * @returns A new MMonProbe message, initialized as OP_DATA, and filled
   * with the necessary Paxos states. */
  MMonProbe *fill_probe_data(MMonProbe *m, Paxos *pax);

  // request routing
  struct RoutedRequest {
    uint64_t tid;
    entity_inst_t client;
    bufferlist request_bl;
    MonSession *session;

    ~RoutedRequest() {
      if (session)
	session->put();
    }
  };
  uint64_t routed_request_tid;
  map<uint64_t, RoutedRequest*> routed_requests;
  
  void forward_request_leader(PaxosServiceMessage *req);
  void handle_forward(MForward *m);
  void try_send_message(Message *m, const entity_inst_t& to);
  void send_reply(PaxosServiceMessage *req, Message *reply);
  void no_reply(PaxosServiceMessage *req);
  void resend_routed_requests();
  void remove_session(MonSession *s);
  void remove_all_sessions();

  void send_command(const entity_inst_t& inst,
		    const vector<string>& com, version_t version);

public:
  struct C_Command : public Context {
    Monitor *mon;
    MMonCommand *m;
    int rc;
    string rs;
    bufferlist rdata;
    version_t version;
    C_Command(Monitor *_mm, MMonCommand *_m, int r, string s, version_t v) :
      mon(_mm), m(_m), rc(r), rs(s), version(v){}
    C_Command(Monitor *_mm, MMonCommand *_m, int r, string s, bufferlist rd, version_t v) :
      mon(_mm), m(_m), rc(r), rs(s), rdata(rd), version(v){}
    void finish(int r) {
      if (r >= 0)
	mon->reply_command(m, rc, rs, rdata, version);
      else if (r == -ECANCELED)
	m->put();
      else if (r == -EAGAIN)
	mon->_ms_dispatch(m);
      else
	assert(0 == "bad C_Command return value");
    }
  };

 private:
  class C_RetryMessage : public Context {
    Monitor *mon;
    Message *msg;
  public:
    C_RetryMessage(Monitor *m, Message *ms) : mon(m), msg(ms) {}
    void finish(int r) {
      if (r == -EAGAIN || r >= 0)
	mon->_ms_dispatch(msg);
      else if (r == -ECANCELED)
	msg->put();
      else
	assert(0 == "bad C_RetryMessage return value");
    }
  };

  //ms_dispatch handles a lot of logic and we want to reuse it
  //on forwarded messages, so we create a non-locking version for this class
  bool _ms_dispatch(Message *m);
  bool ms_dispatch(Message *m) {
    lock.Lock();
    bool ret = _ms_dispatch(m);
    lock.Unlock();
    return ret;
  }
  //mon_caps is used for un-connected messages from monitors
  MonCaps * mon_caps;
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);
  bool ms_verify_authorizer(Connection *con, int peer_type,
			    int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			    bool& isvalid, CryptoKey& session_key);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

  void extract_save_mon_key(KeyRing& keyring);

  // features
  static CompatSet get_supported_features();
  static CompatSet get_legacy_features();
  void read_features();
  void write_features();

 public:
  Monitor(CephContext *cct_, string nm, MonitorStore *s, Messenger *m, MonMap *map);
  ~Monitor();

  static int check_features(MonitorStore *store);

  int preinit();
  int init();
  void shutdown();
  void tick();

  void handle_signal(int sig);

  void stop_cluster();

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

  void do_admin_command(std::string command, std::string args, ostream& ss);

private:
  // don't allow copying
  Monitor(const Monitor& rhs);
  Monitor& operator=(const Monitor &rhs);
};

#define CEPH_MON_FEATURE_INCOMPAT_BASE CompatSet::Feature (1, "initial feature set (~v.18)")
#define CEPH_MON_FEATURE_INCOMPAT_GV CompatSet::Feature (2, "global version sequencing (v0.52)")

long parse_pos_long(const char *s, ostream *pss = NULL);


#endif
