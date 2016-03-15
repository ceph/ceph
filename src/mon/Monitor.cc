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


#include <sstream>
#include <stdlib.h>
#include <signal.h>
#include <limits.h>
#include <cstring>
#include <boost/scope_exit.hpp>

#include "Monitor.h"
#include "common/version.h"

#include "osd/OSDMap.h"

#include "MonitorDBStore.h"

#include "msg/Messenger.h"

#include "messages/PaxosServiceMessage.h"
#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonMetadata.h"
#include "messages/MMonSync.h"
#include "messages/MMonScrub.h"
#include "messages/MMonProbe.h"
#include "messages/MMonJoin.h"
#include "messages/MMonPaxos.h"
#include "messages/MRoute.h"
#include "messages/MForward.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"

#include "messages/MAuthReply.h"

#include "messages/MTimeCheck.h"
#include "messages/MMonHealth.h"
#include "messages/MPing.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/Timer.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"

#include "global/signal_handler.h"

#include "include/color.h"
#include "include/ceph_fs.h"
#include "include/str_list.h"
#include "include/str_map.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "MonmapMonitor.h"
#include "PGMonitor.h"
#include "LogMonitor.h"
#include "AuthMonitor.h"
#include "mon/QuorumService.h"
#include "mon/HealthMonitor.h"
#include "mon/ConfigKeyService.h"

#include "auth/AuthMethodList.h"
#include "auth/KeyRing.h"

#include "common/config.h"
#include "common/cmdparse.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, const Monitor *mon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name() << ") e" << mon->monmap->get_epoch() << " ";
}

const string Monitor::MONITOR_NAME = "monitor";
const string Monitor::MONITOR_STORE_PREFIX = "monitor_store";


#undef FLAG
#undef COMMAND
#undef COMMAND_WITH_FLAG
MonCommand mon_commands[] = {
#define FLAG(f) (MonCommand::FLAG_##f)
#define COMMAND(parsesig, helptext, modulename, req_perms, avail)	\
  {parsesig, helptext, modulename, req_perms, avail, FLAG(NONE)},
#define COMMAND_WITH_FLAG(parsesig, helptext, modulename, req_perms, avail, flags) \
  {parsesig, helptext, modulename, req_perms, avail, flags},
#include <mon/MonCommands.h>
};
#undef COMMAND
MonCommand classic_mon_commands[] = {
#define COMMAND(parsesig, helptext, modulename, req_perms, avail)	\
  {parsesig, helptext, modulename, req_perms, avail},
#include <mon/DumplingMonCommands.h>
};


long parse_pos_long(const char *s, ostream *pss)
{
  if (*s == '-' || *s == '+') {
    if (pss)
      *pss << "expected numerical value, got: " << s;
    return -EINVAL;
  }

  string err;
  long r = strict_strtol(s, 10, &err);
  if ((r == 0) && !err.empty()) {
    if (pss)
      *pss << err;
    return -1;
  }
  if (r < 0) {
    if (pss)
      *pss << "unable to parse positive integer '" << s << "'";
    return -1;
  }
  return r;
}

Monitor::Monitor(CephContext* cct_, string nm, MonitorDBStore *s,
		 Messenger *m, MonMap *map) :
  Dispatcher(cct_),
  name(nm),
  rank(-1), 
  messenger(m),
  con_self(m ? m->get_loopback_connection() : NULL),
  lock("Monitor::lock"),
  timer(cct_, lock),
  has_ever_joined(false),
  logger(NULL), cluster_logger(NULL), cluster_logger_registered(false),
  monmap(map),
  log_client(cct_, messenger, monmap, LogClient::FLAG_MON),
  key_server(cct, &keyring),
  auth_cluster_required(cct,
			cct->_conf->auth_supported.empty() ?
			cct->_conf->auth_cluster_required : cct->_conf->auth_supported),
  auth_service_required(cct,
			cct->_conf->auth_supported.empty() ?
			cct->_conf->auth_service_required : cct->_conf->auth_supported ),
  leader_supported_mon_commands(NULL),
  leader_supported_mon_commands_size(0),
  store(s),
  
  state(STATE_PROBING),
  
  elector(this),
  required_features(0),
  leader(0),
  quorum_features(0),
  // scrub
  scrub_version(0),
  scrub_event(NULL),
  scrub_timeout_event(NULL),

  // sync state
  sync_provider_count(0),
  sync_cookie(0),
  sync_full(false),
  sync_start_version(0),
  sync_timeout_event(NULL),
  sync_last_committed_floor(0),

  timecheck_round(0),
  timecheck_acks(0),
  timecheck_rounds_since_clean(0),
  timecheck_event(NULL),

  probe_timeout_event(NULL),

  paxos_service(PAXOS_NUM),
  admin_hook(NULL),
  health_tick_event(NULL),
  health_interval_event(NULL),
  routed_request_tid(0),
  op_tracker(cct, true, 1)
{
  clog = log_client.create_channel(CLOG_CHANNEL_CLUSTER);
  audit_clog = log_client.create_channel(CLOG_CHANNEL_AUDIT);

  update_log_clients();

  paxos = new Paxos(this, "paxos");

  paxos_service[PAXOS_MDSMAP] = new MDSMonitor(this, paxos, "mdsmap");
  paxos_service[PAXOS_MONMAP] = new MonmapMonitor(this, paxos, "monmap");
  paxos_service[PAXOS_OSDMAP] = new OSDMonitor(cct, this, paxos, "osdmap");
  paxos_service[PAXOS_PGMAP] = new PGMonitor(this, paxos, "pgmap");
  paxos_service[PAXOS_LOG] = new LogMonitor(this, paxos, "logm");
  paxos_service[PAXOS_AUTH] = new AuthMonitor(this, paxos, "auth");

  health_monitor = new HealthMonitor(this);
  config_key_service = new ConfigKeyService(this, paxos);

  mon_caps = new MonCap();
  bool r = mon_caps->parse("allow *", NULL);
  assert(r);

  exited_quorum = ceph_clock_now(g_ceph_context);

  // assume our commands until we have an election.  this only means
  // we won't reply with EINVAL before the election; any command that
  // actually matters will wait until we have quorum etc and then
  // retry (and revalidate).
  const MonCommand *cmds;
  int cmdsize;
  get_locally_supported_monitor_commands(&cmds, &cmdsize);
  set_leader_supported_commands(cmds, cmdsize);
}

PaxosService *Monitor::get_paxos_service_by_name(const string& name)
{
  if (name == "mdsmap")
    return paxos_service[PAXOS_MDSMAP];
  if (name == "monmap")
    return paxos_service[PAXOS_MONMAP];
  if (name == "osdmap")
    return paxos_service[PAXOS_OSDMAP];
  if (name == "pgmap")
    return paxos_service[PAXOS_PGMAP];
  if (name == "logm")
    return paxos_service[PAXOS_LOG];
  if (name == "auth")
    return paxos_service[PAXOS_AUTH];

  assert(0 == "given name does not match known paxos service");
  return NULL;
}

Monitor::~Monitor()
{
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); ++p)
    delete *p;
  delete health_monitor;
  delete config_key_service;
  delete paxos;
  assert(session_map.sessions.empty());
  delete mon_caps;
  if (leader_supported_mon_commands != mon_commands &&
      leader_supported_mon_commands != classic_mon_commands)
    delete[] leader_supported_mon_commands;
}


class AdminHook : public AdminSocketHook {
  Monitor *mon;
public:
  explicit AdminHook(Monitor *m) : mon(m) {}
  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {
    stringstream ss;
    mon->do_admin_command(command, cmdmap, format, ss);
    out.append(ss);
    return true;
  }
};

void Monitor::do_admin_command(string command, cmdmap_t& cmdmap, string format,
			       ostream& ss)
{
  Mutex::Locker l(lock);

  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string args;
  for (cmdmap_t::iterator p = cmdmap.begin();
       p != cmdmap.end(); ++p) {
    if (p->first == "prefix")
      continue;
    if (!args.empty())
      args += ", ";
    args += cmd_vartype_stringify(p->second);
  }
  args = "[" + args + "]";
 
  bool read_only = (command == "mon_status" ||
                    command == "mon metadata" ||
                    command == "quorum_status" ||
                    command == "ops");

  (read_only ? audit_clog->debug() : audit_clog->info())
    << "from='admin socket' entity='admin socket' "
    << "cmd='" << command << "' args=" << args << ": dispatch";

  if (command == "mon_status") {
    get_mon_status(f.get(), ss);
    if (f)
      f->flush(ss);
  } else if (command == "quorum_status") {
    _quorum_status(f.get(), ss);
  } else if (command == "sync_force") {
    string validate;
    if ((!cmd_getval(g_ceph_context, cmdmap, "validate", validate)) ||
	(validate != "--yes-i-really-mean-it")) {
      ss << "are you SURE? this will mean the monitor store will be erased "
            "the next time the monitor is restarted.  pass "
            "'--yes-i-really-mean-it' if you really do.";
      goto abort;
    }
    sync_force(f.get(), ss);
  } else if (command.compare(0, 23, "add_bootstrap_peer_hint") == 0) {
    if (!_add_bootstrap_peer_hint(command, cmdmap, ss))
      goto abort;
  } else if (command == "quorum enter") {
    elector.start_participating();
    start_election();
    ss << "started responding to quorum, initiated new election";
  } else if (command == "quorum exit") {
    start_election();
    elector.stop_participating();
    ss << "stopped responding to quorum, initiated new election";
  } else if (command == "ops") {
    op_tracker.dump_ops_in_flight(f.get());
    if (f) {
      f->flush(ss);
    }
  } else {
    assert(0 == "bad AdminSocket command binding");
  }
  (read_only ? audit_clog->debug() : audit_clog->info())
    << "from='admin socket' "
    << "entity='admin socket' "
    << "cmd=" << command << " "
    << "args=" << args << ": finished";
  return;

abort:
  (read_only ? audit_clog->debug() : audit_clog->info())
    << "from='admin socket' "
    << "entity='admin socket' "
    << "cmd=" << command << " "
    << "args=" << args << ": aborted";
}

void Monitor::handle_signal(int signum)
{
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got Signal " << sig_str(signum) << " ***" << dendl;
  shutdown();
}

CompatSet Monitor::get_initial_supported_features()
{
  CompatSet::FeatureSet ceph_mon_feature_compat;
  CompatSet::FeatureSet ceph_mon_feature_ro_compat;
  CompatSet::FeatureSet ceph_mon_feature_incompat;
  ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_BASE);
  ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_SINGLE_PAXOS);
  return CompatSet(ceph_mon_feature_compat, ceph_mon_feature_ro_compat,
		   ceph_mon_feature_incompat);
}

CompatSet Monitor::get_supported_features()
{
  CompatSet compat = get_initial_supported_features();
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSD_ERASURE_CODES);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V2);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V3);
  return compat;
}

CompatSet Monitor::get_legacy_features()
{
  CompatSet::FeatureSet ceph_mon_feature_compat;
  CompatSet::FeatureSet ceph_mon_feature_ro_compat;
  CompatSet::FeatureSet ceph_mon_feature_incompat;
  ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_BASE);
  return CompatSet(ceph_mon_feature_compat, ceph_mon_feature_ro_compat,
		   ceph_mon_feature_incompat);
}

int Monitor::check_features(MonitorDBStore *store)
{
  CompatSet required = get_supported_features();
  CompatSet ondisk;

  read_features_off_disk(store, &ondisk);

  if (!required.writeable(ondisk)) {
    CompatSet diff = required.unsupported(ondisk);
    generic_derr << "ERROR: on disk data includes unsupported features: " << diff << dendl;
    return -EPERM;
  }

  return 0;
}

void Monitor::read_features_off_disk(MonitorDBStore *store, CompatSet *features)
{
  bufferlist featuresbl;
  store->get(MONITOR_NAME, COMPAT_SET_LOC, featuresbl);
  if (featuresbl.length() == 0) {
    generic_dout(0) << "WARNING: mon fs missing feature list.\n"
            << "Assuming it is old-style and introducing one." << dendl;
    //we only want the baseline ~v.18 features assumed to be on disk.
    //If new features are introduced this code needs to disappear or
    //be made smarter.
    *features = get_legacy_features();

    features->encode(featuresbl);
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    t->put(MONITOR_NAME, COMPAT_SET_LOC, featuresbl);
    store->apply_transaction(t);
  } else {
    bufferlist::iterator it = featuresbl.begin();
    features->decode(it);
  }
}

void Monitor::read_features()
{
  read_features_off_disk(store, &features);
  dout(10) << "features " << features << dendl;

  apply_compatset_features_to_quorum_requirements();
  dout(10) << "required_features " << required_features << dendl;
}

void Monitor::write_features(MonitorDBStore::TransactionRef t)
{
  bufferlist bl;
  features.encode(bl);
  t->put(MONITOR_NAME, COMPAT_SET_LOC, bl);
}

const char** Monitor::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "crushtool", // helpful for testing
    "mon_election_timeout",
    "mon_lease",
    "mon_lease_renew_interval_factor",
    "mon_lease_ack_timeout_factor",
    "mon_accept_timeout_factor",
    // clog & admin clog
    "clog_to_monitors",
    "clog_to_syslog",
    "clog_to_syslog_facility",
    "clog_to_syslog_level",
    "clog_to_graylog",
    "clog_to_graylog_host",
    "clog_to_graylog_port",
    "fsid",
    // periodic health to clog
    "mon_health_to_clog",
    "mon_health_to_clog_interval",
    "mon_health_to_clog_tick_interval",
    // scrub interval
    "mon_scrub_interval",
    NULL
  };
  return KEYS;
}

void Monitor::handle_conf_change(const struct md_config_t *conf,
                                 const std::set<std::string> &changed)
{
  sanitize_options();

  dout(10) << __func__ << " " << changed << dendl;

  if (changed.count("clog_to_monitors") ||
      changed.count("clog_to_syslog") ||
      changed.count("clog_to_syslog_level") ||
      changed.count("clog_to_syslog_facility") ||
      changed.count("clog_to_graylog") ||
      changed.count("clog_to_graylog_host") ||
      changed.count("clog_to_graylog_port") ||
      changed.count("host") ||
      changed.count("fsid")) {
    update_log_clients();
  }

  if (changed.count("mon_health_to_clog") ||
      changed.count("mon_health_to_clog_interval") ||
      changed.count("mon_health_to_clog_tick_interval")) {
    health_to_clog_update_conf(changed);
  }

  if (changed.count("mon_scrub_interval")) {
    scrub_update_interval(conf->mon_scrub_interval);
  }
}

void Monitor::update_log_clients()
{
  map<string,string> log_to_monitors;
  map<string,string> log_to_syslog;
  map<string,string> log_channel;
  map<string,string> log_prio;
  map<string,string> log_to_graylog;
  map<string,string> log_to_graylog_host;
  map<string,string> log_to_graylog_port;
  uuid_d fsid;
  string host;

  if (parse_log_client_options(g_ceph_context, log_to_monitors, log_to_syslog,
			       log_channel, log_prio, log_to_graylog,
			       log_to_graylog_host, log_to_graylog_port,
			       fsid, host))
    return;

  clog->update_config(log_to_monitors, log_to_syslog,
		      log_channel, log_prio, log_to_graylog,
		      log_to_graylog_host, log_to_graylog_port,
		      fsid, host);

  audit_clog->update_config(log_to_monitors, log_to_syslog,
			    log_channel, log_prio, log_to_graylog,
			    log_to_graylog_host, log_to_graylog_port,
			    fsid, host);
}

int Monitor::sanitize_options()
{
  int r = 0;

  // mon_lease must be greater than mon_lease_renewal; otherwise we
  // may incur in leases expiring before they are renewed.
  if (g_conf->mon_lease_renew_interval_factor >= 1.0) {
    clog->error() << "mon_lease_renew_interval_factor ("
		  << g_conf->mon_lease_renew_interval_factor
		  << ") must be less than 1.0";
    r = -EINVAL;
  }

  // mon_lease_ack_timeout must be greater than mon_lease to make sure we've
  // got time to renew the lease and get an ack for it. Having both options
  // with the same value, for a given small vale, could mean timing out if
  // the monitors happened to be overloaded -- or even under normal load for
  // a small enough value.
  if (g_conf->mon_lease_ack_timeout_factor <= 1.0) {
    clog->error() << "mon_lease_ack_timeout_factor ("
		  << g_conf->mon_lease_ack_timeout_factor
		  << ") must be greater than 1.0";
    r = -EINVAL;
  }

  return r;
}

int Monitor::preinit()
{
  lock.Lock();

  dout(1) << "preinit fsid " << monmap->fsid << dendl;

  int r = sanitize_options();
  if (r < 0) {
    derr << "option sanitization failed!" << dendl;
    lock.Unlock();
    return r;
  }

  assert(!logger);
  {
    PerfCountersBuilder pcb(g_ceph_context, "mon", l_mon_first, l_mon_last);
    pcb.add_u64(l_mon_num_sessions, "num_sessions", "Open sessions", "sess");
    pcb.add_u64_counter(l_mon_session_add, "session_add", "Created sessions", "sadd");
    pcb.add_u64_counter(l_mon_session_rm, "session_rm", "Removed sessions", "srm");
    pcb.add_u64_counter(l_mon_session_trim, "session_trim", "Trimmed sessions");
    pcb.add_u64_counter(l_mon_num_elections, "num_elections", "Elections participated in");
    pcb.add_u64_counter(l_mon_election_call, "election_call", "Elections started");
    pcb.add_u64_counter(l_mon_election_win, "election_win", "Elections won");
    pcb.add_u64_counter(l_mon_election_lose, "election_lose", "Elections lost");
    logger = pcb.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
  }

  assert(!cluster_logger);
  {
    PerfCountersBuilder pcb(g_ceph_context, "cluster", l_cluster_first, l_cluster_last);
    pcb.add_u64(l_cluster_num_mon, "num_mon", "Monitors");
    pcb.add_u64(l_cluster_num_mon_quorum, "num_mon_quorum", "Monitors in quorum");
    pcb.add_u64(l_cluster_num_osd, "num_osd", "OSDs");
    pcb.add_u64(l_cluster_num_osd_up, "num_osd_up", "OSDs that are up");
    pcb.add_u64(l_cluster_num_osd_in, "num_osd_in", "OSD in state \"in\" (they are in cluster)");
    pcb.add_u64(l_cluster_osd_epoch, "osd_epoch", "Current epoch of OSD map");
    pcb.add_u64(l_cluster_osd_bytes, "osd_bytes", "Total capacity of cluster");
    pcb.add_u64(l_cluster_osd_bytes_used, "osd_bytes_used", "Used space");
    pcb.add_u64(l_cluster_osd_bytes_avail, "osd_bytes_avail", "Available space");
    pcb.add_u64(l_cluster_num_pool, "num_pool", "Pools");
    pcb.add_u64(l_cluster_num_pg, "num_pg", "Placement groups");
    pcb.add_u64(l_cluster_num_pg_active_clean, "num_pg_active_clean", "Placement groups in active+clean state");
    pcb.add_u64(l_cluster_num_pg_active, "num_pg_active", "Placement groups in active state");
    pcb.add_u64(l_cluster_num_pg_peering, "num_pg_peering", "Placement groups in peering state");
    pcb.add_u64(l_cluster_num_object, "num_object", "Objects");
    pcb.add_u64(l_cluster_num_object_degraded, "num_object_degraded", "Degraded (missing replicas) objects");
    pcb.add_u64(l_cluster_num_object_misplaced, "num_object_misplaced", "Misplaced (wrong location in the cluster) objects");
    pcb.add_u64(l_cluster_num_object_unfound, "num_object_unfound", "Unfound objects");
    pcb.add_u64(l_cluster_num_bytes, "num_bytes", "Size of all objects");
    pcb.add_u64(l_cluster_num_mds_up, "num_mds_up", "MDSs that are up");
    pcb.add_u64(l_cluster_num_mds_in, "num_mds_in", "MDS in state \"in\" (they are in cluster)");
    pcb.add_u64(l_cluster_num_mds_failed, "num_mds_failed", "Failed MDS");
    pcb.add_u64(l_cluster_mds_epoch, "mds_epoch", "Current epoch of MDS map");
    cluster_logger = pcb.create_perf_counters();
  }

  paxos->init_logger();

  // verify cluster_uuid
  {
    int r = check_fsid();
    if (r == -ENOENT)
      r = write_fsid();
    if (r < 0) {
      lock.Unlock();
      return r;
    }
  }

  // open compatset
  read_features();

  // have we ever joined a quorum?
  has_ever_joined = (store->get(MONITOR_NAME, "joined") != 0);
  dout(10) << "has_ever_joined = " << (int)has_ever_joined << dendl;

  if (!has_ever_joined) {
    // impose initial quorum restrictions?
    list<string> initial_members;
    get_str_list(g_conf->mon_initial_members, initial_members);

    if (!initial_members.empty()) {
      dout(1) << " initial_members " << initial_members << ", filtering seed monmap" << dendl;

      monmap->set_initial_members(g_ceph_context, initial_members, name, messenger->get_myaddr(),
				  &extra_probe_peers);

      dout(10) << " monmap is " << *monmap << dendl;
      dout(10) << " extra probe peers " << extra_probe_peers << dendl;
    }
  } else if (!monmap->contains(name)) {
    derr << "not in monmap and have been in a quorum before; "
         << "must have been removed" << dendl;
    if (g_conf->mon_force_quorum_join) {
      dout(0) << "we should have died but "
              << "'mon_force_quorum_join' is set -- allowing boot" << dendl;
    } else {
      derr << "commit suicide!" << dendl;
      lock.Unlock();
      return -ENOENT;
    }
  }

  {
    // We have a potentially inconsistent store state in hands. Get rid of it
    // and start fresh.
    bool clear_store = false;
    if (store->exists("mon_sync", "in_sync")) {
      dout(1) << __func__ << " clean up potentially inconsistent store state"
	      << dendl;
      clear_store = true;
    }

    if (store->get("mon_sync", "force_sync") > 0) {
      dout(1) << __func__ << " force sync by clearing store state" << dendl;
      clear_store = true;
    }

    if (clear_store) {
      set<string> sync_prefixes = get_sync_targets_names();
      store->clear(sync_prefixes);
    }
  }

  sync_last_committed_floor = store->get("mon_sync", "last_committed_floor");
  dout(10) << "sync_last_committed_floor " << sync_last_committed_floor << dendl;

  init_paxos();
  health_monitor->init();

  if (is_keyring_required()) {
    // we need to bootstrap authentication keys so we can form an
    // initial quorum.
    if (authmon()->get_last_committed() == 0) {
      dout(10) << "loading initial keyring to bootstrap authentication for mkfs" << dendl;
      bufferlist bl;
      int err = store->get("mkfs", "keyring", bl);
      if (err == 0 && bl.length() > 0) {
        // Attempt to decode and extract keyring only if it is found.
        KeyRing keyring;
        bufferlist::iterator p = bl.begin();
        ::decode(keyring, p);
        extract_save_mon_key(keyring);
      }
    }

    string keyring_loc = g_conf->mon_data + "/keyring";

    r = keyring.load(cct, keyring_loc);
    if (r < 0) {
      EntityName mon_name;
      mon_name.set_type(CEPH_ENTITY_TYPE_MON);
      EntityAuth mon_key;
      if (key_server.get_auth(mon_name, mon_key)) {
	dout(1) << "copying mon. key from old db to external keyring" << dendl;
	keyring.add(mon_name, mon_key);
	bufferlist bl;
	keyring.encode_plaintext(bl);
	write_default_keyring(bl);
      } else {
	derr << "unable to load initial keyring " << g_conf->keyring << dendl;
	lock.Unlock();
	return r;
      }
    }
  }

  admin_hook = new AdminHook(this);
  AdminSocket* admin_socket = cct->get_admin_socket();

  // unlock while registering to avoid mon_lock -> admin socket lock dependency.
  lock.Unlock();
  r = admin_socket->register_command("mon_status", "mon_status", admin_hook,
				     "show current monitor status");
  assert(r == 0);
  r = admin_socket->register_command("quorum_status", "quorum_status",
				     admin_hook, "show current quorum status");
  assert(r == 0);
  r = admin_socket->register_command("sync_force",
				     "sync_force name=validate,"
				     "type=CephChoices,"
			             "strings=--yes-i-really-mean-it",
				     admin_hook,
				     "force sync of and clear monitor store");
  assert(r == 0);
  r = admin_socket->register_command("add_bootstrap_peer_hint",
				     "add_bootstrap_peer_hint name=addr,"
				     "type=CephIPAddr",
				     admin_hook,
				     "add peer address as potential bootstrap"
				     " peer for cluster bringup");
  assert(r == 0);
  r = admin_socket->register_command("quorum enter", "quorum enter",
                                     admin_hook,
                                     "force monitor back into quorum");
  assert(r == 0);
  r = admin_socket->register_command("quorum exit", "quorum exit",
                                     admin_hook,
                                     "force monitor out of the quorum");
  assert(r == 0);
  r = admin_socket->register_command("ops",
                                     "ops",
                                     admin_hook,
                                     "show the ops currently in flight");
  assert(r == 0);
  lock.Lock();

  // add ourselves as a conf observer
  g_conf->add_observer(this);

  lock.Unlock();
  return 0;
}

int Monitor::init()
{
  dout(2) << "init" << dendl;
  lock.Lock();

  // start ticker
  timer.init();
  new_tick();

  // i'm ready!
  messenger->add_dispatcher_tail(this);


  bootstrap();

  // encode command sets
  const MonCommand *cmds;
  int cmdsize;
  get_locally_supported_monitor_commands(&cmds, &cmdsize);
  MonCommand::encode_array(cmds, cmdsize, supported_commands_bl);
  get_classic_monitor_commands(&cmds, &cmdsize);
  MonCommand::encode_array(cmds, cmdsize, classic_commands_bl);

  lock.Unlock();
  return 0;
}

void Monitor::init_paxos()
{
  dout(10) << __func__ << dendl;
  paxos->init();

  // init services
  for (int i = 0; i < PAXOS_NUM; ++i) {
    paxos_service[i]->init();
  }

  refresh_from_paxos(NULL);
}

void Monitor::refresh_from_paxos(bool *need_bootstrap)
{
  dout(10) << __func__ << dendl;

  bufferlist bl;
  int r = store->get(MONITOR_NAME, "cluster_fingerprint", bl);
  if (r >= 0) {
    try {
      bufferlist::iterator p = bl.begin();
      ::decode(fingerprint, p);
    }
    catch (buffer::error& e) {
      dout(10) << __func__ << " failed to decode cluster_fingerprint" << dendl;
    }
  } else {
    dout(10) << __func__ << " no cluster_fingerprint" << dendl;
  }

  for (int i = 0; i < PAXOS_NUM; ++i) {
    paxos_service[i]->refresh(need_bootstrap);
  }
  for (int i = 0; i < PAXOS_NUM; ++i) {
    paxos_service[i]->post_refresh();
  }
}

void Monitor::register_cluster_logger()
{
  if (!cluster_logger_registered) {
    dout(10) << "register_cluster_logger" << dendl;
    cluster_logger_registered = true;
    cct->get_perfcounters_collection()->add(cluster_logger);
  } else {
    dout(10) << "register_cluster_logger - already registered" << dendl;
  }
}

void Monitor::unregister_cluster_logger()
{
  if (cluster_logger_registered) {
    dout(10) << "unregister_cluster_logger" << dendl;
    cluster_logger_registered = false;
    cct->get_perfcounters_collection()->remove(cluster_logger);
  } else {
    dout(10) << "unregister_cluster_logger - not registered" << dendl;
  }
}

void Monitor::update_logger()
{
  cluster_logger->set(l_cluster_num_mon, monmap->size());
  cluster_logger->set(l_cluster_num_mon_quorum, quorum.size());
}

void Monitor::shutdown()
{
  dout(1) << "shutdown" << dendl;

  lock.Lock();
  wait_for_paxos_write();

  state = STATE_SHUTDOWN;

  g_conf->remove_observer(this);

  if (admin_hook) {
    AdminSocket* admin_socket = cct->get_admin_socket();
    admin_socket->unregister_command("mon_status");
    admin_socket->unregister_command("quorum_status");
    admin_socket->unregister_command("sync_force");
    admin_socket->unregister_command("add_bootstrap_peer_hint");
    admin_socket->unregister_command("quorum enter");
    admin_socket->unregister_command("quorum exit");
    admin_socket->unregister_command("ops");
    delete admin_hook;
    admin_hook = NULL;
  }

  elector.shutdown();
  
  // clean up
  paxos->shutdown();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); ++p)
    (*p)->shutdown();
  health_monitor->shutdown();

  finish_contexts(g_ceph_context, waitfor_quorum, -ECANCELED);
  finish_contexts(g_ceph_context, maybe_wait_for_quorum, -ECANCELED);

  timer.shutdown();

  remove_all_sessions();

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = NULL;
  }
  if (cluster_logger) {
    if (cluster_logger_registered)
      cct->get_perfcounters_collection()->remove(cluster_logger);
    delete cluster_logger;
    cluster_logger = NULL;
  }

  log_client.shutdown();

  // unlock before msgr shutdown...
  lock.Unlock();

  messenger->shutdown();  // last thing!  ceph_mon.cc will delete mon.
}

void Monitor::wait_for_paxos_write()
{
  if (paxos->is_writing() || paxos->is_writing_previous()) {
    dout(10) << __func__ << " flushing pending write" << dendl;
    lock.Unlock();
    store->flush();
    lock.Lock();
    dout(10) << __func__ << " flushed pending write" << dendl;
  }
}

void Monitor::bootstrap()
{
  dout(10) << "bootstrap" << dendl;
  wait_for_paxos_write();

  sync_reset_requester();
  unregister_cluster_logger();
  cancel_probe_timeout();

  // note my rank
  int newrank = monmap->get_rank(messenger->get_myaddr());
  if (newrank < 0 && rank >= 0) {
    // was i ever part of the quorum?
    if (has_ever_joined) {
      dout(0) << " removed from monmap, suicide." << dendl;
      exit(0);
    }
  }
  if (newrank != rank) {
    dout(0) << " my rank is now " << newrank << " (was " << rank << ")" << dendl;
    messenger->set_myname(entity_name_t::MON(newrank));
    rank = newrank;

    // reset all connections, or else our peers will think we are someone else.
    messenger->mark_down_all();
  }

  // reset
  state = STATE_PROBING;

  _reset();

  // sync store
  if (g_conf->mon_compact_on_bootstrap) {
    dout(10) << "bootstrap -- triggering compaction" << dendl;
    store->compact();
    dout(10) << "bootstrap -- finished compaction" << dendl;
  }

  // singleton monitor?
  if (monmap->size() == 1 && rank == 0) {
    win_standalone_election();
    return;
  }

  reset_probe_timeout();

  // i'm outside the quorum
  if (monmap->contains(name))
    outside_quorum.insert(name);

  // probe monitors
  dout(10) << "probing other monitors" << dendl;
  for (unsigned i = 0; i < monmap->size(); i++) {
    if ((int)i != rank)
      messenger->send_message(new MMonProbe(monmap->fsid, MMonProbe::OP_PROBE, name, has_ever_joined),
			      monmap->get_inst(i));
  }
  for (set<entity_addr_t>::iterator p = extra_probe_peers.begin();
       p != extra_probe_peers.end();
       ++p) {
    if (*p != messenger->get_myaddr()) {
      entity_inst_t i;
      i.name = entity_name_t::MON(-1);
      i.addr = *p;
      messenger->send_message(new MMonProbe(monmap->fsid, MMonProbe::OP_PROBE, name, has_ever_joined), i);
    }
  }
}

bool Monitor::_add_bootstrap_peer_hint(string cmd, cmdmap_t& cmdmap, ostream& ss)
{
  string addrstr;
  if (!cmd_getval(g_ceph_context, cmdmap, "addr", addrstr)) {
    ss << "unable to parse address string value '"
         << cmd_vartype_stringify(cmdmap["addr"]) << "'";
    return false;
  }
  dout(10) << "_add_bootstrap_peer_hint '" << cmd << "' '"
           << addrstr << "'" << dendl;

  entity_addr_t addr;
  const char *end = 0;
  if (!addr.parse(addrstr.c_str(), &end)) {
    ss << "failed to parse addr '" << addrstr << "'; syntax is 'add_bootstrap_peer_hint ip[:port]'";
    return false;
  }

  if (is_leader() || is_peon()) {
    ss << "mon already active; ignoring bootstrap hint";
    return true;
  }

  if (addr.get_port() == 0)
    addr.set_port(CEPH_MON_PORT);

  extra_probe_peers.insert(addr);
  ss << "adding peer " << addr << " to list: " << extra_probe_peers;
  return true;
}

// called by bootstrap(), or on leader|peon -> electing
void Monitor::_reset()
{
  dout(10) << __func__ << dendl;

  cancel_probe_timeout();
  timecheck_finish();
  health_events_cleanup();
  scrub_event_cancel();

  leader_since = utime_t();
  if (!quorum.empty()) {
    exited_quorum = ceph_clock_now(g_ceph_context);
  }
  quorum.clear();
  outside_quorum.clear();

  scrub_reset();

  paxos->restart();

  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); ++p)
    (*p)->restart();
  health_monitor->finish();
}


// -----------------------------------------------------------
// sync

set<string> Monitor::get_sync_targets_names()
{
  set<string> targets;
  targets.insert(paxos->get_name());
  for (int i = 0; i < PAXOS_NUM; ++i)
    paxos_service[i]->get_store_prefixes(targets);
  ConfigKeyService *config_key_service_ptr = dynamic_cast<ConfigKeyService*>(config_key_service);
  assert(config_key_service_ptr);
  config_key_service_ptr->get_store_prefixes(targets);
  return targets;
}


void Monitor::sync_timeout()
{
  dout(10) << __func__ << dendl;
  assert(state == STATE_SYNCHRONIZING);
  bootstrap();
}

void Monitor::sync_obtain_latest_monmap(bufferlist &bl)
{
  dout(1) << __func__ << dendl;

  MonMap latest_monmap;

  // Grab latest monmap from MonmapMonitor
  bufferlist monmon_bl;
  int err = monmon()->get_monmap(monmon_bl);
  if (err < 0) {
    if (err != -ENOENT) {
      derr << __func__
           << " something wrong happened while reading the store: "
           << cpp_strerror(err) << dendl;
      assert(0 == "error reading the store");
    }
  } else {
    latest_monmap.decode(monmon_bl);
  }

  // Grab last backed up monmap (if any) and compare epochs
  if (store->exists("mon_sync", "latest_monmap")) {
    bufferlist backup_bl;
    int err = store->get("mon_sync", "latest_monmap", backup_bl);
    if (err < 0) {
      derr << __func__
           << " something wrong happened while reading the store: "
           << cpp_strerror(err) << dendl;
      assert(0 == "error reading the store");
    }
    assert(backup_bl.length() > 0);

    MonMap backup_monmap;
    backup_monmap.decode(backup_bl);

    if (backup_monmap.epoch > latest_monmap.epoch)
      latest_monmap = backup_monmap;
  }

  // Check if our current monmap's epoch is greater than the one we've
  // got so far.
  if (monmap->epoch > latest_monmap.epoch)
    latest_monmap = *monmap;

  dout(1) << __func__ << " obtained monmap e" << latest_monmap.epoch << dendl;

  latest_monmap.encode(bl, CEPH_FEATURES_ALL);
}

void Monitor::sync_reset_requester()
{
  dout(10) << __func__ << dendl;

  if (sync_timeout_event) {
    timer.cancel_event(sync_timeout_event);
    sync_timeout_event = NULL;
  }

  sync_provider = entity_inst_t();
  sync_cookie = 0;
  sync_full = false;
  sync_start_version = 0;
}

void Monitor::sync_reset_provider()
{
  dout(10) << __func__ << dendl;
  sync_providers.clear();
}

void Monitor::sync_start(entity_inst_t &other, bool full)
{
  dout(10) << __func__ << " " << other << (full ? " full" : " recent") << dendl;

  assert(state == STATE_PROBING ||
	 state == STATE_SYNCHRONIZING);
  state = STATE_SYNCHRONIZING;

  // make sure are not a provider for anyone!
  sync_reset_provider();

  sync_full = full;

  if (sync_full) {
    // stash key state, and mark that we are syncing
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    sync_stash_critical_state(t);
    t->put("mon_sync", "in_sync", 1);

    sync_last_committed_floor = MAX(sync_last_committed_floor, paxos->get_version());
    dout(10) << __func__ << " marking sync in progress, storing sync_last_committed_floor "
	     << sync_last_committed_floor << dendl;
    t->put("mon_sync", "last_committed_floor", sync_last_committed_floor);

    store->apply_transaction(t);

    assert(g_conf->mon_sync_requester_kill_at != 1);

    // clear the underlying store
    set<string> targets = get_sync_targets_names();
    dout(10) << __func__ << " clearing prefixes " << targets << dendl;
    store->clear(targets);

    // make sure paxos knows it has been reset.  this prevents a
    // bootstrap and then different probe reply order from possibly
    // deciding a partial or no sync is needed.
    paxos->init();

    assert(g_conf->mon_sync_requester_kill_at != 2);
  }

  // assume 'other' as the leader. We will update the leader once we receive
  // a reply to the sync start.
  sync_provider = other;

  sync_reset_timeout();

  MMonSync *m = new MMonSync(sync_full ? MMonSync::OP_GET_COOKIE_FULL : MMonSync::OP_GET_COOKIE_RECENT);
  if (!sync_full)
    m->last_committed = paxos->get_version();
  messenger->send_message(m, sync_provider);
}

void Monitor::sync_stash_critical_state(MonitorDBStore::TransactionRef t)
{
  dout(10) << __func__ << dendl;
  bufferlist backup_monmap;
  sync_obtain_latest_monmap(backup_monmap);
  assert(backup_monmap.length() > 0);
  t->put("mon_sync", "latest_monmap", backup_monmap);
}

void Monitor::sync_reset_timeout()
{
  dout(10) << __func__ << dendl;
  if (sync_timeout_event)
    timer.cancel_event(sync_timeout_event);
  sync_timeout_event = new C_SyncTimeout(this);
  timer.add_event_after(g_conf->mon_sync_timeout, sync_timeout_event);
}

void Monitor::sync_finish(version_t last_committed)
{
  dout(10) << __func__ << " lc " << last_committed << " from " << sync_provider << dendl;

  assert(g_conf->mon_sync_requester_kill_at != 7);

  if (sync_full) {
    // finalize the paxos commits
    MonitorDBStore::TransactionRef tx(new MonitorDBStore::Transaction);
    paxos->read_and_prepare_transactions(tx, sync_start_version,
					 last_committed);
    tx->put(paxos->get_name(), "last_committed", last_committed);

    dout(30) << __func__ << " final tx dump:\n";
    JSONFormatter f(true);
    tx->dump(&f);
    f.flush(*_dout);
    *_dout << dendl;

    store->apply_transaction(tx);
  }

  assert(g_conf->mon_sync_requester_kill_at != 8);

  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  t->erase("mon_sync", "in_sync");
  t->erase("mon_sync", "force_sync");
  t->erase("mon_sync", "last_committed_floor");
  store->apply_transaction(t);

  assert(g_conf->mon_sync_requester_kill_at != 9);

  init_paxos();

  assert(g_conf->mon_sync_requester_kill_at != 10);

  bootstrap();
}

void Monitor::handle_sync(MonOpRequestRef op)
{
  MMonSync *m = static_cast<MMonSync*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  switch (m->op) {

    // provider ---------

  case MMonSync::OP_GET_COOKIE_FULL:
  case MMonSync::OP_GET_COOKIE_RECENT:
    handle_sync_get_cookie(op);
    break;
  case MMonSync::OP_GET_CHUNK:
    handle_sync_get_chunk(op);
    break;

    // client -----------

  case MMonSync::OP_COOKIE:
    handle_sync_cookie(op);
    break;

  case MMonSync::OP_CHUNK:
  case MMonSync::OP_LAST_CHUNK:
    handle_sync_chunk(op);
    break;
  case MMonSync::OP_NO_COOKIE:
    handle_sync_no_cookie(op);
    break;

  default:
    dout(0) << __func__ << " unknown op " << m->op << dendl;
    assert(0 == "unknown op");
  }
}

// leader

void Monitor::_sync_reply_no_cookie(MonOpRequestRef op)
{
  MMonSync *m = static_cast<MMonSync*>(op->get_req());
  MMonSync *reply = new MMonSync(MMonSync::OP_NO_COOKIE, m->cookie);
  m->get_connection()->send_message(reply);
}

void Monitor::handle_sync_get_cookie(MonOpRequestRef op)
{
  MMonSync *m = static_cast<MMonSync*>(op->get_req());
  if (is_synchronizing()) {
    _sync_reply_no_cookie(op);
    return;
  }

  assert(g_conf->mon_sync_provider_kill_at != 1);

  // make sure they can understand us.
  if ((required_features ^ m->get_connection()->get_features()) &
      required_features) {
    dout(5) << " ignoring peer mon." << m->get_source().num()
	    << " has features " << std::hex
	    << m->get_connection()->get_features()
	    << " but we require " << required_features << std::dec << dendl;
    return;
  }

  // make up a unique cookie.  include election epoch (which persists
  // across restarts for the whole cluster) and a counter for this
  // process instance.  there is no need to be unique *across*
  // monitors, though.
  uint64_t cookie = ((unsigned long long)elector.get_epoch() << 24) + ++sync_provider_count;
  assert(sync_providers.count(cookie) == 0);

  dout(10) << __func__ << " cookie " << cookie << " for " << m->get_source_inst() << dendl;

  SyncProvider& sp = sync_providers[cookie];
  sp.cookie = cookie;
  sp.entity = m->get_source_inst();
  sp.reset_timeout(g_ceph_context, g_conf->mon_sync_timeout * 2);

  set<string> sync_targets;
  if (m->op == MMonSync::OP_GET_COOKIE_FULL) {
    // full scan
    sync_targets = get_sync_targets_names();
    sp.last_committed = paxos->get_version();
    sp.synchronizer = store->get_synchronizer(sp.last_key, sync_targets);
    sp.full = true;
    dout(10) << __func__ << " will sync prefixes " << sync_targets << dendl;
  } else {
    // just catch up paxos
    sp.last_committed = m->last_committed;
  }
  dout(10) << __func__ << " will sync from version " << sp.last_committed << dendl;

  MMonSync *reply = new MMonSync(MMonSync::OP_COOKIE, sp.cookie);
  reply->last_committed = sp.last_committed;
  m->get_connection()->send_message(reply);
}

void Monitor::handle_sync_get_chunk(MonOpRequestRef op)
{
  MMonSync *m = static_cast<MMonSync*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;

  if (sync_providers.count(m->cookie) == 0) {
    dout(10) << __func__ << " no cookie " << m->cookie << dendl;
    _sync_reply_no_cookie(op);
    return;
  }

  assert(g_conf->mon_sync_provider_kill_at != 2);

  SyncProvider& sp = sync_providers[m->cookie];
  sp.reset_timeout(g_ceph_context, g_conf->mon_sync_timeout * 2);

  if (sp.last_committed < paxos->get_first_committed() &&
      paxos->get_first_committed() > 1) {
    dout(10) << __func__ << " sync requester fell behind paxos, their lc " << sp.last_committed
	     << " < our fc " << paxos->get_first_committed() << dendl;
    sync_providers.erase(m->cookie);
    _sync_reply_no_cookie(op);
    return;
  }

  MMonSync *reply = new MMonSync(MMonSync::OP_CHUNK, sp.cookie);
  MonitorDBStore::TransactionRef tx(new MonitorDBStore::Transaction);

  int left = g_conf->mon_sync_max_payload_size;
  while (sp.last_committed < paxos->get_version() && left > 0) {
    bufferlist bl;
    sp.last_committed++;
    store->get(paxos->get_name(), sp.last_committed, bl);
    // TODO: what if store->get returns error or empty bl?
    tx->put(paxos->get_name(), sp.last_committed, bl);
    left -= bl.length();
    dout(20) << __func__ << " including paxos state " << sp.last_committed
	     << dendl;
  }
  reply->last_committed = sp.last_committed;

  if (sp.full && left > 0) {
    sp.synchronizer->get_chunk_tx(tx, left);
    sp.last_key = sp.synchronizer->get_last_key();
    reply->last_key = sp.last_key;
  }

  if ((sp.full && sp.synchronizer->has_next_chunk()) ||
      sp.last_committed < paxos->get_version()) {
    dout(10) << __func__ << " chunk, through version " << sp.last_committed
	     << " key " << sp.last_key << dendl;
  } else {
    dout(10) << __func__ << " last chunk, through version " << sp.last_committed
	     << " key " << sp.last_key << dendl;
    reply->op = MMonSync::OP_LAST_CHUNK;

    assert(g_conf->mon_sync_provider_kill_at != 3);

    // clean up our local state
    sync_providers.erase(sp.cookie);
  }

  ::encode(*tx, reply->chunk_bl);

  m->get_connection()->send_message(reply);
}

// requester

void Monitor::handle_sync_cookie(MonOpRequestRef op)
{
  MMonSync *m = static_cast<MMonSync*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  if (sync_cookie) {
    dout(10) << __func__ << " already have a cookie, ignoring" << dendl;
    return;
  }
  if (m->get_source_inst() != sync_provider) {
    dout(10) << __func__ << " source does not match, discarding" << dendl;
    return;
  }
  sync_cookie = m->cookie;
  sync_start_version = m->last_committed;

  sync_reset_timeout();
  sync_get_next_chunk();

  assert(g_conf->mon_sync_requester_kill_at != 3);
}

void Monitor::sync_get_next_chunk()
{
  dout(20) << __func__ << " cookie " << sync_cookie << " provider " << sync_provider << dendl;
  if (g_conf->mon_inject_sync_get_chunk_delay > 0) {
    dout(20) << __func__ << " injecting delay of " << g_conf->mon_inject_sync_get_chunk_delay << dendl;
    usleep((long long)(g_conf->mon_inject_sync_get_chunk_delay * 1000000.0));
  }
  MMonSync *r = new MMonSync(MMonSync::OP_GET_CHUNK, sync_cookie);
  messenger->send_message(r, sync_provider);

  assert(g_conf->mon_sync_requester_kill_at != 4);
}

void Monitor::handle_sync_chunk(MonOpRequestRef op)
{
  MMonSync *m = static_cast<MMonSync*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;

  if (m->cookie != sync_cookie) {
    dout(10) << __func__ << " cookie does not match, discarding" << dendl;
    return;
  }
  if (m->get_source_inst() != sync_provider) {
    dout(10) << __func__ << " source does not match, discarding" << dendl;
    return;
  }

  assert(state == STATE_SYNCHRONIZING);
  assert(g_conf->mon_sync_requester_kill_at != 5);

  MonitorDBStore::TransactionRef tx(new MonitorDBStore::Transaction);
  tx->append_from_encoded(m->chunk_bl);

  dout(30) << __func__ << " tx dump:\n";
  JSONFormatter f(true);
  tx->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  store->apply_transaction(tx);

  assert(g_conf->mon_sync_requester_kill_at != 6);

  if (!sync_full) {
    dout(10) << __func__ << " applying recent paxos transactions as we go" << dendl;
    MonitorDBStore::TransactionRef tx(new MonitorDBStore::Transaction);
    paxos->read_and_prepare_transactions(tx, paxos->get_version() + 1,
					 m->last_committed);
    tx->put(paxos->get_name(), "last_committed", m->last_committed);

    dout(30) << __func__ << " tx dump:\n";
    JSONFormatter f(true);
    tx->dump(&f);
    f.flush(*_dout);
    *_dout << dendl;

    store->apply_transaction(tx);
    paxos->init();  // to refresh what we just wrote
  }

  if (m->op == MMonSync::OP_CHUNK) {
    sync_reset_timeout();
    sync_get_next_chunk();
  } else if (m->op == MMonSync::OP_LAST_CHUNK) {
    sync_finish(m->last_committed);
  }
}

void Monitor::handle_sync_no_cookie(MonOpRequestRef op)
{
  dout(10) << __func__ << dendl;
  bootstrap();
}

void Monitor::sync_trim_providers()
{
  dout(20) << __func__ << dendl;

  utime_t now = ceph_clock_now(g_ceph_context);
  map<uint64_t,SyncProvider>::iterator p = sync_providers.begin();
  while (p != sync_providers.end()) {
    if (now > p->second.timeout) {
      dout(10) << __func__ << " expiring cookie " << p->second.cookie << " for " << p->second.entity << dendl;
      sync_providers.erase(p++);
    } else {
      ++p;
    }
  }
}

// ---------------------------------------------------
// probe

void Monitor::cancel_probe_timeout()
{
  if (probe_timeout_event) {
    dout(10) << "cancel_probe_timeout " << probe_timeout_event << dendl;
    timer.cancel_event(probe_timeout_event);
    probe_timeout_event = NULL;
  } else {
    dout(10) << "cancel_probe_timeout (none scheduled)" << dendl;
  }
}

void Monitor::reset_probe_timeout()
{
  cancel_probe_timeout();
  probe_timeout_event = new C_ProbeTimeout(this);
  double t = g_conf->mon_probe_timeout;
  timer.add_event_after(t, probe_timeout_event);
  dout(10) << "reset_probe_timeout " << probe_timeout_event << " after " << t << " seconds" << dendl;
}

void Monitor::probe_timeout(int r)
{
  dout(4) << "probe_timeout " << probe_timeout_event << dendl;
  assert(is_probing() || is_synchronizing());
  assert(probe_timeout_event);
  probe_timeout_event = NULL;
  bootstrap();
}

void Monitor::handle_probe(MonOpRequestRef op)
{
  MMonProbe *m = static_cast<MMonProbe*>(op->get_req());
  dout(10) << "handle_probe " << *m << dendl;

  if (m->fsid != monmap->fsid) {
    dout(0) << "handle_probe ignoring fsid " << m->fsid << " != " << monmap->fsid << dendl;
    return;
  }

  switch (m->op) {
  case MMonProbe::OP_PROBE:
    handle_probe_probe(op);
    break;

  case MMonProbe::OP_REPLY:
    handle_probe_reply(op);
    break;

  case MMonProbe::OP_MISSING_FEATURES:
    derr << __func__ << " missing features, have " << CEPH_FEATURES_ALL
	 << ", required " << required_features
	 << ", missing " << (required_features & ~CEPH_FEATURES_ALL)
	 << dendl;
    break;
  }
}

/**
 * @todo fix this. This is going to cause trouble.
 */
void Monitor::handle_probe_probe(MonOpRequestRef op)
{
  MMonProbe *m = static_cast<MMonProbe*>(op->get_req());

  dout(10) << "handle_probe_probe " << m->get_source_inst() << *m
	   << " features " << m->get_connection()->get_features() << dendl;
  uint64_t missing = required_features & ~m->get_connection()->get_features();
  if (missing) {
    dout(1) << " peer " << m->get_source_addr() << " missing features "
	    << missing << dendl;
    if (m->get_connection()->has_feature(CEPH_FEATURE_OSD_PRIMARY_AFFINITY)) {
      MMonProbe *r = new MMonProbe(monmap->fsid, MMonProbe::OP_MISSING_FEATURES,
				   name, has_ever_joined);
      m->required_features = required_features;
      m->get_connection()->send_message(r);
    }
    goto out;
  }

  if (!is_probing() && !is_synchronizing()) {
    // If the probing mon is way ahead of us, we need to re-bootstrap.
    // Normally we capture this case when we initially bootstrap, but
    // it is possible we pass those checks (we overlap with
    // quorum-to-be) but fail to join a quorum before it moves past
    // us.  We need to be kicked back to bootstrap so we can
    // synchonize, not keep calling elections.
    if (paxos->get_version() + 1 < m->paxos_first_version) {
      dout(1) << " peer " << m->get_source_addr() << " has first_committed "
	      << "ahead of us, re-bootstrapping" << dendl;
      bootstrap();
      goto out;

    }
  }
  
  MMonProbe *r;
  r = new MMonProbe(monmap->fsid, MMonProbe::OP_REPLY, name, has_ever_joined);
  r->name = name;
  r->quorum = quorum;
  monmap->encode(r->monmap_bl, m->get_connection()->get_features());
  r->paxos_first_version = paxos->get_first_committed();
  r->paxos_last_version = paxos->get_version();
  m->get_connection()->send_message(r);

  // did we discover a peer here?
  if (!monmap->contains(m->get_source_addr())) {
    dout(1) << " adding peer " << m->get_source_addr()
	    << " to list of hints" << dendl;
    extra_probe_peers.insert(m->get_source_addr());
  }

 out:
  return;
}

void Monitor::handle_probe_reply(MonOpRequestRef op)
{
  MMonProbe *m = static_cast<MMonProbe*>(op->get_req());
  dout(10) << "handle_probe_reply " << m->get_source_inst() << *m << dendl;
  dout(10) << " monmap is " << *monmap << dendl;

  // discover name and addrs during probing or electing states.
  if (!is_probing() && !is_electing()) {
    return;
  }

  // newer map, or they've joined a quorum and we haven't?
  bufferlist mybl;
  monmap->encode(mybl, m->get_connection()->get_features());
  // make sure it's actually different; the checks below err toward
  // taking the other guy's map, which could cause us to loop.
  if (!mybl.contents_equal(m->monmap_bl)) {
    MonMap *newmap = new MonMap;
    newmap->decode(m->monmap_bl);
    if (m->has_ever_joined && (newmap->get_epoch() > monmap->get_epoch() ||
			       !has_ever_joined)) {
      dout(10) << " got newer/committed monmap epoch " << newmap->get_epoch()
	       << ", mine was " << monmap->get_epoch() << dendl;
      delete newmap;
      monmap->decode(m->monmap_bl);

      bootstrap();
      return;
    }
    delete newmap;
  }

  // rename peer?
  string peer_name = monmap->get_name(m->get_source_addr());
  if (monmap->get_epoch() == 0 && peer_name.compare(0, 7, "noname-") == 0) {
    dout(10) << " renaming peer " << m->get_source_addr() << " "
	     << peer_name << " -> " << m->name << " in my monmap"
	     << dendl;
    monmap->rename(peer_name, m->name);

    if (is_electing()) {
      bootstrap();
      return;
    }
  } else {
    dout(10) << " peer name is " << peer_name << dendl;
  }

  // new initial peer?
  if (monmap->get_epoch() == 0 &&
      monmap->contains(m->name) &&
      monmap->get_addr(m->name).is_blank_ip()) {
    dout(1) << " learned initial mon " << m->name << " addr " << m->get_source_addr() << dendl;
    monmap->set_addr(m->name, m->get_source_addr());

    bootstrap();
    return;
  }

  // end discover phase
  if (!is_probing()) {
    return;
  }

  assert(paxos != NULL);

  if (is_synchronizing()) {
    dout(10) << " currently syncing" << dendl;
    return;
  }

  entity_inst_t other = m->get_source_inst();

  if (m->paxos_last_version < sync_last_committed_floor) {
    dout(10) << " peer paxos versions [" << m->paxos_first_version
	     << "," << m->paxos_last_version << "] < my sync_last_committed_floor "
	     << sync_last_committed_floor << ", ignoring"
	     << dendl;
  } else {
    if (paxos->get_version() < m->paxos_first_version &&
	m->paxos_first_version > 1) {  // no need to sync if we're 0 and they start at 1.
      dout(10) << " peer paxos first versions [" << m->paxos_first_version
	       << "," << m->paxos_last_version << "]"
	       << " vs my version " << paxos->get_version()
	       << " (too far ahead)"
	       << dendl;
      cancel_probe_timeout();
      sync_start(other, true);
      return;
    }
    if (paxos->get_version() + g_conf->paxos_max_join_drift < m->paxos_last_version) {
      dout(10) << " peer paxos last version " << m->paxos_last_version
	       << " vs my version " << paxos->get_version()
	       << " (too far ahead)"
	       << dendl;
      cancel_probe_timeout();
      sync_start(other, false);
      return;
    }
  }

  // is there an existing quorum?
  if (m->quorum.size()) {
    dout(10) << " existing quorum " << m->quorum << dendl;

    dout(10) << " peer paxos version " << m->paxos_last_version
             << " vs my version " << paxos->get_version()
             << " (ok)"
             << dendl;

    if (monmap->contains(name) &&
        !monmap->get_addr(name).is_blank_ip()) {
      // i'm part of the cluster; just initiate a new election
      start_election();
    } else {
      dout(10) << " ready to join, but i'm not in the monmap or my addr is blank, trying to join" << dendl;
      messenger->send_message(new MMonJoin(monmap->fsid, name, messenger->get_myaddr()),
                              monmap->get_inst(*m->quorum.begin()));
    }
  } else {
    if (monmap->contains(m->name)) {
      dout(10) << " mon." << m->name << " is outside the quorum" << dendl;
      outside_quorum.insert(m->name);
    } else {
      dout(10) << " mostly ignoring mon." << m->name << ", not part of monmap" << dendl;
      return;
    }

    unsigned need = monmap->size() / 2 + 1;
    dout(10) << " outside_quorum now " << outside_quorum << ", need " << need << dendl;
    if (outside_quorum.size() >= need) {
      if (outside_quorum.count(name)) {
        dout(10) << " that's enough to form a new quorum, calling election" << dendl;
        start_election();
      } else {
        dout(10) << " that's enough to form a new quorum, but it does not include me; waiting" << dendl;
      }
    } else {
      dout(10) << " that's not yet enough for a new quorum, waiting" << dendl;
    }
  }
}

void Monitor::join_election()
{
  dout(10) << __func__ << dendl;
  wait_for_paxos_write();
  _reset();
  state = STATE_ELECTING;

  logger->inc(l_mon_num_elections);
}

void Monitor::start_election()
{
  dout(10) << "start_election" << dendl;
  wait_for_paxos_write();
  _reset();
  state = STATE_ELECTING;

  logger->inc(l_mon_num_elections);
  logger->inc(l_mon_election_call);

  cancel_probe_timeout();

  clog->info() << "mon." << name << " calling new monitor election\n";
  elector.call_election();
}

void Monitor::win_standalone_election()
{
  dout(1) << "win_standalone_election" << dendl;

  // bump election epoch, in case the previous epoch included other
  // monitors; we need to be able to make the distinction.
  elector.init();
  elector.advance_epoch();

  rank = monmap->get_rank(name);
  assert(rank == 0);
  set<int> q;
  q.insert(rank);

  const MonCommand *my_cmds;
  int cmdsize;
  get_locally_supported_monitor_commands(&my_cmds, &cmdsize);
  win_election(elector.get_epoch(), q, CEPH_FEATURES_ALL, my_cmds, cmdsize, NULL);
}

const utime_t& Monitor::get_leader_since() const
{
  assert(state == STATE_LEADER);
  return leader_since;
}

epoch_t Monitor::get_epoch()
{
  return elector.get_epoch();
}

void Monitor::win_election(epoch_t epoch, set<int>& active, uint64_t features,
                           const MonCommand *cmdset, int cmdsize,
                           const set<int> *classic_monitors)
{
  dout(10) << __func__ << " epoch " << epoch << " quorum " << active
	   << " features " << features << dendl;
  assert(is_electing());
  state = STATE_LEADER;
  leader_since = ceph_clock_now(g_ceph_context);
  leader = rank;
  quorum = active;
  quorum_features = features;
  outside_quorum.clear();

  clog->info() << "mon." << name << "@" << rank
		<< " won leader election with quorum " << quorum << "\n";

  set_leader_supported_commands(cmdset, cmdsize);
  if (classic_monitors)
    classic_mons = *classic_monitors;

  paxos->leader_init();
  // NOTE: tell monmap monitor first.  This is important for the
  // bootstrap case to ensure that the very first paxos proposal
  // codifies the monmap.  Otherwise any manner of chaos can ensue
  // when monitors are call elections or participating in a paxos
  // round without agreeing on who the participants are.
  monmon()->election_finished();
  for (vector<PaxosService*>::iterator p = paxos_service.begin();
       p != paxos_service.end(); ++p) {
    if (*p != monmon())
      (*p)->election_finished();
  }
  health_monitor->start(epoch);

  logger->inc(l_mon_election_win);

  finish_election();
  if (monmap->size() > 1 &&
      monmap->get_epoch() > 0) {
    timecheck_start();
    health_tick_start();
    do_health_to_clog_interval();
    scrub_event_start();
  }
  collect_sys_info(&metadata[rank], g_ceph_context);
}

void Monitor::lose_election(epoch_t epoch, set<int> &q, int l, uint64_t features) 
{
  state = STATE_PEON;
  leader_since = utime_t();
  leader = l;
  quorum = q;
  outside_quorum.clear();
  quorum_features = features;
  dout(10) << "lose_election, epoch " << epoch << " leader is mon" << leader
	   << " quorum is " << quorum << " features are " << quorum_features << dendl;

  paxos->peon_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); ++p)
    (*p)->election_finished();
  health_monitor->start(epoch);

  logger->inc(l_mon_election_lose);

  finish_election();

  if (quorum_features & CEPH_FEATURE_MON_METADATA) {
    Metadata sys_info;
    collect_sys_info(&sys_info, g_ceph_context);
    messenger->send_message(new MMonMetadata(sys_info),
			    monmap->get_inst(get_leader()));
  }
}

void Monitor::finish_election()
{
  apply_quorum_to_compatset_features();
  timecheck_finish();
  exited_quorum = utime_t();
  finish_contexts(g_ceph_context, waitfor_quorum);
  finish_contexts(g_ceph_context, maybe_wait_for_quorum);
  resend_routed_requests();
  update_logger();
  register_cluster_logger();

  // am i named properly?
  string cur_name = monmap->get_name(messenger->get_myaddr());
  if (cur_name != name) {
    dout(10) << " renaming myself from " << cur_name << " -> " << name << dendl;
    messenger->send_message(new MMonJoin(monmap->fsid, name, messenger->get_myaddr()),
			    monmap->get_inst(*quorum.begin()));
  }
}

void Monitor::apply_quorum_to_compatset_features()
{
  CompatSet new_features(features);
  if (quorum_features & CEPH_FEATURE_OSD_ERASURE_CODES) {
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSD_ERASURE_CODES);
  }
  if (quorum_features & CEPH_FEATURE_OSDMAP_ENC) {
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC);
  }
  if (quorum_features & CEPH_FEATURE_ERASURE_CODE_PLUGINS_V2) {
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V2);
  }
  if (quorum_features & CEPH_FEATURE_ERASURE_CODE_PLUGINS_V3) {
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V3);
  }
  if (new_features.compare(features) != 0) {
    CompatSet diff = features.unsupported(new_features);
    dout(1) << __func__ << " enabling new quorum features: " << diff << dendl;
    features = new_features;

    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
    write_features(t);
    store->apply_transaction(t);

    apply_compatset_features_to_quorum_requirements();
  }
}

void Monitor::apply_compatset_features_to_quorum_requirements()
{
  required_features = 0;
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_OSD_ERASURE_CODES)) {
    required_features |= CEPH_FEATURE_OSD_ERASURE_CODES;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC)) {
    required_features |= CEPH_FEATURE_OSDMAP_ENC;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V2)) {
    required_features |= CEPH_FEATURE_ERASURE_CODE_PLUGINS_V2;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V3)) {
    required_features |= CEPH_FEATURE_ERASURE_CODE_PLUGINS_V3;
  }
  dout(10) << __func__ << " required_features " << required_features << dendl;
}

void Monitor::sync_force(Formatter *f, ostream& ss)
{
  bool free_formatter = false;

  if (!f) {
    // louzy/lazy hack: default to json if no formatter has been defined
    f = new JSONFormatter();
    free_formatter = true;
  }

  MonitorDBStore::TransactionRef tx(new MonitorDBStore::Transaction);
  sync_stash_critical_state(tx);
  tx->put("mon_sync", "force_sync", 1);
  store->apply_transaction(tx);

  f->open_object_section("sync_force");
  f->dump_int("ret", 0);
  f->dump_stream("msg") << "forcing store sync the next time the monitor starts";
  f->close_section(); // sync_force
  f->flush(ss);
  if (free_formatter)
    delete f;
}

void Monitor::_quorum_status(Formatter *f, ostream& ss)
{
  bool free_formatter = false;

  if (!f) {
    // louzy/lazy hack: default to json if no formatter has been defined
    f = new JSONFormatter();
    free_formatter = true;
  }
  f->open_object_section("quorum_status");
  f->dump_int("election_epoch", get_epoch());

  f->open_array_section("quorum");
  for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
    f->dump_int("mon", *p);
  f->close_section(); // quorum

  list<string> quorum_names = get_quorum_names();
  f->open_array_section("quorum_names");
  for (list<string>::iterator p = quorum_names.begin(); p != quorum_names.end(); ++p)
    f->dump_string("mon", *p);
  f->close_section(); // quorum_names

  f->dump_string("quorum_leader_name", quorum.empty() ? string() : monmap->get_name(*quorum.begin()));

  f->open_object_section("monmap");
  monmap->dump(f);
  f->close_section(); // monmap

  f->close_section(); // quorum_status
  f->flush(ss);
  if (free_formatter)
    delete f;
}

void Monitor::get_mon_status(Formatter *f, ostream& ss)
{
  bool free_formatter = false;

  if (!f) {
    // louzy/lazy hack: default to json if no formatter has been defined
    f = new JSONFormatter();
    free_formatter = true;
  }

  f->open_object_section("mon_status");
  f->dump_string("name", name);
  f->dump_int("rank", rank);
  f->dump_string("state", get_state_name());
  f->dump_int("election_epoch", get_epoch());

  f->open_array_section("quorum");
  for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p) {
    f->dump_int("mon", *p);
  }

  f->close_section(); // quorum

  f->open_array_section("outside_quorum");
  for (set<string>::iterator p = outside_quorum.begin(); p != outside_quorum.end(); ++p)
    f->dump_string("mon", *p);
  f->close_section(); // outside_quorum

  f->open_array_section("extra_probe_peers");
  for (set<entity_addr_t>::iterator p = extra_probe_peers.begin();
       p != extra_probe_peers.end();
       ++p)
    f->dump_stream("peer") << *p;
  f->close_section(); // extra_probe_peers

  f->open_array_section("sync_provider");
  for (map<uint64_t,SyncProvider>::const_iterator p = sync_providers.begin();
       p != sync_providers.end();
       ++p) {
    f->dump_unsigned("cookie", p->second.cookie);
    f->dump_stream("entity") << p->second.entity;
    f->dump_stream("timeout") << p->second.timeout;
    f->dump_unsigned("last_committed", p->second.last_committed);
    f->dump_stream("last_key") << p->second.last_key;
  }
  f->close_section();

  if (is_synchronizing()) {
    f->open_object_section("sync");
    f->dump_stream("sync_provider") << sync_provider;
    f->dump_unsigned("sync_cookie", sync_cookie);
    f->dump_unsigned("sync_start_version", sync_start_version);
    f->close_section();
  }

  if (g_conf->mon_sync_provider_kill_at > 0)
    f->dump_int("provider_kill_at", g_conf->mon_sync_provider_kill_at);
  if (g_conf->mon_sync_requester_kill_at > 0)
    f->dump_int("requester_kill_at", g_conf->mon_sync_requester_kill_at);

  f->open_object_section("monmap");
  monmap->dump(f);
  f->close_section();

  f->close_section(); // mon_status

  if (free_formatter) {
    // flush formatter to ss and delete it iff we created the formatter
    f->flush(ss);
    delete f;
  }
}


// health status to clog

void Monitor::health_tick_start()
{
  if (!cct->_conf->mon_health_to_clog ||
      cct->_conf->mon_health_to_clog_tick_interval <= 0)
    return;

  dout(15) << __func__ << dendl;

  health_tick_stop();
  health_tick_event = new C_HealthToClogTick(this);

  timer.add_event_after(cct->_conf->mon_health_to_clog_tick_interval,
                        health_tick_event);
}

void Monitor::health_tick_stop()
{
  dout(15) << __func__ << dendl;

  if (health_tick_event) {
    timer.cancel_event(health_tick_event);
    health_tick_event = NULL;
  }
}

utime_t Monitor::health_interval_calc_next_update()
{
  utime_t now = ceph_clock_now(cct);

  time_t secs = now.sec();
  int remainder = secs % cct->_conf->mon_health_to_clog_interval;
  int adjustment = cct->_conf->mon_health_to_clog_interval - remainder;
  utime_t next = utime_t(secs + adjustment, 0);

  dout(20) << __func__
    << " now: " << now << ","
    << " next: " << next << ","
    << " interval: " << cct->_conf->mon_health_to_clog_interval
    << dendl;

  return next;
}

void Monitor::health_interval_start()
{
  dout(15) << __func__ << dendl;

  if (!cct->_conf->mon_health_to_clog ||
      cct->_conf->mon_health_to_clog_interval <= 0) {
    return;
  }

  health_interval_stop();
  utime_t next = health_interval_calc_next_update();
  health_interval_event = new C_HealthToClogInterval(this);
  timer.add_event_at(next, health_interval_event);
}

void Monitor::health_interval_stop()
{
  dout(15) << __func__ << dendl;
  if (health_interval_event) {
    timer.cancel_event(health_interval_event);
  }
  health_interval_event = NULL;
}

void Monitor::health_events_cleanup()
{
  health_tick_stop();
  health_interval_stop();
  health_status_cache.reset();
}

void Monitor::health_to_clog_update_conf(const std::set<std::string> &changed)
{
  dout(20) << __func__ << dendl;

  if (changed.count("mon_health_to_clog")) {
    if (!cct->_conf->mon_health_to_clog) {
      health_events_cleanup();
    } else {
      if (!health_tick_event) {
        health_tick_start();
      }
      if (!health_interval_event) {
        health_interval_start();
      }
    }
  }

  if (changed.count("mon_health_to_clog_interval")) {
    if (cct->_conf->mon_health_to_clog_interval <= 0) {
      health_interval_stop();
    } else {
      health_interval_start();
    }
  }

  if (changed.count("mon_health_to_clog_tick_interval")) {
    if (cct->_conf->mon_health_to_clog_tick_interval <= 0) {
      health_tick_stop();
    } else {
      health_tick_start();
    }
  }
}

void Monitor::do_health_to_clog_interval()
{
  // outputting to clog may have been disabled in the conf
  // since we were scheduled.
  if (!cct->_conf->mon_health_to_clog ||
      cct->_conf->mon_health_to_clog_interval <= 0)
    return;

  dout(10) << __func__ << dendl;

  // do we have a cached value for next_clog_update?  if not,
  // do we know when the last update was?

  do_health_to_clog(true);
  health_interval_start();
}

void Monitor::do_health_to_clog(bool force)
{
  // outputting to clog may have been disabled in the conf
  // since we were scheduled.
  if (!cct->_conf->mon_health_to_clog ||
      cct->_conf->mon_health_to_clog_interval <= 0)
    return;

  dout(10) << __func__ << (force ? " (force)" : "") << dendl;

  list<string> status;
  health_status_t overall = get_health(status, NULL, NULL);

  dout(25) << __func__
           << (force ? " (force)" : "")
           << dendl;

  string summary = joinify(status.begin(), status.end(), string("; "));

  if (!force &&
      overall == health_status_cache.overall &&
      !health_status_cache.summary.empty() &&
      health_status_cache.summary == summary) {
    // we got a dup!
    return;
  }

  clog->info() << summary;

  health_status_cache.overall = overall;
  health_status_cache.summary = summary;
}

health_status_t Monitor::get_health(list<string>& status,
                                    bufferlist *detailbl,
                                    Formatter *f)
{
  list<pair<health_status_t,string> > summary;
  list<pair<health_status_t,string> > detail;

  if (f)
    f->open_object_section("health");

  for (vector<PaxosService*>::iterator p = paxos_service.begin();
       p != paxos_service.end();
       ++p) {
    PaxosService *s = *p;
    s->get_health(summary, detailbl ? &detail : NULL, cct);
  }

  health_monitor->get_health(f, summary, (detailbl ? &detail : NULL));

  if (f) {
    f->open_object_section("timechecks");
    f->dump_unsigned("epoch", get_epoch());
    f->dump_int("round", timecheck_round);
    f->dump_stream("round_status")
      << ((timecheck_round%2) ? "on-going" : "finished");
  }

  health_status_t overall = HEALTH_OK;
  if (!timecheck_skews.empty()) {
    list<string> warns;
    if (f)
      f->open_array_section("mons");
    for (map<entity_inst_t,double>::iterator i = timecheck_skews.begin();
         i != timecheck_skews.end(); ++i) {
      entity_inst_t inst = i->first;
      double skew = i->second;
      double latency = timecheck_latencies[inst];
      string name = monmap->get_name(inst.addr);

      ostringstream tcss;
      health_status_t tcstatus = timecheck_status(tcss, skew, latency);
      if (tcstatus != HEALTH_OK) {
        if (overall > tcstatus)
          overall = tcstatus;
        warns.push_back(name);

        ostringstream tmp_ss;
        tmp_ss << "mon." << name
               << " addr " << inst.addr << " " << tcss.str()
	       << " (latency " << latency << "s)";
        detail.push_back(make_pair(tcstatus, tmp_ss.str()));
      }

      if (f) {
        f->open_object_section("mon");
        f->dump_string("name", name.c_str());
        f->dump_float("skew", skew);
        f->dump_float("latency", latency);
        f->dump_stream("health") << tcstatus;
        if (tcstatus != HEALTH_OK)
          f->dump_stream("details") << tcss.str();
        f->close_section();
      }
    }
    if (!warns.empty()) {
      ostringstream ss;
      ss << "clock skew detected on";
      while (!warns.empty()) {
        ss << " mon." << warns.front();
        warns.pop_front();
        if (!warns.empty())
          ss << ",";
      }
      status.push_back(ss.str());
      summary.push_back(make_pair(HEALTH_WARN, "Monitor clock skew detected "));
    }
    if (f)
      f->close_section();
  }
  if (f)
    f->close_section();

  if (f)
    f->open_array_section("summary");
  if (!summary.empty()) {
    while (!summary.empty()) {
      if (overall > summary.front().first)
	overall = summary.front().first;
      status.push_back(summary.front().second);
      if (f) {
        f->open_object_section("item");
        f->dump_stream("severity") <<  summary.front().first;
        f->dump_string("summary", summary.front().second);
        f->close_section();
      }
      summary.pop_front();
    }
  }
  if (f)
    f->close_section();

  stringstream fss;
  fss << overall;
  status.push_front(fss.str());
  if (f)
    f->dump_stream("overall_status") << overall;

  if (f)
    f->open_array_section("detail");
  while (!detail.empty()) {
    if (f)
      f->dump_string("item", detail.front().second);
    else if (detailbl != NULL) {
      detailbl->append(detail.front().second);
      detailbl->append('\n');
    }
    detail.pop_front();
  }
  if (f)
    f->close_section();

  if (f)
    f->close_section();

  return overall;
}

void Monitor::get_cluster_status(stringstream &ss, Formatter *f)
{
  if (f)
    f->open_object_section("status");

  // reply with the status for all the components
  list<string> health;
  get_health(health, NULL, f);

  if (f) {
    f->dump_stream("fsid") << monmap->get_fsid();
    f->dump_unsigned("election_epoch", get_epoch());
    {
      f->open_array_section("quorum");
      for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
	f->dump_int("rank", *p);
      f->close_section();
      f->open_array_section("quorum_names");
      for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
	f->dump_string("id", monmap->get_name(*p));
      f->close_section();
    }
    f->open_object_section("monmap");
    monmap->dump(f);
    f->close_section();
    f->open_object_section("osdmap");
    osdmon()->osdmap.print_summary(f, cout);
    f->close_section();
    f->open_object_section("pgmap");
    pgmon()->pg_map.print_summary(f, NULL);
    f->close_section();
    f->open_object_section("fsmap");
    mdsmon()->fsmap.print_summary(f, NULL);
    f->close_section();
    f->close_section();
  } else {
    ss << "    cluster " << monmap->get_fsid() << "\n";
    ss << "     health " << joinify(health.begin(), health.end(), 
				    string("\n            ")) << "\n";
    ss << "     monmap " << *monmap << "\n";
    ss << "            election epoch " << get_epoch()
       << ", quorum " << get_quorum() << " " << get_quorum_names() << "\n";
    if (mdsmon()->fsmap.any_filesystems()) {
      ss << "     mdsmap " << mdsmon()->fsmap << "\n";
    }

    osdmon()->osdmap.print_summary(NULL, ss);
    pgmon()->pg_map.print_summary(NULL, &ss);
  }
}

void Monitor::_generate_command_map(map<string,cmd_vartype>& cmdmap,
                                    map<string,string> &param_str_map)
{
  for (map<string,cmd_vartype>::const_iterator p = cmdmap.begin();
       p != cmdmap.end(); ++p) {
    if (p->first == "prefix")
      continue;
    if (p->first == "caps") {
      vector<string> cv;
      if (cmd_getval(g_ceph_context, cmdmap, "caps", cv) &&
	  cv.size() % 2 == 0) {
	for (unsigned i = 0; i < cv.size(); i += 2) {
	  string k = string("caps_") + cv[i];
	  param_str_map[k] = cv[i + 1];
	}
	continue;
      }
    }
    param_str_map[p->first] = cmd_vartype_stringify(p->second);
  }
}

const MonCommand *Monitor::_get_moncommand(const string &cmd_prefix,
                                           MonCommand *cmds, int cmds_size)
{
  MonCommand *this_cmd = NULL;
  for (MonCommand *cp = cmds;
       cp < &cmds[cmds_size]; cp++) {
    if (cp->cmdstring.compare(0, cmd_prefix.size(), cmd_prefix) == 0) {
      this_cmd = cp;
      break;
    }
  }
  return this_cmd;
}

bool Monitor::_allowed_command(MonSession *s, string &module, string &prefix,
                               const map<string,cmd_vartype>& cmdmap,
                               const map<string,string>& param_str_map,
                               const MonCommand *this_cmd) {

  bool cmd_r = this_cmd->requires_perm('r');
  bool cmd_w = this_cmd->requires_perm('w');
  bool cmd_x = this_cmd->requires_perm('x');

  bool capable = s->caps.is_capable(g_ceph_context, s->entity_name,
                                    module, prefix, param_str_map,
                                    cmd_r, cmd_w, cmd_x);

  dout(10) << __func__ << " " << (capable ? "" : "not ") << "capable" << dendl;
  return capable;
}

void Monitor::format_command_descriptions(const MonCommand *commands,
					  unsigned commands_size,
					  Formatter *f,
					  bufferlist *rdata)
{
  int cmdnum = 0;
  f->open_object_section("command_descriptions");
  for (const MonCommand *cp = commands;
       cp < &commands[commands_size]; cp++) {

    ostringstream secname;
    secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
    dump_cmddesc_to_json(f, secname.str(),
			 cp->cmdstring, cp->helpstring, cp->module,
			 cp->req_perms, cp->availability);
    cmdnum++;
  }
  f->close_section();	// command_descriptions

  f->flush(*rdata);
}

void Monitor::get_locally_supported_monitor_commands(const MonCommand **cmds,
						     int *count)
{
  *cmds = mon_commands;
  *count = ARRAY_SIZE(mon_commands);
}
void Monitor::get_classic_monitor_commands(const MonCommand **cmds, int *count)
{
  *cmds = classic_mon_commands;
  *count = ARRAY_SIZE(classic_mon_commands);
}
void Monitor::set_leader_supported_commands(const MonCommand *cmds, int size)
{
  if (leader_supported_mon_commands != mon_commands &&
      leader_supported_mon_commands != classic_mon_commands)
    delete[] leader_supported_mon_commands;
  leader_supported_mon_commands = cmds;
  leader_supported_mon_commands_size = size;
}

bool Monitor::is_keyring_required()
{
  string auth_cluster_required = g_conf->auth_supported.empty() ?
    g_conf->auth_cluster_required : g_conf->auth_supported;
  string auth_service_required = g_conf->auth_supported.empty() ?
    g_conf->auth_service_required : g_conf->auth_supported;

  return auth_service_required == "cephx" ||
    auth_cluster_required == "cephx";
}

void Monitor::handle_command(MonOpRequestRef op)
{
  assert(op->is_type_command());
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  if (m->fsid != monmap->fsid) {
    dout(0) << "handle_command on fsid " << m->fsid << " != " << monmap->fsid << dendl;
    reply_command(op, -EPERM, "wrong fsid", 0);
    return;
  }

  MonSession *session = static_cast<MonSession *>(
    m->get_connection()->get_priv());
  if (!session) {
    dout(5) << __func__ << " dropping stray message " << *m << dendl;
    return;
  }
  BOOST_SCOPE_EXIT_ALL(=) {
    session->put();
  };

  if (m->cmd.empty()) {
    string rs = "No command supplied";
    reply_command(op, -EINVAL, rs, 0);
    return;
  }

  string prefix;
  vector<string> fullcmd;
  map<string, cmd_vartype> cmdmap;
  stringstream ss, ds;
  bufferlist rdata;
  string rs;
  int r = -EINVAL;
  rs = "unrecognized command";

  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    r = -EINVAL;
    rs = ss.str();
    if (!m->get_source().is_mon())  // don't reply to mon->mon commands
      reply_command(op, r, rs, 0);
    return;
  }

  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
  if (prefix == "get_command_descriptions") {
    bufferlist rdata;
    Formatter *f = Formatter::create("json");
    format_command_descriptions(leader_supported_mon_commands,
				leader_supported_mon_commands_size, f, &rdata);
    delete f;
    reply_command(op, 0, "", rdata, 0);
    return;
  }

  string module;
  string err;

  dout(0) << "handle_command " << *m << dendl;

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  get_str_vec(prefix, fullcmd);
  module = fullcmd[0];

  // validate command is in leader map

  const MonCommand *leader_cmd;
  leader_cmd = _get_moncommand(prefix,
                               // the boost underlying this isn't const for some reason
                               const_cast<MonCommand*>(leader_supported_mon_commands),
                               leader_supported_mon_commands_size);
  if (!leader_cmd) {
    reply_command(op, -EINVAL, "command not known", 0);
    return;
  }
  // validate command is in our map & matches, or forward if it is allowed
  const MonCommand *mon_cmd = _get_moncommand(prefix, mon_commands,
                                              ARRAY_SIZE(mon_commands));
  if (!is_leader()) {
    if (!mon_cmd) {
      if (leader_cmd->is_noforward()) {
	reply_command(op, -EINVAL,
		      "command not locally supported and not allowed to forward",
		      0);
	return;
      }
      dout(10) << "Command not locally supported, forwarding request "
	       << m << dendl;
      forward_request_leader(op);
      return;
    } else if (!mon_cmd->is_compat(leader_cmd)) {
      if (mon_cmd->is_noforward()) {
	reply_command(op, -EINVAL,
		      "command not compatible with leader and not allowed to forward",
		      0);
	return;
      }
      dout(10) << "Command not compatible with leader, forwarding request "
	       << m << dendl;
      forward_request_leader(op);
      return;
    }
  }

  if (mon_cmd->is_obsolete() ||
      (cct->_conf->mon_debug_deprecated_as_obsolete
       && mon_cmd->is_deprecated())) {
    reply_command(op, -ENOTSUP,
                  "command is obsolete; please check usage and/or man page",
                  0);
    return;
  }

  if (session->proxy_con && mon_cmd->is_noforward()) {
    dout(10) << "Got forward for noforward command " << m << dendl;
    reply_command(op, -EINVAL, "forward for noforward command", rdata, 0);
    return;
  }

  /* what we perceive as being the service the command falls under */
  string service(mon_cmd->module);

  dout(25) << __func__ << " prefix='" << prefix
           << "' module='" << module
           << "' service='" << service << "'" << dendl;

  bool cmd_is_rw =
    (mon_cmd->requires_perm('w') || mon_cmd->requires_perm('x'));

  // validate user's permissions for requested command
  map<string,string> param_str_map;
  _generate_command_map(cmdmap, param_str_map);
  if (!_allowed_command(session, service, prefix, cmdmap,
                        param_str_map, mon_cmd)) {
    dout(1) << __func__ << " access denied" << dendl;
    (cmd_is_rw ? audit_clog->info() : audit_clog->debug())
      << "from='" << session->inst << "' "
      << "entity='" << session->entity_name << "' "
      << "cmd=" << m->cmd << ":  access denied";
    reply_command(op, -EACCES, "access denied", 0);
    return;
  }

  (cmd_is_rw ? audit_clog->info() : audit_clog->debug())
    << "from='" << session->inst << "' "
    << "entity='" << session->entity_name << "' "
    << "cmd=" << m->cmd << ": dispatch";

  if (module == "mds" || module == "fs") {
    mdsmon()->dispatch(op);
    return;
  }
  if (module == "osd") {
    osdmon()->dispatch(op);
    return;
  }

  if (module == "pg") {
    pgmon()->dispatch(op);
    return;
  }
  if (module == "mon" &&
      /* Let the Monitor class handle the following commands:
       *  'mon compact'
       *  'mon scrub'
       *  'mon sync force'
       */
      prefix != "mon compact" &&
      prefix != "mon scrub" &&
      prefix != "mon sync force" &&
      prefix != "mon metadata") {
    monmon()->dispatch(op);
    return;
  }
  if (module == "auth") {
    authmon()->dispatch(op);
    return;
  }
  if (module == "log") {
    logmon()->dispatch(op);
    return;
  }

  if (module == "config-key") {
    config_key_service->dispatch(op);
    return;
  }

  if (prefix == "fsid") {
    if (f) {
      f->open_object_section("fsid");
      f->dump_stream("fsid") << monmap->fsid;
      f->close_section();
      f->flush(rdata);
    } else {
      ds << monmap->fsid;
      rdata.append(ds);
    }
    reply_command(op, 0, "", rdata, 0);
    return;
  }

  if (prefix == "scrub" || prefix == "mon scrub") {
    wait_for_paxos_write();
    if (is_leader()) {
      int r = scrub_start();
      reply_command(op, r, "", rdata, 0);
    } else if (is_peon()) {
      forward_request_leader(op);
    } else {
      reply_command(op, -EAGAIN, "no quorum", rdata, 0);
    }
    return;
  }

  if (prefix == "compact" || prefix == "mon compact") {
    dout(1) << "triggering manual compaction" << dendl;
    utime_t start = ceph_clock_now(g_ceph_context);
    store->compact();
    utime_t end = ceph_clock_now(g_ceph_context);
    end -= start;
    dout(1) << "finished manual compaction in " << end << " seconds" << dendl;
    ostringstream oss;
    oss << "compacted leveldb in " << end;
    rs = oss.str();
    r = 0;
  }
  else if (prefix == "injectargs") {
    vector<string> injected_args;
    cmd_getval(g_ceph_context, cmdmap, "injected_args", injected_args);
    if (!injected_args.empty()) {
      dout(0) << "parsing injected options '" << injected_args << "'" << dendl;
      ostringstream oss;
      r = g_conf->injectargs(str_join(injected_args, " "), &oss);
      ss << "injectargs:"  << oss.str();
      rs = ss.str();
      goto out;
    } else {
      rs = "must supply options to be parsed in a single string";
      r = -EINVAL;
    }
  } else if (prefix == "status" ||
	     prefix == "health" ||
	     prefix == "df") {
    string detail;
    cmd_getval(g_ceph_context, cmdmap, "detail", detail);

    if (prefix == "status") {
      // get_cluster_status handles f == NULL
      get_cluster_status(ds, f.get());

      if (f) {
        f->flush(ds);
        ds << '\n';
      }
      rdata.append(ds);
    } else if (prefix == "health") {
      list<string> health_str;
      get_health(health_str, detail == "detail" ? &rdata : NULL, f.get());
      if (f) {
        f->flush(ds);
        ds << '\n';
      } else {
	assert(!health_str.empty());
	ds << health_str.front();
	health_str.pop_front();
	if (!health_str.empty()) {
	  ds << ' ';
	  ds << joinify(health_str.begin(), health_str.end(), string("; "));
	}
      }
      bufferlist comb;
      comb.append(ds);
      if (detail == "detail")
	comb.append(rdata);
      rdata = comb;
      r = 0;
    } else if (prefix == "df") {
      bool verbose = (detail == "detail");
      if (f)
        f->open_object_section("stats");

      pgmon()->dump_fs_stats(ds, f.get(), verbose);
      if (!f)
        ds << '\n';
      pgmon()->dump_pool_stats(ds, f.get(), verbose);

      if (f) {
        f->close_section();
        f->flush(ds);
        ds << '\n';
      }
    } else {
      assert(0 == "We should never get here!");
      return;
    }
    rdata.append(ds);
    rs = "";
    r = 0;
  } else if (prefix == "report") {

    // this must be formatted, in its current form
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    f->open_object_section("report");
    f->dump_stream("cluster_fingerprint") << fingerprint;
    f->dump_string("version", ceph_version_to_str());
    f->dump_string("commit", git_version_to_str());
    f->dump_stream("timestamp") << ceph_clock_now(NULL);

    vector<string> tagsvec;
    cmd_getval(g_ceph_context, cmdmap, "tags", tagsvec);
    string tagstr = str_join(tagsvec, " ");
    if (!tagstr.empty())
      tagstr = tagstr.substr(0, tagstr.find_last_of(' '));
    f->dump_string("tag", tagstr);

    list<string> hs;
    get_health(hs, NULL, f.get());

    monmon()->dump_info(f.get());
    osdmon()->dump_info(f.get());
    mdsmon()->dump_info(f.get());
    pgmon()->dump_info(f.get());
    authmon()->dump_info(f.get());

    paxos->dump_info(f.get());

    f->close_section();
    f->flush(rdata);

    ostringstream ss2;
    ss2 << "report " << rdata.crc32c(6789);
    rs = ss2.str();
    r = 0;
  } else if (prefix == "node ls") {
    string node_type("all");
    cmd_getval(g_ceph_context, cmdmap, "type", node_type);
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    if (node_type == "all") {
      f->open_object_section("nodes");
      print_nodes(f.get(), ds);
      osdmon()->print_nodes(f.get());
      mdsmon()->print_nodes(f.get());
      f->close_section();
    } else if (node_type == "mon") {
      print_nodes(f.get(), ds);
    } else if (node_type == "osd") {
      osdmon()->print_nodes(f.get());
    } else if (node_type == "mds") {
      mdsmon()->print_nodes(f.get());
    }
    f->flush(ds);
    rdata.append(ds);
    rs = "";
    r = 0;
  } else if (prefix == "mon metadata") {
    string name;
    cmd_getval(g_ceph_context, cmdmap, "id", name);
    int mon = monmap->get_rank(name);
    if (mon < 0) {
      rs = "requested mon not found";
      r = -ENOENT;
      goto out;
    }
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    f->open_object_section("mon_metadata");
    r = get_mon_metadata(mon, f.get(), ds);
    f->close_section();
    f->flush(ds);
    rdata.append(ds);
    rs = "";
  } else if (prefix == "quorum_status") {
    // make sure our map is readable and up to date
    if (!is_leader() && !is_peon()) {
      dout(10) << " waiting for quorum" << dendl;
      waitfor_quorum.push_back(new C_RetryMessage(this, op));
      return;
    }
    _quorum_status(f.get(), ds);
    rdata.append(ds);
    rs = "";
    r = 0;
  } else if (prefix == "mon_status") {
    get_mon_status(f.get(), ds);
    if (f)
      f->flush(ds);
    rdata.append(ds);
    rs = "";
    r = 0;
  } else if (prefix == "sync force" ||
             prefix == "mon sync force") {
    string validate1, validate2;
    cmd_getval(g_ceph_context, cmdmap, "validate1", validate1);
    cmd_getval(g_ceph_context, cmdmap, "validate2", validate2);
    if (validate1 != "--yes-i-really-mean-it" ||
	validate2 != "--i-know-what-i-am-doing") {
      r = -EINVAL;
      rs = "are you SURE? this will mean the monitor store will be "
	   "erased.  pass '--yes-i-really-mean-it "
	   "--i-know-what-i-am-doing' if you really do.";
      goto out;
    }
    sync_force(f.get(), ds);
    rs = ds.str();
    r = 0;
  } else if (prefix == "heap") {
    if (!ceph_using_tcmalloc())
      rs = "tcmalloc not enabled, can't use heap profiler commands\n";
    else {
      string heapcmd;
      cmd_getval(g_ceph_context, cmdmap, "heapcmd", heapcmd);
      // XXX 1-element vector, change at callee or make vector here?
      vector<string> heapcmd_vec;
      get_str_vec(heapcmd, heapcmd_vec);
      ceph_heap_profiler_handle_command(heapcmd_vec, ds);
      rdata.append(ds);
      rs = "";
      r = 0;
    }
  } else if (prefix == "quorum") {
    string quorumcmd;
    cmd_getval(g_ceph_context, cmdmap, "quorumcmd", quorumcmd);
    if (quorumcmd == "exit") {
      start_election();
      elector.stop_participating();
      rs = "stopped responding to quorum, initiated new election";
      r = 0;
    } else if (quorumcmd == "enter") {
      elector.start_participating();
      start_election();
      rs = "started responding to quorum, initiated new election";
      r = 0;
    } else {
      rs = "needs a valid 'quorum' command";
      r = -EINVAL;
    }
  } else if (prefix == "version") {
    if (f) {
      f->open_object_section("version");
      f->dump_string("version", pretty_version_to_str());
      f->close_section();
      f->flush(ds);
    } else {
      ds << pretty_version_to_str();
    }
    rdata.append(ds);
    rs = "";
    r = 0;
  }

 out:
  if (!m->get_source().is_mon())  // don't reply to mon->mon commands
    reply_command(op, r, rs, rdata, 0);
}

void Monitor::reply_command(MonOpRequestRef op, int rc, const string &rs, version_t version)
{
  bufferlist rdata;
  reply_command(op, rc, rs, rdata, version);
}

void Monitor::reply_command(MonOpRequestRef op, int rc, const string &rs,
                            bufferlist& rdata, version_t version)
{
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  assert(m->get_type() == MSG_MON_COMMAND);
  MMonCommandAck *reply = new MMonCommandAck(m->cmd, rc, rs, version);
  reply->set_tid(m->get_tid());
  reply->set_data(rdata);
  send_reply(op, reply);
}


// ------------------------
// request/reply routing
//
// a client/mds/osd will connect to a random monitor.  we need to forward any
// messages requiring state updates to the leader, and then route any replies
// back via the correct monitor and back to them.  (the monitor will not
// initiate any connections.)

void Monitor::forward_request_leader(MonOpRequestRef op)
{
  op->mark_event(__func__);

  int mon = get_leader();
  MonSession *session = op->get_session();
  PaxosServiceMessage *req = op->get_req<PaxosServiceMessage>();
  
  if (req->get_source().is_mon() && req->get_source_addr() != messenger->get_myaddr()) {
    dout(10) << "forward_request won't forward (non-local) mon request " << *req << dendl;
  } else if (session->proxy_con) {
    dout(10) << "forward_request won't double fwd request " << *req << dendl;
  } else if (!session->closed) {
    RoutedRequest *rr = new RoutedRequest;
    rr->tid = ++routed_request_tid;
    rr->client_inst = req->get_source_inst();
    rr->con = req->get_connection();
    rr->con_features = rr->con->get_features();
    encode_message(req, CEPH_FEATURES_ALL, rr->request_bl);   // for my use only; use all features
    rr->session = static_cast<MonSession *>(session->get());
    rr->op = op;
    routed_requests[rr->tid] = rr;
    session->routed_request_tids.insert(rr->tid);
    
    dout(10) << "forward_request " << rr->tid << " request " << *req
	     << " features " << rr->con_features << dendl;

    MForward *forward = new MForward(rr->tid,
                                     req,
				     rr->con_features,
				     rr->session->caps);
    forward->set_priority(req->get_priority());
    if (session->auth_handler) {
      forward->entity_name = session->entity_name;
    } else if (req->get_source().is_mon()) {
      forward->entity_name.set_type(CEPH_ENTITY_TYPE_MON);
    }
    messenger->send_message(forward, monmap->get_inst(mon));
    op->mark_forwarded();
    assert(op->get_req()->get_type() != 0);
  } else {
    dout(10) << "forward_request no session for request " << *req << dendl;
  }
}

// fake connection attached to forwarded messages
struct AnonConnection : public Connection {
  explicit AnonConnection(CephContext *cct) : Connection(cct, NULL) {}

  int send_message(Message *m) override {
    assert(!"send_message on anonymous connection");
  }
  void send_keepalive() override {
    assert(!"send_keepalive on anonymous connection");
  }
  void mark_down() override {
    // silently ignore
  }
  void mark_disposable() override {
    // silengtly ignore
  }
  bool is_connected() override { return false; }
};

//extract the original message and put it into the regular dispatch function
void Monitor::handle_forward(MonOpRequestRef op)
{
  MForward *m = static_cast<MForward*>(op->get_req());
  dout(10) << "received forwarded message from " << m->client
	   << " via " << m->get_source_inst() << dendl;
  MonSession *session = op->get_session();
  assert(session);

  if (!session->is_capable("mon", MON_CAP_X)) {
    dout(0) << "forward from entity with insufficient caps! " 
	    << session->caps << dendl;
  } else {
    // see PaxosService::dispatch(); we rely on this being anon
    // (c->msgr == NULL)
    PaxosServiceMessage *req = m->claim_message();
    assert(req != NULL);

    ConnectionRef c(new AnonConnection(cct));
    MonSession *s = new MonSession(req->get_source_inst(),
				   static_cast<Connection*>(c.get()));
    c->set_priv(s->get());
    c->set_peer_addr(m->client.addr);
    c->set_peer_type(m->client.name.type());
    c->set_features(m->con_features);

    s->caps = m->client_caps;
    dout(10) << " caps are " << s->caps << dendl;
    s->entity_name = m->entity_name;
    dout(10) << " entity name '" << s->entity_name << "' type "
             << s->entity_name.get_type() << dendl;
    s->proxy_con = m->get_connection();
    s->proxy_tid = m->tid;

    req->set_connection(c);

    // not super accurate, but better than nothing.
    req->set_recv_stamp(m->get_recv_stamp());

    /*
     * note which election epoch this is; we will drop the message if
     * there is a future election since our peers will resend routed
     * requests in that case.
     */
    req->rx_election_epoch = get_epoch();

    /* Because this is a special fake connection, we need to break
       the ref loop between Connection and MonSession differently
       than we normally do. Here, the Message refers to the Connection
       which refers to the Session, and nobody else refers to the Connection
       or the Session. And due to the special nature of this message,
       nobody refers to the Connection via the Session. So, clear out that
       half of the ref loop.*/
    s->con.reset(NULL);

    dout(10) << " mesg " << req << " from " << m->get_source_addr() << dendl;

    _ms_dispatch(req);
    s->put();
  }
}

void Monitor::try_send_message(Message *m, const entity_inst_t& to)
{
  dout(10) << "try_send_message " << *m << " to " << to << dendl;

  bufferlist bl;
  encode_message(m, quorum_features, bl);

  messenger->send_message(m, to);

  for (int i=0; i<(int)monmap->size(); i++) {
    if (i != rank)
      messenger->send_message(new MRoute(bl, to), monmap->get_inst(i));
  }
}

void Monitor::send_reply(MonOpRequestRef op, Message *reply)
{
  op->mark_event(__func__);

  MonSession *session = op->get_session();
  assert(session);
  Message *req = op->get_req();
  ConnectionRef con = op->get_connection();

  reply->set_cct(g_ceph_context);
  dout(2) << __func__ << " " << op << " " << reply << " " << *reply << dendl;

  if (!con) {
    dout(2) << "send_reply no connection, dropping reply " << *reply
	    << " to " << req << " " << *req << dendl;
    reply->put();
    op->mark_event("reply: no connection");
    return;
  }

  if (!session->con && !session->proxy_con) {
    dout(2) << "send_reply no connection, dropping reply " << *reply
	    << " to " << req << " " << *req << dendl;
    reply->put();
    op->mark_event("reply: no connection");
    return;
  }

  if (session->proxy_con) {
    dout(15) << "send_reply routing reply to " << con->get_peer_addr()
	     << " via " << session->proxy_con->get_peer_addr()
	     << " for request " << *req << dendl;
    session->proxy_con->send_message(new MRoute(session->proxy_tid, reply));
    op->mark_event("reply: send routed request");
  } else {
    session->con->send_message(reply);
    op->mark_event("reply: send");
  }
}

void Monitor::no_reply(MonOpRequestRef op)
{
  MonSession *session = op->get_session();
  Message *req = op->get_req();

  if (session->proxy_con) {
    if (get_quorum_features() & CEPH_FEATURE_MON_NULLROUTE) {
      dout(10) << "no_reply to " << req->get_source_inst()
	       << " via " << session->proxy_con->get_peer_addr()
	       << " for request " << *req << dendl;
      session->proxy_con->send_message(new MRoute(session->proxy_tid, NULL));
      op->mark_event("no_reply: send routed request");
    } else {
      dout(10) << "no_reply no quorum nullroute feature for "
               << req->get_source_inst()
	       << " via " << session->proxy_con->get_peer_addr()
	       << " for request " << *req << dendl;
      op->mark_event("no_reply: no quorum support");
    }
  } else {
    dout(10) << "no_reply to " << req->get_source_inst()
             << " " << *req << dendl;
    op->mark_event("no_reply");
  }
}

void Monitor::handle_route(MonOpRequestRef op)
{
  MRoute *m = static_cast<MRoute*>(op->get_req());
  MonSession *session = op->get_session();
  //check privileges
  if (!session->is_capable("mon", MON_CAP_X)) {
    dout(0) << "MRoute received from entity without appropriate perms! "
	    << dendl;
    return;
  }
  if (m->msg)
    dout(10) << "handle_route " << *m->msg << " to " << m->dest << dendl;
  else
    dout(10) << "handle_route null to " << m->dest << dendl;
  
  // look it up
  if (m->session_mon_tid) {
    if (routed_requests.count(m->session_mon_tid)) {
      RoutedRequest *rr = routed_requests[m->session_mon_tid];

      // reset payload, in case encoding is dependent on target features
      if (m->msg) {
	m->msg->clear_payload();
	rr->con->send_message(m->msg);
	m->msg = NULL;
      }
      if (m->send_osdmap_first) {
	dout(10) << " sending osdmaps from " << m->send_osdmap_first << dendl;
	osdmon()->send_incremental(m->send_osdmap_first, rr->session,
				   true, MonOpRequestRef());
      }
      assert(rr->tid == m->session_mon_tid && rr->session->routed_request_tids.count(m->session_mon_tid));
      routed_requests.erase(m->session_mon_tid);
      rr->session->routed_request_tids.erase(m->session_mon_tid);
      delete rr;
    } else {
      dout(10) << " don't have routed request tid " << m->session_mon_tid << dendl;
    }
  } else {
    dout(10) << " not a routed request, trying to send anyway" << dendl;
    if (m->msg) {
      messenger->send_message(m->msg, m->dest);
      m->msg = NULL;
    }
  }
}

void Monitor::resend_routed_requests()
{
  dout(10) << "resend_routed_requests" << dendl;
  int mon = get_leader();
  list<Context*> retry;
  for (map<uint64_t, RoutedRequest*>::iterator p = routed_requests.begin();
       p != routed_requests.end();
       ++p) {
    RoutedRequest *rr = p->second;

    if (mon == rank) {
      dout(10) << " requeue for self tid " << rr->tid << dendl;
      rr->op->mark_event("retry routed request");
      retry.push_back(new C_RetryMessage(this, rr->op));
      if (rr->session) {
        assert(rr->session->routed_request_tids.count(p->first));
        rr->session->routed_request_tids.erase(p->first);
      }
      delete rr;
    } else {
      bufferlist::iterator q = rr->request_bl.begin();
      PaxosServiceMessage *req = (PaxosServiceMessage *)decode_message(cct, 0, q);
      rr->op->mark_event("resend forwarded message to leader");
      dout(10) << " resend to mon." << mon << " tid " << rr->tid << " " << *req << dendl;
      MForward *forward = new MForward(rr->tid, req, rr->con_features,
				       rr->session->caps);
      req->put();  // forward takes its own ref; drop ours.
      forward->client = rr->client_inst;
      forward->set_priority(req->get_priority());
      messenger->send_message(forward, monmap->get_inst(mon));
    }
  }
  if (mon == rank) {
    routed_requests.clear();
    finish_contexts(g_ceph_context, retry);
  }
}

void Monitor::remove_session(MonSession *s)
{
  dout(10) << "remove_session " << s << " " << s->inst << dendl;
  assert(s->con);
  assert(!s->closed);
  for (set<uint64_t>::iterator p = s->routed_request_tids.begin();
       p != s->routed_request_tids.end();
       ++p) {
    assert(routed_requests.count(*p));
    RoutedRequest *rr = routed_requests[*p];
    dout(10) << " dropping routed request " << rr->tid << dendl;
    delete rr;
    routed_requests.erase(*p);
  }
  s->routed_request_tids.clear();
  s->con->set_priv(NULL);
  session_map.remove_session(s);
  logger->set(l_mon_num_sessions, session_map.get_size());
  logger->inc(l_mon_session_rm);
}

void Monitor::remove_all_sessions()
{
  while (!session_map.sessions.empty()) {
    MonSession *s = session_map.sessions.front();
    remove_session(s);
    if (logger)
      logger->inc(l_mon_session_rm);
  }
  if (logger)
    logger->set(l_mon_num_sessions, session_map.get_size());
}

void Monitor::send_command(const entity_inst_t& inst,
			   const vector<string>& com)
{
  dout(10) << "send_command " << inst << "" << com << dendl;
  MMonCommand *c = new MMonCommand(monmap->fsid);
  c->cmd = com;
  try_send_message(c, inst);
}

void Monitor::waitlist_or_zap_client(MonOpRequestRef op)
{
  /**
   * Wait list the new session until we're in the quorum, assuming it's
   * sufficiently new.
   * tick() will periodically send them back through so we can send
   * the client elsewhere if we don't think we're getting back in.
   *
   * But we whitelist a few sorts of messages:
   * 1) Monitors can talk to us at any time, of course.
   * 2) auth messages. It's unlikely to go through much faster, but
   * it's possible we've just lost our quorum status and we want to take...
   * 3) command messages. We want to accept these under all possible
   * circumstances.
   */
  Message *m = op->get_req();
  MonSession *s = op->get_session();
  ConnectionRef con = op->get_connection();
  utime_t too_old = ceph_clock_now(g_ceph_context);
  too_old -= g_ceph_context->_conf->mon_lease;
  if (m->get_recv_stamp() > too_old &&
      con->is_connected()) {
    dout(5) << "waitlisting message " << *m << dendl;
    maybe_wait_for_quorum.push_back(new C_RetryMessage(this, op));
    op->mark_wait_for_quorum();
  } else {
    dout(5) << "discarding message " << *m << " and sending client elsewhere" << dendl;
    con->mark_down();
    // proxied sessions aren't registered and don't have a con; don't remove
    // those.
    if (!s->proxy_con)
      remove_session(s);
    op->mark_zap();
  }
}

void Monitor::_ms_dispatch(Message *m)
{
  if (is_shutdown()) {
    m->put();
    return;
  }

  MonOpRequestRef op = op_tracker.create_request<MonOpRequest>(m);
  bool src_is_mon = op->is_src_mon();
  op->mark_event("mon:_ms_dispatch");
  MonSession *s = op->get_session();
  if (s && s->closed) {
    return;
  }
  if (!s) {
    // if the sender is not a monitor, make sure their first message for a
    // session is an MAuth.  If it is not, assume it's a stray message,
    // and considering that we are creating a new session it is safe to
    // assume that the sender hasn't authenticated yet, so we have no way
    // of assessing whether we should handle it or not.
    if (!src_is_mon && (m->get_type() != CEPH_MSG_AUTH &&
			m->get_type() != CEPH_MSG_MON_GET_MAP &&
			m->get_type() != CEPH_MSG_PING)) {
      dout(1) << __func__ << " dropping stray message " << *m
	      << " from " << m->get_source_inst() << dendl;
      return;
    }

    ConnectionRef con = m->get_connection();
    s = session_map.new_session(m->get_source_inst(), con.get());
    assert(s);
    con->set_priv(s->get());
    dout(10) << __func__ << " new session " << s << " " << *s << dendl;
    op->set_session(s);

    logger->set(l_mon_num_sessions, session_map.get_size());
    logger->inc(l_mon_session_add);

    if (src_is_mon) {
      // give it monitor caps; the peer type has been authenticated
      dout(5) << __func__ << " setting monitor caps on this connection" << dendl;
      if (!s->caps.is_allow_all()) // but no need to repeatedly copy
        s->caps = *mon_caps;
    }
    s->put();
  } else {
    dout(20) << __func__ << " existing session " << s << " for " << s->inst
	     << dendl;
  }

  assert(s);

  s->session_timeout = ceph_clock_now(NULL);
  s->session_timeout += g_conf->mon_session_timeout;

  if (s->auth_handler) {
    s->entity_name = s->auth_handler->get_entity_name();
  }
  dout(20) << " caps " << s->caps.get_str() << dendl;

  if ((is_synchronizing() ||
       (s->global_id == 0 && !exited_quorum.is_zero())) &&
      !src_is_mon &&
      m->get_type() != CEPH_MSG_PING) {
    waitlist_or_zap_client(op);
  } else {
    dispatch_op(op);
  }
  return;
}

void Monitor::dispatch_op(MonOpRequestRef op)
{
  op->mark_event("mon:dispatch_op");
  MonSession *s = op->get_session();
  assert(s);
  if (s->closed) {
    dout(10) << " session closed, dropping " << op->get_req() << dendl;
    return;
  }

  /* we will consider the default type as being 'monitor' until proven wrong */
  op->set_type_monitor();
  /* deal with all messages that do not necessarily need caps */
  bool dealt_with = true;
  switch (op->get_req()->get_type()) {
    // auth
    case MSG_MON_GLOBAL_ID:
    case CEPH_MSG_AUTH:
      op->set_type_service();
      /* no need to check caps here */
      paxos_service[PAXOS_AUTH]->dispatch(op);
      break;

    case CEPH_MSG_PING:
      handle_ping(op);
      break;

    /* MMonGetMap may be used by clients to obtain a monmap *before*
     * authenticating with the monitor.  We need to handle these without
     * checking caps because, even on a cluster without cephx, we only set
     * session caps *after* the auth handshake.  A good example of this
     * is when a client calls MonClient::get_monmap_privately(), which does
     * not authenticate when obtaining a monmap.
     */
    case CEPH_MSG_MON_GET_MAP:
      handle_mon_get_map(op);
      break;

    case CEPH_MSG_MON_METADATA:
      return handle_mon_metadata(op);

    default:
      dealt_with = false;
      break;
  }
  if (dealt_with)
    return;

  /* well, maybe the op belongs to a service... */
  op->set_type_service();
  /* deal with all messages which caps should be checked somewhere else */
  dealt_with = true;
  switch (op->get_req()->get_type()) {

    // OSDs
    case CEPH_MSG_MON_GET_OSDMAP:
    case MSG_OSD_MARK_ME_DOWN:
    case MSG_OSD_FAILURE:
    case MSG_OSD_BOOT:
    case MSG_OSD_ALIVE:
    case MSG_OSD_PGTEMP:
    case MSG_REMOVE_SNAPS:
      paxos_service[PAXOS_OSDMAP]->dispatch(op);
      break;

    // MDSs
    case MSG_MDS_BEACON:
    case MSG_MDS_OFFLOAD_TARGETS:
      paxos_service[PAXOS_MDSMAP]->dispatch(op);
      break;


    // pg
    case CEPH_MSG_STATFS:
    case MSG_PGSTATS:
    case MSG_GETPOOLSTATS:
      paxos_service[PAXOS_PGMAP]->dispatch(op);
      break;

    case CEPH_MSG_POOLOP:
      paxos_service[PAXOS_OSDMAP]->dispatch(op);
      break;

    // log
    case MSG_LOG:
      paxos_service[PAXOS_LOG]->dispatch(op);
      break;

    // handle_command() does its own caps checking
    case MSG_MON_COMMAND:
      op->set_type_command();
      handle_command(op);
      break;

    default:
      dealt_with = false;
      break;
  }
  if (dealt_with)
    return;

  /* nop, looks like it's not a service message; revert back to monitor */
  op->set_type_monitor();

  /* messages we, the Monitor class, need to deal with
   * but may be sent by clients. */

  if (!op->get_session()->is_capable("mon", MON_CAP_R)) {
    dout(5) << __func__ << " " << op->get_req()->get_source_inst()
            << " not enough caps for " << *(op->get_req()) << " -- dropping"
            << dendl;
    goto drop;
  }

  dealt_with = true;
  switch (op->get_req()->get_type()) {

    // misc
    case CEPH_MSG_MON_GET_VERSION:
      handle_get_version(op);
      break;

    case CEPH_MSG_MON_SUBSCRIBE:
      /* FIXME: check what's being subscribed, filter accordingly */
      handle_subscribe(op);
      break;

    default:
      dealt_with = false;
      break;
  }
  if (dealt_with)
    return;

  if (!op->is_src_mon()) {
    dout(1) << __func__ << " unexpected monitor message from"
            << " non-monitor entity " << op->get_req()->get_source_inst()
            << " " << *(op->get_req()) << " -- dropping" << dendl;
    goto drop;
  }

  /* messages that should only be sent by another monitor */
  dealt_with = true;
  switch (op->get_req()->get_type()) {

    case MSG_ROUTE:
      handle_route(op);
      break;

    case MSG_MON_PROBE:
      handle_probe(op);
      break;

    // Sync (i.e., the new slurp, but on steroids)
    case MSG_MON_SYNC:
      handle_sync(op);
      break;
    case MSG_MON_SCRUB:
      handle_scrub(op);
      break;

    /* log acks are sent from a monitor we sent the MLog to, and are
       never sent by clients to us. */
    case MSG_LOGACK:
      log_client.handle_log_ack((MLogAck*)op->get_req());
      break;

    // monmap
    case MSG_MON_JOIN:
      op->set_type_service();
      paxos_service[PAXOS_MONMAP]->dispatch(op);
      break;

    // paxos
    case MSG_MON_PAXOS:
      {
        op->set_type_paxos();
        MMonPaxos *pm = static_cast<MMonPaxos*>(op->get_req());
        if (!op->is_src_mon() ||
            !op->get_session()->is_capable("mon", MON_CAP_X)) {
          //can't send these!
          break;
        }

        if (state == STATE_SYNCHRONIZING) {
          // we are synchronizing. These messages would do us no
          // good, thus just drop them and ignore them.
          dout(10) << __func__ << " ignore paxos msg from "
            << pm->get_source_inst() << dendl;
          break;
        }

        // sanitize
        if (pm->epoch > get_epoch()) {
          bootstrap();
          break;
        }
        if (pm->epoch != get_epoch()) {
          break;
        }

        paxos->dispatch(op);
      }
      break;

    // elector messages
    case MSG_MON_ELECTION:
      op->set_type_election();
      //check privileges here for simplicity
      if (!op->get_session()->is_capable("mon", MON_CAP_X)) {
        dout(0) << "MMonElection received from entity without enough caps!"
          << op->get_session()->caps << dendl;
        break;
      }
      if (!is_probing() && !is_synchronizing()) {
        elector.dispatch(op);
      }
      break;

    case MSG_FORWARD:
      handle_forward(op);
      break;

    case MSG_TIMECHECK:
      handle_timecheck(op);
      break;

    case MSG_MON_HEALTH:
      health_monitor->dispatch(op);
      break;

    default:
      dealt_with = false;
      break;
  }
  if (!dealt_with) {
    dout(1) << "dropping unexpected " << *(op->get_req()) << dendl;
    goto drop;
  }
  return;

drop:
  return;
}

void Monitor::handle_ping(MonOpRequestRef op)
{
  MPing *m = static_cast<MPing*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  MPing *reply = new MPing;
  entity_inst_t inst = m->get_source_inst();
  bufferlist payload;
  Formatter *f = new JSONFormatter(true);
  f->open_object_section("pong");

  list<string> health_str;
  get_health(health_str, NULL, f);
  {
    stringstream ss;
    get_mon_status(f, ss);
  }

  f->close_section();
  stringstream ss;
  f->flush(ss);
  ::encode(ss.str(), payload);
  reply->set_payload(payload);
  dout(10) << __func__ << " reply payload len " << reply->get_payload().length() << dendl;
  messenger->send_message(reply, inst);
}

void Monitor::timecheck_start()
{
  dout(10) << __func__ << dendl;
  timecheck_cleanup();
  timecheck_start_round();
}

void Monitor::timecheck_finish()
{
  dout(10) << __func__ << dendl;
  timecheck_cleanup();
}

void Monitor::timecheck_start_round()
{
  dout(10) << __func__ << " curr " << timecheck_round << dendl;
  assert(is_leader());

  if (monmap->size() == 1) {
    assert(0 == "We are alone; this shouldn't have been scheduled!");
    return;
  }

  if (timecheck_round % 2) {
    dout(10) << __func__ << " there's a timecheck going on" << dendl;
    utime_t curr_time = ceph_clock_now(g_ceph_context);
    double max = g_conf->mon_timecheck_interval*3;
    if (curr_time - timecheck_round_start < max) {
      dout(10) << __func__ << " keep current round going" << dendl;
      goto out;
    } else {
      dout(10) << __func__
               << " finish current timecheck and start new" << dendl;
      timecheck_cancel_round();
    }
  }

  assert(timecheck_round % 2 == 0);
  timecheck_acks = 0;
  timecheck_round ++;
  timecheck_round_start = ceph_clock_now(g_ceph_context);
  dout(10) << __func__ << " new " << timecheck_round << dendl;

  timecheck();
out:
  dout(10) << __func__ << " setting up next event" << dendl;
  timecheck_reset_event();
}

void Monitor::timecheck_finish_round(bool success)
{
  dout(10) << __func__ << " curr " << timecheck_round << dendl;
  assert(timecheck_round % 2);
  timecheck_round ++;
  timecheck_round_start = utime_t();

  if (success) {
    assert(timecheck_waiting.empty());
    assert(timecheck_acks == quorum.size());
    timecheck_report();
    timecheck_check_skews();
    return;
  }

  dout(10) << __func__ << " " << timecheck_waiting.size()
           << " peers still waiting:";
  for (map<entity_inst_t,utime_t>::iterator p = timecheck_waiting.begin();
      p != timecheck_waiting.end(); ++p) {
    *_dout << " " << p->first.name;
  }
  *_dout << dendl;
  timecheck_waiting.clear();

  dout(10) << __func__ << " finished to " << timecheck_round << dendl;
}

void Monitor::timecheck_cancel_round()
{
  timecheck_finish_round(false);
}

void Monitor::timecheck_cleanup()
{
  timecheck_round = 0;
  timecheck_acks = 0;
  timecheck_round_start = utime_t();

  if (timecheck_event) {
    timer.cancel_event(timecheck_event);
    timecheck_event = NULL;
  }
  timecheck_waiting.clear();
  timecheck_skews.clear();
  timecheck_latencies.clear();

  timecheck_rounds_since_clean = 0;
}

void Monitor::timecheck_reset_event()
{
  if (timecheck_event) {
    timer.cancel_event(timecheck_event);
    timecheck_event = NULL;
  }

  double delay =
    cct->_conf->mon_timecheck_skew_interval * timecheck_rounds_since_clean;

  if (delay <= 0 || delay > cct->_conf->mon_timecheck_interval) {
    delay = cct->_conf->mon_timecheck_interval;
  }

  dout(10) << __func__ << " delay " << delay
           << " rounds_since_clean " << timecheck_rounds_since_clean
           << dendl;

  timecheck_event = new C_TimeCheck(this);
  timer.add_event_after(delay, timecheck_event);
}

void Monitor::timecheck_check_skews()
{
  dout(10) << __func__ << dendl;
  assert(is_leader());
  assert((timecheck_round % 2) == 0);
  if (monmap->size() == 1) {
    assert(0 == "We are alone; we shouldn't have gotten here!");
    return;
  }
  assert(timecheck_latencies.size() == timecheck_skews.size());

  bool found_skew = false;
  for (map<entity_inst_t, double>::iterator p = timecheck_skews.begin();
       p != timecheck_skews.end(); ++p) {

    double abs_skew;
    if (timecheck_has_skew(p->second, &abs_skew)) {
      dout(10) << __func__
               << " " << p->first << " skew " << abs_skew << dendl;
      found_skew = true;
    }
  }

  if (found_skew) {
    ++timecheck_rounds_since_clean;
    timecheck_reset_event();
  } else if (timecheck_rounds_since_clean > 0) {
    dout(1) << __func__
      << " no clock skews found after " << timecheck_rounds_since_clean
      << " rounds" << dendl;
    // make sure the skews are really gone and not just a transient success
    // this will run just once if not in the presence of skews again.
    timecheck_rounds_since_clean = 1;
    timecheck_reset_event();
    timecheck_rounds_since_clean = 0;
  }

}

void Monitor::timecheck_report()
{
  dout(10) << __func__ << dendl;
  assert(is_leader());
  assert((timecheck_round % 2) == 0);
  if (monmap->size() == 1) {
    assert(0 == "We are alone; we shouldn't have gotten here!");
    return;
  }

  assert(timecheck_latencies.size() == timecheck_skews.size());
  bool do_output = true; // only output report once
  for (set<int>::iterator q = quorum.begin(); q != quorum.end(); ++q) {
    if (monmap->get_name(*q) == name)
      continue;

    MTimeCheck *m = new MTimeCheck(MTimeCheck::OP_REPORT);
    m->epoch = get_epoch();
    m->round = timecheck_round;

    for (map<entity_inst_t, double>::iterator it = timecheck_skews.begin();
         it != timecheck_skews.end(); ++it) {
      double skew = it->second;
      double latency = timecheck_latencies[it->first];

      m->skews[it->first] = skew;
      m->latencies[it->first] = latency;

      if (do_output) {
        dout(25) << __func__ << " " << it->first
                 << " latency " << latency
                 << " skew " << skew << dendl;
      }
    }
    do_output = false;
    entity_inst_t inst = monmap->get_inst(*q);
    dout(10) << __func__ << " send report to " << inst << dendl;
    messenger->send_message(m, inst);
  }
}

void Monitor::timecheck()
{
  dout(10) << __func__ << dendl;
  assert(is_leader());
  if (monmap->size() == 1) {
    assert(0 == "We are alone; we shouldn't have gotten here!");
    return;
  }
  assert(timecheck_round % 2 != 0);

  timecheck_acks = 1; // we ack ourselves

  dout(10) << __func__ << " start timecheck epoch " << get_epoch()
           << " round " << timecheck_round << dendl;

  // we are at the eye of the storm; the point of reference
  timecheck_skews[messenger->get_myinst()] = 0.0;
  timecheck_latencies[messenger->get_myinst()] = 0.0;

  for (set<int>::iterator it = quorum.begin(); it != quorum.end(); ++it) {
    if (monmap->get_name(*it) == name)
      continue;

    entity_inst_t inst = monmap->get_inst(*it);
    utime_t curr_time = ceph_clock_now(g_ceph_context);
    timecheck_waiting[inst] = curr_time;
    MTimeCheck *m = new MTimeCheck(MTimeCheck::OP_PING);
    m->epoch = get_epoch();
    m->round = timecheck_round;
    dout(10) << __func__ << " send " << *m << " to " << inst << dendl;
    messenger->send_message(m, inst);
  }
}

health_status_t Monitor::timecheck_status(ostringstream &ss,
                                          const double skew_bound,
                                          const double latency)
{
  health_status_t status = HEALTH_OK;
  assert(latency >= 0);

  double abs_skew;
  if (timecheck_has_skew(skew_bound, &abs_skew)) {
    status = HEALTH_WARN;
    ss << "clock skew " << abs_skew << "s"
       << " > max " << g_conf->mon_clock_drift_allowed << "s";
  }

  return status;
}

void Monitor::handle_timecheck_leader(MonOpRequestRef op)
{
  MTimeCheck *m = static_cast<MTimeCheck*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  /* handles PONG's */
  assert(m->op == MTimeCheck::OP_PONG);

  entity_inst_t other = m->get_source_inst();
  if (m->epoch < get_epoch()) {
    dout(1) << __func__ << " got old timecheck epoch " << m->epoch
            << " from " << other
            << " curr " << get_epoch()
            << " -- severely lagged? discard" << dendl;
    return;
  }
  assert(m->epoch == get_epoch());

  if (m->round < timecheck_round) {
    dout(1) << __func__ << " got old round " << m->round
            << " from " << other
            << " curr " << timecheck_round << " -- discard" << dendl;
    return;
  }

  utime_t curr_time = ceph_clock_now(g_ceph_context);

  assert(timecheck_waiting.count(other) > 0);
  utime_t timecheck_sent = timecheck_waiting[other];
  timecheck_waiting.erase(other);
  if (curr_time < timecheck_sent) {
    // our clock was readjusted -- drop everything until it all makes sense.
    dout(1) << __func__ << " our clock was readjusted --"
            << " bump round and drop current check"
            << dendl;
    timecheck_cancel_round();
    return;
  }

  /* update peer latencies */
  double latency = (double)(curr_time - timecheck_sent);

  if (timecheck_latencies.count(other) == 0)
    timecheck_latencies[other] = latency;
  else {
    double avg_latency = ((timecheck_latencies[other]*0.8)+(latency*0.2));
    timecheck_latencies[other] = avg_latency;
  }

  /*
   * update skews
   *
   * some nasty thing goes on if we were to do 'a - b' between two utime_t,
   * and 'a' happens to be lower than 'b'; so we use double instead.
   *
   * latency is always expected to be >= 0.
   *
   * delta, the difference between theirs timestamp and ours, may either be
   * lower or higher than 0; will hardly ever be 0.
   *
   * The absolute skew is the absolute delta minus the latency, which is
   * taken as a whole instead of an rtt given that there is some queueing
   * and dispatch times involved and it's hard to assess how long exactly
   * it took for the message to travel to the other side and be handled. So
   * we call it a bounded skew, the worst case scenario.
   *
   * Now, to math!
   *
   * Given that the latency is always positive, we can establish that the
   * bounded skew will be:
   *
   *  1. positive if the absolute delta is higher than the latency and
   *     delta is positive
   *  2. negative if the absolute delta is higher than the latency and
   *     delta is negative.
   *  3. zero if the absolute delta is lower than the latency.
   *
   * On 3. we make a judgement call and treat the skew as non-existent.
   * This is because that, if the absolute delta is lower than the
   * latency, then the apparently existing skew is nothing more than a
   * side-effect of the high latency at work.
   *
   * This may not be entirely true though, as a severely skewed clock
   * may be masked by an even higher latency, but with high latencies
   * we probably have worse issues to deal with than just skewed clocks.
   */
  assert(latency >= 0);

  double delta = ((double) m->timestamp) - ((double) curr_time);
  double abs_delta = (delta > 0 ? delta : -delta);
  double skew_bound = abs_delta - latency;
  if (skew_bound < 0)
    skew_bound = 0;
  else if (delta < 0)
    skew_bound = -skew_bound;

  ostringstream ss;
  health_status_t status = timecheck_status(ss, skew_bound, latency);
  if (status == HEALTH_ERR)
    clog->error() << other << " " << ss.str() << "\n";
  else if (status == HEALTH_WARN)
    clog->warn() << other << " " << ss.str() << "\n";

  dout(10) << __func__ << " from " << other << " ts " << m->timestamp
	   << " delta " << delta << " skew_bound " << skew_bound
	   << " latency " << latency << dendl;

  timecheck_skews[other] = skew_bound;

  timecheck_acks++;
  if (timecheck_acks == quorum.size()) {
    dout(10) << __func__ << " got pongs from everybody ("
             << timecheck_acks << " total)" << dendl;
    assert(timecheck_skews.size() == timecheck_acks);
    assert(timecheck_waiting.empty());
    // everyone has acked, so bump the round to finish it.
    timecheck_finish_round();
  }
}

void Monitor::handle_timecheck_peon(MonOpRequestRef op)
{
  MTimeCheck *m = static_cast<MTimeCheck*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;

  assert(is_peon());
  assert(m->op == MTimeCheck::OP_PING || m->op == MTimeCheck::OP_REPORT);

  if (m->epoch != get_epoch()) {
    dout(1) << __func__ << " got wrong epoch "
            << "(ours " << get_epoch()
            << " theirs: " << m->epoch << ") -- discarding" << dendl;
    return;
  }

  if (m->round < timecheck_round) {
    dout(1) << __func__ << " got old round " << m->round
            << " current " << timecheck_round
            << " (epoch " << get_epoch() << ") -- discarding" << dendl;
    return;
  }

  timecheck_round = m->round;

  if (m->op == MTimeCheck::OP_REPORT) {
    assert((timecheck_round % 2) == 0);
    timecheck_latencies.swap(m->latencies);
    timecheck_skews.swap(m->skews);
    return;
  }

  assert((timecheck_round % 2) != 0);
  MTimeCheck *reply = new MTimeCheck(MTimeCheck::OP_PONG);
  utime_t curr_time = ceph_clock_now(g_ceph_context);
  reply->timestamp = curr_time;
  reply->epoch = m->epoch;
  reply->round = m->round;
  dout(10) << __func__ << " send " << *m
           << " to " << m->get_source_inst() << dendl;
  m->get_connection()->send_message(reply);
}

void Monitor::handle_timecheck(MonOpRequestRef op)
{
  MTimeCheck *m = static_cast<MTimeCheck*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;

  if (is_leader()) {
    if (m->op != MTimeCheck::OP_PONG) {
      dout(1) << __func__ << " drop unexpected msg (not pong)" << dendl;
    } else {
      handle_timecheck_leader(op);
    }
  } else if (is_peon()) {
    if (m->op != MTimeCheck::OP_PING && m->op != MTimeCheck::OP_REPORT) {
      dout(1) << __func__ << " drop unexpected msg (not ping or report)" << dendl;
    } else {
      handle_timecheck_peon(op);
    }
  } else {
    dout(1) << __func__ << " drop unexpected msg" << dendl;
  }
}

void Monitor::handle_subscribe(MonOpRequestRef op)
{
  MMonSubscribe *m = static_cast<MMonSubscribe*>(op->get_req());
  dout(10) << "handle_subscribe " << *m << dendl;
  
  bool reply = false;

  MonSession *s = op->get_session();
  assert(s);

  for (map<string,ceph_mon_subscribe_item>::iterator p = m->what.begin();
       p != m->what.end();
       ++p) {
    // if there are any non-onetime subscriptions, we need to reply to start the resubscribe timer
    if ((p->second.flags & CEPH_SUBSCRIBE_ONETIME) == 0)
      reply = true;

    // remove conflicting subscribes
    if (logmon()->sub_name_to_id(p->first) >= 0) {
      for (map<string, Subscription*>::iterator it = s->sub_map.begin();
	   it != s->sub_map.end(); ) {
	if (it->first != p->first && logmon()->sub_name_to_id(it->first) >= 0) {
	  session_map.remove_sub((it++)->second);
	} else {
	  ++it;
	}
      }
    }

    session_map.add_update_sub(s, p->first, p->second.start, 
			       p->second.flags & CEPH_SUBSCRIBE_ONETIME,
			       m->get_connection()->has_feature(CEPH_FEATURE_INCSUBOSDMAP));

    if (p->first.find("mdsmap") == 0 || p->first == "fsmap") {
      dout(10) << __func__ << ": MDS sub '" << p->first << "'" << dendl;
      if ((int)s->is_capable("mds", MON_CAP_R)) {
        Subscription *sub = s->sub_map[p->first];
        assert(sub != nullptr);
        mdsmon()->check_sub(sub);
      }
    } else if (p->first == "osdmap") {
      if ((int)s->is_capable("osd", MON_CAP_R)) {
	if (s->osd_epoch > p->second.start) {
	  // client needs earlier osdmaps on purpose, so reset the sent epoch
	  s->osd_epoch = 0;
	}
        osdmon()->check_sub(s->sub_map["osdmap"]);
      }
    } else if (p->first == "osd_pg_creates") {
      if ((int)s->is_capable("osd", MON_CAP_W)) {
	pgmon()->check_sub(s->sub_map["osd_pg_creates"]);
      }
    } else if (p->first == "monmap") {
      check_sub(s->sub_map["monmap"]);
    } else if (logmon()->sub_name_to_id(p->first) >= 0) {
      logmon()->check_sub(s->sub_map[p->first]);
    }
  }

  if (reply) {
    // we only need to reply if the client is old enough to think it
    // has to send renewals.
    ConnectionRef con = m->get_connection();
    if (!con->has_feature(CEPH_FEATURE_MON_STATEFUL_SUB))
      m->get_connection()->send_message(new MMonSubscribeAck(
	monmap->get_fsid(), (int)g_conf->mon_subscribe_interval));
  }

}

void Monitor::handle_get_version(MonOpRequestRef op)
{
  MMonGetVersion *m = static_cast<MMonGetVersion*>(op->get_req());
  dout(10) << "handle_get_version " << *m << dendl;
  PaxosService *svc = NULL;

  MonSession *s = op->get_session();
  assert(s);

  if (!is_leader() && !is_peon()) {
    dout(10) << " waiting for quorum" << dendl;
    waitfor_quorum.push_back(new C_RetryMessage(this, op));
    goto out;
  }

  if (m->what == "mdsmap") {
    svc = mdsmon();
  } else if (m->what == "fsmap") {
    svc = mdsmon();
  } else if (m->what == "osdmap") {
    svc = osdmon();
  } else if (m->what == "monmap") {
    svc = monmon();
  } else {
    derr << "invalid map type " << m->what << dendl;
  }

  if (svc) {
    if (!svc->is_readable()) {
      svc->wait_for_readable(op, new C_RetryMessage(this, op));
      goto out;
    }

    MMonGetVersionReply *reply = new MMonGetVersionReply();
    reply->handle = m->handle;
    reply->version = svc->get_last_committed();
    reply->oldest_version = svc->get_first_committed();
    reply->set_tid(m->get_tid());

    m->get_connection()->send_message(reply);
  }
 out:
  return;
}

bool Monitor::ms_handle_reset(Connection *con)
{
  dout(10) << "ms_handle_reset " << con << " " << con->get_peer_addr() << dendl;

  // ignore lossless monitor sessions
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
    return false;

  MonSession *s = static_cast<MonSession *>(con->get_priv());
  if (!s)
    return false;

  // break any con <-> session ref cycle
  s->con->set_priv(NULL);

  if (is_shutdown())
    return false;

  Mutex::Locker l(lock);

  dout(10) << "reset/close on session " << s->inst << dendl;
  if (!s->closed)
    remove_session(s);
  s->put();
  return true;
}

void Monitor::check_subs()
{
  string type = "monmap";
  if (session_map.subs.count(type) == 0)
    return;
  xlist<Subscription*>::iterator p = session_map.subs[type]->begin();
  while (!p.end()) {
    Subscription *sub = *p;
    ++p;
    check_sub(sub);
  }
}

void Monitor::check_sub(Subscription *sub)
{
  dout(10) << "check_sub monmap next " << sub->next << " have " << monmap->get_epoch() << dendl;
  if (sub->next <= monmap->get_epoch()) {
    send_latest_monmap(sub->session->con.get());
    if (sub->onetime)
      session_map.remove_sub(sub);
    else
      sub->next = monmap->get_epoch() + 1;
  }
}


// -----

void Monitor::send_latest_monmap(Connection *con)
{
  bufferlist bl;
  monmap->encode(bl, con->get_features());
  con->send_message(new MMonMap(bl));
}

void Monitor::handle_mon_get_map(MonOpRequestRef op)
{
  MMonGetMap *m = static_cast<MMonGetMap*>(op->get_req());
  dout(10) << "handle_mon_get_map" << dendl;
  send_latest_monmap(m->get_connection().get());
}

void Monitor::handle_mon_metadata(MonOpRequestRef op)
{
  MMonMetadata *m = static_cast<MMonMetadata*>(op->get_req());
  if (is_leader()) {
    dout(10) << __func__ << dendl;
    update_mon_metadata(m->get_source().num(), m->data);
  }
}

void Monitor::update_mon_metadata(int from, const Metadata& m)
{
  metadata[from] = m;

  bufferlist bl;
  int err = store->get(MONITOR_STORE_PREFIX, "last_metadata", bl);
  map<int, Metadata> last_metadata;
  if (!err) {
    bufferlist::iterator iter = bl.begin();
    ::decode(last_metadata, iter);
    metadata.insert(last_metadata.begin(), last_metadata.end());
  }

  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
  bl.clear();
  ::encode(metadata, bl);
  t->put(MONITOR_STORE_PREFIX, "last_metadata", bl);
  paxos->trigger_propose();
}

int Monitor::load_metadata(map<int, Metadata>& metadata)
{
  bufferlist bl;
  int r = store->get(MONITOR_STORE_PREFIX, "last_metadata", bl);
  if (r)
    return r;
  bufferlist::iterator it = bl.begin();
  ::decode(metadata, it);
  return 0;
}

int Monitor::get_mon_metadata(int mon, Formatter *f, ostream& err)
{
  assert(f);
  map<int, Metadata> last_metadata;
  if (int r = load_metadata(last_metadata)) {
    err << "Unable to load metadata: " << cpp_strerror(r);
    return r;
  }
  if (!last_metadata.count(mon)) {
    err << "mon." << mon << " not found";
    return -EINVAL;
  }
  const Metadata& m = last_metadata[mon];
  for (Metadata::const_iterator p = m.begin(); p != m.end(); ++p) {
    f->dump_string(p->first.c_str(), p->second);
  }
  return 0;
}

int Monitor::print_nodes(Formatter *f, ostream& err)
{
  map<int, Metadata> metadata;
  if (int r = load_metadata(metadata)) {
    err << "Unable to load metadata.\n";
    return r;
  }

  map<string, list<int> > mons;	// hostname => mon
  for (map<int, Metadata>::iterator it = metadata.begin();
       it != metadata.end(); ++it) {
    const Metadata& m = it->second;
    Metadata::const_iterator hostname = m.find("hostname");
    if (hostname == m.end()) {
      // not likely though
      continue;
    }
    mons[hostname->second].push_back(it->first);
  }

  dump_services(f, mons, "mon");
  return 0;
}

// ----------------------------------------------
// scrub

int Monitor::scrub_start()
{
  dout(10) << __func__ << dendl;
  assert(is_leader());

  if ((get_quorum_features() & CEPH_FEATURE_MON_SCRUB) == 0) {
    clog->warn() << "scrub not supported by entire quorum\n";
    return -EOPNOTSUPP;
  }

  if (!scrub_result.empty()) {
    clog->info() << "scrub already in progress\n";
    return -EBUSY;
  }

  scrub_event_cancel();
  scrub_result.clear();
  scrub_state.reset(new ScrubState);

  scrub();
  return 0;
}

int Monitor::scrub()
{
  assert(is_leader());
  assert(scrub_state);

  scrub_cancel_timeout();
  wait_for_paxos_write();
  scrub_version = paxos->get_version();


  // scrub all keys if we're the only monitor in the quorum
  int32_t num_keys =
    (quorum.size() == 1 ? -1 : cct->_conf->mon_scrub_max_keys);

  for (set<int>::iterator p = quorum.begin();
       p != quorum.end();
       ++p) {
    if (*p == rank)
      continue;
    MMonScrub *r = new MMonScrub(MMonScrub::OP_SCRUB, scrub_version,
                                 num_keys);
    r->key = scrub_state->last_key;
    messenger->send_message(r, monmap->get_inst(*p));
  }

  // scrub my keys
  bool r = _scrub(&scrub_result[rank],
                  &scrub_state->last_key,
                  &num_keys);

  scrub_state->finished = !r;

  // only after we got our scrub results do we really care whether the
  // other monitors are late on their results.  Also, this way we avoid
  // triggering the timeout if we end up getting stuck in _scrub() for
  // longer than the duration of the timeout.
  scrub_reset_timeout();

  if (quorum.size() == 1) {
    assert(scrub_state->finished == true);
    scrub_finish();
  }
  return 0;
}

void Monitor::handle_scrub(MonOpRequestRef op)
{
  MMonScrub *m = static_cast<MMonScrub*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  switch (m->op) {
  case MMonScrub::OP_SCRUB:
    {
      if (!is_peon())
	break;

      wait_for_paxos_write();

      if (m->version != paxos->get_version())
	break;

      MMonScrub *reply = new MMonScrub(MMonScrub::OP_RESULT,
                                       m->version,
                                       m->num_keys);

      reply->key = m->key;
      _scrub(&reply->result, &reply->key, &reply->num_keys);
      m->get_connection()->send_message(reply);
    }
    break;

  case MMonScrub::OP_RESULT:
    {
      if (!is_leader())
	break;
      if (m->version != scrub_version)
	break;
      // reset the timeout each time we get a result
      scrub_reset_timeout();

      int from = m->get_source().num();
      assert(scrub_result.count(from) == 0);
      scrub_result[from] = m->result;

      if (scrub_result.size() == quorum.size()) {
        scrub_check_results();
        scrub_result.clear();
        if (scrub_state->finished)
          scrub_finish();
        else
          scrub();
      }
    }
    break;
  }
}

bool Monitor::_scrub(ScrubResult *r,
                     pair<string,string> *start,
                     int *num_keys)
{
  assert(r != NULL);
  assert(start != NULL);
  assert(num_keys != NULL);

  set<string> prefixes = get_sync_targets_names();
  prefixes.erase("paxos");  // exclude paxos, as this one may have extra states for proposals, etc.

  dout(10) << __func__ << " start (" << *start << ")"
           << " num_keys " << *num_keys << dendl;

  MonitorDBStore::Synchronizer it = store->get_synchronizer(*start, prefixes);

  int scrubbed_keys = 0;
  pair<string,string> last_key;

  while (it->has_next_chunk()) {

    if (*num_keys > 0 && scrubbed_keys == *num_keys)
      break;

    pair<string,string> k = it->get_next_key();
    if (prefixes.count(k.first) == 0)
      continue;

    if (cct->_conf->mon_scrub_inject_missing_keys > 0.0 &&
        (rand() % 10000 < cct->_conf->mon_scrub_inject_missing_keys*10000.0)) {
      dout(10) << __func__ << " inject missing key, skipping (" << k << ")"
               << dendl;
      continue;
    }

    bufferlist bl;
    //TODO: what when store->get returns error or empty bl?
    store->get(k.first, k.second, bl);
    uint32_t key_crc = bl.crc32c(0);
    dout(30) << __func__ << " " << k << " bl " << bl.length() << " bytes"
                                     << " crc " << key_crc << dendl;
    r->prefix_keys[k.first]++;
    if (r->prefix_crc.count(k.first) == 0)
      r->prefix_crc[k.first] = 0;
    r->prefix_crc[k.first] = bl.crc32c(r->prefix_crc[k.first]);

    if (cct->_conf->mon_scrub_inject_crc_mismatch > 0.0 &&
        (rand() % 10000 < cct->_conf->mon_scrub_inject_crc_mismatch*10000.0)) {
      dout(10) << __func__ << " inject failure at (" << k << ")" << dendl;
      r->prefix_crc[k.first] += 1;
    }

    ++scrubbed_keys;
    last_key = k;
  }

  dout(20) << __func__ << " last_key (" << last_key << ")"
                       << " scrubbed_keys " << scrubbed_keys
                       << " has_next " << it->has_next_chunk() << dendl;

  *start = last_key;
  *num_keys = scrubbed_keys;

  return it->has_next_chunk();
}

void Monitor::scrub_check_results()
{
  dout(10) << __func__ << dendl;

  // compare
  int errors = 0;
  ScrubResult& mine = scrub_result[rank];
  for (map<int,ScrubResult>::iterator p = scrub_result.begin();
       p != scrub_result.end();
       ++p) {
    if (p->first == rank)
      continue;
    if (p->second != mine) {
      ++errors;
      clog->error() << "scrub mismatch" << "\n";
      clog->error() << " mon." << rank << " " << mine << "\n";
      clog->error() << " mon." << p->first << " " << p->second << "\n";
    }
  }
  if (!errors)
    clog->info() << "scrub ok on " << quorum << ": " << mine << "\n";
}

inline void Monitor::scrub_timeout()
{
  dout(1) << __func__ << " restarting scrub" << dendl;
  scrub_reset();
  scrub_start();
}

void Monitor::scrub_finish()
{
  dout(10) << __func__ << dendl;
  scrub_reset();
  scrub_event_start();
}

void Monitor::scrub_reset()
{
  dout(10) << __func__ << dendl;
  scrub_cancel_timeout();
  scrub_version = 0;
  scrub_result.clear();
  scrub_state.reset();
}

inline void Monitor::scrub_update_interval(int secs)
{
  // we don't care about changes if we are not the leader.
  // changes will be visible if we become the leader.
  if (!is_leader())
    return;

  dout(1) << __func__ << " new interval = " << secs << dendl;

  // if scrub already in progress, all changes will already be visible during
  // the next round.  Nothing to do.
  if (scrub_state != NULL)
    return;

  scrub_event_cancel();
  scrub_event_start();
}

void Monitor::scrub_event_start()
{
  dout(10) << __func__ << dendl;

  if (scrub_event)
    scrub_event_cancel();

  if (cct->_conf->mon_scrub_interval <= 0) {
    dout(1) << __func__ << " scrub event is disabled"
            << " (mon_scrub_interval = " << cct->_conf->mon_scrub_interval
            << ")" << dendl;
    return;
  }

  scrub_event = new C_Scrub(this);
  timer.add_event_after(cct->_conf->mon_scrub_interval, scrub_event);
}

void Monitor::scrub_event_cancel()
{
  dout(10) << __func__ << dendl;
  if (scrub_event) {
    timer.cancel_event(scrub_event);
    scrub_event = NULL;
  }
}

inline void Monitor::scrub_cancel_timeout()
{
  if (scrub_timeout_event) {
    timer.cancel_event(scrub_timeout_event);
    scrub_timeout_event = NULL;
  }
}

void Monitor::scrub_reset_timeout()
{
  dout(15) << __func__ << " reset timeout event" << dendl;
  scrub_cancel_timeout();
  scrub_timeout_event = new C_ScrubTimeout(this);
  timer.add_event_after(g_conf->mon_scrub_timeout, scrub_timeout_event);
}

/************ TICK ***************/

class C_Mon_Tick : public Context {
  Monitor *mon;
public:
  explicit C_Mon_Tick(Monitor *m) : mon(m) {}
  void finish(int r) {
    mon->tick();
  }
};

void Monitor::new_tick()
{
  C_Mon_Tick *ctx = new C_Mon_Tick(this);
  timer.add_event_after(g_conf->mon_tick_interval, ctx);
}

void Monitor::tick()
{
  // ok go.
  dout(11) << "tick" << dendl;
  
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); ++p) {
    (*p)->tick();
    (*p)->maybe_trim();
  }
  
  // trim sessions
  utime_t now = ceph_clock_now(g_ceph_context);
  xlist<MonSession*>::iterator p = session_map.sessions.begin();

  bool out_for_too_long = (!exited_quorum.is_zero()
      && now > (exited_quorum + 2*g_conf->mon_lease));

  while (!p.end()) {
    MonSession *s = *p;
    ++p;
    
    // don't trim monitors
    if (s->inst.name.is_mon())
      continue;

    if (s->session_timeout < now && s->con) {
      // check keepalive, too
      s->session_timeout = s->con->get_last_keepalive();
      s->session_timeout += g_conf->mon_session_timeout;
    }
    if (s->session_timeout < now) {
      dout(10) << " trimming session " << s->con << " " << s->inst
	       << " (timeout " << s->session_timeout
	       << " < now " << now << ")" << dendl;
    } else if (out_for_too_long) {
      // boot the client Session because we've taken too long getting back in
      dout(10) << " trimming session " << s->con << " " << s->inst
        << " because we've been out of quorum too long" << dendl;
    } else {
      continue;
    }

    s->con->mark_down();
    remove_session(s);
    logger->inc(l_mon_session_trim);
  }

  sync_trim_providers();

  if (!maybe_wait_for_quorum.empty()) {
    finish_contexts(g_ceph_context, maybe_wait_for_quorum);
  }

  if (is_leader() && paxos->is_active() && fingerprint.is_zero()) {
    // this is only necessary on upgraded clusters.
    MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
    prepare_new_fingerprint(t);
    paxos->trigger_propose();
  }

  new_tick();
}

void Monitor::prepare_new_fingerprint(MonitorDBStore::TransactionRef t)
{
  uuid_d nf;
  nf.generate_random();
  dout(10) << __func__ << " proposing cluster_fingerprint " << nf << dendl;

  bufferlist bl;
  ::encode(nf, bl);
  t->put(MONITOR_NAME, "cluster_fingerprint", bl);
}

int Monitor::check_fsid()
{
  bufferlist ebl;
  int r = store->get(MONITOR_NAME, "cluster_uuid", ebl);
  if (r == -ENOENT)
    return r;
  assert(r == 0);

  string es(ebl.c_str(), ebl.length());

  // only keep the first line
  size_t pos = es.find_first_of('\n');
  if (pos != string::npos)
    es.resize(pos);

  dout(10) << "check_fsid cluster_uuid contains '" << es << "'" << dendl;
  uuid_d ondisk;
  if (!ondisk.parse(es.c_str())) {
    derr << "error: unable to parse uuid" << dendl;
    return -EINVAL;
  }

  if (monmap->get_fsid() != ondisk) {
    derr << "error: cluster_uuid file exists with value " << ondisk
	 << ", != our uuid " << monmap->get_fsid() << dendl;
    return -EEXIST;
  }

  return 0;
}

int Monitor::write_fsid()
{
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
  int r = write_fsid(t);
  store->apply_transaction(t);
  return r;
}

int Monitor::write_fsid(MonitorDBStore::TransactionRef t)
{
  ostringstream ss;
  ss << monmap->get_fsid() << "\n";
  string us = ss.str();

  bufferlist b;
  b.append(us);

  t->put(MONITOR_NAME, "cluster_uuid", b);
  return 0;
}

/*
 * this is the closest thing to a traditional 'mkfs' for ceph.
 * initialize the monitor state machines to their initial values.
 */
int Monitor::mkfs(bufferlist& osdmapbl)
{
  MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);

  // verify cluster fsid
  int r = check_fsid();
  if (r < 0 && r != -ENOENT)
    return r;

  bufferlist magicbl;
  magicbl.append(CEPH_MON_ONDISK_MAGIC);
  magicbl.append("\n");
  t->put(MONITOR_NAME, "magic", magicbl);


  features = get_initial_supported_features();
  write_features(t);

  // save monmap, osdmap, keyring.
  bufferlist monmapbl;
  monmap->encode(monmapbl, CEPH_FEATURES_ALL);
  monmap->set_epoch(0);     // must be 0 to avoid confusing first MonmapMonitor::update_from_paxos()
  t->put("mkfs", "monmap", monmapbl);

  if (osdmapbl.length()) {
    // make sure it's a valid osdmap
    try {
      OSDMap om;
      om.decode(osdmapbl);
    }
    catch (buffer::error& e) {
      derr << "error decoding provided osdmap: " << e.what() << dendl;
      return -EINVAL;
    }
    t->put("mkfs", "osdmap", osdmapbl);
  }

  if (is_keyring_required()) {
    KeyRing keyring;
    string keyring_filename;

    r = ceph_resolve_file_search(g_conf->keyring, keyring_filename);
    if (r) {
      derr << "unable to find a keyring file on " << g_conf->keyring
	   << ": " << cpp_strerror(r) << dendl;
      if (g_conf->key != "") {
	string keyring_plaintext = "[mon.]\n\tkey = " + g_conf->key +
	  "\n\tcaps mon = \"allow *\"\n";
	bufferlist bl;
	bl.append(keyring_plaintext);
	try {
	  bufferlist::iterator i = bl.begin();
	  keyring.decode_plaintext(i);
	}
	catch (const buffer::error& e) {
	  derr << "error decoding keyring " << keyring_plaintext
	       << ": " << e.what() << dendl;
	  return -EINVAL;
	}
      } else {
	return -ENOENT;
      }
    } else {
      r = keyring.load(g_ceph_context, keyring_filename);
      if (r < 0) {
	derr << "unable to load initial keyring " << g_conf->keyring << dendl;
	return r;
      }
    }

    // put mon. key in external keyring; seed with everything else.
    extract_save_mon_key(keyring);

    bufferlist keyringbl;
    keyring.encode_plaintext(keyringbl);
    t->put("mkfs", "keyring", keyringbl);
  }
  write_fsid(t);
  store->apply_transaction(t);

  return 0;
}

int Monitor::write_default_keyring(bufferlist& bl)
{
  ostringstream os;
  os << g_conf->mon_data << "/keyring";

  int err = 0;
  int fd = ::open(os.str().c_str(), O_WRONLY|O_CREAT, 0600);
  if (fd < 0) {
    err = -errno;
    dout(0) << __func__ << " failed to open " << os.str() 
	    << ": " << cpp_strerror(err) << dendl;
    return err;
  }

  err = bl.write_fd(fd);
  if (!err)
    ::fsync(fd);
  ::close(fd);

  return err;
}

void Monitor::extract_save_mon_key(KeyRing& keyring)
{
  EntityName mon_name;
  mon_name.set_type(CEPH_ENTITY_TYPE_MON);
  EntityAuth mon_key;
  if (keyring.get_auth(mon_name, mon_key)) {
    dout(10) << "extract_save_mon_key moving mon. key to separate keyring" << dendl;
    KeyRing pkey;
    pkey.add(mon_name, mon_key);
    bufferlist bl;
    pkey.encode_plaintext(bl);
    write_default_keyring(bl);
    keyring.remove(mon_name);
  }
}

bool Monitor::ms_get_authorizer(int service_id, AuthAuthorizer **authorizer, bool force_new)
{
  dout(10) << "ms_get_authorizer for " << ceph_entity_type_name(service_id) << dendl;

  if (is_shutdown())
    return false;

  // we only connect to other monitors; every else connects to us.
  if (service_id != CEPH_ENTITY_TYPE_MON)
    return false;

  if (!auth_cluster_required.is_supported_auth(CEPH_AUTH_CEPHX))
    return false;

  CephXServiceTicketInfo auth_ticket_info;
  CephXSessionAuthInfo info;
  int ret;
  EntityName name;
  name.set_type(CEPH_ENTITY_TYPE_MON);

  auth_ticket_info.ticket.name = name;
  auth_ticket_info.ticket.global_id = 0;

  CryptoKey secret;
  if (!keyring.get_secret(name, secret) &&
      !key_server.get_secret(name, secret)) {
    dout(0) << " couldn't get secret for mon service from keyring or keyserver" << dendl;
    stringstream ss, ds;
    int err = key_server.list_secrets(ds);
    if (err < 0)
      ss << "no installed auth entries!";
    else
      ss << "installed auth entries:";
    dout(0) << ss.str() << "\n" << ds.str() << dendl;
    return false;
  }

  /* mon to mon authentication uses the private monitor shared key and not the
     rotating key */
  ret = key_server.build_session_auth_info(service_id, auth_ticket_info, info, secret, (uint64_t)-1);
  if (ret < 0) {
    dout(0) << "ms_get_authorizer failed to build session auth_info for use with mon ret " << ret << dendl;
    return false;
  }

  CephXTicketBlob blob;
  if (!cephx_build_service_ticket_blob(cct, info, blob)) {
    dout(0) << "ms_get_authorizer failed to build service ticket use with mon" << dendl;
    return false;
  }
  bufferlist ticket_data;
  ::encode(blob, ticket_data);

  bufferlist::iterator iter = ticket_data.begin();
  CephXTicketHandler handler(g_ceph_context, service_id);
  ::decode(handler.ticket, iter);

  handler.session_key = info.session_key;

  *authorizer = handler.build_authorizer(0);
  
  return true;
}

bool Monitor::ms_verify_authorizer(Connection *con, int peer_type,
				   int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
				   bool& isvalid, CryptoKey& session_key)
{
  dout(10) << "ms_verify_authorizer " << con->get_peer_addr()
	   << " " << ceph_entity_type_name(peer_type)
	   << " protocol " << protocol << dendl;

  if (is_shutdown())
    return false;

  if (peer_type == CEPH_ENTITY_TYPE_MON &&
      auth_cluster_required.is_supported_auth(CEPH_AUTH_CEPHX)) {
    // monitor, and cephx is enabled
    isvalid = false;
    if (protocol == CEPH_AUTH_CEPHX) {
      bufferlist::iterator iter = authorizer_data.begin();
      CephXServiceTicketInfo auth_ticket_info;
      
      if (authorizer_data.length()) {
	bool ret = cephx_verify_authorizer(g_ceph_context, &keyring, iter,
					  auth_ticket_info, authorizer_reply);
	if (ret) {
	  session_key = auth_ticket_info.session_key;
	  isvalid = true;
	} else {
	  dout(0) << "ms_verify_authorizer bad authorizer from mon " << con->get_peer_addr() << dendl;
        }
      }
    } else {
      dout(0) << "ms_verify_authorizer cephx enabled, but no authorizer (required for mon)" << dendl;
    }
  } else {
    // who cares.
    isvalid = true;
  }
  return true;
}
