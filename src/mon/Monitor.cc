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


#include <iterator>
#include <sstream>
#include <tuple>
#include <stdlib.h>
#include <signal.h>
#include <limits.h>
#include <cstring>
#include <boost/scope_exit.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "json_spirit/json_spirit_reader.h"
#include "json_spirit/json_spirit_writer.h"

#include "Monitor.h"
#include "common/version.h"
#include "common/blkdev.h"
#include "common/cmdparse.h"
#include "common/signal.h"

#include "osd/OSDMap.h"

#include "MonitorDBStore.h"

#include "messages/PaxosServiceMessage.h"
#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonSync.h"
#include "messages/MMonScrub.h"
#include "messages/MMonProbe.h"
#include "messages/MMonJoin.h"
#include "messages/MMonPaxos.h"
#include "messages/MRoute.h"
#include "messages/MForward.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "messages/MTimeCheck2.h"
#include "messages/MPing.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/Timer.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"
#include "global/signal_handler.h"
#include "common/Formatter.h"
#include "include/stringify.h"
#include "include/color.h"
#include "include/ceph_fs.h"
#include "include/str_list.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "MonmapMonitor.h"
#include "LogMonitor.h"
#include "AuthMonitor.h"
#include "MgrMonitor.h"
#include "MgrStatMonitor.h"
#include "ConfigMonitor.h"
#include "KVMonitor.h"
#include "NVMeofGwMon.h"
#include "mon/HealthMonitor.h"
#include "common/config.h"
#include "common/cmdparse.h"
#include "include/ceph_assert.h"
#include "include/compat.h"
#include "perfglue/heap_profiler.h"

#include "auth/none/AuthNoneClientHandler.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
using namespace TOPNSPC::common;

using std::cout;
using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::set;
using std::setfill;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;
using std::unique_ptr;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::ErasureCodeInterfaceRef;
using ceph::ErasureCodeProfile;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_message;
using ceph::mono_clock;
using ceph::mono_time;
using ceph::timespan_str;


static ostream& _prefix(std::ostream *_dout, const Monitor *mon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name() << ") e" << mon->monmap->get_epoch() << " ";
}

const string Monitor::MONITOR_NAME = "monitor";
const string Monitor::MONITOR_STORE_PREFIX = "monitor_store";


#undef FLAG
#undef COMMAND
#undef COMMAND_WITH_FLAG
#define FLAG(f) (MonCommand::FLAG_##f)
#define COMMAND(parsesig, helptext, modulename, req_perms)	\
  {parsesig, helptext, modulename, req_perms, FLAG(NONE)},
#define COMMAND_WITH_FLAG(parsesig, helptext, modulename, req_perms, flags) \
  {parsesig, helptext, modulename, req_perms, flags},
MonCommand mon_commands[] = {
#include <mon/MonCommands.h>
};
#undef COMMAND
#undef COMMAND_WITH_FLAG

Monitor::Monitor(CephContext* cct_, string nm, MonitorDBStore *s,
		 Messenger *m, Messenger *mgr_m, MonMap *map) :
  Dispatcher(cct_),
  AuthServer(cct_),
  name(nm),
  rank(-1), 
  messenger(m),
  con_self(m ? m->get_loopback_connection() : NULL),
  timer(cct_, lock),
  finisher(cct_, "mon_finisher", "fin"),
  cpu_tp(cct, "Monitor::cpu_tp", "cpu_tp", g_conf()->mon_cpu_threads),
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
			cct->_conf->auth_service_required : cct->_conf->auth_supported),
  mgr_messenger(mgr_m),
  mgr_client(cct_, mgr_m, monmap),
  gss_ktfile_client(cct->_conf.get_val<std::string>("gss_ktab_client_file")),
  store(s),
  
  elector(this, map->strategy),
  required_features(0),
  leader(0),
  quorum_con_features(0),
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

  admin_hook(NULL),
  routed_request_tid(0),
  op_tracker(cct, g_conf().get_val<bool>("mon_enable_op_tracker"), 1)
{
  clog = log_client.create_channel(CLOG_CHANNEL_CLUSTER);
  audit_clog = log_client.create_channel(CLOG_CHANNEL_AUDIT);

  update_log_clients();

  if (!gss_ktfile_client.empty()) {
    // Assert we can export environment variable 
    /* 
        The default client keytab is used, if it is present and readable,
        to automatically obtain initial credentials for GSSAPI client
        applications. The principal name of the first entry in the client
        keytab is used by default when obtaining initial credentials.
        1. The KRB5_CLIENT_KTNAME environment variable.
        2. The default_client_keytab_name profile variable in [libdefaults].
        3. The hardcoded default, DEFCKTNAME.
    */
    const int32_t set_result(setenv("KRB5_CLIENT_KTNAME", 
                                    gss_ktfile_client.c_str(), 1));
    ceph_assert(set_result == 0);
  }

  op_tracker.set_complaint_and_threshold(
      g_conf().get_val<std::chrono::seconds>("mon_op_complaint_time").count(),
      g_conf().get_val<int64_t>("mon_op_log_threshold"));
  op_tracker.set_history_size_and_duration(
      g_conf().get_val<uint64_t>("mon_op_history_size"),
      g_conf().get_val<std::chrono::seconds>("mon_op_history_duration").count());
  op_tracker.set_history_slow_op_size_and_threshold(
      g_conf().get_val<uint64_t>("mon_op_history_slow_op_size"),
      g_conf().get_val<std::chrono::seconds>("mon_op_history_slow_op_threshold").count());

  paxos = std::make_unique<Paxos>(*this, "paxos");

  paxos_service[PAXOS_MDSMAP].reset(new MDSMonitor(*this, *paxos, "mdsmap"));
  paxos_service[PAXOS_MONMAP].reset(new MonmapMonitor(*this, *paxos, "monmap"));
  paxos_service[PAXOS_OSDMAP].reset(new OSDMonitor(cct, *this, *paxos, "osdmap"));
  paxos_service[PAXOS_LOG].reset(new LogMonitor(*this, *paxos, "logm"));
  paxos_service[PAXOS_AUTH].reset(new AuthMonitor(*this, *paxos, "auth"));
  paxos_service[PAXOS_MGR].reset(new MgrMonitor(*this, *paxos, "mgr"));
  paxos_service[PAXOS_MGRSTAT].reset(new MgrStatMonitor(*this, *paxos, "mgrstat"));
  paxos_service[PAXOS_HEALTH].reset(new HealthMonitor(*this, *paxos, "health"));
  paxos_service[PAXOS_CONFIG].reset(new ConfigMonitor(*this, *paxos, "config"));
  paxos_service[PAXOS_KV].reset(new KVMonitor(*this, *paxos, "kv"));
  paxos_service[PAXOS_NVMEGW].reset(new NVMeofGwMon(*this, *paxos, "nvmeofgw"));

  bool r = mon_caps.parse("allow *", NULL);
  ceph_assert(r);

  exited_quorum = ceph_clock_now();

  // prepare local commands
  local_mon_commands.resize(std::size(mon_commands));
  for (unsigned i = 0; i < std::size(mon_commands); ++i) {
    local_mon_commands[i] = mon_commands[i];
  }
  MonCommand::encode_vector(local_mon_commands, local_mon_commands_bl);

  prenautilus_local_mon_commands = local_mon_commands;
  for (auto& i : prenautilus_local_mon_commands) {
    std::string n = cmddesc_get_prenautilus_compat(i.cmdstring);
    if (n != i.cmdstring) {
      dout(20) << " pre-nautilus cmd " << i.cmdstring << " -> " << n << dendl;
      i.cmdstring = n;
    }
  }
  MonCommand::encode_vector(prenautilus_local_mon_commands, prenautilus_local_mon_commands_bl);

  // assume our commands until we have an election.  this only means
  // we won't reply with EINVAL before the election; any command that
  // actually matters will wait until we have quorum etc and then
  // retry (and revalidate).
  leader_mon_commands = local_mon_commands;
}

Monitor::~Monitor()
{
  op_tracker.on_shutdown();
  delete logger;
  ceph_assert(session_map.sessions.empty());
}


class AdminHook : public AdminSocketHook {
  Monitor *mon;
public:
  explicit AdminHook(Monitor *m) : mon(m) {}
  int call(std::string_view command, const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& errss,
	   bufferlist& out) override {
    stringstream outss;
    int r = mon->do_admin_command(command, cmdmap, f, errss, outss);
    out.append(outss);
    return r;
  }
};

int Monitor::do_admin_command(
  std::string_view command,
  const cmdmap_t& cmdmap,
  Formatter *f,
  std::ostream& err,
  std::ostream& out)
{
  std::lock_guard l(lock);

  int r = 0;
  string args;
  for (auto p = cmdmap.begin();
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
                    command == "ops" ||
                    command == "sessions");

  (read_only ? audit_clog->debug() : audit_clog->info())
    << "from='admin socket' entity='admin socket' "
    << "cmd='" << command << "' args=" << args << ": dispatch";

  if (command == "mon_status") {
    get_mon_status(f);
  } else if (command == "quorum_status") {
    _quorum_status(f, out);
  } else if (command == "sync_force") {
    bool validate = false;
    if (!cmd_getval(cmdmap, "yes_i_really_mean_it", validate)) {
      std::string v;
      if (cmd_getval(cmdmap, "validate", v) &&
	  v == "--yes-i-really-mean-it") {
	validate = true;
      }
    }
    if (!validate) {
      err << "are you SURE? this will mean the monitor store will be erased "
	"the next time the monitor is restarted.  pass "
	"'--yes-i-really-mean-it' if you really do.";
      r = -EPERM;
      goto abort;
    }
    sync_force(f);
  } else if (command.compare(0, 23, "add_bootstrap_peer_hint") == 0 ||
	     command.compare(0, 24, "add_bootstrap_peer_hintv") == 0) {
    if (!_add_bootstrap_peer_hint(command, cmdmap, out))
      goto abort;
  } else if (command == "quorum enter") {
    elector.start_participating();
    start_election();
    out << "started responding to quorum, initiated new election";
  } else if (command == "quorum exit") {
    start_election();
    elector.stop_participating();
    out << "stopped responding to quorum, initiated new election";
  } else if (command == "ops") {
    (void)op_tracker.dump_ops_in_flight(f);
  } else if (command == "sessions") {
    f->open_array_section("sessions");
    for (auto p : session_map.sessions) {
      f->dump_object("session", *p);
    }
    f->close_section();
  } else if (command == "dump_historic_ops") {
    if (!op_tracker.dump_historic_ops(f)) {
      err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
    }
  } else if (command == "dump_historic_ops_by_duration" ) {
    if (op_tracker.dump_historic_ops(f, true)) {
      err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
    }
  } else if (command == "dump_historic_slow_ops") {
    if (op_tracker.dump_historic_slow_ops(f, {})) {
      err << "op_tracker tracking is not enabled now, so no ops are tracked currently, even those get stuck. \
        please enable \"mon_enable_op_tracker\", and the tracker will start to track new ops received afterwards.";
    }
  } else if (command == "quorum") {
    string quorumcmd;
    cmd_getval(cmdmap, "quorumcmd", quorumcmd);
    if (quorumcmd == "exit") {
      start_election();
      elector.stop_participating();
      out << "stopped responding to quorum, initiated new election" << std::endl;
    } else if (quorumcmd == "enter") {
      elector.start_participating();
      start_election();
      out << "started responding to quorum, initiated new election" << std::endl;
    } else {
      err << "needs a valid 'quorum' command" << std::endl;
    }
  } else if (command == "connection scores dump") {
    if (!get_quorum_mon_features().contains_all(
				   ceph::features::mon::FEATURE_PINGING)) {
      err << "Not all monitors support changing election strategies; \
              please upgrade them first!";
    }
    elector.dump_connection_scores(f);
  } else if (command == "connection scores reset") {
    if (!get_quorum_mon_features().contains_all(
				   ceph::features::mon::FEATURE_PINGING)) {
      err << "Not all monitors support changing election strategies; \
              please upgrade them first!";
    }
    elector.notify_clear_peer_state();
  } else if (command == "smart") {
    string want_devid;
    cmd_getval(cmdmap, "devid", want_devid);

    string devname = store->get_devname();
    if (devname.empty()) {
      err << "could not determine device name for " << store->get_path();
      r = -ENOENT;
      goto abort;
    }
    set<string> devnames;
    get_raw_devices(devname, &devnames);
    json_spirit::mObject json_map;
    uint64_t smart_timeout = cct->_conf.get_val<uint64_t>(
      "mon_smart_report_timeout");
    for (auto& devname : devnames) {
      string err;
      string devid = get_device_id(devname, &err);
      if (want_devid.size() && want_devid != devid) {
	derr << "get_device_id failed on " << devname << ": " << err << dendl;
	continue;
      }
      json_spirit::mValue smart_json;
      if (block_device_get_metrics(devname, smart_timeout,
				   &smart_json)) {
	dout(10) << "block_device_get_metrics failed for /dev/" << devname
		 << dendl;
	continue;
      }
      json_map[devid] = smart_json;
    }
    json_spirit::write(json_map, out, json_spirit::pretty_print);
  } else if (command == "heap") {
    if (!ceph_using_tcmalloc()) {
      err << "could not issue heap profiler command -- not using tcmalloc!";
      r = -EOPNOTSUPP;
      goto abort;
    }
    string cmd;
    if (!cmd_getval(cmdmap, "heapcmd", cmd)) {
      err << "unable to get value for command \"" << cmd << "\"";
      r = -EINVAL;
      goto abort;
    }
    std::vector<std::string> cmd_vec;
    get_str_vec(cmd, cmd_vec);
    string val;
    if (cmd_getval(cmdmap, "value", val)) {
      cmd_vec.push_back(val);
    }
    ceph_heap_profiler_handle_command(cmd_vec, out);
  } else if (command == "compact") {
    dout(1) << "triggering manual compaction" << dendl;
    auto start = ceph::coarse_mono_clock::now();
    store->compact_async();
    auto end = ceph::coarse_mono_clock::now();
    auto duration = ceph::to_seconds<double>(end - start);
    dout(1) << "finished manual compaction in "
	    << duration << " seconds" << dendl;
    out << "compacted " << g_conf().get_val<std::string>("mon_keyvaluedb")
	<< " in " << duration << " seconds";
 } else {
    ceph_abort_msg("bad AdminSocket command binding");
  }
  (read_only ? audit_clog->debug() : audit_clog->info())
    << "from='admin socket' "
    << "entity='admin socket' "
    << "cmd=" << command << " "
    << "args=" << args << ": finished";
  return r;

abort:
  (read_only ? audit_clog->debug() : audit_clog->info())
    << "from='admin socket' "
    << "entity='admin socket' "
    << "cmd=" << command << " "
    << "args=" << args << ": aborted";
  return r;
}

void Monitor::handle_signal(int signum)
{
  derr << "*** Got Signal " << sig_str(signum) << " ***" << dendl;
  if (signum == SIGHUP) {
    sighup_handler(signum);
    logmon()->reopen_logs();
  } else {
    ceph_assert(signum == SIGINT || signum == SIGTERM);
    shutdown();
  }
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
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_KRAKEN);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_LUMINOUS);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_MIMIC);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_NAUTILUS);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OCTOPUS);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_PACIFIC);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_QUINCY);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_REEF);
  compat.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_SQUID);
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
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    t->put(MONITOR_NAME, COMPAT_SET_LOC, featuresbl);
    store->apply_transaction(t);
  } else {
    auto it = featuresbl.cbegin();
    features->decode(it);
  }
}

void Monitor::read_features()
{
  read_features_off_disk(store, &features);
  dout(10) << "features " << features << dendl;

  calc_quorum_requirements();
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
    "mon_cluster_log_to_file",
    "host",
    "fsid",
    // periodic health to clog
    "mon_health_to_clog",
    "mon_health_to_clog_interval",
    "mon_health_to_clog_tick_interval",
    // scrub interval
    "mon_scrub_interval",
    "mon_allow_pool_delete",
    // osdmap pruning - observed, not handled.
    "mon_osdmap_full_prune_enabled",
    "mon_osdmap_full_prune_min",
    "mon_osdmap_full_prune_interval",
    "mon_osdmap_full_prune_txsize",
    // debug options - observed, not handled
    "mon_debug_extra_checks",
    "mon_debug_block_osdmap_trim",
    NULL
  };
  return KEYS;
}

void Monitor::handle_conf_change(const ConfigProxy& conf,
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
    finisher.queue(new C_MonContext{this, [this, changed](int) {
      std::lock_guard l{lock};
      health_to_clog_update_conf(changed);
    }});
  }

  if (changed.count("mon_scrub_interval")) {
    auto scrub_interval =
      conf.get_val<std::chrono::seconds>("mon_scrub_interval");
    finisher.queue(new C_MonContext{this, [this, scrub_interval](int) {
      std::lock_guard l{lock};
      scrub_update_interval(scrub_interval);
    }});
  }
}

void Monitor::update_log_clients()
{
  clog->parse_client_options(g_ceph_context);
  audit_clog->parse_client_options(g_ceph_context);
}

int Monitor::sanitize_options()
{
  int r = 0;

  // mon_lease must be greater than mon_lease_renewal; otherwise we
  // may incur in leases expiring before they are renewed.
  if (g_conf()->mon_lease_renew_interval_factor >= 1.0) {
    clog->error() << "mon_lease_renew_interval_factor ("
		  << g_conf()->mon_lease_renew_interval_factor
		  << ") must be less than 1.0";
    r = -EINVAL;
  }

  // mon_lease_ack_timeout must be greater than mon_lease to make sure we've
  // got time to renew the lease and get an ack for it. Having both options
  // with the same value, for a given small vale, could mean timing out if
  // the monitors happened to be overloaded -- or even under normal load for
  // a small enough value.
  if (g_conf()->mon_lease_ack_timeout_factor <= 1.0) {
    clog->error() << "mon_lease_ack_timeout_factor ("
		  << g_conf()->mon_lease_ack_timeout_factor
		  << ") must be greater than 1.0";
    r = -EINVAL;
  }

  return r;
}

int Monitor::preinit()
{
  std::unique_lock l(lock);

  dout(1) << "preinit fsid " << monmap->fsid << dendl;

  int r = sanitize_options();
  if (r < 0) {
    derr << "option sanitization failed!" << dendl;
    return r;
  }

  ceph_assert(!logger);
  {
    PerfCountersBuilder pcb(g_ceph_context, "mon", l_mon_first, l_mon_last);
    pcb.add_u64(l_mon_num_sessions, "num_sessions", "Open sessions", "sess",
        PerfCountersBuilder::PRIO_USEFUL);
    pcb.add_u64_counter(l_mon_session_add, "session_add", "Created sessions",
        "sadd", PerfCountersBuilder::PRIO_INTERESTING);
    pcb.add_u64_counter(l_mon_session_rm, "session_rm", "Removed sessions",
        "srm", PerfCountersBuilder::PRIO_INTERESTING);
    pcb.add_u64_counter(l_mon_session_trim, "session_trim", "Trimmed sessions",
        "strm", PerfCountersBuilder::PRIO_USEFUL);
    pcb.add_u64_counter(l_mon_num_elections, "num_elections", "Elections participated in",
        "ecnt", PerfCountersBuilder::PRIO_USEFUL);
    pcb.add_u64_counter(l_mon_election_call, "election_call", "Elections started",
        "estt", PerfCountersBuilder::PRIO_INTERESTING);
    pcb.add_u64_counter(l_mon_election_win, "election_win", "Elections won",
        "ewon", PerfCountersBuilder::PRIO_INTERESTING);
    pcb.add_u64_counter(l_mon_election_lose, "election_lose", "Elections lost",
        "elst", PerfCountersBuilder::PRIO_INTERESTING);
    logger = pcb.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
  }

  ceph_assert(!cluster_logger);
  {
    PerfCountersBuilder pcb(g_ceph_context, "cluster", l_cluster_first, l_cluster_last);
    pcb.add_u64(l_cluster_num_mon, "num_mon", "Monitors");
    pcb.add_u64(l_cluster_num_mon_quorum, "num_mon_quorum", "Monitors in quorum");
    pcb.add_u64(l_cluster_num_osd, "num_osd", "OSDs");
    pcb.add_u64(l_cluster_num_osd_up, "num_osd_up", "OSDs that are up");
    pcb.add_u64(l_cluster_num_osd_in, "num_osd_in", "OSD in state \"in\" (they are in cluster)");
    pcb.add_u64(l_cluster_osd_epoch, "osd_epoch", "Current epoch of OSD map");
    pcb.add_u64(l_cluster_osd_bytes, "osd_bytes", "Total capacity of cluster", NULL, 0, unit_t(UNIT_BYTES));
    pcb.add_u64(l_cluster_osd_bytes_used, "osd_bytes_used", "Used space", NULL, 0, unit_t(UNIT_BYTES));
    pcb.add_u64(l_cluster_osd_bytes_avail, "osd_bytes_avail", "Available space", NULL, 0, unit_t(UNIT_BYTES));
    pcb.add_u64(l_cluster_num_pool, "num_pool", "Pools");
    pcb.add_u64(l_cluster_num_pg, "num_pg", "Placement groups");
    pcb.add_u64(l_cluster_num_pg_active_clean, "num_pg_active_clean", "Placement groups in active+clean state");
    pcb.add_u64(l_cluster_num_pg_active, "num_pg_active", "Placement groups in active state");
    pcb.add_u64(l_cluster_num_pg_peering, "num_pg_peering", "Placement groups in peering state");
    pcb.add_u64(l_cluster_num_object, "num_object", "Objects");
    pcb.add_u64(l_cluster_num_object_degraded, "num_object_degraded", "Degraded (missing replicas) objects");
    pcb.add_u64(l_cluster_num_object_misplaced, "num_object_misplaced", "Misplaced (wrong location in the cluster) objects");
    pcb.add_u64(l_cluster_num_object_unfound, "num_object_unfound", "Unfound objects");
    pcb.add_u64(l_cluster_num_bytes, "num_bytes", "Size of all objects", NULL, 0, unit_t(UNIT_BYTES));
    cluster_logger = pcb.create_perf_counters();
  }

  paxos->init_logger();

  // verify cluster_uuid
  {
    int r = check_fsid();
    if (r == -ENOENT)
      r = write_fsid();
    if (r < 0) {
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
    get_str_list(g_conf()->mon_initial_members, initial_members);

    if (!initial_members.empty()) {
      dout(1) << " initial_members " << initial_members << ", filtering seed monmap" << dendl;

      monmap->set_initial_members(
	g_ceph_context, initial_members, name, messenger->get_myaddrs(),
	&extra_probe_peers);

      dout(10) << " monmap is " << *monmap << dendl;
      dout(10) << " extra probe peers " << extra_probe_peers << dendl;
    }
  } else if (!monmap->contains(name)) {
    derr << "not in monmap and have been in a quorum before; "
         << "must have been removed" << dendl;
    if (g_conf()->mon_force_quorum_join) {
      dout(0) << "we should have died but "
              << "'mon_force_quorum_join' is set -- allowing boot" << dendl;
    } else {
      derr << "commit suicide!" << dendl;
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
        auto p = bl.cbegin();
        decode(keyring, p);
        extract_save_mon_key(keyring);
      }
    }

    string keyring_loc = g_conf()->mon_data + "/keyring";

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
	derr << "unable to load initial keyring " << g_conf()->keyring << dendl;
	return r;
      }
    }
  }

  admin_hook = new AdminHook(this);
  AdminSocket* admin_socket = cct->get_admin_socket();

  // unlock while registering to avoid mon_lock -> admin socket lock dependency.
  l.unlock();
  // register tell/asock commands
  for (const auto& command : local_mon_commands) {
    if (!command.is_tell()) {
      continue;
    }
    const auto prefix = cmddesc_get_prefix(command.cmdstring);
    if (prefix == "injectargs" ||
	prefix == "version" ||
	prefix == "tell") {
      // not registerd by me
      continue;
    }
    r = admin_socket->register_command(command.cmdstring, admin_hook,
				       command.helpstring);
    ceph_assert(r == 0);
  }
  l.lock();

  // add ourselves as a conf observer
  g_conf().add_observer(this);

  messenger->set_auth_client(this);
  messenger->set_auth_server(this);
  mgr_messenger->set_auth_client(this);

  auth_registry.refresh_config();

  return 0;
}

int Monitor::init()
{
  dout(2) << "init" << dendl;
  std::lock_guard l(lock);

  finisher.start();

  // start ticker
  timer.init();
  new_tick();

  cpu_tp.start();

  // i'm ready!
  messenger->add_dispatcher_tail(this);

  // kickstart pet mgrclient
  mgr_client.init();
  mgr_messenger->add_dispatcher_tail(&mgr_client);
  mgr_messenger->add_dispatcher_tail(this);  // for auth ms_* calls
  mgrmon()->prime_mgr_client();

  state = STATE_PROBING;

  bootstrap();

  if (!elector.peer_tracker_is_clean()){
    dout(10) << "peer_tracker looks inconsistent"
      << " previous bad logic, clearing ..." << dendl;
    elector.notify_clear_peer_state();
  }

  // add features of myself into feature_map
  session_map.feature_map.add_mon(con_self->get_features());
  return 0;
}

void Monitor::init_paxos()
{
  dout(10) << __func__ << dendl;
  paxos->init();

  // init services
  for (auto& svc : paxos_service) {
    svc->init();
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
      auto p = bl.cbegin();
      decode(fingerprint, p);
    }
    catch (ceph::buffer::error& e) {
      dout(10) << __func__ << " failed to decode cluster_fingerprint" << dendl;
    }
  } else {
    dout(10) << __func__ << " no cluster_fingerprint" << dendl;
  }

  for (auto& svc : paxos_service) {
    svc->refresh(need_bootstrap);
  }
  for (auto& svc : paxos_service) {
    svc->post_refresh();
  }
  load_metadata();
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

  lock.lock();

  wait_for_paxos_write();

  {
    std::lock_guard l(auth_lock);
    authmon()->_set_mon_num_rank(0, 0);
  }

  state = STATE_SHUTDOWN;

  lock.unlock();
  g_conf().remove_observer(this);
  lock.lock();

  if (admin_hook) {
    cct->get_admin_socket()->unregister_commands(admin_hook);
    delete admin_hook;
    admin_hook = NULL;
  }

  elector.shutdown();

  mgr_client.shutdown();

  lock.unlock();
  finisher.wait_for_empty();
  finisher.stop();
  lock.lock();

  // clean up
  paxos->shutdown();
  for (auto& svc : paxos_service) {
    svc->shutdown();
  }

  finish_contexts(g_ceph_context, waitfor_quorum, -ECANCELED);
  finish_contexts(g_ceph_context, maybe_wait_for_quorum, -ECANCELED);

  timer.shutdown();

  cpu_tp.stop();

  remove_all_sessions();

  log_client.shutdown();

  // unlock before msgr shutdown...
  lock.unlock();

  // shutdown messenger before removing logger from perfcounter collection, 
  // otherwise _ms_dispatch() will try to update deleted logger
  messenger->shutdown();
  mgr_messenger->shutdown();

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
  }
  if (cluster_logger) {
    if (cluster_logger_registered)
      cct->get_perfcounters_collection()->remove(cluster_logger);
    delete cluster_logger;
    cluster_logger = NULL;
  }
}

void Monitor::wait_for_paxos_write()
{
  if (paxos->is_writing() || paxos->is_writing_previous()) {
    dout(10) << __func__ << " flushing pending write" << dendl;
    lock.unlock();
    store->flush();
    lock.lock();
    dout(10) << __func__ << " flushed pending write" << dendl;
  }
}

void Monitor::respawn()
{
  // --- WARNING TO FUTURE COPY/PASTERS ---
  // You must also add a call like
  //
  //   ceph_pthread_setname(pthread_self(), "ceph-mon");
  //
  // to main() so that /proc/$pid/stat field 2 contains "(ceph-mon)"
  // instead of "(exe)", so that killall (and log rotation) will work.

  dout(0) << __func__ << dendl;

  char *new_argv[orig_argc+1];
  dout(1) << " e: '" << orig_argv[0] << "'" << dendl;
  for (int i=0; i<orig_argc; i++) {
    new_argv[i] = (char *)orig_argv[i];
    dout(1) << " " << i << ": '" << orig_argv[i] << "'" << dendl;
  }
  new_argv[orig_argc] = NULL;

  /* Determine the path to our executable, test if Linux /proc/self/exe exists.
   * This allows us to exec the same executable even if it has since been
   * unlinked.
   */
  char exe_path[PATH_MAX] = "";
#ifdef PROCPREFIX
  if (readlink(PROCPREFIX "/proc/self/exe", exe_path, PATH_MAX-1) != -1) {
    dout(1) << "respawning with exe " << exe_path << dendl;
    strcpy(exe_path, PROCPREFIX "/proc/self/exe");
  } else {
#else
  {
#endif
    /* Print CWD for the user's interest */
    char buf[PATH_MAX];
    char *cwd = getcwd(buf, sizeof(buf));
    ceph_assert(cwd);
    dout(1) << " cwd " << cwd << dendl;

    /* Fall back to a best-effort: just running in our CWD */
    strncpy(exe_path, orig_argv[0], PATH_MAX-1);
  }

  dout(1) << " exe_path " << exe_path << dendl;

  unblock_all_signals(NULL);
  execv(exe_path, new_argv);

  dout(0) << "respawn execv " << orig_argv[0]
	  << " failed with " << cpp_strerror(errno) << dendl;

  // We have to assert out here, because suicide() returns, and callers
  // to respawn expect it never to return.
  ceph_abort();
}

void Monitor::bootstrap()
{
  dout(10) << "bootstrap" << dendl;
  wait_for_paxos_write();

  sync_reset_requester();
  unregister_cluster_logger();
  cancel_probe_timeout();

  if (monmap->get_epoch() == 0) {
    dout(10) << "reverting to legacy ranks for seed monmap (epoch 0)" << dendl;
    monmap->calc_legacy_ranks();
  }
  dout(10) << "monmap " << *monmap << dendl;
  {
    auto from_release = monmap->min_mon_release;
    ostringstream err;
    if (!can_upgrade_from(from_release, "min_mon_release", err)) {
      derr << "current monmap has " << err.str() << " stopping." << dendl;
      exit(0);
    }
  }
  // note my rank
  int newrank = monmap->get_rank(messenger->get_myaddrs());
  if (newrank < 0 && rank >= 0) {
    // was i ever part of the quorum?
    if (has_ever_joined) {
      dout(0) << " removed from monmap, suicide." << dendl;
      exit(0);
    }
    elector.notify_clear_peer_state();
  }
  if (newrank >= 0 &&
      monmap->get_addrs(newrank) != messenger->get_myaddrs()) {
    dout(0) << " monmap addrs for rank " << newrank << " changed, i am "
	    << messenger->get_myaddrs()
	    << ", monmap is " << monmap->get_addrs(newrank) << ", respawning"
	    << dendl;

    if (monmap->get_epoch()) {
      // store this map in temp mon_sync location so that we use it on
      // our next startup
      derr << " stashing newest monmap " << monmap->get_epoch()
	   << " for next startup" << dendl;
      bufferlist bl;
      monmap->encode(bl, -1);
      auto t(std::make_shared<MonitorDBStore::Transaction>());
      t->put("mon_sync", "temp_newer_monmap", bl);
      store->apply_transaction(t);
    }

    respawn();
  }
  if (newrank != rank) {
    dout(0) << " my rank is now " << newrank << " (was " << rank << ")" << dendl;
    messenger->set_myname(entity_name_t::MON(newrank));
    rank = newrank;
    elector.notify_rank_changed(rank);

    // reset all connections, or else our peers will think we are someone else.
    messenger->mark_down_all();
  }

  // reset
  state = STATE_PROBING;

  _reset();

  // sync store
  if (g_conf()->mon_compact_on_bootstrap) {
    dout(10) << "bootstrap -- triggering compaction" << dendl;
    store->compact();
    dout(10) << "bootstrap -- finished compaction" << dendl;
  }

  // stretch mode bits
  set_elector_disallowed_leaders(false);

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
      send_mon_message(
	new MMonProbe(monmap->fsid, MMonProbe::OP_PROBE, name, has_ever_joined,
		      ceph_release()),
	i);
  }
  for (auto& av : extra_probe_peers) {
    if (av != messenger->get_myaddrs()) {
      messenger->send_to_mon(
	new MMonProbe(monmap->fsid, MMonProbe::OP_PROBE, name, has_ever_joined,
		      ceph_release()),
	av);
    }
  }
}

bool Monitor::_add_bootstrap_peer_hint(std::string_view cmd,
				       const cmdmap_t& cmdmap,
				       ostream& ss)
{
  if (is_leader() || is_peon()) {
    ss << "mon already active; ignoring bootstrap hint";
    return true;
  }

  entity_addrvec_t addrs;
  string addrstr;
  if (cmd_getval(cmdmap, "addr", addrstr)) {
    dout(10) << "_add_bootstrap_peer_hint '" << cmd << "' addr '"
	     << addrstr << "'" << dendl;

    entity_addr_t addr;
    if (!addr.parse(addrstr, entity_addr_t::TYPE_ANY)) {
      ss << "failed to parse addrs '" << addrstr
	 << "'; syntax is 'add_bootstrap_peer_hint ip[:port]'";
      return false;
    }

    addrs.v.push_back(addr);
    if (addr.get_port() == 0) {
      addrs.v[0].set_type(entity_addr_t::TYPE_MSGR2);
      addrs.v[0].set_port(CEPH_MON_PORT_IANA);
      addrs.v.push_back(addr);
      addrs.v[1].set_type(entity_addr_t::TYPE_LEGACY);
      addrs.v[1].set_port(CEPH_MON_PORT_LEGACY);
    } else if (addr.get_type() == entity_addr_t::TYPE_ANY) {
      if (addr.get_port() == CEPH_MON_PORT_LEGACY) {
	addrs.v[0].set_type(entity_addr_t::TYPE_LEGACY);
      } else {
	addrs.v[0].set_type(entity_addr_t::TYPE_MSGR2);
      }
    }
  } else if (cmd_getval(cmdmap, "addrv", addrstr)) {
    dout(10) << "_add_bootstrap_peer_hintv '" << cmd << "' addrv '"
	     << addrstr << "'" << dendl;
    const char *end = 0;
    if (!addrs.parse(addrstr.c_str(), &end)) {
      ss << "failed to parse addrs '" << addrstr
	 << "'; syntax is 'add_bootstrap_peer_hintv v2:ip:port[,v1:ip:port]'";
      return false;
    }
  } else {
    ss << "no addr or addrv provided";
    return false;
  }

  extra_probe_peers.insert(addrs);
  ss << "adding peer " << addrs << " to list: " << extra_probe_peers;
  return true;
}

// called by bootstrap(), or on leader|peon -> electing
void Monitor::_reset()
{
  dout(10) << __func__ << dendl;

  // disable authentication
  {
    std::lock_guard l(auth_lock);
    authmon()->_set_mon_num_rank(0, 0);
  }

  cancel_probe_timeout();
  timecheck_finish();
  health_events_cleanup();
  health_check_log_times.clear();
  scrub_event_cancel();

  leader_since = utime_t();
  quorum_since = {};
  if (!quorum.empty()) {
    exited_quorum = ceph_clock_now();
  }
  quorum.clear();
  outside_quorum.clear();
  quorum_feature_map.clear();

  scrub_reset();

  paxos->restart();

  for (auto& svc : paxos_service) {
    svc->restart();
  }
}


// -----------------------------------------------------------
// sync

set<string> Monitor::get_sync_targets_names()
{
  set<string> targets;
  targets.insert(paxos->get_name());
  for (auto& svc : paxos_service) {
    svc->get_store_prefixes(targets);
  }
  return targets;
}


void Monitor::sync_timeout()
{
  dout(10) << __func__ << dendl;
  ceph_assert(state == STATE_SYNCHRONIZING);
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
      ceph_abort_msg("error reading the store");
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
      ceph_abort_msg("error reading the store");
    }
    ceph_assert(backup_bl.length() > 0);

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

  sync_provider = entity_addrvec_t();
  sync_cookie = 0;
  sync_full = false;
  sync_start_version = 0;
}

void Monitor::sync_reset_provider()
{
  dout(10) << __func__ << dendl;
  sync_providers.clear();
}

void Monitor::sync_start(entity_addrvec_t &addrs, bool full)
{
  dout(10) << __func__ << " " << addrs << (full ? " full" : " recent") << dendl;

  ceph_assert(state == STATE_PROBING ||
	 state == STATE_SYNCHRONIZING);
  state = STATE_SYNCHRONIZING;

  // make sure are not a provider for anyone!
  sync_reset_provider();

  sync_full = full;

  if (sync_full) {
    // stash key state, and mark that we are syncing
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    sync_stash_critical_state(t);
    t->put("mon_sync", "in_sync", 1);

    sync_last_committed_floor = std::max(sync_last_committed_floor, paxos->get_version());
    dout(10) << __func__ << " marking sync in progress, storing sync_last_committed_floor "
	     << sync_last_committed_floor << dendl;
    t->put("mon_sync", "last_committed_floor", sync_last_committed_floor);

    store->apply_transaction(t);

    ceph_assert(g_conf()->mon_sync_requester_kill_at != 1);

    // clear the underlying store
    set<string> targets = get_sync_targets_names();
    dout(10) << __func__ << " clearing prefixes " << targets << dendl;
    store->clear(targets);

    // make sure paxos knows it has been reset.  this prevents a
    // bootstrap and then different probe reply order from possibly
    // deciding a partial or no sync is needed.
    paxos->init();

    ceph_assert(g_conf()->mon_sync_requester_kill_at != 2);
  }

  // assume 'other' as the leader. We will update the leader once we receive
  // a reply to the sync start.
  sync_provider = addrs;

  sync_reset_timeout();

  MMonSync *m = new MMonSync(sync_full ? MMonSync::OP_GET_COOKIE_FULL : MMonSync::OP_GET_COOKIE_RECENT);
  if (!sync_full)
    m->last_committed = paxos->get_version();
  messenger->send_to_mon(m, sync_provider);
}

void Monitor::sync_stash_critical_state(MonitorDBStore::TransactionRef t)
{
  dout(10) << __func__ << dendl;
  bufferlist backup_monmap;
  sync_obtain_latest_monmap(backup_monmap);
  ceph_assert(backup_monmap.length() > 0);
  t->put("mon_sync", "latest_monmap", backup_monmap);
}

void Monitor::sync_reset_timeout()
{
  dout(10) << __func__ << dendl;
  if (sync_timeout_event)
    timer.cancel_event(sync_timeout_event);
  sync_timeout_event = timer.add_event_after(
    g_conf()->mon_sync_timeout,
    new C_MonContext{this, [this](int) {
	sync_timeout();
      }});
}

void Monitor::sync_finish(version_t last_committed)
{
  dout(10) << __func__ << " lc " << last_committed << " from " << sync_provider << dendl;

  ceph_assert(g_conf()->mon_sync_requester_kill_at != 7);

  if (sync_full) {
    // finalize the paxos commits
    auto tx(std::make_shared<MonitorDBStore::Transaction>());
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

  ceph_assert(g_conf()->mon_sync_requester_kill_at != 8);

  auto t(std::make_shared<MonitorDBStore::Transaction>());
  t->erase("mon_sync", "in_sync");
  t->erase("mon_sync", "force_sync");
  t->erase("mon_sync", "last_committed_floor");
  store->apply_transaction(t);

  ceph_assert(g_conf()->mon_sync_requester_kill_at != 9);

  init_paxos();

  ceph_assert(g_conf()->mon_sync_requester_kill_at != 10);

  bootstrap();
}

void Monitor::handle_sync(MonOpRequestRef op)
{
  auto m = op->get_req<MMonSync>();
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
    ceph_abort_msg("unknown op");
  }
}

// leader

void Monitor::_sync_reply_no_cookie(MonOpRequestRef op)
{
  auto m = op->get_req<MMonSync>();
  MMonSync *reply = new MMonSync(MMonSync::OP_NO_COOKIE, m->cookie);
  m->get_connection()->send_message(reply);
}

void Monitor::handle_sync_get_cookie(MonOpRequestRef op)
{
  auto m = op->get_req<MMonSync>();
  if (is_synchronizing()) {
    _sync_reply_no_cookie(op);
    return;
  }

  ceph_assert(g_conf()->mon_sync_provider_kill_at != 1);

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
  ceph_assert(sync_providers.count(cookie) == 0);

  dout(10) << __func__ << " cookie " << cookie << " for " << m->get_source_inst() << dendl;

  SyncProvider& sp = sync_providers[cookie];
  sp.cookie = cookie;
  sp.addrs = m->get_source_addrs();
  sp.reset_timeout(g_ceph_context, g_conf()->mon_sync_timeout * 2);

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
  auto m = op->get_req<MMonSync>();
  dout(10) << __func__ << " " << *m << dendl;

  if (sync_providers.count(m->cookie) == 0) {
    dout(10) << __func__ << " no cookie " << m->cookie << dendl;
    _sync_reply_no_cookie(op);
    return;
  }

  ceph_assert(g_conf()->mon_sync_provider_kill_at != 2);

  SyncProvider& sp = sync_providers[m->cookie];
  sp.reset_timeout(g_ceph_context, g_conf()->mon_sync_timeout * 2);

  if (sp.last_committed < paxos->get_first_committed() &&
      paxos->get_first_committed() > 1) {
    dout(10) << __func__ << " sync requester fell behind paxos, their lc " << sp.last_committed
	     << " < our fc " << paxos->get_first_committed() << dendl;
    sync_providers.erase(m->cookie);
    _sync_reply_no_cookie(op);
    return;
  }

  MMonSync *reply = new MMonSync(MMonSync::OP_CHUNK, sp.cookie);
  auto tx(std::make_shared<MonitorDBStore::Transaction>());

  int bytes_left = g_conf()->mon_sync_max_payload_size;
  int keys_left = g_conf()->mon_sync_max_payload_keys;
  while (sp.last_committed < paxos->get_version() &&
	 bytes_left > 0 &&
	 keys_left > 0) {
    bufferlist bl;
    sp.last_committed++;

    int err = store->get(paxos->get_name(), sp.last_committed, bl);
    ceph_assert(err == 0);

    tx->put(paxos->get_name(), sp.last_committed, bl);
    bytes_left -= bl.length();
    --keys_left;
    dout(20) << __func__ << " including paxos state " << sp.last_committed
	     << dendl;
  }
  reply->last_committed = sp.last_committed;

  if (sp.full && bytes_left > 0 && keys_left > 0) {
    sp.synchronizer->get_chunk_tx(tx, bytes_left, keys_left);
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

    ceph_assert(g_conf()->mon_sync_provider_kill_at != 3);

    // clean up our local state
    sync_providers.erase(sp.cookie);
  }

  encode(*tx, reply->chunk_bl);

  m->get_connection()->send_message(reply);
}

// requester

void Monitor::handle_sync_cookie(MonOpRequestRef op)
{
  auto m = op->get_req<MMonSync>();
  dout(10) << __func__ << " " << *m << dendl;
  if (sync_cookie) {
    dout(10) << __func__ << " already have a cookie, ignoring" << dendl;
    return;
  }
  if (m->get_source_addrs() != sync_provider) {
    dout(10) << __func__ << " source does not match, discarding" << dendl;
    return;
  }
  sync_cookie = m->cookie;
  sync_start_version = m->last_committed;

  sync_reset_timeout();
  sync_get_next_chunk();

  ceph_assert(g_conf()->mon_sync_requester_kill_at != 3);
}

void Monitor::sync_get_next_chunk()
{
  dout(20) << __func__ << " cookie " << sync_cookie << " provider " << sync_provider << dendl;
  if (g_conf()->mon_inject_sync_get_chunk_delay > 0) {
    dout(20) << __func__ << " injecting delay of " << g_conf()->mon_inject_sync_get_chunk_delay << dendl;
    usleep((long long)(g_conf()->mon_inject_sync_get_chunk_delay * 1000000.0));
  }
  MMonSync *r = new MMonSync(MMonSync::OP_GET_CHUNK, sync_cookie);
  messenger->send_to_mon(r, sync_provider);

  ceph_assert(g_conf()->mon_sync_requester_kill_at != 4);
}

void Monitor::handle_sync_chunk(MonOpRequestRef op)
{
  auto m = op->get_req<MMonSync>();
  dout(10) << __func__ << " " << *m << dendl;

  if (m->cookie != sync_cookie) {
    dout(10) << __func__ << " cookie does not match, discarding" << dendl;
    return;
  }
  if (m->get_source_addrs() != sync_provider) {
    dout(10) << __func__ << " source does not match, discarding" << dendl;
    return;
  }

  ceph_assert(state == STATE_SYNCHRONIZING);
  ceph_assert(g_conf()->mon_sync_requester_kill_at != 5);

  auto tx(std::make_shared<MonitorDBStore::Transaction>());
  tx->append_from_encoded(m->chunk_bl);

  dout(30) << __func__ << " tx dump:\n";
  JSONFormatter f(true);
  tx->dump(&f);
  f.flush(*_dout);
  *_dout << dendl;

  store->apply_transaction(tx);

  ceph_assert(g_conf()->mon_sync_requester_kill_at != 6);

  if (!sync_full) {
    dout(10) << __func__ << " applying recent paxos transactions as we go" << dendl;
    auto tx(std::make_shared<MonitorDBStore::Transaction>());
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

  utime_t now = ceph_clock_now();
  map<uint64_t,SyncProvider>::iterator p = sync_providers.begin();
  while (p != sync_providers.end()) {
    if (now > p->second.timeout) {
      dout(10) << __func__ << " expiring cookie " << p->second.cookie
	       << " for " << p->second.addrs << dendl;
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
  probe_timeout_event = new C_MonContext{this, [this](int r) {
      probe_timeout(r);
    }};
  double t = g_conf()->mon_probe_timeout;
  if (timer.add_event_after(t, probe_timeout_event)) {
    dout(10) << "reset_probe_timeout " << probe_timeout_event
	     << " after " << t << " seconds" << dendl;
  } else {
    probe_timeout_event = nullptr;
  }
}

void Monitor::probe_timeout(int r)
{
  dout(4) << "probe_timeout " << probe_timeout_event << dendl;
  ceph_assert(is_probing() || is_synchronizing());
  ceph_assert(probe_timeout_event);
  probe_timeout_event = NULL;
  bootstrap();
}

void Monitor::handle_probe(MonOpRequestRef op)
{
  auto m = op->get_req<MMonProbe>();
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
    derr << __func__ << " require release " << (int)m->mon_release << " > "
	 << (int)ceph_release()
	 << ", or missing features (have " << CEPH_FEATURES_ALL
	 << ", required " << m->required_features
	 << ", missing " << (m->required_features & ~CEPH_FEATURES_ALL) << ")"
	 << dendl;
    break;
  }
}

void Monitor::handle_probe_probe(MonOpRequestRef op)
{
  auto m = op->get_req<MMonProbe>();

  dout(10) << "handle_probe_probe " << m->get_source_inst() << " " << *m
	   << " features " << m->get_connection()->get_features() << dendl;
  uint64_t missing = required_features & ~m->get_connection()->get_features();
  if ((m->mon_release != ceph_release_t::unknown &&
       m->mon_release < monmap->min_mon_release) ||
      missing) {
    dout(1) << " peer " << m->get_source_addr()
	    << " release " << m->mon_release
	    << " < min_mon_release " << monmap->min_mon_release
	    << ", or missing features " << missing << dendl;
    MMonProbe *r = new MMonProbe(monmap->fsid, MMonProbe::OP_MISSING_FEATURES,
				 name, has_ever_joined, monmap->min_mon_release);
    m->required_features = required_features;
    m->get_connection()->send_message(r);
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
  r = new MMonProbe(monmap->fsid, MMonProbe::OP_REPLY, name, has_ever_joined,
		    ceph_release());
  r->name = name;
  r->quorum = quorum;
  r->leader = leader;
  monmap->encode(r->monmap_bl, m->get_connection()->get_features());
  r->paxos_first_version = paxos->get_first_committed();
  r->paxos_last_version = paxos->get_version();
  m->get_connection()->send_message(r);

  // did we discover a peer here?
  if (!monmap->contains(m->get_source_addr())) {
    dout(1) << " adding peer " << m->get_source_addrs()
	    << " to list of hints" << dendl;
    extra_probe_peers.insert(m->get_source_addrs());
  } else {
    elector.begin_peer_ping(monmap->get_rank(m->get_source_addr()));
  }

 out:
  return;
}

void Monitor::handle_probe_reply(MonOpRequestRef op)
{
  auto m = op->get_req<MMonProbe>();
  dout(10) << "handle_probe_reply " << m->get_source_inst()
	   << " " << *m << dendl;
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
      int epoch_diff = newmap->get_epoch() - monmap->get_epoch();
      dout(20) << " new monmap is " << *newmap  << dendl;
      delete newmap;
      monmap->decode(m->monmap_bl);
      dout(20) << "has_ever_joined: " << has_ever_joined << dendl;
      if (epoch_diff == 1 && has_ever_joined) {
        notify_new_monmap(false);
      } else {
        notify_new_monmap(false, false);
        elector.notify_clear_peer_state();
      }
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
  } else if (peer_name.size()) {
    dout(10) << " peer name is " << peer_name << dendl;
  } else {
    dout(10) << " peer " << m->get_source_addr() << " not in map" << dendl;
  }

  // new initial peer?
  if (monmap->get_epoch() == 0 &&
      monmap->contains(m->name) &&
      monmap->get_addrs(m->name).front().is_blank_ip()) {
    dout(1) << " learned initial mon " << m->name
	    << " addrs " << m->get_source_addrs() << dendl;
    monmap->set_addrvec(m->name, m->get_source_addrs());

    bootstrap();
    return;
  }

  // end discover phase
  if (!is_probing()) {
    return;
  }

  ceph_assert(paxos != NULL);

  if (is_synchronizing()) {
    dout(10) << " currently syncing" << dendl;
    return;
  }

  entity_addrvec_t other = m->get_source_addrs();

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
    if (paxos->get_version() + g_conf()->paxos_max_join_drift < m->paxos_last_version) {
      dout(10) << " peer paxos last version " << m->paxos_last_version
	       << " vs my version " << paxos->get_version()
	       << " (too far ahead)"
	       << dendl;
      cancel_probe_timeout();
      sync_start(other, false);
      return;
    }
  }

  // did the existing cluster complete upgrade to luminous?
  if (osdmon()->osdmap.get_epoch()) {
    if (osdmon()->osdmap.require_osd_release < ceph_release_t::luminous) {
      derr << __func__ << " existing cluster has not completed upgrade to"
	   << " luminous; 'ceph osd require_osd_release luminous' before"
	   << " upgrading" << dendl;
      exit(0);
    }
    if (!osdmon()->osdmap.test_flag(CEPH_OSDMAP_PURGED_SNAPDIRS) ||
	!osdmon()->osdmap.test_flag(CEPH_OSDMAP_RECOVERY_DELETES)) {
      derr << __func__ << " existing cluster has not completed a full luminous"
	   << " scrub to purge legacy snapdir objects; please scrub before"
	   << " upgrading beyond luminous." << dendl;
      exit(0);
    }
  }

  // is there an existing quorum?
  if (m->quorum.size()) {
    dout(10) << " existing quorum " << m->quorum << dendl;

    dout(10) << " peer paxos version " << m->paxos_last_version
             << " vs my version " << paxos->get_version()
             << " (ok)"
             << dendl;
    bool in_map = false;
    const auto my_info = monmap->mon_info.find(name);
    const map<string,string> *map_crush_loc{nullptr};
    if (my_info != monmap->mon_info.end()) {
      in_map = true;
      map_crush_loc = &my_info->second.crush_loc;
    }
    if (in_map &&
	!monmap->get_addrs(name).front().is_blank_ip() &&
	(!need_set_crush_loc || (*map_crush_loc == crush_loc))) {
      // i'm part of the cluster; just initiate a new election
      start_election();
    } else {
      dout(10) << " ready to join, but i'm not in the monmap/"
	"my addr is blank/location is wrong, trying to join" << dendl;
      send_mon_message(new MMonJoin(monmap->fsid, name,
				    messenger->get_myaddrs(), crush_loc,
				    need_set_crush_loc),
		       m->leader);
    }
  } else {
    if (monmap->contains(m->name)) {
      dout(10) << " mon." << m->name << " is outside the quorum" << dendl;
      outside_quorum.insert(m->name);
    } else {
      dout(10) << " mostly ignoring mon." << m->name << ", not part of monmap" << dendl;
      return;
    }

    unsigned need = monmap->min_quorum_size();
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

  clog->info() << "mon." << name << " calling monitor election";
  elector.call_election();
}

void Monitor::win_standalone_election()
{
  dout(1) << "win_standalone_election" << dendl;

  // bump election epoch, in case the previous epoch included other
  // monitors; we need to be able to make the distinction.
  elector.declare_standalone_victory();

  rank = monmap->get_rank(name);
  ceph_assert(rank == 0);
  set<int> q;
  q.insert(rank);

  map<int,Metadata> metadata;
  collect_metadata(&metadata[0]);

  win_election(elector.get_epoch(), q,
               CEPH_FEATURES_ALL,
               ceph::features::mon::get_supported(),
	       ceph_release(),
	       metadata);
}

const utime_t& Monitor::get_leader_since() const
{
  ceph_assert(state == STATE_LEADER);
  return leader_since;
}

epoch_t Monitor::get_epoch()
{
  return elector.get_epoch();
}

void Monitor::_finish_svc_election()
{
  ceph_assert(state == STATE_LEADER || state == STATE_PEON);

  for (auto& svc : paxos_service) {
    // we already called election_finished() on monmon(); avoid callig twice
    if (state == STATE_LEADER && svc.get() == monmon())
      continue;
    svc->election_finished();
  }
}

void Monitor::win_election(epoch_t epoch, const set<int>& active, uint64_t features,
                           const mon_feature_t& mon_features,
			   ceph_release_t min_mon_release,
			   const map<int,Metadata>& metadata)
{
  dout(10) << __func__ << " epoch " << epoch << " quorum " << active
	   << " features " << features
           << " mon_features " << mon_features
	   << " min_mon_release " << min_mon_release
           << dendl;
  ceph_assert(is_electing());
  state = STATE_LEADER;
  leader_since = ceph_clock_now();
  quorum_since = mono_clock::now();
  leader = rank;
  quorum = active;
  quorum_con_features = features;
  quorum_mon_features = mon_features;
  quorum_min_mon_release = min_mon_release;
  pending_metadata = metadata;
  outside_quorum.clear();

  clog->info() << "mon." << name << " is new leader, mons " << get_quorum_names()
      << " in quorum (ranks " << quorum << ")";

  set_leader_commands(get_local_commands(mon_features));

  paxos->leader_init();
  // NOTE: tell monmap monitor first.  This is important for the
  // bootstrap case to ensure that the very first paxos proposal
  // codifies the monmap.  Otherwise any manner of chaos can ensue
  // when monitors are call elections or participating in a paxos
  // round without agreeing on who the participants are.
  monmon()->election_finished();
  _finish_svc_election();

  logger->inc(l_mon_election_win);

  // inject new metadata in first transaction.
  {
    // include previous metadata for missing mons (that aren't part of
    // the current quorum).
    map<int,Metadata> m = metadata;
    for (unsigned rank = 0; rank < monmap->size(); ++rank) {
      if (m.count(rank) == 0 &&
	  mon_metadata.count(rank)) {
	m[rank] = mon_metadata[rank];
      }
    }

    // FIXME: This is a bit sloppy because we aren't guaranteed to submit
    // a new transaction immediately after the election finishes.  We should
    // do that anyway for other reasons, though.
    MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
    bufferlist bl;
    encode(m, bl);
    t->put(MONITOR_STORE_PREFIX, "last_metadata", bl);
  }

  finish_election();
  if (monmap->size() > 1 &&
      monmap->get_epoch() > 0) {
    timecheck_start();
    health_tick_start();

    // Freshen the health status before doing health_to_clog in case
    // our just-completed election changed the health
    healthmon()->wait_for_active_ctx(new LambdaContext([this](int r){
      dout(20) << "healthmon now active" << dendl;
      healthmon()->tick();
      if (healthmon()->is_proposing()) {
        dout(20) << __func__ << " healthmon proposing, waiting" << dendl;
        healthmon()->wait_for_finished_proposal(nullptr, new C_MonContext{this,
              [this](int r){
                ceph_assert(ceph_mutex_is_locked_by_me(lock));
                do_health_to_clog_interval();
              }});

      } else {
        do_health_to_clog_interval();
      }
    }));

    scrub_event_start();
  }
}

void Monitor::lose_election(epoch_t epoch, set<int> &q, int l,
                            uint64_t features,
                            const mon_feature_t& mon_features,
			    ceph_release_t min_mon_release)
{
  state = STATE_PEON;
  leader_since = utime_t();
  quorum_since = mono_clock::now();
  leader = l;
  quorum = q;
  outside_quorum.clear();
  quorum_con_features = features;
  quorum_mon_features = mon_features;
  quorum_min_mon_release = min_mon_release;
  dout(10) << "lose_election, epoch " << epoch << " leader is mon" << leader
	   << " quorum is " << quorum << " features are " << quorum_con_features
           << " mon_features are " << quorum_mon_features
	   << " min_mon_release " << min_mon_release
           << dendl;

  paxos->peon_init();
  _finish_svc_election();

  logger->inc(l_mon_election_lose);

  finish_election();
}

namespace {
std::string collect_compression_algorithms()
{
  ostringstream os;
  bool printed = false;
  for (auto [name, key] : Compressor::compression_algorithms) {
    if (printed) {
      os << ", ";
    } else {
      printed = true;
    }
    std::ignore = key;
    os << name;
  }
  return os.str();
}
}

void Monitor::collect_metadata(Metadata *m)
{
  collect_sys_info(m, g_ceph_context);
  (*m)["addrs"] = stringify(messenger->get_myaddrs());
  (*m)["compression_algorithms"] = collect_compression_algorithms();

  // infer storage device
  string devname = store->get_devname();
  set<string> devnames;
  get_raw_devices(devname, &devnames);
  map<string,string> errs;
  get_device_metadata(devnames, m, &errs);
  for (auto& i : errs) {
    dout(1) << __func__ << " " << i.first << ": " << i.second << dendl;
  }

  string ceph_version_when_created;
  int r = store->read_meta("ceph_version_when_created", &ceph_version_when_created);
  if (r < 0 || ceph_version_when_created.empty()) {
    ceph_version_when_created = "";
  }
  (*m)["ceph_version_when_created"] = ceph_version_when_created;
  string created_at;
  r = store->read_meta("created_at", &created_at);
  if (r < 0 || created_at.empty()) {
    created_at = "";
  }
  (*m)["created_at"] = created_at;
}

void Monitor::finish_election()
{
  apply_quorum_to_compatset_features();
  apply_monmap_to_compatset_features();
  timecheck_finish();
  exited_quorum = utime_t();
  finish_contexts(g_ceph_context, waitfor_quorum);
  finish_contexts(g_ceph_context, maybe_wait_for_quorum);
  resend_routed_requests();
  update_logger();
  register_cluster_logger();

  // enable authentication
  {
    std::lock_guard l(auth_lock);
    authmon()->_set_mon_num_rank(monmap->size(), rank);
  }

  // am i named and located properly?
  string cur_name = monmap->get_name(messenger->get_myaddrs());
  const auto my_infop = monmap->mon_info.find(cur_name);
  const map<string,string>& map_crush_loc = my_infop->second.crush_loc;
  
  if (cur_name != name ||
      (need_set_crush_loc && map_crush_loc != crush_loc)) {
    dout(10) << " renaming/moving myself from " << cur_name << "/"
	     << map_crush_loc <<" -> " << name << "/" << crush_loc << dendl;
    send_mon_message(new MMonJoin(monmap->fsid, name, messenger->get_myaddrs(),
				  crush_loc, need_set_crush_loc),
		     leader);
    return;
  }
  do_stretch_mode_election_work();
}

void Monitor::_apply_compatset_features(CompatSet &new_features)
{
  if (new_features.compare(features) != 0) {
    CompatSet diff = features.unsupported(new_features);
    dout(1) << __func__ << " enabling new quorum features: " << diff << dendl;
    features = new_features;

    auto t = std::make_shared<MonitorDBStore::Transaction>();
    write_features(t);
    store->apply_transaction(t);

    calc_quorum_requirements();
  }
}

void Monitor::apply_quorum_to_compatset_features()
{
  CompatSet new_features(features);
  new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSD_ERASURE_CODES);
  if (quorum_con_features & CEPH_FEATURE_OSDMAP_ENC) {
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC);
  }
  new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V2);
  new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_ERASURE_CODE_PLUGINS_V3);
  dout(5) << __func__ << dendl;
  _apply_compatset_features(new_features);
}

void Monitor::apply_monmap_to_compatset_features()
{
  CompatSet new_features(features);
  mon_feature_t monmap_features = monmap->get_required_features();

  /* persistent monmap features may go into the compatset.
   * optional monmap features may not - why?
   *   because optional monmap features may be set/unset by the admin,
   *   and possibly by other means that haven't yet been thought out,
   *   so we can't make the monitor enforce them on start - because they
   *   may go away.
   *   this, of course, does not invalidate setting a compatset feature
   *   for an optional feature - as long as you make sure to clean it up
   *   once you unset it.
   */
  if (monmap_features.contains_all(ceph::features::mon::FEATURE_KRAKEN)) {
    ceph_assert(ceph::features::mon::get_persistent().contains_all(
           ceph::features::mon::FEATURE_KRAKEN));
    // this feature should only ever be set if the quorum supports it.
    ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_KRAKEN));
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_KRAKEN);
  }
  if (monmap_features.contains_all(ceph::features::mon::FEATURE_LUMINOUS)) {
    ceph_assert(ceph::features::mon::get_persistent().contains_all(
           ceph::features::mon::FEATURE_LUMINOUS));
    // this feature should only ever be set if the quorum supports it.
    ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_LUMINOUS));
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_LUMINOUS);
  }
  if (monmap_features.contains_all(ceph::features::mon::FEATURE_MIMIC)) {
    ceph_assert(ceph::features::mon::get_persistent().contains_all(
           ceph::features::mon::FEATURE_MIMIC));
    // this feature should only ever be set if the quorum supports it.
    ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_MIMIC));
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_MIMIC);
  }
  if (monmap_features.contains_all(ceph::features::mon::FEATURE_NAUTILUS)) {
    ceph_assert(ceph::features::mon::get_persistent().contains_all(
           ceph::features::mon::FEATURE_NAUTILUS));
    // this feature should only ever be set if the quorum supports it.
    ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_NAUTILUS));
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_NAUTILUS);
  }
  if (monmap_features.contains_all(ceph::features::mon::FEATURE_OCTOPUS)) {
    ceph_assert(ceph::features::mon::get_persistent().contains_all(
           ceph::features::mon::FEATURE_OCTOPUS));
    // this feature should only ever be set if the quorum supports it.
    ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_OCTOPUS));
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_OCTOPUS);
  }
  if (monmap_features.contains_all(ceph::features::mon::FEATURE_PACIFIC)) {
    ceph_assert(ceph::features::mon::get_persistent().contains_all(
           ceph::features::mon::FEATURE_PACIFIC));
    // this feature should only ever be set if the quorum supports it.
    ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_PACIFIC));
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_PACIFIC);
  }
  if (monmap_features.contains_all(ceph::features::mon::FEATURE_QUINCY)) {
    ceph_assert(ceph::features::mon::get_persistent().contains_all(
           ceph::features::mon::FEATURE_QUINCY));
    // this feature should only ever be set if the quorum supports it.
    ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_QUINCY));
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_QUINCY);
  }
  if (monmap_features.contains_all(ceph::features::mon::FEATURE_REEF)) {
    ceph_assert(ceph::features::mon::get_persistent().contains_all(
           ceph::features::mon::FEATURE_REEF));
    // this feature should only ever be set if the quorum supports it.
    ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_REEF));
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_REEF);
  }
  if (monmap_features.contains_all(ceph::features::mon::FEATURE_SQUID)) {
    ceph_assert(ceph::features::mon::get_persistent().contains_all(
           ceph::features::mon::FEATURE_SQUID));
    // this feature should only ever be set if the quorum supports it.
    ceph_assert(HAVE_FEATURE(quorum_con_features, SERVER_SQUID));
    new_features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_SQUID);
  }

  dout(5) << __func__ << dendl;
  _apply_compatset_features(new_features);
}

void Monitor::calc_quorum_requirements()
{
  required_features = 0;

  // compatset
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_OSDMAP_ENC)) {
    required_features |= CEPH_FEATURE_OSDMAP_ENC;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_KRAKEN)) {
    required_features |= CEPH_FEATUREMASK_SERVER_KRAKEN;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_LUMINOUS)) {
    required_features |= CEPH_FEATUREMASK_SERVER_LUMINOUS;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_MIMIC)) {
    required_features |= CEPH_FEATUREMASK_SERVER_MIMIC;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_NAUTILUS)) {
    required_features |= CEPH_FEATUREMASK_SERVER_NAUTILUS |
      CEPH_FEATUREMASK_CEPHX_V2;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_OCTOPUS)) {
    required_features |= CEPH_FEATUREMASK_SERVER_OCTOPUS;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_PACIFIC)) {
    required_features |= CEPH_FEATUREMASK_SERVER_PACIFIC;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_QUINCY)) {
    required_features |= CEPH_FEATUREMASK_SERVER_QUINCY;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_REEF)) {
    required_features |= CEPH_FEATUREMASK_SERVER_REEF;
  }
  if (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_SQUID)) {
    required_features |= CEPH_FEATUREMASK_SERVER_SQUID;
  }

  // monmap
  if (monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_KRAKEN)) {
    required_features |= CEPH_FEATUREMASK_SERVER_KRAKEN;
  }
  if (monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS)) {
    required_features |= CEPH_FEATUREMASK_SERVER_LUMINOUS;
  }
  if (monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_MIMIC)) {
    required_features |= CEPH_FEATUREMASK_SERVER_MIMIC;
  }
  if (monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_NAUTILUS)) {
    required_features |= CEPH_FEATUREMASK_SERVER_NAUTILUS |
      CEPH_FEATUREMASK_CEPHX_V2;
  }
  dout(10) << __func__ << " required_features " << required_features << dendl;
}

void Monitor::get_combined_feature_map(FeatureMap *fm)
{
  *fm += session_map.feature_map;
  for (auto id : quorum) {
    if (id != rank) {
      *fm += quorum_feature_map[id];
    }
  }
}

void Monitor::sync_force(Formatter *f)
{
  auto tx(std::make_shared<MonitorDBStore::Transaction>());
  sync_stash_critical_state(tx);
  tx->put("mon_sync", "force_sync", 1);
  store->apply_transaction(tx);

  f->open_object_section("sync_force");
  f->dump_int("ret", 0);
  f->dump_stream("msg") << "forcing store sync the next time the monitor starts";
  f->close_section(); // sync_force
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

  f->dump_string("quorum_leader_name", quorum.empty() ? string() : monmap->get_name(leader));

  if (!quorum.empty()) {
    f->dump_int(
      "quorum_age",
      quorum_age());
  }

  f->open_object_section("features");
  f->dump_stream("quorum_con") << quorum_con_features;
  quorum_mon_features.dump(f, "quorum_mon");
  f->close_section();

  f->open_object_section("monmap");
  monmap->dump(f);
  f->close_section(); // monmap

  f->close_section(); // quorum_status
  f->flush(ss);
  if (free_formatter)
    delete f;
}

void Monitor::get_mon_status(Formatter *f)
{
  f->open_object_section("mon_status");
  f->dump_string("name", name);
  f->dump_int("rank", rank);
  f->dump_string("state", get_state_name());
  f->dump_int("election_epoch", get_epoch());
  f->dump_int("uptime", get_uptime().count());

  f->open_array_section("quorum");
  for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p) {
    f->dump_int("mon", *p);
  }
  f->close_section(); // quorum

  if (!quorum.empty()) {
    f->dump_int(
      "quorum_age",
      quorum_age());
  }

  f->open_object_section("features");
  f->dump_stream("required_con") << required_features;
  mon_feature_t req_mon_features = get_required_mon_features();
  req_mon_features.dump(f, "required_mon");
  f->dump_stream("quorum_con") << quorum_con_features;
  quorum_mon_features.dump(f, "quorum_mon");
  f->close_section(); // features

  f->open_array_section("outside_quorum");
  for (set<string>::iterator p = outside_quorum.begin(); p != outside_quorum.end(); ++p)
    f->dump_string("mon", *p);
  f->close_section(); // outside_quorum

  f->open_array_section("extra_probe_peers");
  for (set<entity_addrvec_t>::iterator p = extra_probe_peers.begin();
       p != extra_probe_peers.end();
       ++p) {
    f->dump_object("peer", *p);
  }
  f->close_section(); // extra_probe_peers

  f->open_array_section("sync_provider");
  for (map<uint64_t,SyncProvider>::const_iterator p = sync_providers.begin();
       p != sync_providers.end();
       ++p) {
    f->dump_unsigned("cookie", p->second.cookie);
    f->dump_object("addrs", p->second.addrs);
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

  if (g_conf()->mon_sync_provider_kill_at > 0)
    f->dump_int("provider_kill_at", g_conf()->mon_sync_provider_kill_at);
  if (g_conf()->mon_sync_requester_kill_at > 0)
    f->dump_int("requester_kill_at", g_conf()->mon_sync_requester_kill_at);

  f->open_object_section("monmap");
  monmap->dump(f);
  f->close_section();

  f->dump_object("feature_map", session_map.feature_map);
  f->dump_bool("stretch_mode", stretch_mode_engaged);
  f->close_section(); // mon_status
}


// health status to clog

void Monitor::health_tick_start()
{
  if (!cct->_conf->mon_health_to_clog ||
      cct->_conf->mon_health_to_clog_tick_interval <= 0)
    return;

  dout(15) << __func__ << dendl;

  health_tick_stop();
  health_tick_event = timer.add_event_after(
    cct->_conf->mon_health_to_clog_tick_interval,
    new C_MonContext{this, [this](int r) {
	if (r < 0)
	  return;
	health_tick_start();
      }});
}

void Monitor::health_tick_stop()
{
  dout(15) << __func__ << dendl;

  if (health_tick_event) {
    timer.cancel_event(health_tick_event);
    health_tick_event = NULL;
  }
}

ceph::real_clock::time_point Monitor::health_interval_calc_next_update()
{
  auto now = ceph::real_clock::now();

  auto secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
  int remainder = secs.count() % cct->_conf->mon_health_to_clog_interval;
  int adjustment = cct->_conf->mon_health_to_clog_interval - remainder;
  auto next = secs + std::chrono::seconds(adjustment);

  dout(20) << __func__
    << " now: " << now << ","
    << " next: " << next << ","
    << " interval: " << cct->_conf->mon_health_to_clog_interval
    << dendl;

  return ceph::real_clock::time_point{next};
}

void Monitor::health_interval_start()
{
  dout(15) << __func__ << dendl;

  if (!cct->_conf->mon_health_to_clog ||
      cct->_conf->mon_health_to_clog_interval <= 0) {
    return;
  }

  health_interval_stop();
  auto next = health_interval_calc_next_update();
  health_interval_event = new C_MonContext{this, [this](int r) {
      if (r < 0)
        return;
      do_health_to_clog_interval();
    }};
  if (!timer.add_event_at(next, health_interval_event)) {
    health_interval_event = nullptr;
  }
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
      return;
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

  string summary;
  health_status_t level = healthmon()->get_health_status(false, nullptr, &summary);
  if (!force &&
      summary == health_status_cache.summary &&
      level == health_status_cache.overall)
    return;

  if (g_conf()->mon_health_detail_to_clog &&
      summary != health_status_cache.summary &&
      level != HEALTH_OK) {
    string details;
    level = healthmon()->get_health_status(true, nullptr, &details);
    clog->health(level) << "Health detail: " << details;
  } else {
    clog->health(level) << "overall " << summary;
  }
  health_status_cache.summary = summary;
  health_status_cache.overall = level;
}

void Monitor::log_health(
  const health_check_map_t& updated,
  const health_check_map_t& previous,
  MonitorDBStore::TransactionRef t)
{
  if (!g_conf()->mon_health_to_clog) {
    return;
  }

  const utime_t now = ceph_clock_now();

  // FIXME: log atomically as part of @t instead of using clog.
  dout(10) << __func__ << " updated " << updated.checks.size()
	   << " previous " << previous.checks.size()
	   << dendl;
  const auto min_log_period = g_conf().get_val<int64_t>(
      "mon_health_log_update_period");
  for (auto& p : updated.checks) {
    auto q = previous.checks.find(p.first);
    bool logged = false;
    if (q == previous.checks.end()) {
      // new
      ostringstream ss;
      ss << "Health check failed: " << p.second.summary << " ("
         << p.first << ")";
      clog->health(p.second.severity) << ss.str();

      logged = true;
    } else {
      if (p.second.summary != q->second.summary ||
	  p.second.severity != q->second.severity) {

        auto status_iter = health_check_log_times.find(p.first);
        if (status_iter != health_check_log_times.end()) {
          if (p.second.severity == q->second.severity &&
              now - status_iter->second.updated_at < min_log_period) {
            // We already logged this recently and the severity is unchanged,
            // so skip emitting an update of the summary string.
            // We'll get an update out of tick() later if the check
            // is still failing.
            continue;
          }
        }

        // summary or severity changed (ignore detail changes at this level)
        ostringstream ss;
        ss << "Health check update: " << p.second.summary << " (" << p.first << ")";
        clog->health(p.second.severity) << ss.str();

        logged = true;
      }
    }
    // Record the time at which we last logged, so that we can check this
    // when considering whether/when to print update messages.
    if (logged) {
      auto iter = health_check_log_times.find(p.first);
      if (iter == health_check_log_times.end()) {
        health_check_log_times.emplace(p.first, HealthCheckLogStatus(
          p.second.severity, p.second.summary, now));
      } else {
        iter->second = HealthCheckLogStatus(
          p.second.severity, p.second.summary, now);
      }
    }
  }
  for (auto& p : previous.checks) {
    if (!updated.checks.count(p.first)) {
      // cleared
      ostringstream ss;
      if (p.first == "DEGRADED_OBJECTS") {
        clog->info() << "All degraded objects recovered";
      } else if (p.first == "OSD_FLAGS") {
        clog->info() << "OSD flags cleared";
      } else {
        clog->info() << "Health check cleared: " << p.first << " (was: "
                     << p.second.summary << ")";
      }

      if (health_check_log_times.count(p.first)) {
        health_check_log_times.erase(p.first);
      }
    }
  }

  if (previous.checks.size() && updated.checks.size() == 0) {
    // We might be going into a fully healthy state, check
    // other subsystems
    bool any_checks = false;
    for (auto& svc : paxos_service) {
      if (&(svc->get_health_checks()) == &(previous)) {
        // Ignore the ones we're clearing right now
        continue;
      }

      if (svc->get_health_checks().checks.size() > 0) {
        any_checks = true;
        break;
      }
    }
    if (!any_checks) {
      clog->info() << "Cluster is now healthy";
    }
  }
}

void Monitor::update_pending_metadata()
{
  Metadata metadata;
  collect_metadata(&metadata);
  size_t version_size = mon_metadata[rank]["ceph_version_short"].size();
  const std::string current_version = mon_metadata[rank]["ceph_version_short"];
  const std::string pending_version = metadata["ceph_version_short"];

  if (current_version.compare(0, version_size, pending_version) != 0) {
    mgr_client.update_daemon_metadata("mon", name, metadata);
  }
}

void Monitor::get_cluster_status(stringstream &ss, Formatter *f,
				 MonSession *session)
{
  if (f)
    f->open_object_section("status");

  const auto&& fs_names = session->get_allowed_fs_names();

  if (f) {
    f->dump_stream("fsid") << monmap->get_fsid();
    healthmon()->get_health_status(false, f, nullptr);
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
      f->dump_int(
	"quorum_age",
        quorum_age());
    }
    f->open_object_section("monmap");
    monmap->dump_summary(f);
    f->close_section();
    f->open_object_section("osdmap");
    osdmon()->osdmap.print_summary(f, cout, string(12, ' '));
    f->close_section();
    f->open_object_section("pgmap");
    mgrstatmon()->print_summary(f, NULL);
    f->close_section();
    f->open_object_section("fsmap");

    FSMap fsmap_copy = mdsmon()->get_fsmap();
    if (!fs_names.empty()) {
      fsmap_copy.filter(fs_names);
    }
    const FSMap *fsmapp = &fsmap_copy;

    fsmapp->print_summary(f, NULL);
    f->close_section();
    f->open_object_section("mgrmap");
    mgrmon()->get_map().print_summary(f, nullptr);
    f->close_section();

    f->dump_object("servicemap", mgrstatmon()->get_service_map());

    f->open_object_section("progress_events");
    for (auto& i : mgrstatmon()->get_progress_events()) {
      f->dump_object(i.first.c_str(), i.second);
    }
    f->close_section();

    f->close_section();
  } else {
    ss << "  cluster:\n";
    ss << "    id:     " << monmap->get_fsid() << "\n";

    string health;
    healthmon()->get_health_status(false, nullptr, &health,
				   "\n            ", "\n            ");
    ss << "    health: " << health << "\n";

    ss << "\n \n  services:\n";
    {
      size_t maxlen = 3;
      auto& service_map = mgrstatmon()->get_service_map();
      for (auto& p : service_map.services) {
	maxlen = std::max(maxlen, p.first.size());
      }
      string spacing(maxlen - 3, ' ');
      const auto quorum_names = get_quorum_names();
      const auto mon_count = monmap->mon_info.size();
      auto mnow = ceph::mono_clock::now();
      ss << "    mon: " << spacing << mon_count << " daemons, quorum "
	 << quorum_names << " (age " << timespan_str(mnow - quorum_since) << ")";
      if (quorum_names.size() != mon_count) {
	std::list<std::string> out_of_q;
	for (size_t i = 0; i < monmap->ranks.size(); ++i) {
	  if (quorum.count(i) == 0) {
	    out_of_q.push_back(monmap->ranks[i]);
	  }
	}
	ss << ", out of quorum: " << joinify(out_of_q.begin(),
					     out_of_q.end(), std::string(", "));
      }
      ss << "\n";
      if (mgrmon()->in_use()) {
	ss << "    mgr: " << spacing;
	mgrmon()->get_map().print_summary(nullptr, &ss);
	ss << "\n";
      }

      FSMap fsmap_copy = mdsmon()->get_fsmap();
      if (!fs_names.empty()) {
	fsmap_copy.filter(fs_names);
      }
      const FSMap *fsmapp = &fsmap_copy;

      if (fsmapp->filesystem_count() > 0 and mdsmon()->should_print_status()){
        ss << "    mds: " << spacing;
	fsmapp->print_daemon_summary(ss);
	ss << "\n";
      }

      ss << "    osd: " << spacing;
      osdmon()->osdmap.print_summary(NULL, ss, string(maxlen + 6, ' '));
      ss << "\n";
      for (auto& p : service_map.services) {
        const std::string &service = p.first;
        // filter out normal ceph entity types
        if (ServiceMap::is_normal_ceph_entity(service)) {
          continue;
        }
	ss << "    " << p.first << ": " << string(maxlen - p.first.size(), ' ')
	   << p.second.get_summary() << "\n";
      }
    }

    if (auto& service_map = mgrstatmon()->get_service_map();
        std::any_of(service_map.services.begin(),
                    service_map.services.end(),
                    [](auto& service) {
                      return service.second.has_running_tasks();
                    })) {
      ss << "\n \n  task status:\n";
      for (auto& [name, service] : service_map.services) {
	ss << service.get_task_summary(name);
      }
    }

    ss << "\n \n  data:\n";
    mdsmon()->print_fs_summary(ss);
    mgrstatmon()->print_summary(NULL, &ss);

    auto& pem = mgrstatmon()->get_progress_events();
    if (!pem.empty()) {
      ss << "\n \n  progress:\n";
      for (auto& i : pem) {
	if (i.second.add_to_ceph_s){
	ss << "    " << i.second.message << "\n";
	}
      }
    }
    ss << "\n ";
  }
}

void Monitor::_generate_command_map(cmdmap_t& cmdmap,
                                    map<string,string> &param_str_map)
{
  for (auto p = cmdmap.begin(); p != cmdmap.end(); ++p) {
    if (p->first == "prefix")
      continue;
    if (p->first == "caps") {
      vector<string> cv;
      if (cmd_getval(cmdmap, "caps", cv) &&
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

const MonCommand *Monitor::_get_moncommand(
  const string &cmd_prefix,
  const vector<MonCommand>& cmds)
{
  for (auto& c : cmds) {
    if (c.cmdstring.compare(0, cmd_prefix.size(), cmd_prefix) == 0) {
      return &c;
    }
  }
  return nullptr;
}

bool Monitor::_allowed_command(MonSession *s, const string &module,
			       const string &prefix, const cmdmap_t& cmdmap,
                               const map<string,string>& param_str_map,
                               const MonCommand *this_cmd) {

  bool cmd_r = this_cmd->requires_perm('r');
  bool cmd_w = this_cmd->requires_perm('w');
  bool cmd_x = this_cmd->requires_perm('x');

  bool capable = s->caps.is_capable(
    g_ceph_context,
    s->entity_name,
    module, prefix, param_str_map,
    cmd_r, cmd_w, cmd_x,
    s->get_peer_socket_addr());

  dout(10) << __func__ << " " << (capable ? "" : "not ") << "capable" << dendl;
  return capable;
}

void Monitor::format_command_descriptions(const std::vector<MonCommand> &commands,
					  Formatter *f,
					  uint64_t features,
					  bufferlist *rdata)
{
  int cmdnum = 0;
  f->open_object_section("command_descriptions");
  for (const auto &cmd : commands) {
    unsigned flags = cmd.flags;
    ostringstream secname;
    secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
    dump_cmddesc_to_json(f, features, secname.str(),
			 cmd.cmdstring, cmd.helpstring, cmd.module,
			 cmd.req_perms, flags);
    cmdnum++;
  }
  f->close_section();	// command_descriptions

  f->flush(*rdata);
}

bool Monitor::is_keyring_required()
{
  return auth_cluster_required.is_supported_auth(CEPH_AUTH_CEPHX) || 
         auth_service_required.is_supported_auth(CEPH_AUTH_CEPHX) || 
         auth_cluster_required.is_supported_auth(CEPH_AUTH_GSS)   || 
         auth_service_required.is_supported_auth(CEPH_AUTH_GSS);
}

struct C_MgrProxyCommand : public Context {
  Monitor *mon;
  MonOpRequestRef op;
  uint64_t size;
  bufferlist outbl;
  string outs;
  C_MgrProxyCommand(Monitor *mon, MonOpRequestRef op, uint64_t s)
    : mon(mon), op(op), size(s) { }
  void finish(int r) {
    std::lock_guard l(mon->lock);
    mon->mgr_proxy_bytes -= size;
    mon->reply_command(op, r, outs, outbl, 0);
  }
};

void Monitor::handle_tell_command(MonOpRequestRef op)
{
  ceph_assert(op->is_type_command());
  MCommand *m = static_cast<MCommand*>(op->get_req());
  if (m->fsid != monmap->fsid) {
    dout(0) << "handle_command on fsid " << m->fsid << " != " << monmap->fsid << dendl;
    return reply_tell_command(op, -EACCES, "wrong fsid");
  }
  MonSession *session = op->get_session();
  if (!session) {
    dout(5) << __func__ << " dropping stray message " << *m << dendl;
    return;
  }
  cmdmap_t cmdmap;
  if (stringstream ss; !cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    return reply_tell_command(op, -EINVAL, ss.str());
  }
  map<string,string> param_str_map;
  _generate_command_map(cmdmap, param_str_map);
  string prefix;
  if (!cmd_getval(cmdmap, "prefix", prefix)) {
    return reply_tell_command(op, -EINVAL, "no prefix");
  }
  if (auto cmd = _get_moncommand(prefix,
				 get_local_commands(quorum_mon_features));
      cmd) {
    if (cmd->is_obsolete() ||
	(cct->_conf->mon_debug_deprecated_as_obsolete &&
	 cmd->is_deprecated())) {
      return reply_tell_command(op, -ENOTSUP,
				"command is obsolete; "
				"please check usage and/or man page");
    }
  }
  // see if command is allowed
  if (!session->caps.is_capable(
      g_ceph_context,
      session->entity_name,
      "mon", prefix, param_str_map,
      true, true, true,
      session->get_peer_socket_addr())) {
    return reply_tell_command(op, -EACCES, "insufficient caps");
  }
  // pass it to asok
  cct->get_admin_socket()->queue_tell_command(m);
}

void Monitor::handle_command(MonOpRequestRef op)
{
  ceph_assert(op->is_type_command());
  auto m = op->get_req<MMonCommand>();
  if (m->fsid != monmap->fsid) {
    dout(0) << "handle_command on fsid " << m->fsid << " != " << monmap->fsid
	    << dendl;
    reply_command(op, -EPERM, "wrong fsid", 0);
    return;
  }

  MonSession *session = op->get_session();
  if (!session) {
    dout(5) << __func__ << " dropping stray message " << *m << dendl;
    return;
  }

  if (m->cmd.empty()) {
    reply_command(op, -EINVAL, "no command specified", 0);
    return;
  }

  string prefix;
  vector<string> fullcmd;
  cmdmap_t cmdmap;
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

  // check return value. If no prefix parameter provided,
  // return value will be false, then return error info.
  if (!cmd_getval(cmdmap, "prefix", prefix)) {
    reply_command(op, -EINVAL, "command prefix not found", 0);
    return;
  }

  // check prefix is empty
  if (prefix.empty()) {
    reply_command(op, -EINVAL, "command prefix must not be empty", 0);
    return;
  }

  if (prefix == "get_command_descriptions") {
    bufferlist rdata;
    Formatter *f = Formatter::create("json");

    std::vector<MonCommand> commands = static_cast<MgrMonitor*>(
        paxos_service[PAXOS_MGR].get())->get_command_descs();

    for (auto& c : leader_mon_commands) {
      commands.push_back(c);
    }

    auto features = m->get_connection()->get_features();
    format_command_descriptions(commands, f, features, &rdata);
    delete f;
    reply_command(op, 0, "", rdata, 0);
    return;
  }

  dout(0) << "handle_command " << *m << dendl;

  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  get_str_vec(prefix, fullcmd);

  // make sure fullcmd is not empty.
  // invalid prefix will cause empty vector fullcmd.
  // such as, prefix=";,,;"
  if (fullcmd.empty()) {
    reply_command(op, -EINVAL, "command requires a prefix to be valid", 0);
    return;
  }

  std::string_view module = fullcmd[0];

  // validate command is in leader map

  const MonCommand *leader_cmd;
  const auto& mgr_cmds = mgrmon()->get_command_descs();
  const MonCommand *mgr_cmd = nullptr;
  if (!mgr_cmds.empty()) {
    mgr_cmd = _get_moncommand(prefix, mgr_cmds);
  }
  leader_cmd = _get_moncommand(prefix, leader_mon_commands);
  if (!leader_cmd) {
    leader_cmd = mgr_cmd;
    if (!leader_cmd) {
      reply_command(op, -EINVAL, "command not known", 0);
      return;
    }
  }
  // validate command is in our map & matches, or forward if it is allowed
  const MonCommand *mon_cmd = _get_moncommand(
    prefix,
    get_local_commands(quorum_mon_features));
  if (!mon_cmd) {
    mon_cmd = mgr_cmd;
  }
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

  // Catch bad_cmd_get exception if _generate_command_map() throws it
  try {
    _generate_command_map(cmdmap, param_str_map);
  } catch (const bad_cmd_get& e) {
    reply_command(op, -EINVAL, e.what(), 0);
    return;
  }

  if (!_allowed_command(session, service, prefix, cmdmap,
                        param_str_map, mon_cmd)) {
    dout(1) << __func__ << " access denied" << dendl;
    if (prefix != "config set" && prefix != "config-key set")
      (cmd_is_rw ? audit_clog->info() : audit_clog->debug())
        << "from='" << session->name << " " << session->addrs << "' "
        << "entity='" << session->entity_name << "' "
        << "cmd=" << m->cmd << ":  access denied";
    reply_command(op, -EACCES, "access denied", 0);
    return;
  }

  if (prefix != "config set" && prefix != "config-key set")
    (cmd_is_rw ? audit_clog->info() : audit_clog->debug())
        << "from='" << session->name << " " << session->addrs << "' "
        << "entity='" << session->entity_name << "' "
        << "cmd=" << m->cmd << ": dispatch";

  // compat kludge for legacy clients trying to tell commands that are
  // new.  see bottom of MonCommands.h.  we need to handle both (1)
  // pre-octopus clients and (2) octopus clients with a mix of pre-octopus
  // and octopus mons.
  if ((!HAVE_FEATURE(m->get_connection()->get_features(), SERVER_OCTOPUS) ||
       monmap->min_mon_release < ceph_release_t::octopus) &&
      (prefix == "injectargs" ||
       prefix == "smart" ||
       prefix == "mon_status" ||
       prefix == "heap")) {
    if (m->get_connection()->get_messenger() == 0) {
      // Prior to octopus, monitors might forward these messages
      // around. that was broken at baseline, and if we try to process
      // this message now, it will assert out when we try to send a
      // message in reply from the asok/tell worker (see
      // AnonConnection).  Just reply with an error.
      dout(5) << __func__ << " failing forwarded command from a (presumably) "
	      << "pre-octopus peer" << dendl;
      reply_command(
	op, -EBUSY,
	"failing forwarded tell command in mixed-version mon cluster", 0);
      return;
    }
    dout(5) << __func__ << " passing command to tell/asok" << dendl;
    cct->get_admin_socket()->queue_tell_command(m);
    return;
  }

  if (mon_cmd->is_mgr()) {
    const auto& hdr = m->get_header();
    uint64_t size = hdr.front_len + hdr.middle_len + hdr.data_len;
    uint64_t max = g_conf().get_val<Option::size_t>("mon_client_bytes")
                 * g_conf().get_val<double>("mon_mgr_proxy_client_bytes_ratio");
    if (mgr_proxy_bytes + size > max) {
      dout(10) << __func__ << " current mgr proxy bytes " << mgr_proxy_bytes
	       << " + " << size << " > max " << max << dendl;
      reply_command(op, -EAGAIN, "hit limit on proxied mgr commands", rdata, 0);
      return;
    }
    mgr_proxy_bytes += size;
    dout(10) << __func__ << " proxying mgr command (+" << size
	     << " -> " << mgr_proxy_bytes << ")" << dendl;
    C_MgrProxyCommand *fin = new C_MgrProxyCommand(this, op, size);
    mgr_client.start_command(m->cmd,
			     m->get_data(),
			     &fin->outbl,
			     &fin->outs,
			     new C_OnFinisher(fin, &finisher));
    return;
  }

  if ((module == "mds" || module == "fs")  &&
      prefix != "fs authorize") {
    mdsmon()->dispatch(op);
    return;
  }
  if ((module == "osd" ||
       prefix == "pg map" ||
       prefix == "pg repeer") &&
      prefix != "osd last-stat-seq") {
    osdmon()->dispatch(op);
    return;
  }
  if (module == "config") {
    configmon()->dispatch(op);
    return;
  }

  if (module == "mon" &&
      /* Let the Monitor class handle the following commands:
       *  'mon scrub'
       */
      prefix != "mon scrub" &&
      prefix != "mon metadata" &&
      prefix != "mon versions" &&
      prefix != "mon count-metadata" &&
      prefix != "mon ok-to-stop" &&
      prefix != "mon ok-to-add-offline" &&
      prefix != "mon ok-to-rm") {
    monmon()->dispatch(op);
    return;
  }
  if (module == "health" && prefix != "health") {
    healthmon()->dispatch(op);
    return;
  }
  if (module == "auth" || prefix == "fs authorize") {
    authmon()->dispatch(op);
    return;
  }
  if (module == "log") {
    logmon()->dispatch(op);
    return;
  }

  if (module == "config-key") {
    kvmon()->dispatch(op);
    return;
  }

  if (module == "mgr") {
    mgrmon()->dispatch(op);
    return;
  }
  if (module == "nvme-gw"){
      nvmegwmon()->dispatch(op);
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

  if (prefix == "mon scrub") {
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

  if (prefix == "time-sync-status") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    f->open_object_section("time_sync");
    if (!timecheck_skews.empty()) {
      f->open_object_section("time_skew_status");
      for (auto& i : timecheck_skews) {
	double skew = i.second;
	double latency = timecheck_latencies[i.first];
	string name = monmap->get_name(i.first);
	ostringstream tcss;
	health_status_t tcstatus = timecheck_status(tcss, skew, latency);
	f->open_object_section(name.c_str());
	f->dump_float("skew", skew);
	f->dump_float("latency", latency);
	f->dump_stream("health") << tcstatus;
	if (tcstatus != HEALTH_OK) {
	  f->dump_stream("details") << tcss.str();
	}
	f->close_section();
      }
      f->close_section();
    }
    f->open_object_section("timechecks");
    f->dump_unsigned("epoch", get_epoch());
    f->dump_int("round", timecheck_round);
    f->dump_stream("round_status") << ((timecheck_round%2) ?
				       "on-going" : "finished");
    f->close_section();
    f->close_section();
    f->flush(rdata);
    r = 0;
    rs = "";
  } else if (prefix == "status" ||
	     prefix == "health" ||
	     prefix == "df") {
    string detail;
    cmd_getval(cmdmap, "detail", detail);

    if (prefix == "status") {
      // get_cluster_status handles f == NULL
      get_cluster_status(ds, f.get(), session);

      if (f) {
        f->flush(ds);
        ds << '\n';
      }
      rdata.append(ds);
    } else if (prefix == "health") {
      string plain;
      healthmon()->get_health_status(detail == "detail", f.get(), f ? nullptr : &plain);
      if (f) {
	f->flush(ds);
	rdata.append(ds);
      } else {
	rdata.append(plain);
      }
    } else if (prefix == "df") {
      bool verbose = (detail == "detail");
      if (f)
        f->open_object_section("stats");

      mgrstatmon()->dump_cluster_stats(&ds, f.get(), verbose);
      if (!f) {
	ds << "\n \n";
      }
      mgrstatmon()->dump_pool_stats(osdmon()->osdmap, &ds, f.get(), verbose);

      if (f) {
        f->close_section();
        f->flush(ds);
        ds << '\n';
      }
    } else {
      ceph_abort_msg("We should never get here!");
      return;
    }
    rdata.append(ds);
    rs = "";
    r = 0;
  } else if (prefix == "report") {
    // some of the report data is only known by leader, e.g. osdmap_clean_epochs
    if (!is_leader() && !is_peon()) {
      dout(10) << " waiting for quorum" << dendl;
      waitfor_quorum.push_back(new C_RetryMessage(this, op));
      return;
    }
    if (!is_leader()) {
      forward_request_leader(op);
      return;
    }
    // this must be formatted, in its current form
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    f->open_object_section("report");
    f->dump_stream("cluster_fingerprint") << fingerprint;
    f->dump_string("version", ceph_version_to_str());
    f->dump_string("commit", git_version_to_str());
    f->dump_stream("timestamp") << ceph_clock_now();

    vector<string> tagsvec;
    cmd_getval(cmdmap, "tags", tagsvec);
    string tagstr = str_join(tagsvec, " ");
    if (!tagstr.empty())
      tagstr = tagstr.substr(0, tagstr.find_last_of(' '));
    f->dump_string("tag", tagstr);

    healthmon()->get_health_status(true, f.get(), nullptr);

    monmon()->dump_info(f.get());
    osdmon()->dump_info(f.get());
    mdsmon()->dump_info(f.get());
    authmon()->dump_info(f.get());
    mgrstatmon()->dump_info(f.get());
    logmon()->dump_info(f.get());

    paxos->dump_info(f.get());

    f->close_section();
    f->flush(rdata);

    ostringstream ss2;
    ss2 << "report " << rdata.crc32c(CEPH_MON_PORT_LEGACY);
    rs = ss2.str();
    r = 0;
  } else if (prefix == "osd last-stat-seq") {
    int64_t osd = 0;
    cmd_getval(cmdmap, "id", osd);
    uint64_t seq = mgrstatmon()->get_last_osd_stat_seq(osd);
    if (f) {
      f->dump_unsigned("seq", seq);
      f->flush(ds);
    } else {
      ds << seq;
      rdata.append(ds);
    }
    rs = "";
    r = 0;
  } else if (prefix == "node ls") {
    string node_type("all");
    cmd_getval(cmdmap, "type", node_type);
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    if (node_type == "all") {
      f->open_object_section("nodes");
      print_nodes(f.get(), ds);
      osdmon()->print_nodes(f.get());
      mdsmon()->print_nodes(f.get());
      mgrmon()->print_nodes(f.get());
      f->close_section();
    } else if (node_type == "mon") {
      print_nodes(f.get(), ds);
    } else if (node_type == "osd") {
      osdmon()->print_nodes(f.get());
    } else if (node_type == "mds") {
      mdsmon()->print_nodes(f.get());
    } else if (node_type == "mgr") {
      mgrmon()->print_nodes(f.get());
    }
    f->flush(ds);
    rdata.append(ds);
    rs = "";
    r = 0;
  } else if (prefix == "features") {
    if (!is_leader() && !is_peon()) {
      dout(10) << " waiting for quorum" << dendl;
      waitfor_quorum.push_back(new C_RetryMessage(this, op));
      return;
    }
    if (!is_leader()) {
      forward_request_leader(op);
      return;
    }
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    FeatureMap fm;
    get_combined_feature_map(&fm);
    f->dump_object("features", fm);
    f->flush(rdata);
    rs = "";
    r = 0;
  } else if (prefix == "mon metadata") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));

    string name;
    bool all = !cmd_getval(cmdmap, "id", name);
    if (!all) {
      // Dump a single mon's metadata
      int mon = monmap->get_rank(name);
      if (mon < 0) {
        rs = "requested mon not found";
        r = -ENOENT;
        goto out;
      }
      f->open_object_section("mon_metadata");
      r = get_mon_metadata(mon, f.get(), ds);
      f->close_section();
    } else {
      // Dump all mons' metadata
      r = 0;
      f->open_array_section("mon_metadata");
      for (unsigned int rank = 0; rank < monmap->size(); ++rank) {
        std::ostringstream get_err;
        f->open_object_section("mon");
        f->dump_string("name", monmap->get_name(rank));
        r = get_mon_metadata(rank, f.get(), get_err);
        f->close_section();
        if (r == -ENOENT || r == -EINVAL) {
          dout(1) << get_err.str() << dendl;
          // Drop error, list what metadata we do have
          r = 0;
        } else if (r != 0) {
          derr << "Unexpected error from get_mon_metadata: "
               << cpp_strerror(r) << dendl;
          ds << get_err.str();
          break;
        }
      }
      f->close_section();
    }

    f->flush(ds);
    rdata.append(ds);
    rs = "";
  } else if (prefix == "mon versions") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    count_metadata("ceph_version", f.get());
    f->flush(ds);
    rdata.append(ds);
    rs = "";
    r = 0;
  } else if (prefix == "mon count-metadata") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    string field;
    cmd_getval(cmdmap, "property", field);
    count_metadata(field, f.get());
    f->flush(ds);
    rdata.append(ds);
    rs = "";
    r = 0;
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
  } else if (prefix == "mon ok-to-stop") {
    vector<string> ids, invalid_ids;
    if (!cmd_getval(cmdmap, "ids", ids)) {
      r = -EINVAL;
      goto out;
    }
    set<string> wouldbe;
    for (auto rank : quorum) {
      wouldbe.insert(monmap->get_name(rank));
    }
    for (auto& n : ids) {
      if (monmap->contains(n)) {
	wouldbe.erase(n);
      } else {
        invalid_ids.push_back(n);
      }
    }
    if (!invalid_ids.empty()) {
      r = 0;
      rs = "invalid mon(s) specified: " + stringify(invalid_ids);
      goto out;
    }

    if (wouldbe.size() < monmap->min_quorum_size()) {
      r = -EBUSY;
      rs = "not enough monitors would be available (" + stringify(wouldbe) +
	") after stopping mons " + stringify(ids);
      goto out;
    }
    r = 0;
    rs = "quorum should be preserved (" + stringify(wouldbe) +
      ") after stopping " + stringify(ids);
  } else if (prefix == "mon ok-to-add-offline") {
    if (quorum.size() < monmap->min_quorum_size(monmap->size() + 1)) {
      rs = "adding a monitor may break quorum (until that monitor starts)";
      r = -EBUSY;
      goto out;
    }
    rs = "adding another mon that is not yet online will not break quorum";
    r = 0;
  } else if (prefix == "mon ok-to-rm") {
    string id;
    if (!cmd_getval(cmdmap, "id", id)) {
      r = -EINVAL;
      rs = "must specify a monitor id";
      goto out;
    }
    if (!monmap->contains(id)) {
      r = 0;
      rs = "mon." + id + " does not exist";
      goto out;
    }
    int rank = monmap->get_rank(id);
    if (quorum.count(rank) &&
	quorum.size() - 1 < monmap->min_quorum_size(monmap->size() - 1)) {
      r = -EBUSY;
      rs = "removing mon." + id + " would break quorum";
      goto out;
    }
    r = 0;
    rs = "safe to remove mon." + id;
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
  } else if (prefix == "versions") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    map<string,int> overall;
    f->open_object_section("version");
    map<string,int> mon, mgr, osd, mds;

    count_metadata("ceph_version", &mon);
    f->open_object_section("mon");
    for (auto& p : mon) {
      f->dump_int(p.first.c_str(), p.second);
      overall[p.first] += p.second;
    }
    f->close_section();

    mgrmon()->count_metadata("ceph_version", &mgr);
    if (!mgr.empty()) {
      f->open_object_section("mgr");
      for (auto& p : mgr) {
        f->dump_int(p.first.c_str(), p.second);
        overall[p.first] += p.second;
      }
      f->close_section();
    }

    osdmon()->count_metadata("ceph_version", &osd);
    if (!osd.empty()) {
      f->open_object_section("osd");
      for (auto& p : osd) {
        f->dump_int(p.first.c_str(), p.second);
        overall[p.first] += p.second;
      }
      f->close_section();
    }

    mdsmon()->count_metadata("ceph_version", &mds);
    if (!mds.empty()) {
      f->open_object_section("mds");
      for (auto& p : mds) {
        f->dump_int(p.first.c_str(), p.second);
        overall[p.first] += p.second;
      }
      f->close_section();
    }

    for (auto& p : mgrstatmon()->get_service_map().services) {
      auto &service = p.first;
      if (ServiceMap::is_normal_ceph_entity(service)) {
        continue;
      }
      f->open_object_section(service.c_str());
      map<string,int> m;
      p.second.count_metadata("ceph_version", &m);
      for (auto& q : m) {
	f->dump_int(q.first.c_str(), q.second);
	overall[q.first] += q.second;
      }
      f->close_section();
    }

    f->open_object_section("overall");
    for (auto& p : overall) {
      f->dump_int(p.first.c_str(), p.second);
    }
    f->close_section();
    f->close_section();
    f->flush(rdata);
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
  auto m = op->get_req<MMonCommand>();
  ceph_assert(m->get_type() == MSG_MON_COMMAND);
  MMonCommandAck *reply = new MMonCommandAck(m->cmd, rc, rs, version);
  reply->set_tid(m->get_tid());
  reply->set_data(rdata);
  send_reply(op, reply);
}

void Monitor::reply_tell_command(
  MonOpRequestRef op, int rc, const string &rs)
{
  MCommand *m = static_cast<MCommand*>(op->get_req());
  ceph_assert(m->get_type() == MSG_COMMAND);
  MCommandReply *reply = new MCommandReply(rc, rs);
  reply->set_tid(m->get_tid());
  m->get_connection()->send_message(reply);
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
  
  if (req->get_source().is_mon() && req->get_source_addrs() != messenger->get_myaddrs()) {
    dout(10) << "forward_request won't forward (non-local) mon request " << *req << dendl;
  } else if (session->proxy_con) {
    dout(10) << "forward_request won't double fwd request " << *req << dendl;
  } else if (!session->closed) {
    RoutedRequest *rr = new RoutedRequest;
    rr->tid = ++routed_request_tid;
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
    send_mon_message(forward, mon);
    op->mark_forwarded();
    ceph_assert(op->get_req()->get_type() != 0);
  } else {
    dout(10) << "forward_request no session for request " << *req << dendl;
  }
}

// fake connection attached to forwarded messages
struct AnonConnection : public Connection {
  entity_addr_t socket_addr;

  int send_message(Message *m) override {
    ceph_assert(!"send_message on anonymous connection");
  }
  void send_keepalive() override {
    ceph_assert(!"send_keepalive on anonymous connection");
  }
  void mark_down() override {
    // silently ignore
  }
  void mark_disposable() override {
    // silengtly ignore
  }
  bool is_connected() override { return false; }
  entity_addr_t get_peer_socket_addr() const override {
    return socket_addr;
  }

private:
  FRIEND_MAKE_REF(AnonConnection);
  explicit AnonConnection(CephContext *cct, const entity_addr_t& sa)
    : Connection(cct, nullptr),
      socket_addr(sa) {}
};

//extract the original message and put it into the regular dispatch function
void Monitor::handle_forward(MonOpRequestRef op)
{
  auto m = op->get_req<MForward>();
  dout(10) << "received forwarded message from "
	   << ceph_entity_type_name(m->client_type)
	   << " " << m->client_addrs
	   << " via " << m->get_source_inst() << dendl;
  MonSession *session = op->get_session();
  ceph_assert(session);

  if (!session->is_capable("mon", MON_CAP_X)) {
    dout(0) << "forward from entity with insufficient caps! " 
	    << session->caps << dendl;
  } else {
    // see PaxosService::dispatch(); we rely on this being anon
    // (c->msgr == NULL)
    PaxosServiceMessage *req = m->claim_message();
    ceph_assert(req != NULL);

    auto c = ceph::make_ref<AnonConnection>(cct, m->client_socket_addr);
    MonSession *s = new MonSession(static_cast<Connection*>(c.get()));
    s->_ident(req->get_source(),
	      req->get_source_addrs());
    c->set_priv(RefCountedPtr{s, false});
    c->set_peer_addrs(m->client_addrs);
    c->set_peer_type(m->client_type);
    c->set_features(m->con_features);

    s->authenticated = true;
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

    dout(10) << " mesg " << req << " from " << m->get_source_addr() << dendl;
    _ms_dispatch(req);

    // break the session <-> con ref loop by removing the con->session
    // reference, which is no longer needed once the MonOpRequest is
    // set up.
    c->set_priv(NULL);
  }
}

void Monitor::send_reply(MonOpRequestRef op, Message *reply)
{
  op->mark_event(__func__);

  MonSession *session = op->get_session();
  ceph_assert(session);
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
    dout(10) << "no_reply to " << req->get_source_inst()
	     << " via " << session->proxy_con->get_peer_addr()
	     << " for request " << *req << dendl;
    session->proxy_con->send_message(new MRoute(session->proxy_tid, NULL));
    op->mark_event("no_reply: send routed request");
  } else {
    dout(10) << "no_reply to " << req->get_source_inst()
             << " " << *req << dendl;
    op->mark_event("no_reply");
  }
}

void Monitor::handle_route(MonOpRequestRef op)
{
  auto m = op->get_req<MRoute>();
  MonSession *session = op->get_session();
  //check privileges
  if (!session->is_capable("mon", MON_CAP_X)) {
    dout(0) << "MRoute received from entity without appropriate perms! "
	    << dendl;
    return;
  }
  if (m->msg)
    dout(10) << "handle_route tid " << m->session_mon_tid << " " << *m->msg
	     << dendl;
  else
    dout(10) << "handle_route tid " << m->session_mon_tid << " null" << dendl;
  
  // look it up
  if (!m->session_mon_tid) {
    dout(10) << " not a routed request, ignoring" << dendl;
    return;
  }
  auto found = routed_requests.find(m->session_mon_tid);
  if (found == routed_requests.end()) {
    dout(10) << " don't have routed request tid " << m->session_mon_tid << dendl;
    return;
  }
  std::unique_ptr<RoutedRequest> rr{found->second};
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
  ceph_assert(rr->tid == m->session_mon_tid && rr->session->routed_request_tids.count(m->session_mon_tid));
  routed_requests.erase(found);
  rr->session->routed_request_tids.erase(m->session_mon_tid);
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
        ceph_assert(rr->session->routed_request_tids.count(p->first));
        rr->session->routed_request_tids.erase(p->first);
      }
      delete rr;
    } else {
      auto q = rr->request_bl.cbegin();
      PaxosServiceMessage *req =
	(PaxosServiceMessage *)decode_message(cct, 0, q);
      rr->op->mark_event("resend forwarded message to leader");
      dout(10) << " resend to mon." << mon << " tid " << rr->tid << " " << *req
	       << dendl;
      MForward *forward = new MForward(rr->tid,
				       req,
				       rr->con_features,
				       rr->session->caps);
      req->put();  // forward takes its own ref; drop ours.
      forward->client_type = rr->con->get_peer_type();
      forward->client_addrs = rr->con->get_peer_addrs();
      forward->client_socket_addr = rr->con->get_peer_socket_addr();
      forward->set_priority(req->get_priority());
      send_mon_message(forward, mon);
    }
  }
  if (mon == rank) {
    routed_requests.clear();
    finish_contexts(g_ceph_context, retry);
  }
}

void Monitor::remove_session(MonSession *s)
{
  dout(10) << "remove_session " << s << " " << s->name << " " << s->addrs
	   << " features 0x" << std::hex << s->con_features << std::dec << dendl;
  ceph_assert(s->con);
  ceph_assert(!s->closed);
  for (set<uint64_t>::iterator p = s->routed_request_tids.begin();
       p != s->routed_request_tids.end();
       ++p) {
    ceph_assert(routed_requests.count(*p));
    RoutedRequest *rr = routed_requests[*p];
    dout(10) << " dropping routed request " << rr->tid << dendl;
    delete rr;
    routed_requests.erase(*p);
  }
  s->routed_request_tids.clear();
  s->con->set_priv(nullptr);
  session_map.remove_session(s);
  logger->set(l_mon_num_sessions, session_map.get_size());
  logger->inc(l_mon_session_rm);
}

void Monitor::remove_all_sessions()
{
  std::lock_guard l(session_map_lock);
  while (!session_map.sessions.empty()) {
    MonSession *s = session_map.sessions.front();
    remove_session(s);
    logger->inc(l_mon_session_rm);
  }
  if (logger)
    logger->set(l_mon_num_sessions, session_map.get_size());
}

void Monitor::send_mon_message(Message *m, int rank)
{
  messenger->send_to_mon(m, monmap->get_addrs(rank));
}

void Monitor::waitlist_or_zap_client(MonOpRequestRef op)
{
  /**
   * Wait list the new session until we're in the quorum, assuming it's
   * sufficiently new.
   * tick() will periodically send them back through so we can send
   * the client elsewhere if we don't think we're getting back in.
   *
   * But we allow a few sorts of messages:
   * 1) Monitors can talk to us at any time, of course.
   * 2) auth messages. It's unlikely to go through much faster, but
   * it's possible we've just lost our quorum status and we want to take...
   * 3) command messages. We want to accept these under all possible
   * circumstances.
   */
  Message *m = op->get_req();
  MonSession *s = op->get_session();
  ConnectionRef con = op->get_connection();
  utime_t too_old = ceph_clock_now();
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
    if (!s->proxy_con) {
      std::lock_guard l(session_map_lock);
      remove_session(s);
    }
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

  if (src_is_mon && s) {
    ConnectionRef con = m->get_connection();
    if (con->get_messenger() && con->get_features() != s->con_features) {
      // only update features if this is a non-anonymous connection
      dout(10) << __func__ << " feature change for " << m->get_source_inst()
               << " (was " << s->con_features
               << ", now " << con->get_features() << ")" << dendl;
      // connection features changed - recreate session.
      if (s->con && s->con != con) {
        dout(10) << __func__ << " connection for " << m->get_source_inst()
                 << " changed from session; mark down and replace" << dendl;
        s->con->mark_down();
      }
      if (s->item.is_on_list()) {
        // forwarded messages' sessions are not in the sessions map and
        // exist only while the op is being handled.
        std::lock_guard l(session_map_lock);
        remove_session(s);
      }
      s = nullptr;
    }
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
    {
      std::lock_guard l(session_map_lock);
      s = session_map.new_session(m->get_source(),
				  m->get_source_addrs(),
				  con.get());
    }
    ceph_assert(s);
    con->set_priv(RefCountedPtr{s, false});
    dout(10) << __func__ << " new session " << s << " " << *s
	     << " features 0x" << std::hex
	     << s->con_features << std::dec << dendl;
    op->set_session(s);

    logger->set(l_mon_num_sessions, session_map.get_size());
    logger->inc(l_mon_session_add);

    if (src_is_mon) {
      // give it monitor caps; the peer type has been authenticated
      dout(5) << __func__ << " setting monitor caps on this connection" << dendl;
      if (!s->caps.is_allow_all()) // but no need to repeatedly copy
        s->caps = mon_caps;
      s->authenticated = true;
    }
  } else {
    dout(20) << __func__ << " existing session " << s << " for " << s->name
	     << dendl;
  }

  ceph_assert(s);

  s->session_timeout = ceph_clock_now();
  s->session_timeout += g_conf()->mon_session_timeout;

  if (s->auth_handler) {
    s->entity_name = s->auth_handler->get_entity_name();
    s->global_id = s->auth_handler->get_global_id();
    s->global_id_status = s->auth_handler->get_global_id_status();
  }
  dout(20) << " entity_name " << s->entity_name
	   << " global_id " << s->global_id
	   << " (" << s->global_id_status
	   << ") caps " << s->caps.get_str() << dendl;

  if (!session_stretch_allowed(s, op)) {
    return;
  }
  if ((is_synchronizing() ||
       (!s->authenticated && !exited_quorum.is_zero())) &&
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
  ceph_assert(s);
  if (s->closed) {
    dout(10) << " session closed, dropping " << op->get_req() << dendl;
    return;
  }

  /* we will consider the default type as being 'monitor' until proven wrong */
  op->set_type_monitor();
  /* deal with all messages that do not necessarily need caps */
  switch (op->get_req()->get_type()) {
    // auth
    case MSG_MON_GLOBAL_ID:
    case MSG_MON_USED_PENDING_KEYS:
    case CEPH_MSG_AUTH:
      op->set_type_service();
      /* no need to check caps here */
      paxos_service[PAXOS_AUTH]->dispatch(op);
      return;

    case CEPH_MSG_PING:
      handle_ping(op);
      return;
    case MSG_COMMAND:
      op->set_type_command();
      handle_tell_command(op);
      return;
  }

  if (!op->get_session()->authenticated) {
    dout(5) << __func__ << " " << op->get_req()->get_source_inst()
            << " is not authenticated, dropping " << *(op->get_req())
            << dendl;
    return;
  }

  // global_id_status == NONE: all sessions for auth_none and krb,
  // mon <-> mon sessions (including proxied sessions) for cephx
  ceph_assert(s->global_id_status == global_id_status_t::NONE ||
              s->global_id_status == global_id_status_t::NEW_OK ||
              s->global_id_status == global_id_status_t::NEW_NOT_EXPOSED ||
              s->global_id_status == global_id_status_t::RECLAIM_OK ||
              s->global_id_status == global_id_status_t::RECLAIM_INSECURE);

  // let mon_getmap through for "ping" (which doesn't reconnect)
  // and "tell" (which reconnects but doesn't attempt to preserve
  // its global_id and stays in NEW_NOT_EXPOSED, retrying until
  // ->send_attempts reaches 0)
  if (cct->_conf->auth_expose_insecure_global_id_reclaim &&
      s->global_id_status == global_id_status_t::NEW_NOT_EXPOSED &&
      op->get_req()->get_type() != CEPH_MSG_MON_GET_MAP) {
    dout(5) << __func__ << " " << op->get_req()->get_source_inst()
            << " may omit old_ticket on reconnects, discarding "
            << *op->get_req() << " and forcing reconnect" << dendl;
    ceph_assert(s->con && !s->proxy_con);
    s->con->mark_down();
    {
      std::lock_guard l(session_map_lock);
      remove_session(s);
    }
    op->mark_zap();
    return;
  }

  switch (op->get_req()->get_type()) {
    case CEPH_MSG_MON_GET_MAP:
      handle_mon_get_map(op);
      return;

    case MSG_GET_CONFIG:
      configmon()->handle_get_config(op);
      return;

    case CEPH_MSG_MON_SUBSCRIBE:
      /* FIXME: check what's being subscribed, filter accordingly */
      handle_subscribe(op);
      return;
  }

  /* well, maybe the op belongs to a service... */
  op->set_type_service();
  /* deal with all messages which caps should be checked somewhere else */
  switch (op->get_req()->get_type()) {

    // OSDs
    case CEPH_MSG_MON_GET_OSDMAP:
    case CEPH_MSG_POOLOP:
    case MSG_OSD_BEACON:
    case MSG_OSD_MARK_ME_DOWN:
    case MSG_OSD_MARK_ME_DEAD:
    case MSG_OSD_FULL:
    case MSG_OSD_FAILURE:
    case MSG_OSD_BOOT:
    case MSG_OSD_ALIVE:
    case MSG_OSD_PGTEMP:
    case MSG_OSD_PG_CREATED:
    case MSG_REMOVE_SNAPS:
    case MSG_MON_GET_PURGED_SNAPS:
    case MSG_OSD_PG_READY_TO_MERGE:
      paxos_service[PAXOS_OSDMAP]->dispatch(op);
      return;

    // MDSs
    case MSG_MDS_BEACON:
    case MSG_MDS_OFFLOAD_TARGETS:
      paxos_service[PAXOS_MDSMAP]->dispatch(op);
      return;

    // Mgrs
    case MSG_MGR_BEACON:
      paxos_service[PAXOS_MGR]->dispatch(op);
      return;

    case MSG_MNVMEOF_GW_BEACON:
       paxos_service[PAXOS_NVMEGW]->dispatch(op);
       return;


    // MgrStat
    case MSG_MON_MGR_REPORT:
    case CEPH_MSG_STATFS:
    case MSG_GETPOOLSTATS:
      paxos_service[PAXOS_MGRSTAT]->dispatch(op);
      return;

      // log
    case MSG_LOG:
      paxos_service[PAXOS_LOG]->dispatch(op);
      return;

    // handle_command() does its own caps checking
    case MSG_MON_COMMAND:
      op->set_type_command();
      handle_command(op);
      return;
  }

  /* nop, looks like it's not a service message; revert back to monitor */
  op->set_type_monitor();

  /* messages we, the Monitor class, need to deal with
   * but may be sent by clients. */

  if (!op->get_session()->is_capable("mon", MON_CAP_R)) {
    dout(5) << __func__ << " " << op->get_req()->get_source_inst()
            << " not enough caps for " << *(op->get_req()) << " -- dropping"
            << dendl;
    return;
  }

  switch (op->get_req()->get_type()) {
    // misc
    case CEPH_MSG_MON_GET_VERSION:
      handle_get_version(op);
      return;
  }

  if (!op->is_src_mon()) {
    dout(1) << __func__ << " unexpected monitor message from"
            << " non-monitor entity " << op->get_req()->get_source_inst()
            << " " << *(op->get_req()) << " -- dropping" << dendl;
    return;
  }

  /* messages that should only be sent by another monitor */
  switch (op->get_req()->get_type()) {

    case MSG_ROUTE:
      handle_route(op);
      return;

    case MSG_MON_PROBE:
      handle_probe(op);
      return;

    // Sync (i.e., the new slurp, but on steroids)
    case MSG_MON_SYNC:
      handle_sync(op);
      return;
    case MSG_MON_SCRUB:
      handle_scrub(op);
      return;

    /* log acks are sent from a monitor we sent the MLog to, and are
       never sent by clients to us. */
    case MSG_LOGACK:
      log_client.handle_log_ack((MLogAck*)op->get_req());
      return;

    // monmap
    case MSG_MON_JOIN:
      op->set_type_service();
      paxos_service[PAXOS_MONMAP]->dispatch(op);
      return;

    // paxos
    case MSG_MON_PAXOS:
      {
        op->set_type_paxos();
        auto pm = op->get_req<MMonPaxos>();
        if (!op->get_session()->is_capable("mon", MON_CAP_X)) {
          //can't send these!
          return;
        }

        if (state == STATE_SYNCHRONIZING) {
          // we are synchronizing. These messages would do us no
          // good, thus just drop them and ignore them.
          dout(10) << __func__ << " ignore paxos msg from "
            << pm->get_source_inst() << dendl;
          return;
        }

        // sanitize
        if (pm->epoch > get_epoch()) {
          bootstrap();
          return;
        }
        if (pm->epoch != get_epoch()) {
          return;
        }

        paxos->dispatch(op);
      }
      return;

    // elector messages
    case MSG_MON_ELECTION:
      op->set_type_election_or_ping();
      //check privileges here for simplicity
      if (!op->get_session()->is_capable("mon", MON_CAP_X)) {
        dout(0) << "MMonElection received from entity without enough caps!"
          << op->get_session()->caps << dendl;
        return;;
      }
      if (!is_probing() && !is_synchronizing()) {
        elector.dispatch(op);
      }
      return;

    case MSG_MON_PING:
      op->set_type_election_or_ping();
      elector.dispatch(op);
      return;

    case MSG_FORWARD:
      handle_forward(op);
      return;

    case MSG_TIMECHECK:
      dout(5) << __func__ << " ignoring " << op << dendl;
      return;
    case MSG_TIMECHECK2:
      handle_timecheck(op);
      return;

    case MSG_MON_HEALTH:
      dout(5) << __func__ << " dropping deprecated message: "
	      << *op->get_req() << dendl;
      break;
    case MSG_MON_HEALTH_CHECKS:
      op->set_type_service();
      paxos_service[PAXOS_HEALTH]->dispatch(op);
      return;
  }
  dout(1) << "dropping unexpected " << *(op->get_req()) << dendl;
  return;
}

void Monitor::handle_ping(MonOpRequestRef op)
{
  auto m = op->get_req<MPing>();
  dout(10) << __func__ << " " << *m << dendl;
  MPing *reply = new MPing;
  bufferlist payload;
  boost::scoped_ptr<Formatter> f(new JSONFormatter(true));
  f->open_object_section("pong");

  healthmon()->get_health_status(false, f.get(), nullptr);
  get_mon_status(f.get());

  f->close_section();
  stringstream ss;
  f->flush(ss);
  encode(ss.str(), payload);
  reply->set_payload(payload);
  dout(10) << __func__ << " reply payload len " << reply->get_payload().length() << dendl;
  m->get_connection()->send_message(reply);
}

void Monitor::timecheck_start()
{
  dout(10) << __func__ << dendl;
  timecheck_cleanup();
  if (get_quorum_mon_features().contains_all(
	ceph::features::mon::FEATURE_NAUTILUS)) {
    timecheck_start_round();
  }
}

void Monitor::timecheck_finish()
{
  dout(10) << __func__ << dendl;
  timecheck_cleanup();
}

void Monitor::timecheck_start_round()
{
  dout(10) << __func__ << " curr " << timecheck_round << dendl;
  ceph_assert(is_leader());

  if (monmap->size() == 1) {
    ceph_abort_msg("We are alone; this shouldn't have been scheduled!");
    return;
  }

  if (timecheck_round % 2) {
    dout(10) << __func__ << " there's a timecheck going on" << dendl;
    utime_t curr_time = ceph_clock_now();
    double max = g_conf()->mon_timecheck_interval*3;
    if (curr_time - timecheck_round_start < max) {
      dout(10) << __func__ << " keep current round going" << dendl;
      goto out;
    } else {
      dout(10) << __func__
               << " finish current timecheck and start new" << dendl;
      timecheck_cancel_round();
    }
  }

  ceph_assert(timecheck_round % 2 == 0);
  timecheck_acks = 0;
  timecheck_round ++;
  timecheck_round_start = ceph_clock_now();
  dout(10) << __func__ << " new " << timecheck_round << dendl;

  timecheck();
out:
  dout(10) << __func__ << " setting up next event" << dendl;
  timecheck_reset_event();
}

void Monitor::timecheck_finish_round(bool success)
{
  dout(10) << __func__ << " curr " << timecheck_round << dendl;
  ceph_assert(timecheck_round % 2);
  timecheck_round ++;
  timecheck_round_start = utime_t();

  if (success) {
    ceph_assert(timecheck_waiting.empty());
    ceph_assert(timecheck_acks == quorum.size());
    timecheck_report();
    timecheck_check_skews();
    return;
  }

  dout(10) << __func__ << " " << timecheck_waiting.size()
           << " peers still waiting:";
  for (auto& p : timecheck_waiting) {
    *_dout << " mon." << p.first;
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

  timecheck_event = timer.add_event_after(
    delay,
    new C_MonContext{this, [this](int) {
	timecheck_start_round();
      }});
}

void Monitor::timecheck_check_skews()
{
  dout(10) << __func__ << dendl;
  ceph_assert(is_leader());
  ceph_assert((timecheck_round % 2) == 0);
  if (monmap->size() == 1) {
    ceph_abort_msg("We are alone; we shouldn't have gotten here!");
    return;
  }
  ceph_assert(timecheck_latencies.size() == timecheck_skews.size());

  bool found_skew = false;
  for (auto& p : timecheck_skews) {
    double abs_skew;
    if (timecheck_has_skew(p.second, &abs_skew)) {
      dout(10) << __func__
               << " " << p.first << " skew " << abs_skew << dendl;
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
  ceph_assert(is_leader());
  ceph_assert((timecheck_round % 2) == 0);
  if (monmap->size() == 1) {
    ceph_abort_msg("We are alone; we shouldn't have gotten here!");
    return;
  }

  ceph_assert(timecheck_latencies.size() == timecheck_skews.size());
  bool do_output = true; // only output report once
  for (set<int>::iterator q = quorum.begin(); q != quorum.end(); ++q) {
    if (monmap->get_name(*q) == name)
      continue;

    MTimeCheck2 *m = new MTimeCheck2(MTimeCheck2::OP_REPORT);
    m->epoch = get_epoch();
    m->round = timecheck_round;

    for (auto& it : timecheck_skews) {
      double skew = it.second;
      double latency = timecheck_latencies[it.first];

      m->skews[it.first] = skew;
      m->latencies[it.first] = latency;

      if (do_output) {
        dout(25) << __func__ << " mon." << it.first
                 << " latency " << latency
                 << " skew " << skew << dendl;
      }
    }
    do_output = false;
    dout(10) << __func__ << " send report to mon." << *q << dendl;
    send_mon_message(m, *q);
  }
}

void Monitor::timecheck()
{
  dout(10) << __func__ << dendl;
  ceph_assert(is_leader());
  if (monmap->size() == 1) {
    ceph_abort_msg("We are alone; we shouldn't have gotten here!");
    return;
  }
  ceph_assert(timecheck_round % 2 != 0);

  timecheck_acks = 1; // we ack ourselves

  dout(10) << __func__ << " start timecheck epoch " << get_epoch()
           << " round " << timecheck_round << dendl;

  // we are at the eye of the storm; the point of reference
  timecheck_skews[rank] = 0.0;
  timecheck_latencies[rank] = 0.0;

  for (set<int>::iterator it = quorum.begin(); it != quorum.end(); ++it) {
    if (monmap->get_name(*it) == name)
      continue;

    utime_t curr_time = ceph_clock_now();
    timecheck_waiting[*it] = curr_time;
    MTimeCheck2 *m = new MTimeCheck2(MTimeCheck2::OP_PING);
    m->epoch = get_epoch();
    m->round = timecheck_round;
    dout(10) << __func__ << " send " << *m << " to mon." << *it << dendl;
    send_mon_message(m, *it);
  }
}

health_status_t Monitor::timecheck_status(ostringstream &ss,
                                          const double skew_bound,
                                          const double latency)
{
  health_status_t status = HEALTH_OK;
  ceph_assert(latency >= 0);

  double abs_skew;
  if (timecheck_has_skew(skew_bound, &abs_skew)) {
    status = HEALTH_WARN;
    ss << "clock skew " << abs_skew << "s"
       << " > max " << g_conf()->mon_clock_drift_allowed << "s";
  }

  return status;
}

void Monitor::handle_timecheck_leader(MonOpRequestRef op)
{
  auto m = op->get_req<MTimeCheck2>();
  dout(10) << __func__ << " " << *m << dendl;
  /* handles PONG's */
  ceph_assert(m->op == MTimeCheck2::OP_PONG);

  int other = m->get_source().num();
  if (m->epoch < get_epoch()) {
    dout(1) << __func__ << " got old timecheck epoch " << m->epoch
            << " from " << other
            << " curr " << get_epoch()
            << " -- severely lagged? discard" << dendl;
    return;
  }
  ceph_assert(m->epoch == get_epoch());

  if (m->round < timecheck_round) {
    dout(1) << __func__ << " got old round " << m->round
            << " from " << other
            << " curr " << timecheck_round << " -- discard" << dendl;
    return;
  }

  utime_t curr_time = ceph_clock_now();

  ceph_assert(timecheck_waiting.count(other) > 0);
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
  ceph_assert(latency >= 0);

  double delta = ((double) m->timestamp) - ((double) curr_time);
  double abs_delta = (delta > 0 ? delta : -delta);
  double skew_bound = abs_delta - latency;
  if (skew_bound < 0)
    skew_bound = 0;
  else if (delta < 0)
    skew_bound = -skew_bound;

  ostringstream ss;
  health_status_t status = timecheck_status(ss, skew_bound, latency);
  if (status != HEALTH_OK) {
    clog->health(status) << other << " " << ss.str();
  }

  dout(10) << __func__ << " from " << other << " ts " << m->timestamp
	   << " delta " << delta << " skew_bound " << skew_bound
	   << " latency " << latency << dendl;

  timecheck_skews[other] = skew_bound;

  timecheck_acks++;
  if (timecheck_acks == quorum.size()) {
    dout(10) << __func__ << " got pongs from everybody ("
             << timecheck_acks << " total)" << dendl;
    ceph_assert(timecheck_skews.size() == timecheck_acks);
    ceph_assert(timecheck_waiting.empty());
    // everyone has acked, so bump the round to finish it.
    timecheck_finish_round();
  }
}

void Monitor::handle_timecheck_peon(MonOpRequestRef op)
{
  auto m = op->get_req<MTimeCheck2>();
  dout(10) << __func__ << " " << *m << dendl;

  ceph_assert(is_peon());
  ceph_assert(m->op == MTimeCheck2::OP_PING || m->op == MTimeCheck2::OP_REPORT);

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

  if (m->op == MTimeCheck2::OP_REPORT) {
    ceph_assert((timecheck_round % 2) == 0);
    timecheck_latencies.swap(m->latencies);
    timecheck_skews.swap(m->skews);
    return;
  }

  ceph_assert((timecheck_round % 2) != 0);
  MTimeCheck2 *reply = new MTimeCheck2(MTimeCheck2::OP_PONG);
  utime_t curr_time = ceph_clock_now();
  reply->timestamp = curr_time;
  reply->epoch = m->epoch;
  reply->round = m->round;
  dout(10) << __func__ << " send " << *m
           << " to " << m->get_source_inst() << dendl;
  m->get_connection()->send_message(reply);
}

void Monitor::handle_timecheck(MonOpRequestRef op)
{
  auto m = op->get_req<MTimeCheck2>();
  dout(10) << __func__ << " " << *m << dendl;

  if (is_leader()) {
    if (m->op != MTimeCheck2::OP_PONG) {
      dout(1) << __func__ << " drop unexpected msg (not pong)" << dendl;
    } else {
      handle_timecheck_leader(op);
    }
  } else if (is_peon()) {
    if (m->op != MTimeCheck2::OP_PING && m->op != MTimeCheck2::OP_REPORT) {
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
  auto m = op->get_req<MMonSubscribe>();
  dout(10) << "handle_subscribe " << *m << dendl;
  
  bool reply = false;

  MonSession *s = op->get_session();
  ceph_assert(s);

  if (m->hostname.size()) {
    s->remote_host = m->hostname;
  }

  for (map<string,ceph_mon_subscribe_item>::iterator p = m->what.begin();
       p != m->what.end();
       ++p) {
    if (p->first == "monmap" || p->first == "config") {
      // these require no caps
    } else if (!s->is_capable("mon", MON_CAP_R)) {
      dout(5) << __func__ << " " << op->get_req()->get_source_inst()
	      << " not enough caps for " << *(op->get_req()) << " -- dropping"
	      << dendl;
      continue;
    }

    // if there are any non-onetime subscriptions, we need to reply to start the resubscribe timer
    if ((p->second.flags & CEPH_SUBSCRIBE_ONETIME) == 0)
      reply = true;

    // remove conflicting subscribes
    if (logmon()->sub_name_to_id(p->first) >= 0) {
      for (map<string, Subscription*>::iterator it = s->sub_map.begin();
	   it != s->sub_map.end(); ) {
	if (it->first != p->first && logmon()->sub_name_to_id(it->first) >= 0) {
	  std::lock_guard l(session_map_lock);
	  session_map.remove_sub((it++)->second);
	} else {
	  ++it;
	}
      }
    }

    {
      std::lock_guard l(session_map_lock);
      session_map.add_update_sub(s, p->first, p->second.start,
				 p->second.flags & CEPH_SUBSCRIBE_ONETIME,
				 m->get_connection()->has_feature(CEPH_FEATURE_INCSUBOSDMAP));
    }

    if (p->first.compare(0, 6, "mdsmap") == 0 || p->first.compare(0, 5, "fsmap") == 0) {
      dout(10) << __func__ << ": MDS sub '" << p->first << "'" << dendl;
      if ((int)s->is_capable("mds", MON_CAP_R)) {
        Subscription *sub = s->sub_map[p->first];
        ceph_assert(sub != nullptr);
        mdsmon()->check_sub(sub);
      }
    } else if (p->first == "osdmap") {
      if ((int)s->is_capable("osd", MON_CAP_R)) {
	if (s->osd_epoch > p->second.start) {
	  // client needs earlier osdmaps on purpose, so reset the sent epoch
	  s->osd_epoch = 0;
	}
        osdmon()->check_osdmap_sub(s->sub_map["osdmap"]);
      }
    } else if (p->first == "osd_pg_creates") {
      if ((int)s->is_capable("osd", MON_CAP_W)) {
	osdmon()->check_pg_creates_sub(s->sub_map["osd_pg_creates"]);
      }
    } else if (p->first == "monmap") {
      monmon()->check_sub(s->sub_map[p->first]);
    } else if (logmon()->sub_name_to_id(p->first) >= 0) {
      logmon()->check_sub(s->sub_map[p->first]);
    } else if (p->first == "mgrmap" || p->first == "mgrdigest") {
      mgrmon()->check_sub(s->sub_map[p->first]);
    } else if (p->first == "servicemap") {
      mgrstatmon()->check_sub(s->sub_map[p->first]);
    } else if (p->first == "config") {
      configmon()->check_sub(s);
    } else if (p->first.find("kv:") == 0) {
      kvmon()->check_sub(s->sub_map[p->first]);
    }
    else if (p->first == "NVMeofGw") {
        nvmegwmon()->check_sub(s->sub_map[p->first]);
    }
  }

  if (reply) {
    // we only need to reply if the client is old enough to think it
    // has to send renewals.
    ConnectionRef con = m->get_connection();
    if (!con->has_feature(CEPH_FEATURE_MON_STATEFUL_SUB))
      m->get_connection()->send_message(new MMonSubscribeAck(
	monmap->get_fsid(), (int)g_conf()->mon_subscribe_interval));
  }

}

void Monitor::handle_get_version(MonOpRequestRef op)
{
  auto m = op->get_req<MMonGetVersion>();
  dout(10) << "handle_get_version " << *m << dendl;
  PaxosService *svc = NULL;

  MonSession *s = op->get_session();
  ceph_assert(s);

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

  auto priv = con->get_priv();
  auto s = static_cast<MonSession*>(priv.get());
  if (!s)
    return false;

  // break any con <-> session ref cycle
  s->con->set_priv(nullptr);

  if (is_shutdown())
    return false;

  std::lock_guard l(lock);

  dout(10) << "reset/close on session " << s->name << " " << s->addrs << dendl;
  if (!s->closed && s->item.is_on_list()) {
    std::lock_guard l(session_map_lock);
    remove_session(s);
  }
  return true;
}

bool Monitor::ms_handle_refused(Connection *con)
{
  // just log for now...
  dout(10) << "ms_handle_refused " << con << " " << con->get_peer_addr() << dendl;
  return false;
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
  auto m = op->get_req<MMonGetMap>();
  dout(10) << "handle_mon_get_map" << dendl;
  send_latest_monmap(m->get_connection().get());
}

int Monitor::load_metadata()
{
  bufferlist bl;
  int r = store->get(MONITOR_STORE_PREFIX, "last_metadata", bl);
  if (r)
    return r;
  auto it = bl.cbegin();
  decode(mon_metadata, it);

  pending_metadata = mon_metadata;
  return 0;
}

int Monitor::get_mon_metadata(int mon, Formatter *f, ostream& err)
{
  ceph_assert(f);
  if (!mon_metadata.count(mon)) {
    err << "mon." << mon << " not found";
    return -EINVAL;
  }
  const Metadata& m = mon_metadata[mon];
  for (Metadata::const_iterator p = m.begin(); p != m.end(); ++p) {
    f->dump_string(p->first.c_str(), p->second);
  }
  return 0;
}

void Monitor::count_metadata(const string& field, map<string,int> *out)
{
  for (auto& p : mon_metadata) {
    auto q = p.second.find(field);
    if (q == p.second.end()) {
      (*out)["unknown"]++;
    } else {
      (*out)[q->second]++;
    }
  }
}

void Monitor::count_metadata(const string& field, Formatter *f)
{
  map<string,int> by_val;
  count_metadata(field, &by_val);
  f->open_object_section(field.c_str());
  for (auto& p : by_val) {
    f->dump_int(p.first.c_str(), p.second);
  }
  f->close_section();
}

void Monitor::get_all_versions(std::map<string, list<string> > &versions)
{
  // mon
  get_versions(versions);
  // osd
  osdmon()->get_versions(versions);
  // mgr
  mgrmon()->get_versions(versions);
  // mds
  mdsmon()->get_versions(versions);
  dout(20) << __func__ << " all versions=" << versions << dendl;
}

void Monitor::get_versions(std::map<string, list<string> > &versions)
{
  for (auto& [rank, metadata] : mon_metadata) {
    auto q = metadata.find("ceph_version_short");
    if (q == metadata.end()) {
      // not likely
      continue;
    }
    versions[q->second].push_back(string("mon.") + monmap->get_name(rank));
  }
}

int Monitor::print_nodes(Formatter *f, ostream& err)
{
  map<string, list<string> > mons;	// hostname => mon
  for (map<int, Metadata>::iterator it = mon_metadata.begin();
       it != mon_metadata.end(); ++it) {
    const Metadata& m = it->second;
    Metadata::const_iterator hostname = m.find("hostname");
    if (hostname == m.end()) {
      // not likely though
      continue;
    }
    mons[hostname->second].push_back(monmap->get_name(it->first));
  }

  dump_services(f, mons, "mon");
  return 0;
}

// ----------------------------------------------
// scrub

int Monitor::scrub_start()
{
  dout(10) << __func__ << dendl;
  ceph_assert(is_leader());

  if (!scrub_result.empty()) {
    clog->info() << "scrub already in progress";
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
  ceph_assert(is_leader());
  ceph_assert(scrub_state);

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
    send_mon_message(r, *p);
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
    ceph_assert(scrub_state->finished == true);
    scrub_finish();
  }
  return 0;
}

void Monitor::handle_scrub(MonOpRequestRef op)
{
  auto m = op->get_req<MMonScrub>();
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
      ceph_assert(scrub_result.count(from) == 0);
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
  ceph_assert(r != NULL);
  ceph_assert(start != NULL);
  ceph_assert(num_keys != NULL);

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
    int err = store->get(k.first, k.second, bl);
    ceph_assert(err == 0);
    
    uint32_t key_crc = bl.crc32c(0);
    dout(30) << __func__ << " " << k << " bl " << bl.length() << " bytes"
                                     << " crc " << key_crc << dendl;
    r->prefix_keys[k.first]++;
    if (r->prefix_crc.count(k.first) == 0) {
      r->prefix_crc[k.first] = 0;
    }
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
      clog->error() << "scrub mismatch";
      clog->error() << " mon." << rank << " " << mine;
      clog->error() << " mon." << p->first << " " << p->second;
    }
  }
  if (!errors)
    clog->debug() << "scrub ok on " << quorum << ": " << mine;
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

inline void Monitor::scrub_update_interval(ceph::timespan interval)
{
  // we don't care about changes if we are not the leader.
  // changes will be visible if we become the leader.
  if (!is_leader())
    return;

  dout(1) << __func__ << " new interval = " << interval << dendl;

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

  auto scrub_interval =
    cct->_conf.get_val<std::chrono::seconds>("mon_scrub_interval");
  if (scrub_interval == std::chrono::seconds::zero()) {
    dout(1) << __func__ << " scrub event is disabled"
            << " (mon_scrub_interval = " << scrub_interval
            << ")" << dendl;
    return;
  }

  scrub_event = timer.add_event_after(
    scrub_interval,
    new C_MonContext{this, [this](int) {
      scrub_start();
      }});
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
  scrub_timeout_event = timer.add_event_after(
    g_conf()->mon_scrub_timeout,
    new C_MonContext{this, [this](int) {
      scrub_timeout();
    }});
}

/************ TICK ***************/
void Monitor::new_tick()
{
  timer.add_event_after(g_conf()->mon_tick_interval, new C_MonContext{this, [this](int) {
	tick();
      }});
}

void Monitor::tick()
{
  // ok go.
  dout(11) << "tick" << dendl;
  const utime_t now = ceph_clock_now();
  
  // Check if we need to emit any delayed health check updated messages
  if (is_leader()) {
    const auto min_period = g_conf().get_val<int64_t>(
                              "mon_health_log_update_period");
    for (auto& svc : paxos_service) {
      auto health = svc->get_health_checks();

      for (const auto &i : health.checks) {
        const std::string &code = i.first;
        const std::string &summary = i.second.summary;
        const health_status_t severity = i.second.severity;

        auto status_iter = health_check_log_times.find(code);
        if (status_iter == health_check_log_times.end()) {
          continue;
        }

        auto &log_status = status_iter->second;
        bool const changed = log_status.last_message != summary
                             || log_status.severity != severity;

        if (changed && now - log_status.updated_at > min_period) {
          log_status.last_message = summary;
          log_status.updated_at = now;
          log_status.severity = severity;

          ostringstream ss;
          ss << "Health check update: " << summary << " (" << code << ")";
          clog->health(severity) << ss.str();
        }
      }
    }
  }


  for (auto& svc : paxos_service) {
    svc->tick();
    svc->maybe_trim();
  }
  
  // trim sessions
  {
    std::lock_guard l(session_map_lock);
    auto p = session_map.sessions.begin();

    bool out_for_too_long = (!exited_quorum.is_zero() &&
			     now > (exited_quorum + 2*g_conf()->mon_lease));

    while (!p.end()) {
      MonSession *s = *p;
      ++p;
    
      // don't trim monitors
      if (s->name.is_mon())
	continue;

      if (s->session_timeout < now && s->con) {
	// check keepalive, too
	s->session_timeout = s->con->get_last_keepalive();
	s->session_timeout += g_conf()->mon_session_timeout;
      }
      if (s->session_timeout < now) {
	dout(10) << " trimming session " << s->con << " " << s->name
		 << " " << s->addrs
		 << " (timeout " << s->session_timeout
		 << " < now " << now << ")" << dendl;
      } else if (out_for_too_long) {
	// boot the client Session because we've taken too long getting back in
	dout(10) << " trimming session " << s->con << " " << s->name
		 << " because we've been out of quorum too long" << dendl;
      } else {
	continue;
      }

      s->con->mark_down();
      remove_session(s);
      logger->inc(l_mon_session_trim);
    }
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

  mgr_client.update_daemon_health(get_health_metrics());
  new_tick();
}

vector<DaemonHealthMetric> Monitor::get_health_metrics() 
{
  vector<DaemonHealthMetric> metrics;

  utime_t oldest_secs;
  const utime_t now = ceph_clock_now();
  auto too_old = now;
  too_old -= g_conf().get_val<std::chrono::seconds>("mon_op_complaint_time").count();
  int slow = 0;
  TrackedOpRef oldest_op;
  auto count_slow_ops = [&](TrackedOp& op) {
    if (op.get_initiated() < too_old) {
      slow++;
      if (!oldest_op || op.get_initiated() < oldest_op->get_initiated()) {
	oldest_op = &op;
      }
      return true;
    } else {
      return false;
    }
  };
  if (op_tracker.visit_ops_in_flight(&oldest_secs, count_slow_ops)) {
    if (slow) {
      derr << __func__ << " reporting " << slow << " slow ops, oldest is "
	   << oldest_op->get_desc() << dendl;
    }
    metrics.emplace_back(daemon_metric::SLOW_OPS, slow, oldest_secs);
  } else {
    metrics.emplace_back(daemon_metric::SLOW_OPS, 0, 0);
  }
  return metrics;
}

void Monitor::prepare_new_fingerprint(MonitorDBStore::TransactionRef t)
{
  uuid_d nf;
  nf.generate_random();
  dout(10) << __func__ << " proposing cluster_fingerprint " << nf << dendl;

  bufferlist bl;
  encode(nf, bl);
  t->put(MONITOR_NAME, "cluster_fingerprint", bl);
}

int Monitor::check_fsid()
{
  bufferlist ebl;
  int r = store->get(MONITOR_NAME, "cluster_uuid", ebl);
  if (r == -ENOENT)
    return r;
  ceph_assert(r == 0);

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
  auto t(std::make_shared<MonitorDBStore::Transaction>());
  write_fsid(t);
  int r = store->apply_transaction(t);
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
  auto t(std::make_shared<MonitorDBStore::Transaction>());

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
    catch (ceph::buffer::error& e) {
      derr << "error decoding provided osdmap: " << e.what() << dendl;
      return -EINVAL;
    }
    t->put("mkfs", "osdmap", osdmapbl);
  }

  if (is_keyring_required()) {
    KeyRing keyring;
    string keyring_filename;

    r = ceph_resolve_file_search(g_conf()->keyring, keyring_filename);
    if (r) {
      if (g_conf()->key != "") {
	string keyring_plaintext = "[mon.]\n\tkey = " + g_conf()->key +
	  "\n\tcaps mon = \"allow *\"\n";
	bufferlist bl;
	bl.append(keyring_plaintext);
	try {
	  auto i = bl.cbegin();
	  keyring.decode(i);
	}
	catch (const ceph::buffer::error& e) {
	  derr << "error decoding keyring " << keyring_plaintext
	       << ": " << e.what() << dendl;
	  return -EINVAL;
	}
      } else {
	derr << "unable to find a keyring on " << g_conf()->keyring
	     << ": " << cpp_strerror(r) << dendl;
	return r;
      }
    } else {
      r = keyring.load(g_ceph_context, keyring_filename);
      if (r < 0) {
	derr << "unable to load initial keyring " << g_conf()->keyring << dendl;
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
  os << g_conf()->mon_data << "/keyring";

  int err = 0;
  int fd = ::open(os.str().c_str(), O_WRONLY|O_CREAT|O_CLOEXEC, 0600);
  if (fd < 0) {
    err = -errno;
    dout(0) << __func__ << " failed to open " << os.str() 
	    << ": " << cpp_strerror(err) << dendl;
    return err;
  }

  err = bl.write_fd(fd);
  if (!err)
    ::fsync(fd);
  VOID_TEMP_FAILURE_RETRY(::close(fd));

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

// AuthClient methods -- for mon <-> mon communication
int Monitor::get_auth_request(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  uint32_t *method,
  vector<uint32_t> *preferred_modes,
  bufferlist *out)
{
  std::scoped_lock l(auth_lock);
  if (con->get_peer_type() != CEPH_ENTITY_TYPE_MON &&
      con->get_peer_type() != CEPH_ENTITY_TYPE_MGR) {
    return -EACCES;
  }
  AuthAuthorizer *auth;
  if (!get_authorizer(con->get_peer_type(), &auth)) {
    return -EACCES;
  }
  auth_meta->authorizer.reset(auth);
  auth_registry.get_supported_modes(con->get_peer_type(),
				    auth->protocol,
				    preferred_modes);
  *method = auth->protocol;
  *out = auth->bl;
  return 0;
}

int Monitor::handle_auth_reply_more(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  const bufferlist& bl,
  bufferlist *reply)
{
  std::scoped_lock l(auth_lock);
  if (!auth_meta->authorizer) {
    derr << __func__ << " no authorizer?" << dendl;
    return -EACCES;
  }
  auth_meta->authorizer->add_challenge(cct, bl);
  *reply = auth_meta->authorizer->bl;
  return 0;
}

int Monitor::handle_auth_done(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  uint64_t global_id,
  uint32_t con_mode,
  const bufferlist& bl,
  CryptoKey *session_key,
  std::string *connection_secret)
{
  std::scoped_lock l(auth_lock);
  // verify authorizer reply
  auto p = bl.begin();
  if (!auth_meta->authorizer->verify_reply(p, connection_secret)) {
    dout(0) << __func__ << " failed verifying authorizer reply" << dendl;
    return -EACCES;
  }
  auth_meta->session_key = auth_meta->authorizer->session_key;
  return 0;
}

int Monitor::handle_auth_bad_method(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  uint32_t old_auth_method,
  int result,
  const std::vector<uint32_t>& allowed_methods,
  const std::vector<uint32_t>& allowed_modes)
{
  derr << __func__ << " hmm, they didn't like " << old_auth_method
       << " result " << cpp_strerror(result) << dendl;
  return -EACCES;
}

bool Monitor::get_authorizer(int service_id, AuthAuthorizer **authorizer)
{
  dout(10) << "get_authorizer for " << ceph_entity_type_name(service_id)
	   << dendl;

  if (is_shutdown())
    return false;

  // we only connect to other monitors and mgr; every else connects to us.
  if (service_id != CEPH_ENTITY_TYPE_MON &&
      service_id != CEPH_ENTITY_TYPE_MGR)
    return false;

  if (!auth_cluster_required.is_supported_auth(CEPH_AUTH_CEPHX)) {
    // auth_none
    dout(20) << __func__ << " building auth_none authorizer" << dendl;
    AuthNoneClientHandler handler{g_ceph_context};
    handler.set_global_id(0);
    *authorizer = handler.build_authorizer(service_id);
    return true;
  }

  CephXServiceTicketInfo auth_ticket_info;
  CephXSessionAuthInfo info;
  int ret;

  EntityName name;
  name.set_type(CEPH_ENTITY_TYPE_MON);
  auth_ticket_info.ticket.name = name;
  auth_ticket_info.ticket.global_id = 0;

  if (service_id == CEPH_ENTITY_TYPE_MON) {
    // mon to mon authentication uses the private monitor shared key and not the
    // rotating key
    CryptoKey secret;
    if (!keyring.get_secret(name, secret) &&
	!key_server.get_secret(name, secret)) {
      dout(0) << " couldn't get secret for mon service from keyring or keyserver"
	      << dendl;
      stringstream ss, ds;
      int err = key_server.list_secrets(ds);
      if (err < 0)
	ss << "no installed auth entries!";
      else
	ss << "installed auth entries:";
      dout(0) << ss.str() << "\n" << ds.str() << dendl;
      return false;
    }

    ret = key_server.build_session_auth_info(
      service_id, auth_ticket_info.ticket, secret, (uint64_t)-1, info);
    if (ret < 0) {
      dout(0) << __func__ << " failed to build mon session_auth_info "
	      << cpp_strerror(ret) << dendl;
      return false;
    }
  } else if (service_id == CEPH_ENTITY_TYPE_MGR) {
    // mgr
    ret = key_server.build_session_auth_info(
      service_id, auth_ticket_info.ticket, info);
    if (ret < 0) {
      derr << __func__ << " failed to build mgr service session_auth_info "
	   << cpp_strerror(ret) << dendl;
      return false;
    }
  } else {
    ceph_abort();  // see check at top of fn
  }

  CephXTicketBlob blob;
  if (!cephx_build_service_ticket_blob(cct, info, blob)) {
    dout(0) << "get_authorizer failed to build service ticket" << dendl;
    return false;
  }
  bufferlist ticket_data;
  encode(blob, ticket_data);

  auto iter = ticket_data.cbegin();
  CephXTicketHandler handler(g_ceph_context, service_id);
  decode(handler.ticket, iter);

  handler.session_key = info.session_key;

  *authorizer = handler.build_authorizer(0);
  
  return true;
}

int Monitor::handle_auth_request(
  Connection *con,
  AuthConnectionMeta *auth_meta,
  bool more,
  uint32_t auth_method,
  const bufferlist &payload,
  bufferlist *reply)
{
  std::scoped_lock l(auth_lock);

  // NOTE: be careful, the Connection hasn't fully negotiated yet, so
  // e.g., peer_features, peer_addrs, and others are still unknown.

  dout(10) << __func__ << " con " << con << (more ? " (more)":" (start)")
	   << " method " << auth_method
	   << " payload " << payload.length()
	   << dendl;
  if (!payload.length()) {
    if (!con->is_msgr2() &&
	con->get_peer_type() != CEPH_ENTITY_TYPE_MON) {
      // for v1 connections, we tolerate no authorizer (from
      // non-monitors), because authentication happens via MAuth
      // messages.
      return 1;
    }
    return -EACCES;
  }
  if (!more) {
    auth_meta->auth_mode = payload[0];
  }

  if (auth_meta->auth_mode >= AUTH_MODE_AUTHORIZER &&
      auth_meta->auth_mode <= AUTH_MODE_AUTHORIZER_MAX) {
    AuthAuthorizeHandler *ah = get_auth_authorize_handler(con->get_peer_type(),
							  auth_method);
    if (!ah) {
      lderr(cct) << __func__ << " no AuthAuthorizeHandler found for auth method "
		 << auth_method << dendl;
      return -EOPNOTSUPP;
    }
    bool was_challenge = (bool)auth_meta->authorizer_challenge;
    bool isvalid = ah->verify_authorizer(
      cct,
      keyring,
      payload,
      auth_meta->get_connection_secret_length(),
      reply,
      &con->peer_name,
      &con->peer_global_id,
      &con->peer_caps_info,
      &auth_meta->session_key,
      &auth_meta->connection_secret,
      &auth_meta->authorizer_challenge);
    if (isvalid) {
      if (!ms_handle_fast_authentication(con)) {
        return -EACCES;
      }
      return 1;
    }
    if (!more && !was_challenge && auth_meta->authorizer_challenge) {
      return 0;
    }
    dout(10) << __func__ << " bad authorizer on " << con << dendl;
    return -EACCES;
  } else if (auth_meta->auth_mode < AUTH_MODE_MON ||
	     auth_meta->auth_mode > AUTH_MODE_MON_MAX) {
    derr << __func__ << " unrecognized auth mode " << auth_meta->auth_mode
	 << dendl;
    return -EACCES;
  }

  // wait until we've formed an initial quorum on mkfs so that we have
  // the initial keys (e.g., client.admin).
  if (authmon()->get_last_committed() == 0) {
    dout(10) << __func__ << " haven't formed initial quorum, EBUSY" << dendl;
    return -EBUSY;
  }

  RefCountedPtr priv;
  MonSession *s;
  int32_t r = 0;
  auto p = payload.begin();
  if (!more) {
    if (con->get_priv()) {
      return -EACCES; // wtf
    }

    // handler?
    unique_ptr<AuthServiceHandler> auth_handler{get_auth_service_handler(
      auth_method, g_ceph_context, &key_server)};
    if (!auth_handler) {
      dout(1) << __func__ << " auth_method " << auth_method << " not supported"
	      << dendl;
      return -EOPNOTSUPP;
    }

    uint8_t mode;
    EntityName entity_name;

    try {
      decode(mode, p);
      if (mode < AUTH_MODE_MON ||
	  mode > AUTH_MODE_MON_MAX) {
	dout(1) << __func__ << " invalid mode " << (int)mode << dendl;
	return -EACCES;
      }
      assert(mode >= AUTH_MODE_MON && mode <= AUTH_MODE_MON_MAX);
      decode(entity_name, p);
      decode(con->peer_global_id, p);
    } catch (ceph::buffer::error& e) {
      dout(1) << __func__ << " failed to decode, " << e.what() << dendl;
      return -EACCES;
    }

    // supported method?
    if (entity_name.get_type() == CEPH_ENTITY_TYPE_MON ||
	entity_name.get_type() == CEPH_ENTITY_TYPE_OSD ||
	entity_name.get_type() == CEPH_ENTITY_TYPE_MDS ||
	entity_name.get_type() == CEPH_ENTITY_TYPE_MGR) {
      if (!auth_cluster_required.is_supported_auth(auth_method)) {
	dout(10) << __func__ << " entity " << entity_name << " method "
		 << auth_method << " not among supported "
		 << auth_cluster_required.get_supported_set() << dendl;
	return -EOPNOTSUPP;
      }
    } else {
      if (!auth_service_required.is_supported_auth(auth_method)) {
	dout(10) << __func__ << " entity " << entity_name << " method "
		 << auth_method << " not among supported "
		 << auth_cluster_required.get_supported_set() << dendl;
	return -EOPNOTSUPP;
      }
    }

    // for msgr1 we would do some weirdness here to ensure signatures
    // are supported by the client if we require it.  for msgr2 that
    // is not necessary.

    bool is_new_global_id = false;
    if (!con->peer_global_id) {
      con->peer_global_id = authmon()->_assign_global_id();
      if (!con->peer_global_id) {
	dout(1) << __func__ << " failed to assign global_id" << dendl;
	return -EBUSY;
      }
      is_new_global_id = true;
    }

    // set up partial session
    s = new MonSession(con);
    s->auth_handler = auth_handler.release();
    con->set_priv(RefCountedPtr{s, false});

    r = s->auth_handler->start_session(
      entity_name,
      con->peer_global_id,
      is_new_global_id,
      reply,
      &con->peer_caps_info);
  } else {
    priv = con->get_priv();
    if (!priv) {
      // this can happen if the async ms_handle_reset event races with
      // the unlocked call into handle_auth_request
      return -EACCES;
    }
    s = static_cast<MonSession*>(priv.get());
    r = s->auth_handler->handle_request(
      p,
      auth_meta->get_connection_secret_length(),
      reply,
      &con->peer_caps_info,
      &auth_meta->session_key,
      &auth_meta->connection_secret);
  }
  if (r > 0 &&
      !s->authenticated) {
    if (!ms_handle_fast_authentication(con)) {
      return -EACCES;
    }
  }

  dout(30) << " r " << r << " reply:\n";
  reply->hexdump(*_dout);
  *_dout << dendl;
  return r;
}

void Monitor::ms_handle_accept(Connection *con)
{
  auto priv = con->get_priv();
  MonSession *s = static_cast<MonSession*>(priv.get());
  if (!s) {
    // legacy protocol v1?
    dout(10) << __func__ << " con " << con << " no session" << dendl;
    return;
  }

  if (s->item.is_on_list()) {
    dout(10) << __func__ << " con " << con << " session " << s
	     << " already on list" << dendl;
  } else {
    std::lock_guard l(session_map_lock);
    if (state == STATE_SHUTDOWN) {
      dout(10) << __func__ << " ignoring new con " << con << " (shutdown)" << dendl;
      con->mark_down();
      return;
    }
    dout(10) << __func__ << " con " << con << " session " << s
	     << " registering session for "
	     << con->get_peer_addrs() << dendl;
    s->_ident(entity_name_t(con->get_peer_type(), con->get_peer_id()),
	      con->get_peer_addrs());
    session_map.add_session(s);
  }
}

bool Monitor::ms_handle_fast_authentication(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    // mon <-> mon connections need no Session, and setting one up
    // creates an awkward ref cycle between Session and Connection.
    return true;
  }

  auto priv = con->get_priv();
  MonSession *s = static_cast<MonSession*>(priv.get());
  if (!s) {
    // must be msgr2, otherwise dispatch would have set up the session.
    if (state == STATE_SHUTDOWN) {
      dout(10) << __func__ << " ignoring new con " << con << " (shutdown)" << dendl;
      con->mark_down();
      return false;
    }
    s = session_map.new_session(
      entity_name_t(con->get_peer_type(), -1),  // we don't know yet
      con->get_peer_addrs(),
      con);
    assert(s);
    dout(10) << __func__ << " adding session " << s << " to con " << con
	     << dendl;
    con->set_priv(s);
    logger->set(l_mon_num_sessions, session_map.get_size());
    logger->inc(l_mon_session_add);
  }
  dout(10) << __func__ << " session " << s << " con " << con
	   << " addr " << s->con->get_peer_addr()
	   << " " << *s << dendl;

  AuthCapsInfo &caps_info = con->get_peer_caps_info();
  if (caps_info.allow_all) {
    s->caps.set_allow_all();
    s->authenticated = true;
    return true;
  } else if (caps_info.caps.length()) {
    bufferlist::const_iterator p = caps_info.caps.cbegin();
    string str;
    try {
      decode(str, p);
    } catch (const ceph::buffer::error &err) {
      derr << __func__ << " corrupt cap data for " << con->get_peer_entity_name()
	   << " in auth db" << dendl;
      return false;
    }
    if (s->caps.parse(str, NULL)) {
      s->authenticated = true;
      return true;
    } else {
      derr << __func__ << " unparseable caps '" << str << "' for "
           << con->get_peer_entity_name() << dendl;
      return false;
    }
  } else {
    return false;
  }
}

void Monitor::set_mon_crush_location(const string& loc)
{
  if (loc.empty()) {
    return;
  }
  vector<string> loc_vec;
  loc_vec.push_back(loc);
  CrushWrapper::parse_loc_map(loc_vec, &crush_loc);
  need_set_crush_loc = true;
}

void Monitor::notify_new_monmap(bool can_change_external_state, bool remove_rank_elector)
{
  if (need_set_crush_loc) {
    auto my_info_i = monmap->mon_info.find(name);
    if (my_info_i != monmap->mon_info.end() &&
	my_info_i->second.crush_loc == crush_loc) {
      need_set_crush_loc = false;
    }
  }
  elector.notify_strategy_maybe_changed(monmap->strategy);
  if (remove_rank_elector){
    dout(10) << __func__ << " we have " << monmap->ranks.size()<< " ranks" << dendl;
    dout(10) << __func__ << " we have " << monmap->removed_ranks.size() << " removed ranks" << dendl;
    for (auto i = monmap->removed_ranks.rbegin();
        i != monmap->removed_ranks.rend(); ++i) {
      int remove_rank = *i;
      dout(10) << __func__ << " removing rank " << remove_rank << dendl;
      if (rank == remove_rank) {
        dout(5) << "We are removing our own rank, probably we"
          << " are removed from monmap before we shutdown ... dropping." << dendl;
        continue;
      }
      int new_rank = monmap->get_rank(messenger->get_myaddrs());
      if (new_rank == -1) {
        dout(5) << "We no longer exists in the monmap! ... dropping." << dendl;
        continue;
      }
      elector.notify_rank_removed(remove_rank, new_rank);
    }
  }

  if (monmap->stretch_mode_enabled) {
    try_engage_stretch_mode();
  }

  if (is_stretch_mode()) {
    if (!monmap->stretch_marked_down_mons.empty()) {
      dout(20) << __func__ << " stretch_marked_down_mons: " << monmap->stretch_marked_down_mons << dendl;
      set_degraded_stretch_mode();
    }
  }
  set_elector_disallowed_leaders(can_change_external_state);
}

void Monitor::set_elector_disallowed_leaders(bool allow_election)
{
  set<int> dl;
  // inherit dl from monmap
  for (auto name : monmap->disallowed_leaders) {
    dl.insert(monmap->get_rank(name));
  } // unconditionally add stretch_marked_down_mons to the new dl copy
  for (auto name : monmap->stretch_marked_down_mons) {
    dl.insert(monmap->get_rank(name));
  } // add the tiebreaker_mon incase it is not in monmap->disallowed_leaders
  if (!monmap->tiebreaker_mon.empty() &&
      monmap->contains(monmap->tiebreaker_mon)) {
      dl.insert(monmap->get_rank(monmap->tiebreaker_mon));
  }

  bool disallowed_changed = elector.set_disallowed_leaders(dl);
  if (disallowed_changed && allow_election) {
    elector.call_election();
  }
}

struct CMonEnableStretchMode : public Context {
  Monitor *m;
  CMonEnableStretchMode(Monitor *mon) : m(mon) {}
  void finish(int r) {
    m->try_engage_stretch_mode();
  }
};
void Monitor::try_engage_stretch_mode()
{
  dout(20) << __func__ << dendl;
  if (stretch_mode_engaged) return;
  if (!osdmon()->is_readable()) {
    dout(20) << "osdmon is not readable" << dendl;
    osdmon()->wait_for_readable_ctx(new CMonEnableStretchMode(this));
    return;
  }
  if (osdmon()->osdmap.stretch_mode_enabled &&
      monmap->stretch_mode_enabled) {
    dout(10) << "Engaging stretch mode!" << dendl;
    stretch_mode_engaged = true;
    int32_t stretch_divider_id = osdmon()->osdmap.stretch_mode_bucket;
    stretch_bucket_divider = osdmon()->osdmap.
      crush->get_type_name(stretch_divider_id);
    disconnect_disallowed_stretch_sessions();
  }
}

void Monitor::do_stretch_mode_election_work()
{
  dout(20) << __func__ << dendl;
  if (!is_stretch_mode() ||
      !is_leader()) return;
  dout(20) << "checking for degraded stretch mode" << dendl;
  map<string, set<string>> old_dead_buckets;
  old_dead_buckets.swap(dead_mon_buckets);
  up_mon_buckets.clear();
  // identify if we've lost a CRUSH bucket, request OSDMonitor check for death
  map<string,set<string>> down_mon_buckets;
  for (unsigned i = 0; i < monmap->size(); ++i) {
    const auto &mi = monmap->mon_info[monmap->get_name(i)];
    auto ci = mi.crush_loc.find(stretch_bucket_divider);
    ceph_assert(ci != mi.crush_loc.end());
    if (quorum.count(i)) {
      up_mon_buckets.insert(ci->second);
    } else {
      down_mon_buckets[ci->second].insert(mi.name);
    }
  }
  dout(20) << "prior dead_mon_buckets: " << old_dead_buckets
	   << "; down_mon_buckets: " << down_mon_buckets
	   << "; up_mon_buckets: " << up_mon_buckets << dendl;
  for (const auto& di : down_mon_buckets) {
    if (!up_mon_buckets.count(di.first)) {
      dead_mon_buckets[di.first] = di.second;
    }
  }
  dout(20) << "new dead_mon_buckets " << dead_mon_buckets << dendl;

  if (dead_mon_buckets != old_dead_buckets &&
      dead_mon_buckets.size() >= old_dead_buckets.size()) {
    maybe_go_degraded_stretch_mode();
  }
}

struct CMonGoDegraded : public Context {
  Monitor *m;
  CMonGoDegraded(Monitor *mon) : m(mon) {}
  void finish(int r) {
    m->maybe_go_degraded_stretch_mode();
  }
};

struct CMonGoRecovery : public Context {
  Monitor *m;
  CMonGoRecovery(Monitor *mon) : m(mon) {}
  void finish(int r) {
    m->go_recovery_stretch_mode();
  }
};
void Monitor::go_recovery_stretch_mode()
{
  dout(20) << __func__ << dendl;
  dout(20) << "is_leader(): " << is_leader() << dendl;
  if (!is_leader()) return;
  dout(20) << "is_degraded_stretch_mode(): " << is_degraded_stretch_mode() << dendl;
  if (!is_degraded_stretch_mode()) return;
  dout(20) << "is_recovering_stretch_mode(): " << is_recovering_stretch_mode() << dendl;
  if (is_recovering_stretch_mode()) return;
  dout(20) << "dead_mon_buckets.size(): " << dead_mon_buckets.size() << dendl;
  dout(20) << "dead_mon_buckets: " << dead_mon_buckets << dendl;
  if (dead_mon_buckets.size()) {
    ceph_assert( 0 == "how did we try and do stretch recovery while we have dead monitor buckets?");
    // we can't recover if we are missing monitors in a zone!
    return;
  }
  
  if (!osdmon()->is_readable()) {
    dout(20) << "osdmon is not readable" << dendl;
    osdmon()->wait_for_readable_ctx(new CMonGoRecovery(this));
    return;
  }

  if (!osdmon()->is_writeable()) {
    dout(20) << "osdmon is not writeable" << dendl;
    osdmon()->wait_for_writeable_ctx(new CMonGoRecovery(this));
    return;
  }
  osdmon()->trigger_recovery_stretch_mode();
}

void Monitor::set_recovery_stretch_mode()
{
  degraded_stretch_mode = true;
  recovering_stretch_mode = true;
  osdmon()->set_recovery_stretch_mode();
}

void Monitor::maybe_go_degraded_stretch_mode()
{
  dout(20) << __func__ << dendl;
  if (is_degraded_stretch_mode()) return;
  if (!is_leader()) return;
  if (dead_mon_buckets.empty()) return;
  if (!osdmon()->is_readable()) {
    osdmon()->wait_for_readable_ctx(new CMonGoDegraded(this));
    return;
  }
  ceph_assert(monmap->contains(monmap->tiebreaker_mon));
  // filter out the tiebreaker zone and check if remaining sites are down by OSDs too
  const auto &mi = monmap->mon_info[monmap->tiebreaker_mon];
  auto ci = mi.crush_loc.find(stretch_bucket_divider);
  map<string, set<string>> filtered_dead_buckets = dead_mon_buckets;
  filtered_dead_buckets.erase(ci->second);

  set<int> matched_down_buckets;
  set<string> matched_down_mons;
  bool dead = osdmon()->check_for_dead_crush_zones(filtered_dead_buckets,
						   &matched_down_buckets,
						   &matched_down_mons);
  if (dead) {
    if (!osdmon()->is_writeable()) {
      dout(20) << "osdmon is not writeable" << dendl;
      osdmon()->wait_for_writeable_ctx(new CMonGoDegraded(this));
      return;
    }
    if (!monmon()->is_writeable()) {
      dout(20) << "monmon is not writeable" << dendl;
      monmon()->wait_for_writeable_ctx(new CMonGoDegraded(this));
      return;
    }
    trigger_degraded_stretch_mode(matched_down_mons, matched_down_buckets);
  }
}

void Monitor::trigger_degraded_stretch_mode(const set<string>& dead_mons,
					    const set<int>& dead_buckets)
{
  dout(20) << __func__ << dendl;
  ceph_assert(osdmon()->is_writeable());
  ceph_assert(monmon()->is_writeable());

  // figure out which OSD zone(s) remains alive by removing
  // tiebreaker mon from up_mon_buckets
  set<string> live_zones = up_mon_buckets;
  ceph_assert(monmap->contains(monmap->tiebreaker_mon));
  const auto &mi = monmap->mon_info[monmap->tiebreaker_mon];
  auto ci = mi.crush_loc.find(stretch_bucket_divider);
  live_zones.erase(ci->second);
  ceph_assert(live_zones.size() == 1); // only support 2 zones right now
  
  osdmon()->trigger_degraded_stretch_mode(dead_buckets, live_zones);
  monmon()->trigger_degraded_stretch_mode(dead_mons);
  set_degraded_stretch_mode();
}

void Monitor::set_degraded_stretch_mode()
{
  dout(20) << __func__ << dendl;
  degraded_stretch_mode = true;
  recovering_stretch_mode = false;
  osdmon()->set_degraded_stretch_mode();
}

struct CMonGoHealthy : public Context {
  Monitor *m;
  CMonGoHealthy(Monitor *mon) : m(mon) {}
  void finish(int r) {
    m->trigger_healthy_stretch_mode();
  }
};


void Monitor::trigger_healthy_stretch_mode()
{
  dout(20) << __func__ << dendl;
  if (!is_degraded_stretch_mode()) return;
  if (!is_leader()) return;
  if (!osdmon()->is_writeable()) {
    dout(20) << "osdmon is not writeable" << dendl;
    osdmon()->wait_for_writeable_ctx(new CMonGoHealthy(this));
    return;
  }
  if (!monmon()->is_writeable()) {
    dout(20) << "monmon is not writeable" << dendl;
    monmon()->wait_for_writeable_ctx(new CMonGoHealthy(this));
    return;
  }

  ceph_assert(osdmon()->osdmap.recovering_stretch_mode);
  osdmon()->trigger_healthy_stretch_mode();
  monmon()->trigger_healthy_stretch_mode();
}

void Monitor::set_healthy_stretch_mode()
{
  degraded_stretch_mode = false;
  recovering_stretch_mode = false;
  osdmon()->set_healthy_stretch_mode();
}

bool Monitor::session_stretch_allowed(MonSession *s, MonOpRequestRef& op)
{
  if (!is_stretch_mode()) return true;
  if (s->proxy_con) return true;
  if (s->validated_stretch_connection) return true;
  if (!s->con) return true;
  if (s->con->peer_is_osd()) {
    dout(20) << __func__ << "checking OSD session" << s << dendl;
    // okay, check the crush location
    int barrier_id = [&] {
      auto type_id = osdmon()->osdmap.crush->get_validated_type_id(
	stretch_bucket_divider);
      ceph_assert(type_id.has_value());
      return *type_id;
    }();
    int osd_bucket_id = osdmon()->osdmap.crush->get_parent_of_type(s->con->peer_id,
								   barrier_id);
    const auto &mi = monmap->mon_info.find(name);
    ceph_assert(mi != monmap->mon_info.end());
    auto ci = mi->second.crush_loc.find(stretch_bucket_divider);
    ceph_assert(ci != mi->second.crush_loc.end());
    int mon_bucket_id = osdmon()->osdmap.crush->get_item_id(ci->second);
    
    if (osd_bucket_id != mon_bucket_id) {
      dout(5) << "discarding session " << *s
	      << " and sending OSD to matched zone" << dendl;
      s->con->mark_down();
      std::lock_guard l(session_map_lock);
      remove_session(s);
      if (op) {
	op->mark_zap();
      }
      return false;
    }
  }

  s->validated_stretch_connection = true;
  return true;
}

void Monitor::disconnect_disallowed_stretch_sessions()
{
  dout(20) << __func__ << dendl;
  MonOpRequestRef blank;
  auto i = session_map.sessions.begin();
  while (i != session_map.sessions.end()) {
    auto j = i;
    ++i;
    session_stretch_allowed(*j, blank);
  }
}
