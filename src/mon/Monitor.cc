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

#include "Monitor.h"
#include "common/version.h"

#include "osd/OSDMap.h"

#include "MonitorStore.h"
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
#include "messages/MMonSync.h"
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

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/Timer.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"

#include "include/color.h"
#include "include/ceph_fs.h"
#include "include/str_list.h"

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
  lock("Monitor::lock"),
  timer(cct_, lock),
  has_ever_joined(false),
  logger(NULL), cluster_logger(NULL), cluster_logger_registered(false),
  monmap(map),
  clog(cct_, messenger, monmap, LogClient::FLAG_MON),
  key_server(cct, &keyring),
  auth_cluster_required(cct,
			cct->_conf->auth_supported.length() ?
			cct->_conf->auth_supported : cct->_conf->auth_cluster_required),
  auth_service_required(cct,
			cct->_conf->auth_supported.length() ?
			cct->_conf->auth_supported : cct->_conf->auth_service_required),
  store(s),
  
  state(STATE_PROBING),
  
  elector(this),
  leader(0),
  quorum_features(0),

  // trim & store sync
  sync_role(SYNC_ROLE_NONE),
  trim_lock("Monitor::trim_lock"),
  trim_enable_timer(NULL),
  sync_rng(getpid()),
  sync_state(SYNC_STATE_NONE),
  sync_leader(),
  sync_provider(),

  timecheck_round(0),
  timecheck_acks(0),
  timecheck_event(NULL),

  probe_timeout_event(NULL),

  paxos_service(PAXOS_NUM),
  admin_hook(NULL),
  routed_request_tid(0)
{
  rank = -1;

  paxos = new Paxos(this, "paxos");

  paxos_service[PAXOS_MDSMAP] = new MDSMonitor(this, paxos, "mdsmap");
  paxos_service[PAXOS_MONMAP] = new MonmapMonitor(this, paxos, "monmap");
  paxos_service[PAXOS_OSDMAP] = new OSDMonitor(this, paxos, "osdmap");
  paxos_service[PAXOS_PGMAP] = new PGMonitor(this, paxos, "pgmap");
  paxos_service[PAXOS_LOG] = new LogMonitor(this, paxos, "logm");
  paxos_service[PAXOS_AUTH] = new AuthMonitor(this, paxos, "auth");

  health_monitor = new HealthMonitor(this);
  config_key_service = new ConfigKeyService(this, paxos);

  mon_caps = new MonCap();
  bool r = mon_caps->parse("allow *", NULL);
  assert(r);

  exited_quorum = ceph_clock_now(g_ceph_context);
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
}


enum {
  l_mon_first = 456000,
  l_mon_last,
};


class AdminHook : public AdminSocketHook {
  Monitor *mon;
public:
  AdminHook(Monitor *m) : mon(m) {}
  bool call(std::string command, std::string args, bufferlist& out) {
    stringstream ss;
    mon->do_admin_command(command, args, ss);
    out.append(ss);
    return true;
  }
};

void Monitor::do_admin_command(string command, string args, ostream& ss)
{
  Mutex::Locker l(lock);
  if (command == "mon_status")
    _mon_status(ss);
  else if (command == "quorum_status")
    _quorum_status(ss);
  else if (command == "sync_status")
    _sync_status(ss);
  else if (command == "sync_force") {
    if (args != "--yes-i-really-mean-it") {
      ss << "are you SURE? this will mean the monitor store will be erased "
            "the next time the monitor is restarted.  pass "
            "'--yes-i-really-mean-it' if you really do.";
      return;
    }
    _sync_force(ss);
  } else if (command.find("add_bootstrap_peer_hint") == 0)
    _add_bootstrap_peer_hint(command, args, ss);
  else
    assert(0 == "bad AdminSocket command binding");
}

void Monitor::handle_signal(int signum)
{
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got Signal " << sys_siglist[signum] << " ***" << dendl;
  shutdown();
}

CompatSet Monitor::get_supported_features()
{
  CompatSet::FeatureSet ceph_mon_feature_compat;
  CompatSet::FeatureSet ceph_mon_feature_ro_compat;
  CompatSet::FeatureSet ceph_mon_feature_incompat;
  ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_BASE);
  ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_SINGLE_PAXOS);
  return CompatSet(ceph_mon_feature_compat, ceph_mon_feature_ro_compat,
		   ceph_mon_feature_incompat);
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

  bufferlist features;
  store->get(MONITOR_NAME, COMPAT_SET_LOC, features);
  if (features.length() == 0) {
    generic_dout(0) << "WARNING: mon fs missing feature list.\n"
	    << "Assuming it is old-style and introducing one." << dendl;
    //we only want the baseline ~v.18 features assumed to be on disk.
    //If new features are introduced this code needs to disappear or
    //be made smarter.
    ondisk = get_legacy_features();

    bufferlist bl;
    ondisk.encode(bl);
    MonitorDBStore::Transaction t;
    t.put(MONITOR_NAME, COMPAT_SET_LOC, bl);
    store->apply_transaction(t);
  } else {
    bufferlist::iterator it = features.begin();
    ondisk.decode(it);
  }

  if (!required.writeable(ondisk)) {
    CompatSet diff = required.unsupported(ondisk);
    generic_derr << "ERROR: on disk data includes unsupported features: " << diff << dendl;
    return -EPERM;
  }

  return 0;
}

void Monitor::read_features()
{
  bufferlist bl;
  store->get(MONITOR_NAME, COMPAT_SET_LOC, bl);
  assert(bl.length());

  bufferlist::iterator p = bl.begin();
  ::decode(features, p);
  dout(10) << "features " << features << dendl;
}

void Monitor::write_features(MonitorDBStore::Transaction &t)
{
  bufferlist bl;
  features.encode(bl);
  t.put(MONITOR_NAME, COMPAT_SET_LOC, bl);
}

int Monitor::preinit()
{
  lock.Lock();

  dout(1) << "preinit fsid " << monmap->fsid << dendl;
  
  assert(!logger);
  {
    PerfCountersBuilder pcb(g_ceph_context, "mon", l_mon_first, l_mon_last);
    // ...
    logger = pcb.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
  }

  assert(!cluster_logger);
  {
    PerfCountersBuilder pcb(g_ceph_context, "cluster", l_cluster_first, l_cluster_last);
    pcb.add_u64(l_cluster_num_mon, "num_mon");
    pcb.add_u64(l_cluster_num_mon_quorum, "num_mon_quorum");
    pcb.add_u64(l_cluster_num_osd, "num_osd");
    pcb.add_u64(l_cluster_num_osd_up, "num_osd_up");
    pcb.add_u64(l_cluster_num_osd_in, "num_osd_in");
    pcb.add_u64(l_cluster_osd_epoch, "osd_epoch");
    pcb.add_u64(l_cluster_osd_kb, "osd_kb");
    pcb.add_u64(l_cluster_osd_kb_used, "osd_kb_used");
    pcb.add_u64(l_cluster_osd_kb_avail, "osd_kb_avail");
    pcb.add_u64(l_cluster_num_pool, "num_pool");
    pcb.add_u64(l_cluster_num_pg, "num_pg");
    pcb.add_u64(l_cluster_num_pg_active_clean, "num_pg_active_clean");
    pcb.add_u64(l_cluster_num_pg_active, "num_pg_active");
    pcb.add_u64(l_cluster_num_pg_peering, "num_pg_peering");
    pcb.add_u64(l_cluster_num_object, "num_object");
    pcb.add_u64(l_cluster_num_object_degraded, "num_object_degraded");
    pcb.add_u64(l_cluster_num_object_unfound, "num_object_unfound");
    pcb.add_u64(l_cluster_num_bytes, "num_bytes");
    pcb.add_u64(l_cluster_num_mds_up, "num_mds_up");
    pcb.add_u64(l_cluster_num_mds_in, "num_mds_in");
    pcb.add_u64(l_cluster_num_mds_failed, "num_mds_failed");
    pcb.add_u64(l_cluster_mds_epoch, "mds_epoch");
    cluster_logger = pcb.create_perf_counters();
  }

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
  }

  {
    // We have a potentially inconsistent store state in hands. Get rid of it
    // and start fresh.
    bool clear_store = false;
    if (is_sync_on_going()) {
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

      MonitorDBStore::Transaction t;
      t.erase("mon_sync", "in_sync");
      t.erase("mon_sync", "force_sync");
      store->apply_transaction(t);
    }
  }

  init_paxos();
  health_monitor->init();

  // we need to bootstrap authentication keys so we can form an
  // initial quorum.
  if (authmon()->get_version() == 0) {
    dout(10) << "loading initial keyring to bootstrap authentication for mkfs" << dendl;
    bufferlist bl;
    store->get("mkfs", "keyring", bl);
    KeyRing keyring;
    bufferlist::iterator p = bl.begin();
    ::decode(keyring, p);
    extract_save_mon_key(keyring);
  }

  string keyring_loc = g_conf->mon_data + "/keyring";

  int r = keyring.load(cct, keyring_loc);
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
  r = admin_socket->register_command("sync_status", "sync_status", admin_hook,
				     "show current synchronization status");
  assert(r == 0);
  r = admin_socket->register_command("add_bootstrap_peer_hint",
				     "add_bootstrap_peer_hint name=addr,type=CephIPAddr",
				     admin_hook,
				     "add peer address as potential bootstrap peer for cluster bringup");
  assert(r == 0);
  lock.Lock();

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

  lock.Unlock();
  return 0;
}

void Monitor::init_paxos()
{
  dout(10) << __func__ << dendl;
  paxos->init();

  // update paxos
  for (int i = 0; i < PAXOS_NUM; ++i) {
    if (paxos->is_consistent()) {
      paxos_service[i]->update_from_paxos();
    }
  }

  // init services
  for (int i = 0; i < PAXOS_NUM; ++i) {
    if (paxos->is_consistent()) {
      paxos_service[i]->init();
    }
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

  state = STATE_SHUTDOWN;

  if (admin_hook) {
    AdminSocket* admin_socket = cct->get_admin_socket();
    admin_socket->unregister_command("mon_status");
    admin_socket->unregister_command("quorum_status");
    admin_socket->unregister_command("sync_status");
    admin_socket->unregister_command("add_bootstrap_peer_hint");
    delete admin_hook;
    admin_hook = NULL;
  }

  elector.shutdown();

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
  
  // clean up
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); ++p)
    (*p)->shutdown();
  health_monitor->shutdown();

  finish_contexts(g_ceph_context, waitfor_quorum, -ECANCELED);
  finish_contexts(g_ceph_context, maybe_wait_for_quorum, -ECANCELED);


  timer.shutdown();

  // unlock before msgr shutdown...
  lock.Unlock();

  remove_all_sessions();
  messenger->shutdown();  // last thing!  ceph_mon.cc will delete mon.
}

void Monitor::bootstrap()
{
  dout(10) << "bootstrap" << dendl;

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

  reset_sync();

  // reset
  state = STATE_PROBING;

  reset();

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

void Monitor::_add_bootstrap_peer_hint(string cmd, string args, ostream& ss)
{
  dout(10) << "_add_bootstrap_peer_hint '" << cmd << "' '" << args << "'" << dendl;

  entity_addr_t addr;
  const char *end = 0;
  if (!addr.parse(args.c_str(), &end)) {
    ss << "failed to parse addr '" << args << "'; syntax is 'add_bootstrap_peer_hint ip[:port]'";
    return;
  }

  if (is_leader() || is_peon()) {
    ss << "mon already active; ignoring bootstrap hint";
    return;
  }

  if (addr.get_port() == 0)
    addr.set_port(CEPH_MON_PORT);

  extra_probe_peers.insert(addr);
  ss << "adding peer " << addr << " to list: " << extra_probe_peers;
}

// called by bootstrap(), or on leader|peon -> electing
void Monitor::reset()
{
  dout(10) << "reset" << dendl;

  timecheck_finish();

  leader_since = utime_t();
  if (!quorum.empty()) {
    exited_quorum = ceph_clock_now(g_ceph_context);
  }
  quorum.clear();
  outside_quorum.clear();

  paxos->restart();

  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); ++p)
    (*p)->restart();
  health_monitor->finish();
}

set<string> Monitor::get_sync_targets_names() {
  set<string> targets;
  targets.insert(paxos->get_name());
  for (int i = 0; i < PAXOS_NUM; ++i)
    targets.insert(paxos_service[i]->get_service_name());

  return targets;
}

/**
 * Reset any lingering sync/trim informations we might have.
 */
void Monitor::reset_sync(bool abort)
{
  dout(10) << __func__ << dendl;
  // clear everything trim/sync related
  {
    map<entity_inst_t,Context*>::iterator iter = trim_timeouts.begin();
    for (; iter != trim_timeouts.end(); ++iter) {
      if (!iter->second)
        continue;

      timer.cancel_event(iter->second);
      if (abort) {
        MMonSync *msg = new MMonSync(MMonSync::OP_ABORT);
        entity_inst_t other = iter->first;
        messenger->send_message(msg, other);
      }
    }
    trim_timeouts.clear();
  }
  {
    map<entity_inst_t,SyncEntity>::iterator iter = sync_entities.begin();
    for (; iter != sync_entities.end(); ++iter) {
      (*iter).second->cancel_timeout();
    }
    sync_entities.clear();
  }

  sync_entities_states.clear();
  trim_entities_states.clear();

  sync_leader.reset();
  sync_provider.reset();

  sync_state = SYNC_STATE_NONE;
  sync_role = SYNC_ROLE_NONE;
}

// leader

void Monitor::sync_send_heartbeat(entity_inst_t &other, bool reply)
{
  dout(10) << __func__ << " " << other << " reply(" << reply << ")" << dendl;
  uint32_t op = (reply ? MMonSync::OP_HEARTBEAT_REPLY : MMonSync::OP_HEARTBEAT);
  MMonSync *msg = new MMonSync(op);
  messenger->send_message(msg, other);
}

void Monitor::handle_sync_start(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  /* If we are not the leader, then some monitor picked us as the point of
   * entry to the quorum during its synchronization process. Therefore, we
   * have an obligation of forwarding this message to leader, so the sender
   * can start synchronizing.
   */
  if (!is_leader() && !quorum.empty()) {
    assert(!(sync_role & SYNC_ROLE_REQUESTER));
    assert(!(sync_role & SYNC_ROLE_LEADER));
    assert(!is_synchronizing());

    entity_inst_t leader = monmap->get_inst(get_leader());
    MMonSync *msg = new MMonSync(m);
    // keep forwarding the message up the chain if it has already been
    // forwarded.
    if (!(m->flags & MMonSync::FLAG_REPLY_TO)) {
      msg->set_reply_to(m->get_source_inst());
    }
    dout(10) << __func__ << " forward " << *m
	     << " to leader at " << leader << dendl;
    assert(g_conf->mon_sync_provider_kill_at != 1);
    messenger->send_message(msg, leader);
    assert(g_conf->mon_sync_provider_kill_at != 2);
    m->put();
    return;
  }

  // If we are synchronizing, then it means that we know someone who has a
  // higher version than the one we have; and if someone attempted to sync
  // from us, then that must mean they have a lower version than us.
  // Therefore, they must be much more interested in synchronizing from the
  // one we are trying to synchronize from than they are from us.
  // Moreover, if we are already synchronizing under the REQUESTER role, then
  // we must know someone who is in the quorum, either because we were lucky
  // enough to contact them in the first place or we managed to contact
  // someone who knew who they were. Therefore, just forward this request to
  // our sync leader.
  if (is_synchronizing()) {
    assert(!(sync_role & SYNC_ROLE_LEADER));
    assert(!(sync_role & SYNC_ROLE_PROVIDER));
    assert(quorum.empty());
    assert(sync_leader.get() != NULL);

    /**
     * This looks a bit odd, but we've seen cases where sync start messages
     * get bounced around and end up at the originator without anybody
     * noticing!* If it happens, just drop the message and the timeouts
     * will clean everything up -- eventually.
     * [*] If a leader gets elected who is too far behind, he'll drop into
     * bootstrap and sync, but the person he sends his sync to thinks he's
     * still the leader and forwards the reply back.
     */
    if (m->reply_to == messenger->get_myinst()) {
      m->put();
      return;
    }

    dout(10) << __func__ << " forward " << *m
             << " to our sync leader at "
             << sync_leader->entity << dendl;

    MMonSync *msg = new MMonSync(m);
    // keep forwarding the message up the chain if it has already been
    // forwarded.
    if (!(m->flags & MMonSync::FLAG_REPLY_TO)) {
      msg->set_reply_to(m->get_source_inst());
    }
    messenger->send_message(msg, sync_leader->entity);
    m->put();
    return;
  }

  // At this point we may or may not be the leader.  If we are not the leader,
  // it means that we are not in the quorum, but someone would still very much
  // like to synchronize with us.  In certain circumstances, letting them
  // synchronize with us is by far our only option -- for instance, when we
  // need them to form a quorum and they have started fresh or are severely
  // out of date --, but it won't hurt to let them sync from us anyway: if
  // they chose us, then they must have noticed that we had a higher version
  // than they do, so it makes sense to let them try their luck and join the
  // party.

  Mutex::Locker l(trim_lock);
  entity_inst_t other =
    (m->flags & MMonSync::FLAG_REPLY_TO ? m->reply_to : m->get_source_inst());

  assert(g_conf->mon_sync_leader_kill_at != 1);

  if (trim_timeouts.count(other) > 0) {
    dout(1) << __func__ << " sync session already in progress for " << other
	    << dendl;

    if (trim_entities_states[other] != SYNC_STATE_NONE) {
      dout(1) << __func__ << "    ignore stray message" << dendl;
      m->put();
      return;
    }

    dout(1) << __func__<< "    destroying current state and creating new"
	    << dendl;

    if (trim_timeouts[other])
      timer.cancel_event(trim_timeouts[other]);
    trim_timeouts.erase(other);
    trim_entities_states.erase(other);
  }

  MMonSync *msg = new MMonSync(MMonSync::OP_START_REPLY);

  if ((!quorum.empty() && paxos->should_trim())
      || (trim_enable_timer != NULL)) {
    msg->flags |= MMonSync::FLAG_RETRY;
  } else {
    trim_timeouts.insert(make_pair(other, new C_TrimTimeout(this, other)));
    timer.add_event_after(g_conf->mon_sync_trim_timeout, trim_timeouts[other]);

    trim_entities_states[other] = SYNC_STATE_START;
    sync_role |= SYNC_ROLE_LEADER;

    paxos->trim_disable();

    // Is the one that contacted us in the quorum?
    if (quorum.count(m->get_source().num())) {
      // Was it forwarded by someone?
      if (m->flags & MMonSync::FLAG_REPLY_TO) {
        // Then they must not be synchronizing. What sense would that make, eh?
        assert(trim_timeouts.count(m->get_source_inst()) == 0);
        // Set the provider as the one that contacted us. He's in the
        // quorum, so he's up to the job (i.e., he's not synchronizing)
        msg->set_reply_to(m->get_source_inst());
        dout(10) << __func__ << " set provider to " << msg->reply_to << dendl;
      } else {
        // Then they must have gotten into the quorum, and boostrapped.
        // We should tell them to abort and bootstrap ourselves.
        msg->put();
        msg = new MMonSync(MMonSync::OP_ABORT);
        messenger->send_message(msg, other);
        m->put();
        bootstrap();
        return;
      }
    } else if (!quorum.empty()) {
      // grab someone from the quorum and assign them as the sync provider
      int n = _pick_random_quorum_mon(rank);
      if (n >= 0) {
        msg->set_reply_to(monmap->get_inst(n));
        dout(10) << __func__ << " set quorum-based provider to "
                 << msg->reply_to << dendl;
      } else {
        assert(0 == "We shouldn't get here!");
      }
    } else {
      // There is no quorum, so we must either be in a quorum-less cluster,
      // or we must be mid-election.  Either way, tell them it is okay to
      // sync from us by not setting the reply-to field.
      assert(!(msg->flags & MMonSync::FLAG_REPLY_TO));
    }
  }
  messenger->send_message(msg, other);
  m->put();

  assert(g_conf->mon_sync_leader_kill_at != 2);
}

void Monitor::handle_sync_heartbeat(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();
  if (!(sync_role & SYNC_ROLE_LEADER)
      || !trim_entities_states.count(other)
      || (trim_entities_states[other] != SYNC_STATE_START)) {
    // stray message; ignore.
    dout(1) << __func__ << " ignored stray message " << *m << dendl;
    m->put();
    return;
  }

  if (!is_leader() && !quorum.empty()
      && (trim_timeouts.count(other) > 0)) {
    // we must have been the leader before, but we lost leadership to
    // someone else.
    sync_finish_abort(other);
    m->put();
    return;
  }

  assert(trim_timeouts.count(other) > 0);

  if (trim_timeouts[other])
    timer.cancel_event(trim_timeouts[other]);
  trim_timeouts[other] = new C_TrimTimeout(this, other);
  timer.add_event_after(g_conf->mon_sync_trim_timeout, trim_timeouts[other]);

  assert(g_conf->mon_sync_leader_kill_at != 3);
  sync_send_heartbeat(other, true);
  assert(g_conf->mon_sync_leader_kill_at != 4);

  m->put();
}

void Monitor::sync_finish(entity_inst_t &entity, bool abort)
{
  dout(10) << __func__ << " entity(" << entity << ")" << dendl;

  Mutex::Locker l(trim_lock);

  if (!trim_timeouts.count(entity)) {
    dout(1) << __func__ << " we know of no sync effort from "
	    << entity << " -- ignore it." << dendl;
    return;
  }

  if (trim_timeouts[entity] != NULL)
    timer.cancel_event(trim_timeouts[entity]);

  trim_timeouts.erase(entity);
  trim_entities_states.erase(entity);

  if (abort) {
    MMonSync *m = new MMonSync(MMonSync::OP_ABORT);
    assert(g_conf->mon_sync_leader_kill_at != 5);
    messenger->send_message(m, entity);
    assert(g_conf->mon_sync_leader_kill_at != 6);
  }

  if (!trim_timeouts.empty())
    return;

  dout(10) << __func__ << " no longer a sync leader" << dendl;
  sync_role &= ~SYNC_ROLE_LEADER;

  // we may have been the leader, but by now we may no longer be.
  // this can happen when the we sync'ed a monitor that became the
  // leader, or that same monitor simply came back to life and got
  // elected as the new leader.
  if (is_leader() && paxos->is_trim_disabled()) {
    trim_enable_timer = new C_TrimEnable(this);
    timer.add_event_after(30.0, trim_enable_timer);
  }

  finish_contexts(g_ceph_context, maybe_wait_for_quorum);
}

void Monitor::handle_sync_finish(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();

  if (!trim_timeouts.count(other) || !trim_entities_states.count(other)
      || (trim_entities_states[other] != SYNC_STATE_START)) {
    dout(1) << __func__ << " ignored stray message from " << other << dendl;
    if (!trim_timeouts.count(other))
      dout(1) << __func__ << "  not on trim_timeouts" << dendl;
    if (!trim_entities_states.count(other))
      dout(1) << __func__ << "  not on trim_entities_states" << dendl;
    else if (trim_entities_states[other] != SYNC_STATE_START)
      dout(1) << __func__ << "  state " << trim_entities_states[other] << dendl;
    m->put();
    return;
  }

  // We may no longer the leader. In such case, we should just inform the
  // other monitor that he should abort his sync. However, it appears that
  // his sync has finished, so there is no use in scraping the whole thing
  // now. Therefore, just go along and acknowledge.
  if (!is_leader()) {
    dout(10) << __func__ << " We are no longer the leader; reply nonetheless"
	     << dendl;
  }

  MMonSync *msg = new MMonSync(MMonSync::OP_FINISH_REPLY);
  assert(g_conf->mon_sync_leader_kill_at != 7);
  messenger->send_message(msg, other);
  assert(g_conf->mon_sync_leader_kill_at != 8);

  sync_finish(other);
  m->put();
}

// end of leader

// synchronization provider

int Monitor::_pick_random_mon(int other)
{
  assert(monmap->size() > 0);
  if (monmap->size() == 1)
    return 0;

  int max = monmap->size();
  if (other >= 0)
    max--;
  int n = sync_rng() % max;
  if (other >= 0 && n >= other)
    n++;
  return n;
}

int Monitor::_pick_random_quorum_mon(int other)
{
  assert(monmap->size() > 0);
  if (quorum.empty())
    return -1;
  set<int>::iterator p = quorum.begin();
  for (int n = sync_rng() % quorum.size(); p != quorum.end() && n; ++p, --n);
  if (other >= 0 && p != quorum.end() && *p == other)
    ++p;

  return (p == quorum.end() ? *(quorum.rbegin()) : *p);
}

void Monitor::sync_timeout(entity_inst_t &entity)
{
  if (state == STATE_SYNCHRONIZING) {
    assert(sync_role == SYNC_ROLE_REQUESTER);
    assert(sync_state == SYNC_STATE_CHUNKS);

    // we are a sync requester; our provider just timed out, so find another
    // monitor to synchronize with.
    dout(1) << __func__ << " " << sync_provider->entity << dendl;

    sync_provider->attempts++;
    if ((sync_provider->attempts > g_conf->mon_sync_max_retries)
	|| (monmap->size() == 2)) {
      // We either tried too many times to sync, or there's just us and the
      // monitor we were attempting to sync with.
      // Therefore, just abort the whole sync and start off fresh whenever he
      // (or somebody else) comes back.
      sync_requester_abort();
      return;
    }

    unsigned int i = 0;
    int entity_rank = monmap->get_rank(entity.addr);
    int debug_mon = g_conf->mon_sync_debug_provider;
    int debug_fallback = g_conf->mon_sync_debug_provider_fallback;

    dout(10) << __func__ << " entity " << entity << " rank " << entity_rank << dendl;
    dout(10) << __func__ << " our-rank " << rank << dendl;

    while ((i++) < 2*monmap->size()) {
      // we are trying to pick a random monitor, but we cannot do this forever.
      // in case something goes awfully wrong, just stop doing it after a
      // couple of attempts and try again later.
      int new_mon = _pick_random_mon(rank);

      if (debug_fallback >= 0) {
	if (entity_rank != debug_fallback)
	  new_mon = debug_fallback;
	else if (debug_mon >= 0 && (entity_rank != debug_mon))
	  new_mon = debug_mon;
      }

      dout(10) << __func__ << " randomly picking mon rank " << new_mon << dendl;

      if ((new_mon != rank) && (new_mon != entity_rank)) {
	sync_provider->entity = monmap->get_inst(new_mon);
	dout(10) << __func__ << " randomly choosing " << sync_provider->entity << " rank " << new_mon << dendl;
	sync_state = SYNC_STATE_START;
	sync_start_chunks(sync_provider);
	return;
      }
    }

    // well that sucks. Let's see if we can find a monitor to connect to
    for (int i = 0; i < (int)monmap->size(); ++i) {
      entity_inst_t i_inst = monmap->get_inst(i);
      if (i != rank && i_inst != entity) {
	sync_provider->entity = i_inst;
	sync_state = SYNC_STATE_START;
	sync_start_chunks(sync_provider);
	return;
      }
    }

    assert(0 == "Unable to find a new monitor to connect to. Not cool.");
  } else if (sync_role & SYNC_ROLE_PROVIDER) {
    dout(10) << __func__ << " cleanup " << entity << dendl;
    sync_provider_cleanup(entity);
    return;
  } else
    assert(0 == "We should never reach this");
}

void Monitor::sync_provider_cleanup(entity_inst_t &entity)
{
  dout(10) << __func__ << " " << entity << dendl;
  if (sync_entities.count(entity) > 0) {
    sync_entities[entity]->cancel_timeout();
    sync_entities.erase(entity);
    sync_entities_states.erase(entity);
  }

  if (sync_entities.empty()) {
    dout(1) << __func__ << " no longer a sync provider" << dendl;
    sync_role &= ~SYNC_ROLE_PROVIDER;
  }
}

void Monitor::handle_sync_start_chunks(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  assert(!(sync_role & SYNC_ROLE_REQUESTER));

  entity_inst_t other = m->get_source_inst();

  // if we have a sync going on for this entity, just drop the message. If it
  // was a stray message, we did the right thing. If it wasn't, then it means
  // that we still have an old state of this entity, and that the said entity
  // failed in the meantime and is now up again; therefore, just let the
  // timeout timers fulfill their purpose and deal with state cleanup when
  // they are triggered. Until then, no Sir, we won't accept your messages.
  if (sync_entities.count(other) > 0) {
    dout(1) << __func__ << " sync session already in progress for " << other
	    << " -- assumed as stray message." << dendl;
    m->put();
    return;
  }

  SyncEntity sync = get_sync_entity(other, this);
  sync->version = paxos->get_version();

  if (!m->last_key.first.empty() && !m->last_key.second.empty()) {
    sync->last_received_key = m->last_key;
    dout(10) << __func__ << " set last received key to ("
	     << sync->last_received_key.first << ","
	     << sync->last_received_key.second << ")" << dendl;
  }

  sync->sync_init();

  sync_entities.insert(make_pair(other, sync));
  sync_entities_states[other] = SYNC_STATE_START;
  sync_role |= SYNC_ROLE_PROVIDER;

  sync_send_chunks(sync);
  m->put();
}

void Monitor::handle_sync_chunk_reply(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();

  if (!(sync_role & SYNC_ROLE_PROVIDER)
      || !sync_entities.count(other)
      || (sync_entities_states[other] != SYNC_STATE_START)) {
    dout(1) << __func__ << " ignored stray message from " << other << dendl;
    m->put();
    return;
  }

  if (m->flags & MMonSync::FLAG_LAST) {
    // they acked the last chunk. Clean up.
    sync_provider_cleanup(other);
    m->put();
    return;
  }

  sync_send_chunks(sync_entities[other]);
  m->put();
}

void Monitor::sync_send_chunks(SyncEntity sync)
{
  dout(10) << __func__ << " entity(" << sync->entity << ")" << dendl;

  sync->cancel_timeout();

  assert(sync->synchronizer.use_count() > 0);
  assert(sync->synchronizer->has_next_chunk());

  MMonSync *msg = new MMonSync(MMonSync::OP_CHUNK);

  sync->synchronizer->get_chunk(msg->chunk_bl);
  msg->last_key = sync->synchronizer->get_last_key();
  dout(10) << __func__ << " last key ("
	   << msg->last_key.first << ","
	   << msg->last_key.second << ")" << dendl;

  sync->sync_update();

  if (sync->has_crc()) {
    msg->flags |= MMonSync::FLAG_CRC;
    msg->crc = sync->crc_get();
    sync->crc_clear();
  }

  if (!sync->synchronizer->has_next_chunk()) {
    msg->flags |= MMonSync::FLAG_LAST;
    msg->version = sync->get_version();
    sync->synchronizer.reset();
  }

  sync->set_timeout(new C_SyncTimeout(this, sync->entity),
		    g_conf->mon_sync_timeout);
  assert(g_conf->mon_sync_provider_kill_at != 3);
  messenger->send_message(msg, sync->entity);
  assert(g_conf->mon_sync_provider_kill_at != 4);

  // kill the monitor as soon as we move into synchronizing the paxos versions.
  // This is intended as debug.
  if (sync->sync_state == SyncEntityImpl::STATE_PAXOS)
    assert(g_conf->mon_sync_provider_kill_at != 5);


}
// end of synchronization provider

// start of synchronization requester

void Monitor::sync_requester_abort()
{
  dout(10) << __func__;
  assert(state == STATE_SYNCHRONIZING);
  assert(sync_role == SYNC_ROLE_REQUESTER);

  if (sync_leader.get() != NULL) {
    *_dout << " " << sync_leader->entity;
    sync_leader->cancel_timeout();
    sync_leader.reset();
  }

  if (sync_provider.get() != NULL) {
    *_dout << " " << sync_provider->entity;
    sync_provider->cancel_timeout();

    MMonSync *msg = new MMonSync(MMonSync::OP_ABORT);
    messenger->send_message(msg, sync_provider->entity);

    sync_provider.reset();
  }
  *_dout << " clearing potentially inconsistent store" << dendl;

  // Given that we are explicitely aborting the whole sync process, we should
  // play it safe and clear the store.
  set<string> targets = get_sync_targets_names();
  targets.insert("mon_sync");
  store->clear(targets);

  dout(1) << __func__ << " no longer a sync requester" << dendl;
  sync_role = SYNC_ROLE_NONE;
  sync_state = SYNC_STATE_NONE;

  state = 0;

  bootstrap();
}

/**
 *
 */
void Monitor::sync_store_init()
{
  MonitorDBStore::Transaction t;
  t.put("mon_sync", "in_sync", 1);

  bufferlist latest_monmap;
  int err = monmon()->get_monmap(latest_monmap);
  if (err < 0) {
    if (err != -ENOENT) {
      derr << __func__
           << " something wrong happened while reading the store: "
           << cpp_strerror(err) << dendl;
      assert(0 == "error reading the store");
      return; // this is moot
    } else {
      dout(10) << __func__ << " backup current monmap" << dendl;
      monmap->encode(latest_monmap, CEPH_FEATURES_ALL);
    }
  }

  t.put("mon_sync", "latest_monmap", latest_monmap);

  store->apply_transaction(t);
}

void Monitor::sync_store_cleanup()
{
  MonitorDBStore::Transaction t;
  t.erase("mon_sync", "in_sync");
  t.erase("mon_sync", "latest_monmap");
  store->apply_transaction(t);
}

bool Monitor::is_sync_on_going()
{
  return store->exists("mon_sync", "in_sync");
}

/**
 * Start Sync process
 *
 * Create SyncEntity instances for the leader and the provider;
 * Send OP_START message to the leader;
 * Set trim timeout on the leader
 *
 * @param other Synchronization provider to-be.
 */
void Monitor::sync_start(entity_inst_t &other)
{
  cancel_probe_timeout();

  dout(10) << __func__ << " entity( " << other << " )" << dendl;
  if ((state == STATE_SYNCHRONIZING) && (sync_role == SYNC_ROLE_REQUESTER)) {
    dout(1) << __func__ << " already synchronizing; drop it" << dendl;
    return;
  }

  // Looks like we are the acting leader for someone.  Better force them to
  // abort their endeavours.  After all, if they are trying to sync from us,
  // it means that we must have a higher paxos version than the one they
  // have; however, if we are trying to sync as well, it must mean that
  // someone has a higher version than the one we have.  Everybody wins if
  // we force them to cancel their sync and try again.
  if (sync_role & SYNC_ROLE_LEADER) {
    dout(10) << __func__ << " we are acting as a leader to someone; "
             << "destroy their dreams" << dendl;

    assert(!trim_timeouts.empty());
    reset_sync();
  }

  assert(sync_role == SYNC_ROLE_NONE);
  assert(sync_state == SYNC_STATE_NONE);

  state = STATE_SYNCHRONIZING;
  sync_role = SYNC_ROLE_REQUESTER;
  sync_state = SYNC_STATE_START;

  // clear the underlying store, since we are starting a whole
  // sync process from the bare beginning.
  set<string> targets = get_sync_targets_names();
  targets.insert("mon_sync");
  store->clear(targets);


  sync_store_init();

  // assume 'other' as the leader. We will update the leader once we receive
  // a reply to the sync start.
  entity_inst_t leader = other;
  entity_inst_t provider = other;

  if (g_conf->mon_sync_debug_leader >= 0) {
    leader = monmap->get_inst(g_conf->mon_sync_debug_leader);
    dout(10) << __func__ << " assuming " << leader
	     << " as the leader for debug" << dendl;
  }

  if (g_conf->mon_sync_debug_provider >= 0) {
    provider = monmap->get_inst(g_conf->mon_sync_debug_provider);
    dout(10) << __func__ << " assuming " << provider
	     << " as the provider for debug" << dendl;
  }

  sync_leader = get_sync_entity(leader, this);
  sync_provider = get_sync_entity(provider, this);

  // this message may bounce through 'other' (if 'other' is not the leader)
  // in order to reach the leader. Therefore, set a higher timeout to allow
  // breathing room for the reply message to reach us.
  sync_leader->set_timeout(new C_SyncStartTimeout(this),
			   g_conf->mon_sync_trim_timeout*2);

  MMonSync *m = new MMonSync(MMonSync::OP_START);
  messenger->send_message(m, other);
  assert(g_conf->mon_sync_requester_kill_at != 1);
}

void Monitor::sync_start_chunks(SyncEntity provider)
{
  dout(10) << __func__ << " provider(" << provider->entity << ")" << dendl;

  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_START);

  sync_state = SYNC_STATE_CHUNKS;

  provider->set_timeout(new C_SyncTimeout(this, provider->entity),
			g_conf->mon_sync_timeout);
  MMonSync *msg = new MMonSync(MMonSync::OP_START_CHUNKS);
  pair<string,string> last_key = provider->last_received_key;
  if (!last_key.first.empty() && !last_key.second.empty())
    msg->last_key = last_key;

  assert(g_conf->mon_sync_requester_kill_at != 4);
  messenger->send_message(msg, provider->entity);
  assert(g_conf->mon_sync_requester_kill_at != 5);
}

void Monitor::sync_start_reply_timeout()
{
  dout(10) << __func__ << dendl;

  assert(state == STATE_SYNCHRONIZING);
  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_START);

  // Restart the sync attempt. It's not as if we were going to lose a vast
  // amount of work, and if we take into account that we are timing out while
  // waiting for a reply from the Leader, it sure seems like the right path
  // to take.
  sync_requester_abort();
}

void Monitor::handle_sync_start_reply(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();

  if ((sync_role != SYNC_ROLE_REQUESTER)
      || (sync_state != SYNC_STATE_START)) {
    // If the leader has sent this message before we failed, there is no point
    // in replying to it, as he has no idea that we actually received it. On
    // the other hand, if he received one of our stray messages (because it was
    // delivered once he got back up after failing) and replied accordingly,
    // there is a chance that he did stopped trimming on our behalf. However,
    // we have no way to know it, and we really don't want to mess with his
    // state if that is not the case. Therefore, just drop it and let the
    // timeouts figure it out. Eventually.
    dout(1) << __func__ << " stray message -- drop it." << dendl;
    goto out;
  }

  assert(state == STATE_SYNCHRONIZING);
  assert(sync_leader.get() != NULL);
  assert(sync_provider.get() != NULL);

  // We now know for sure who the leader is.
  sync_leader->entity = other;
  sync_leader->cancel_timeout();

  if (m->flags & MMonSync::FLAG_RETRY) {
    dout(10) << __func__ << " retrying sync at a later time" << dendl;
    sync_role = SYNC_ROLE_NONE;
    sync_state = SYNC_STATE_NONE;
    sync_leader->set_timeout(new C_SyncStartRetry(this, sync_leader->entity),
			     g_conf->mon_sync_backoff_timeout);
    goto out;
  }

  if (m->flags & MMonSync::FLAG_REPLY_TO) {
    dout(10) << __func__ << " leader told us to use " << m->reply_to
             << " as sync provider" << dendl;
    sync_provider->entity = m->reply_to;
  } else {
    dout(10) << __func__ << " synchronizing from leader at " << other << dendl;
    sync_provider->entity = other;
  }

  sync_leader->set_timeout(new C_HeartbeatTimeout(this),
			   g_conf->mon_sync_heartbeat_timeout);

  assert(g_conf->mon_sync_requester_kill_at != 2);
  sync_send_heartbeat(sync_leader->entity);
  assert(g_conf->mon_sync_requester_kill_at != 3);

  sync_start_chunks(sync_provider);
out:
  m->put();
}

void Monitor::handle_sync_heartbeat_reply(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();
  if ((sync_role != SYNC_ROLE_REQUESTER)
      || (sync_state == SYNC_STATE_NONE)
      || (sync_leader.get() == NULL)
      || (other != sync_leader->entity)) {
    dout(1) << __func__ << " stray message -- drop it." << dendl;
    m->put();
    return;
  }

  assert(state == STATE_SYNCHRONIZING);
  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state != SYNC_STATE_NONE);

  assert(sync_leader.get() != NULL);
  assert(sync_leader->entity == other);

  sync_leader->cancel_timeout();
  sync_leader->set_timeout(new C_HeartbeatInterval(this, sync_leader->entity),
			   g_conf->mon_sync_heartbeat_interval);
  m->put();
}

void Monitor::handle_sync_chunk(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  entity_inst_t other = m->get_source_inst();

  if ((sync_role != SYNC_ROLE_REQUESTER)
      || (sync_state != SYNC_STATE_CHUNKS)
      || (sync_provider.get() == NULL)
      || (other != sync_provider->entity)) {
    dout(1) << __func__ << " stray message -- drop it." << dendl;
    m->put();
    return;
  }

  assert(state == STATE_SYNCHRONIZING);
  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_CHUNKS);

  assert(sync_leader.get() != NULL);

  assert(sync_provider.get() != NULL);
  assert(other == sync_provider->entity);

  sync_provider->cancel_timeout();

  MonitorDBStore::Transaction tx;
  tx.append_from_encoded(m->chunk_bl);

  sync_provider->set_timeout(new C_SyncTimeout(this, sync_provider->entity),
			     g_conf->mon_sync_timeout);
  sync_provider->last_received_key = m->last_key;

  MMonSync *msg = new MMonSync(MMonSync::OP_CHUNK_REPLY);

  bool stop = false;
  if (m->flags & MMonSync::FLAG_LAST) {
    msg->flags |= MMonSync::FLAG_LAST;
    assert(m->version > 0);
    tx.put(paxos->get_name(), "last_committed", m->version);
    stop = true;
  }
  assert(g_conf->mon_sync_requester_kill_at != 8);
  messenger->send_message(msg, sync_provider->entity);

  store->apply_transaction(tx);

  if (g_conf->mon_sync_debug && (m->flags & MMonSync::FLAG_CRC)) {
    dout(10) << __func__ << " checking CRC" << dendl;
    MonitorDBStore::Synchronizer sync;
    if (m->flags & MMonSync::FLAG_LAST) {
      dout(10) << __func__ << " checking CRC only for Paxos" << dendl;
      string paxos_name("paxos");
      sync = store->get_synchronizer(paxos_name);
    } else {
      dout(10) << __func__ << " checking CRC for all prefixes" << dendl;
      set<string> prefixes = get_sync_targets_names();
      pair<string,string> empty_key;
      sync = store->get_synchronizer(empty_key, prefixes);
    }

    while (sync->has_next_chunk()) {
      bufferlist bl;
      sync->get_chunk(bl);
    }
    __u32 got_crc = sync->crc();
    dout(10) << __func__ << " expected crc " << m->crc
	     << " got " << got_crc << dendl;

    assert(m->crc == got_crc);
    dout(10) << __func__ << " CRC matches" << dendl;
  }

  m->put();
  if (stop)
    sync_stop();
}

void Monitor::sync_stop()
{
  dout(10) << __func__ << dendl;

  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_CHUNKS);

  sync_state = SYNC_STATE_STOP;

  sync_leader->cancel_timeout();
  sync_provider->cancel_timeout();
  sync_provider.reset();

  entity_inst_t leader = sync_leader->entity;

  sync_leader->set_timeout(new C_SyncFinishReplyTimeout(this),
			   g_conf->mon_sync_timeout);

  MMonSync *msg = new MMonSync(MMonSync::OP_FINISH);
  assert(g_conf->mon_sync_requester_kill_at != 9);
  messenger->send_message(msg, leader);
  assert(g_conf->mon_sync_requester_kill_at != 10);
}

void Monitor::sync_finish_reply_timeout()
{
  dout(10) << __func__ << dendl;
  assert(state == STATE_SYNCHRONIZING);
  assert(sync_leader.get() != NULL);
  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_STOP);

  sync_requester_abort();
}

void Monitor::handle_sync_finish_reply(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  entity_inst_t other = m->get_source_inst();

  if ((sync_role != SYNC_ROLE_REQUESTER)
      || (sync_state != SYNC_STATE_STOP)
      || (sync_leader.get() == NULL)
      || (sync_leader->entity != other)) {
    dout(1) << __func__ << " stray message -- drop it." << dendl;
    m->put();
    return;
  }

  assert(sync_role == SYNC_ROLE_REQUESTER);
  assert(sync_state == SYNC_STATE_STOP);

  assert(sync_leader.get() != NULL);
  assert(sync_leader->entity == other);

  sync_role = SYNC_ROLE_NONE;
  sync_state = SYNC_STATE_NONE;

  sync_leader->cancel_timeout();
  sync_leader.reset();

  paxos->reapply_all_versions();

  sync_store_cleanup();

  init_paxos();

  assert(g_conf->mon_sync_requester_kill_at != 11);

  m->put();

  bootstrap();
}

void Monitor::handle_sync_abort(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  /* This function's responsabilities are manifold, and they depend on
   * who we (the monitor) are and what is our role in the sync.
   *
   * If we are the sync requester (i.e., if we are synchronizing), it
   * means that we *must* abort the current sync and bootstrap. This may
   * be required if there was a leader change and we are talking to the
   * wrong leader, which makes continuing with the current sync way too
   * risky, given that a Paxos trim may be underway and we certainly incur
   * in the chance of ending up with an inconsistent store state.
   *
   * If we are the sync provider, it means that the requester wants to
   * abort his sync, either because he lost connectivity to the leader
   * (i.e., his heartbeat timeout was triggered) or he became aware of a
   * leader change.
   *
   * As a leader, we should never receive such a message though, unless we
   * have just won an election, in which case we should have been a sync
   * provider before. In such a case, we should behave as if we were a sync
   * provider and clean up the requester's state.
   */
  entity_inst_t other = m->get_source_inst();

  if ((sync_role == SYNC_ROLE_REQUESTER)
      && (sync_leader.get() != NULL)
      && (sync_leader->entity == other)) {

    sync_requester_abort();
  } else if ((sync_role & SYNC_ROLE_PROVIDER)
	     && (sync_entities.count(other) > 0)
	     && (sync_entities_states[other] == SYNC_STATE_START)) {

    sync_provider_cleanup(other);
  } else {
    dout(1) << __func__ << " stray message -- drop it." << dendl;
  }
  m->put();
}

void Monitor::handle_sync(MMonSync *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  switch (m->op) {
  case MMonSync::OP_START:
    handle_sync_start(m);
    break;
  case MMonSync::OP_START_REPLY:
    handle_sync_start_reply(m);
    break;
  case MMonSync::OP_HEARTBEAT:
    handle_sync_heartbeat(m);
    break;
  case MMonSync::OP_HEARTBEAT_REPLY:
    handle_sync_heartbeat_reply(m);
    break;
  case MMonSync::OP_FINISH:
    handle_sync_finish(m);
    break;
  case MMonSync::OP_START_CHUNKS:
    handle_sync_start_chunks(m);
    break;
  case MMonSync::OP_CHUNK:
    handle_sync_chunk(m);
    break;
  case MMonSync::OP_CHUNK_REPLY:
    handle_sync_chunk_reply(m);
    break;
  case MMonSync::OP_FINISH_REPLY:
    handle_sync_finish_reply(m);
    break;
  case MMonSync::OP_ABORT:
    handle_sync_abort(m);
    break;
  default:
    dout(0) << __func__ << " unknown op " << m->op << dendl;
    m->put();
    assert(0 == "unknown op");
    break;
  }
}

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

void Monitor::handle_probe(MMonProbe *m)
{
  dout(10) << "handle_probe " << *m << dendl;

  if (m->fsid != monmap->fsid) {
    dout(0) << "handle_probe ignoring fsid " << m->fsid << " != " << monmap->fsid << dendl;
    m->put();
    return;
  }

  switch (m->op) {
  case MMonProbe::OP_PROBE:
    handle_probe_probe(m);
    break;

  case MMonProbe::OP_REPLY:
    handle_probe_reply(m);
    break;

  default:
    m->put();
  }
}

/**
 * @todo fix this. This is going to cause trouble.
 */
void Monitor::handle_probe_probe(MMonProbe *m)
{
  dout(10) << "handle_probe_probe " << m->get_source_inst() << *m << dendl;
  MMonProbe *r = new MMonProbe(monmap->fsid, MMonProbe::OP_REPLY, name, has_ever_joined);
  r->name = name;
  r->quorum = quorum;
  monmap->encode(r->monmap_bl, m->get_connection()->get_features());
  r->paxos_first_version = paxos->get_first_committed();
  r->paxos_last_version = paxos->get_version();
  messenger->send_message(r, m->get_connection());

  // did we discover a peer here?
  if (!monmap->contains(m->get_source_addr())) {
    dout(1) << " adding peer " << m->get_source_addr() << " to list of hints" << dendl;
    extra_probe_peers.insert(m->get_source_addr());
  }
  m->put();
}

void Monitor::handle_probe_reply(MMonProbe *m)
{
  dout(10) << "handle_probe_reply " << m->get_source_inst() << *m << dendl;
  dout(10) << " monmap is " << *monmap << dendl;

  if (!is_probing()) {
    m->put();
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
      m->put();

      bootstrap();
      return;
    }
    delete newmap;
  }

  // rename peer?
  string peer_name = monmap->get_name(m->get_source_addr());
  if (monmap->get_epoch() == 0 && peer_name.find("noname-") == 0) {
    dout(10) << " renaming peer " << m->get_source_addr() << " "
	     << peer_name << " -> " << m->name << " in my monmap"
	     << dendl;
    monmap->rename(peer_name, m->name);
  } else {
    dout(10) << " peer name is " << peer_name << dendl;
  }

  // new initial peer?
  if (monmap->contains(m->name)) {
    if (monmap->get_addr(m->name).is_blank_ip()) {
      dout(1) << " learned initial mon " << m->name << " addr " << m->get_source_addr() << dendl;
      monmap->set_addr(m->name, m->get_source_addr());
      m->put();

      bootstrap();
      return;
    }
  }

  assert(paxos != NULL);

  if (is_synchronizing()) {
    dout(10) << " we are currently synchronizing, so that will continue."
             << dendl;
    m->put();
    return;
  }

  entity_inst_t other = m->get_source_inst();
  // is there an existing quorum?
  if (m->quorum.size()) {
    dout(10) << " existing quorum " << m->quorum << dendl;

    if (paxos->get_version() < m->paxos_first_version) {
      dout(10) << " peer paxos versions [" << m->paxos_first_version
               << "," << m->paxos_last_version << "]"
	       << " vs my version " << paxos->get_version()
	       << " (too far ahead)"
	       << dendl;
      sync_start(other);
      m->put();
      return;
    }
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
      m->put();
      return;
    }

    if (paxos->get_version() + g_conf->paxos_max_join_drift < m->paxos_last_version) {
      dout(10) << " peer paxos version " << m->paxos_last_version
	       << " vs my version " << paxos->get_version()
	       << " (too far ahead)"
	       << dendl;
      sync_start(other);
      m->put();
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
  m->put();
}

void Monitor::start_election()
{
  dout(10) << "start_election" << dendl;

  cancel_probe_timeout();

  // call a new election
  state = STATE_ELECTING;
  clog.info() << "mon." << name << " calling new monitor election\n";
  elector.call_election();
}

void Monitor::win_standalone_election()
{
  dout(1) << "win_standalone_election" << dendl;

  // bump election epoch, in case the previous epoch included other
  // monitors; we need to be able to make the distinction.
  elector.advance_epoch();

  rank = monmap->get_rank(name);
  assert(rank == 0);
  set<int> q;
  q.insert(rank);
  win_election(1, q, CEPH_FEATURES_ALL);
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

void Monitor::win_election(epoch_t epoch, set<int>& active, uint64_t features) 
{
  if (!is_electing())
    reset();

  state = STATE_LEADER;
  leader_since = ceph_clock_now(g_ceph_context);
  leader = rank;
  quorum = active;
  quorum_features = features;
  outside_quorum.clear();
  dout(10) << "win_election, epoch " << epoch << " quorum is " << quorum
	   << " features are " << quorum_features
	   << dendl;

  clog.info() << "mon." << name << "@" << rank
		<< " won leader election with quorum " << quorum << "\n";

  paxos->leader_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); ++p)
    (*p)->election_finished();
  health_monitor->start(epoch);

  finish_election();
  if (monmap->size() > 1)
    timecheck_start();
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

  // let everyone currently syncing know that we are no longer the leader and
  // that they should all abort their on-going syncs
  for (map<entity_inst_t,Context*>::iterator iter = trim_timeouts.begin();
       iter != trim_timeouts.end();
       ++iter) {
    timer.cancel_event((*iter).second);
    entity_inst_t entity = (*iter).first;
    MMonSync *msg = new MMonSync(MMonSync::OP_ABORT);
    messenger->send_message(msg, entity);
  }
  trim_timeouts.clear();
  sync_role &= ~SYNC_ROLE_LEADER;
  
  paxos->peon_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); ++p)
    (*p)->election_finished();
  health_monitor->start(epoch);

  finish_election();
}

void Monitor::finish_election()
{
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


bool Monitor::_allowed_command(MonSession *s, map<string, cmd_vartype>& cmd)
{
  bool retval = false;

  if (s->caps.is_allow_all())
    return true;

  string prefix;
  cmd_getval(g_ceph_context, cmd, "prefix", prefix);

  map<string,string> strmap;
  for (map<string, cmd_vartype>::const_iterator p = cmd.begin();
       p != cmd.end(); ++p) {
    if (p->first != "prefix") {
      strmap[p->first] = cmd_vartype_stringify(p->second);
    }
  }

  if (s->caps.is_capable(g_ceph_context, s->inst.name,
			 "", prefix, strmap, false, false, false)) {
    retval = true; 
  }

  return retval;
}

void Monitor::_sync_status(ostream& ss)
{
  JSONFormatter jf(true);
  jf.open_object_section("sync_status");
  jf.dump_string("state", get_state_name());
  jf.dump_unsigned("paxos_version", paxos->get_version());

  if (is_leader() || (sync_role == SYNC_ROLE_LEADER)) {
    Mutex::Locker l(trim_lock);
    jf.open_object_section("trim");
    jf.dump_int("disabled", paxos->is_trim_disabled());
    jf.dump_int("should_trim", paxos->should_trim());
    if (!trim_timeouts.empty()) {
      jf.open_array_section("mons");
      for (map<entity_inst_t,Context*>::iterator it = trim_timeouts.begin();
	   it != trim_timeouts.end();
	   ++it) {
	entity_inst_t e = (*it).first;
	jf.dump_stream("mon") << e;
	int s = -1;
	if (trim_entities_states.count(e))
	  s = trim_entities_states[e];
	jf.dump_stream("sync_state") << get_sync_state_name(s);
      }
    }
    jf.close_section();
  }

  if (!sync_entities.empty() || (sync_role == SYNC_ROLE_PROVIDER)) {
    jf.open_array_section("on_going");
    for (map<entity_inst_t,SyncEntity>::iterator it = sync_entities.begin();
	 it != sync_entities.end();
	 ++it) {
      entity_inst_t e = (*it).first;
      jf.open_object_section("mon");
      jf.dump_stream("addr") << e;
      jf.dump_string("state", (*it).second->get_state());
      int s = -1;
      if (sync_entities_states.count(e))
	  s = sync_entities_states[e];
      jf.dump_stream("sync_state") << get_sync_state_name(s);

      jf.close_section();
    }
    jf.close_section();
  }

  if (is_synchronizing() || (sync_role == SYNC_ROLE_REQUESTER)) {
    jf.open_object_section("leader");
    SyncEntity sync_entity = sync_leader;
    if (sync_entity.get() != NULL)
      jf.dump_stream("addr") << sync_entity->entity;
    jf.close_section();

    jf.open_object_section("provider");
    sync_entity = sync_provider;
    if (sync_entity.get() != NULL)
      jf.dump_stream("addr") << sync_entity->entity;
    jf.close_section();
  }

  if (g_conf->mon_sync_leader_kill_at > 0)
    jf.dump_int("leader_kill_at", g_conf->mon_sync_leader_kill_at);
  if (g_conf->mon_sync_provider_kill_at > 0)
    jf.dump_int("provider_kill_at", g_conf->mon_sync_provider_kill_at);
  if (g_conf->mon_sync_requester_kill_at > 0)
    jf.dump_int("requester_kill_at", g_conf->mon_sync_requester_kill_at);

  jf.close_section();
  jf.flush(ss);
}

void Monitor::_sync_force(ostream& ss)
{
  MonitorDBStore::Transaction tx;
  tx.put("mon_sync", "force_sync", 1);
  store->apply_transaction(tx);

  JSONFormatter jf(true);
  jf.open_object_section("sync_force");
  jf.dump_int("ret", 0);
  jf.dump_stream("msg") << "forcing store sync the next time the monitor starts";
  jf.close_section();
  jf.flush(ss);
}

void Monitor::_quorum_status(ostream& ss)
{
  JSONFormatter jf(true);
  jf.open_object_section("quorum_status");
  jf.dump_int("election_epoch", get_epoch());
  
  jf.open_array_section("quorum");
  for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
    jf.dump_int("mon", *p);
  jf.close_section();

  jf.open_object_section("monmap");
  monmap->dump(&jf);
  jf.close_section();

  jf.close_section();
  jf.flush(ss);
}

void Monitor::_mon_status(ostream& ss)
{
  JSONFormatter jf(true);
  jf.open_object_section("mon_status");
  jf.dump_string("name", name);
  jf.dump_int("rank", rank);
  jf.dump_string("state", get_state_name());
  jf.dump_int("election_epoch", get_epoch());

  jf.open_array_section("quorum");
  for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
    jf.dump_int("mon", *p);
  jf.close_section();

  jf.open_array_section("outside_quorum");
  for (set<string>::iterator p = outside_quorum.begin(); p != outside_quorum.end(); ++p)
    jf.dump_string("mon", *p);
  jf.close_section();

  jf.open_array_section("extra_probe_peers");
  for (set<entity_addr_t>::iterator p = extra_probe_peers.begin();
       p != extra_probe_peers.end();
       ++p)
    jf.dump_stream("peer") << *p;
  jf.close_section();

  if (is_synchronizing()) {
    if (sync_leader)
      jf.dump_stream("sync_leader") << sync_leader->entity;
    else
      jf.dump_string("sync_leader", "");
    if (sync_provider)
      jf.dump_stream("sync_provider") << sync_provider->entity;
    else
      jf.dump_string("sync_provider", "");
  }

  jf.open_object_section("monmap");
  monmap->dump(&jf);
  jf.close_section();

  jf.close_section();
  
  jf.flush(ss);
}

void Monitor::get_health(string& status, bufferlist *detailbl, Formatter *f)
{
  list<pair<health_status_t,string> > summary;
  list<pair<health_status_t,string> > detail;

  if (f)
    f->open_object_section("health");

  for (vector<PaxosService*>::iterator p = paxos_service.begin();
       p != paxos_service.end();
       ++p) {
    PaxosService *s = *p;
    s->get_health(summary, detailbl ? &detail : NULL);
  }

  if (f)
    f->open_array_section("summary");
  stringstream ss;
  health_status_t overall = HEALTH_OK;
  if (!summary.empty()) {
    if (f) {
      f->open_object_section("item");
      f->dump_stream("severity") <<  summary.front().first;
      f->dump_string("summary", summary.front().second);
      f->close_section();
    }
    ss << ' ';
    while (!summary.empty()) {
      if (overall > summary.front().first)
	overall = summary.front().first;
      ss << summary.front().second;
      summary.pop_front();
      if (!summary.empty())
	ss << "; ";
    }
  }
  if (f)
    f->close_section();

  if (f) {
    f->open_object_section("timechecks");
    f->dump_int("epoch", get_epoch());
    f->dump_int("round", timecheck_round);
    f->dump_stream("round_status")
      << ((timecheck_round%2) ? "on-going" : "finished");
  }

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
        f->open_object_section(name.c_str());
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
      if (!ss.str().empty())
        ss << ";";
      ss << " clock skew detected on";
      while (!warns.empty()) {
        ss << " mon." << warns.front();
        warns.pop_front();
        if (!warns.empty())
          ss << ",";
      }
    }
    if (f)
      f->close_section();
  }
  if (f)
    f->close_section();

  health_status_t hmstatus = health_monitor->get_health(f, (detailbl ? &detail : NULL));
  if (overall > hmstatus)
    overall = hmstatus;

  stringstream fss;
  fss << overall;
  status = fss.str() + ss.str();
  if (f)
    f->dump_stream("overall_status") << overall;

  if (f)
    f->open_array_section("detail");
  while (!detail.empty()) {
    if (f)
      f->dump_string("item", detail.front().second);
    if (detailbl != NULL) {
      detailbl->append(detail.front().second);
      detailbl->append('\n');
    }
    detail.pop_front();
  }
  if (f)
    f->close_section();

  if (f)
    f->close_section();
}

void Monitor::get_status(stringstream &ss, Formatter *f)
{
  if (f)
    f->open_object_section("status");

  // reply with the status for all the components
  string health;
  get_health(health, NULL, f);

  if (f) {
    f->dump_stream("fsid") << monmap->get_fsid();
    f->dump_stream("monmap") << *monmap;
    f->dump_stream("election_epoch") << get_epoch();
    f->dump_stream("quorum") << get_quorum();
    f->dump_stream("quorum_names") << get_quorum_names();
    f->dump_stream("osdmap") << osdmon()->osdmap;
    f->dump_stream("pgmap") << pgmon()->pg_map;
    f->dump_stream("mdsmap") << mdsmon()->mdsmap;
    f->close_section();
  } else {
    ss << "  cluster " << monmap->get_fsid() << "\n";
    ss << "   health " << health << "\n";
    ss << "   monmap " << *monmap << ", election epoch " << get_epoch()
      << ", quorum " << get_quorum() << " " << get_quorum_names() << "\n";
    ss << "   osdmap " << osdmon()->osdmap << "\n";
    ss << "    pgmap " << pgmon()->pg_map << "\n";
    ss << "   mdsmap " << mdsmon()->mdsmap << "\n";
  }
}

#undef COMMAND
struct MonCommand {
  string cmdstring;
  string helpstring;
} mon_commands[] = {
#define COMMAND(parsesig, helptext) \
  {parsesig, helptext},
#include <mon/MonCommands.h>
};

void Monitor::handle_command(MMonCommand *m)
{
  if (m->fsid != monmap->fsid) {
    dout(0) << "handle_command on fsid " << m->fsid << " != " << monmap->fsid << dendl;
    reply_command(m, -EPERM, "wrong fsid", 0);
    return;
  }

  MonSession *session = m->get_session();
  if (!session) {
    string rs = "Access denied";
    reply_command(m, -EACCES, rs, 0);
    return;
  }

  if (m->cmd.empty()) {
    string rs = "No command supplied";
    reply_command(m, -EINVAL, rs, 0);
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
      reply_command(m, r, rs, 0);
    else
      m->put();
    return;
  }

  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
  if (prefix == "get_command_descriptions") {
    int cmdnum = 0;
    JSONFormatter *f = new JSONFormatter();
    f->open_object_section("command_descriptions");
    for (MonCommand *cp = mon_commands;
	 cp < &mon_commands[ARRAY_SIZE(mon_commands)]; cp++) {

      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmd_and_help_to_json(f, secname.str(),
				cp->cmdstring, cp->helpstring);
      cmdnum++;
    }
    f->close_section();	// command_descriptions

    bufferlist rdata;
    f->flush(ds);
    delete f;
    rdata.append(ds);
    reply_command(m, 0, "", rdata, 0);
    return;
  }

  bool access_cmd;
  bool access_r;
  bool access_all;

  string module;
  string err;

  dout(0) << "handle_command " << *m << dendl;

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  get_str_vec(prefix, fullcmd);
  module = fullcmd[0];

  access_cmd = _allowed_command(session, cmdmap);
  access_r = (session->is_capable("mon", MON_CAP_R) || access_cmd);
  access_all = (session->caps.is_allow_all() || access_cmd);

  if (module == "mds") {
    mdsmon()->dispatch(m);
    return;
  }
  if (module == "osd") {
    osdmon()->dispatch(m);
    return;
  }

  if (module == "pg") {
    pgmon()->dispatch(m);
    return;
  }
  if (module == "mon") {
    monmon()->dispatch(m);
    return;
  }
  if (module == "auth") {
    authmon()->dispatch(m);
    return;
  }

  if (module == "config-key") {
    if (!access_all) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    config_key_service->dispatch(m);
    return;
  }

  if (prefix == "fsid") {
    ds << monmap->fsid;
    rdata.append(ds);
    reply_command(m, 0, "", rdata, 0);
    return;
  }
  if (prefix == "log") {
    if (!access_r) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    vector<string> logtext;
    cmd_getval(g_ceph_context, cmdmap, "logtext", logtext);
    std::copy(logtext.begin(), logtext.end(),
	      ostream_iterator<string>(ds, " "));
    clog.info(ds);
    rs = "ok";
    reply_command(m, 0, rs, 0);
    return;
  }

  if (prefix == "compact") {
    if (!access_all) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
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
    if (!access_all) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    vector<string> injected_args;
    cmd_getval(g_ceph_context, cmdmap, "injected_args", injected_args);
    if (!injected_args.empty()) {
      dout(0) << "parsing injected options '" << injected_args << "'" << dendl;
      ostringstream argss;
      std::copy(injected_args.begin(), injected_args.end(),
		ostream_iterator<string>(argss, " "));
      ostringstream oss;
      r = g_conf->injectargs(argss.str().c_str(), &oss);
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
    if (!access_r) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }

    string detail;
    cmd_getval(g_ceph_context, cmdmap, "detail", detail);

    if (prefix == "status") {
      // get_status handles f == NULL
      get_status(ds, f.get());

      if (f) {
        f->flush(ds);
        ds << '\n';
      }
      rdata.append(ds);
    } else if (prefix == "health") {
      string health_str;
      get_health(health_str, detail == "detail" ? &rdata : NULL, f.get());
      if (f) {
        f->flush(ds);
        ds << '\n';
      } else {
        ds << health_str;
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
    if (!access_r) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }

    // this must be formatted, in its current form
    if (!f)
      f.reset(new_formatter("json-pretty"));
    f->open_object_section("report");
    f->dump_string("version", ceph_version_to_str());
    f->dump_string("commit", git_version_to_str());
    f->dump_stream("timestamp") << ceph_clock_now(NULL);

    vector<string> tagsvec;
    cmd_getval(g_ceph_context, cmdmap, "tags", tagsvec);
    stringstream tags;
    std::copy(tagsvec.begin(), tagsvec.end(),
	      ostream_iterator<string>(tags, " "));
    string tagstr = tags.str();
    if (!tagstr.empty())
      tagstr = tagstr.substr(0, tagstr.find_last_of(' '));
    f->dump_string("tag", tagstr);

    string hs;
    get_health(hs, NULL, f.get());

    monmon()->dump_info(f.get());
    osdmon()->dump_info(f.get());
    mdsmon()->dump_info(f.get());
    pgmon()->dump_info(f.get());

    f->close_section();
    f->flush(ds);

    bufferlist bl;
    bl.append("-------- BEGIN REPORT --------\n");
    bl.append(ds);
    ostringstream ss2;
    ss2 << "\n-------- END REPORT " << bl.crc32c(6789) << " --------\n";
    rdata.append(bl);
    rdata.append(ss2.str());
    rs = string();
    r = 0;
  } else if (prefix == "quorum_status") {
    if (!access_r) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    // make sure our map is readable and up to date
    if (!is_leader() && !is_peon()) {
      dout(10) << " waiting for quorum" << dendl;
      waitfor_quorum.push_back(new C_RetryMessage(this, m));
      return;
    }
    _quorum_status(ds);
    rdata.append(ds);
    rs = "";
    r = 0;
  } else if (prefix == "mon_status") {
    if (!access_r) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    _mon_status(ds);
    rdata.append(ds);
    rs = "";
    r = 0;
  } else if (prefix == "sync status") {
      if (!access_r) {
	r = -EACCES;
	rs = "access denied";
	goto out;
      }
      _sync_status(ds);
      rdata.append(ds);
      rs = "";
      r = 0;
  } else if (prefix == "sync force") {
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
    _sync_force(ds);
    rs = ds.str();
    r = 0;
  } else if (prefix == "heap") {
    if (!access_all) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
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
    if (!access_all) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    string quorumcmd;
    cmd_getval(g_ceph_context, cmdmap, "quorumcmd", quorumcmd);
    if (quorumcmd == "exit") {
      reset();
      start_election();
      elector.stop_participating();
      rs = "stopped responding to quorum, initiated new election";
      r = 0;
    } else if (quorumcmd == "enter") {
      elector.start_participating();
      reset();
      start_election();
      rs = "started responding to quorum, initiated new election";
      r = 0;
    }
  } else if (!access_cmd) {
    r = -EACCES;
    rs = "access denied";
  }

 out:
  if (!m->get_source().is_mon())  // don't reply to mon->mon commands
    reply_command(m, r, rs, rdata, 0);
  else
    m->put();
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs, version_t version)
{
  bufferlist rdata;
  reply_command(m, rc, rs, rdata, version);
}

void Monitor::reply_command(MMonCommand *m, int rc, const string &rs, bufferlist& rdata, version_t version)
{
  MMonCommandAck *reply = new MMonCommandAck(m->cmd, rc, rs, version);
  reply->set_tid(m->get_tid());
  reply->set_data(rdata);
  send_reply(m, reply);
  m->put();
}


// ------------------------
// request/reply routing
//
// a client/mds/osd will connect to a random monitor.  we need to forward any
// messages requiring state updates to the leader, and then route any replies
// back via the correct monitor and back to them.  (the monitor will not
// initiate any connections.)

void Monitor::forward_request_leader(PaxosServiceMessage *req)
{
  int mon = get_leader();
  MonSession *session = 0;
  if (req->get_connection())
    session = static_cast<MonSession *>(req->get_connection()->get_priv());
  if (req->get_source().is_mon() && req->get_source_addr() != messenger->get_myaddr()) {
    dout(10) << "forward_request won't forward (non-local) mon request " << *req << dendl;
    req->put();
  } else if (session && session->proxy_con) {
    dout(10) << "forward_request won't double fwd request " << *req << dendl;
    req->put();
  } else if (session && !session->closed) {
    RoutedRequest *rr = new RoutedRequest;
    rr->tid = ++routed_request_tid;
    rr->client_inst = req->get_source_inst();
    rr->con = req->get_connection();
    rr->con->get();
    encode_message(req, CEPH_FEATURES_ALL, rr->request_bl);   // for my use only; use all features
    rr->session = static_cast<MonSession *>(session->get());
    routed_requests[rr->tid] = rr;
    session->routed_request_tids.insert(rr->tid);
    
    dout(10) << "forward_request " << rr->tid << " request " << *req << dendl;

    MForward *forward = new MForward(rr->tid, req, rr->session->caps);
    forward->set_priority(req->get_priority());
    messenger->send_message(forward, monmap->get_inst(mon));
  } else {
    dout(10) << "forward_request no session for request " << *req << dendl;
    req->put();
  }
  if (session)
    session->put();
}

//extract the original message and put it into the regular dispatch function
void Monitor::handle_forward(MForward *m)
{
  dout(10) << "received forwarded message from " << m->client
	   << " via " << m->get_source_inst() << dendl;
  MonSession *session = static_cast<MonSession *>(m->get_connection()->get_priv());
  assert(session);

  if (!session->is_capable("mon", MON_CAP_X)) {
    dout(0) << "forward from entity with insufficient caps! " 
	    << session->caps << dendl;
  } else {
    Connection *c = new Connection(NULL);  // msgr must be null; see PaxosService::dispatch()
    MonSession *s = new MonSession(m->msg->get_source_inst(), c);
    c->set_priv(s);
    c->set_peer_addr(m->client.addr);
    c->set_peer_type(m->client.name.type());

    s->caps = m->client_caps;
    dout(10) << " caps are " << s->caps << dendl;
    s->proxy_con = m->get_connection()->get();
    s->proxy_tid = m->tid;

    PaxosServiceMessage *req = m->msg;
    m->msg = NULL;  // so ~MForward doesn't delete it
    req->set_connection(c);

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
    s->con->put();
    s->con = NULL;

    dout(10) << " mesg " << req << " from " << m->get_source_addr() << dendl;

    _ms_dispatch(req);
  }
  session->put();
  m->put();
}

void Monitor::try_send_message(Message *m, const entity_inst_t& to)
{
  dout(10) << "try_send_message " << *m << " to " << to << dendl;

  bufferlist bl;
  encode_message(m, CEPH_FEATURES_ALL, bl);  // fixme: assume peers have all features we do.

  messenger->send_message(m, to);

  for (int i=0; i<(int)monmap->size(); i++) {
    if (i != rank)
      messenger->send_message(new MRoute(bl, to), monmap->get_inst(i));
  }
}

void Monitor::send_reply(PaxosServiceMessage *req, Message *reply)
{
  MonSession *session = static_cast<MonSession*>(req->get_connection()->get_priv());
  if (!session) {
    dout(2) << "send_reply no session, dropping reply " << *reply
	    << " to " << req << " " << *req << dendl;
    reply->put();
    return;
  }
  if (session->proxy_con) {
    dout(15) << "send_reply routing reply to " << req->get_connection()->get_peer_addr()
	     << " via " << session->proxy_con->get_peer_addr()
	     << " for request " << *req << dendl;
    messenger->send_message(new MRoute(session->proxy_tid, reply),
			    session->proxy_con);    
  } else {
    messenger->send_message(reply, session->con);
  }
  session->put();
}

void Monitor::no_reply(PaxosServiceMessage *req)
{
  MonSession *session = static_cast<MonSession*>(req->get_connection()->get_priv());
  if (!session) {
    dout(2) << "no_reply no session, dropping non-reply to " << req << " " << *req << dendl;
    return;
  }
  if (session->proxy_con) {
    if (get_quorum_features() & CEPH_FEATURE_MON_NULLROUTE) {
      dout(10) << "no_reply to " << req->get_source_inst()
	       << " via " << session->proxy_con->get_peer_addr()
	       << " for request " << *req << dendl;
      messenger->send_message(new MRoute(session->proxy_tid, NULL),
			      session->proxy_con);
    } else {
      dout(10) << "no_reply no quorum nullroute feature for " << req->get_source_inst()
	       << " via " << session->proxy_con->get_peer_addr()
	       << " for request " << *req << dendl;
    }
  } else {
    dout(10) << "no_reply to " << req->get_source_inst() << " " << *req << dendl;
  }
  session->put();
}

void Monitor::handle_route(MRoute *m)
{
  MonSession *session = static_cast<MonSession *>(m->get_connection()->get_priv());
  //check privileges
  if (session && !session->is_capable("mon", MON_CAP_X)) {
    dout(0) << "MRoute received from entity without appropriate perms! "
	    << dendl;
    session->put();
    m->put();
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
	messenger->send_message(m->msg, rr->con);
	m->msg = NULL;
      }
      routed_requests.erase(m->session_mon_tid);
      rr->session->routed_request_tids.insert(rr->tid);
      delete rr;
    } else {
      dout(10) << " don't have routed request tid " << m->session_mon_tid << dendl;
    }
  } else {
    dout(10) << " not a routed request, trying to send anyway" << dendl;
    if (m->msg) {
      messenger->lazy_send_message(m->msg, m->dest);
      m->msg = NULL;
    }
  }
  m->put();
  if (session)
    session->put();
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

    bufferlist::iterator q = rr->request_bl.begin();
    PaxosServiceMessage *req = (PaxosServiceMessage *)decode_message(cct, q);

    if (mon == rank) {
      dout(10) << " requeue for self tid " << rr->tid << " " << *req << dendl;
      req->set_connection(rr->con);
      rr->con->get();
      retry.push_back(new C_RetryMessage(this, req));
      delete rr;
    } else {
      dout(10) << " resend to mon." << mon << " tid " << rr->tid << " " << *req << dendl;
      MForward *forward = new MForward(rr->tid, req, rr->session->caps);
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
  assert(!s->closed);
  for (set<uint64_t>::iterator p = s->routed_request_tids.begin();
       p != s->routed_request_tids.end();
       ++p) {
    if (routed_requests.count(*p)) {
      RoutedRequest *rr = routed_requests[*p];
      dout(10) << " dropping routed request " << rr->tid << dendl;
      delete rr;
      routed_requests.erase(*p);
    }
  }
  s->con->set_priv(NULL);
  session_map.remove_session(s);
}

void Monitor::remove_all_sessions()
{
  while (!session_map.sessions.empty()) {
    MonSession *s = session_map.sessions.front();
    remove_session(s);
  }
}

void Monitor::send_command(const entity_inst_t& inst,
			   const vector<string>& com, version_t version)
{
  dout(10) << "send_command " << inst << "" << com << dendl;
  MMonCommand *c = new MMonCommand(monmap->fsid, version);
  c->cmd = com;
  try_send_message(c, inst);
}

void Monitor::waitlist_or_zap_client(Message *m)
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
  Connection *con = m->get_connection();
  utime_t too_old = ceph_clock_now(g_ceph_context);
  too_old -= g_ceph_context->_conf->mon_lease;
  if (m->get_recv_stamp() > too_old &&
      con->is_connected()) {
    dout(5) << "waitlisting message " << *m << dendl;
    maybe_wait_for_quorum.push_back(new C_RetryMessage(this, m));
  } else {
    dout(1) << "discarding message " << *m << " and sending client elsewhere" << dendl;
    messenger->mark_down(con);
    m->put();
  }
}

bool Monitor::_ms_dispatch(Message *m)
{
  bool ret = true;

  if (state == STATE_SHUTDOWN) {
    m->put();
    return true;
  }

  Connection *connection = m->get_connection();
  MonSession *s = NULL;
  MonCap caps;
  EntityName entity_name;
  bool src_is_mon;

  src_is_mon = !connection || (connection->get_peer_type() & CEPH_ENTITY_TYPE_MON);

  if (connection) {
    bool reuse_caps = false;
    dout(20) << "have connection" << dendl;
    s = static_cast<MonSession *>(connection->get_priv());
    if (s && s->closed) {
      caps = s->caps;
      reuse_caps = true;
      s->put();
      s = NULL;
    }
    if (!s) {
      if (!exited_quorum.is_zero() && !src_is_mon) {
	waitlist_or_zap_client(m);
	return true;
      }
      dout(10) << "do not have session, making new one" << dendl;
      s = session_map.new_session(m->get_source_inst(), m->get_connection());
      m->get_connection()->set_priv(s->get());
      dout(10) << "ms_dispatch new session " << s << " for " << s->inst << dendl;

      if (m->get_connection()->get_peer_type() != CEPH_ENTITY_TYPE_MON) {
	dout(10) << "setting timeout on session" << dendl;
	// set an initial timeout here, so we will trim this session even if they don't
	// do anything.
	s->until = ceph_clock_now(g_ceph_context);
	s->until += g_conf->mon_subscribe_interval;
      } else {
	//give it monitor caps; the peer type has been authenticated
	reuse_caps = false;
	dout(5) << "setting monitor caps on this connection" << dendl;
	if (!s->caps.is_allow_all()) //but no need to repeatedly copy
	  s->caps = *mon_caps;
      }
      if (reuse_caps)
        s->caps = caps;
    } else {
      dout(20) << "ms_dispatch existing session " << s << " for " << s->inst << dendl;
    }
    if (s->auth_handler) {
      entity_name = s->auth_handler->get_entity_name();
    }
  }

  if (s)
    dout(20) << " caps " << s->caps.get_str() << dendl;

  if (is_synchronizing() && !src_is_mon) {
    waitlist_or_zap_client(m);
    return true;
  }

  {
    switch (m->get_type()) {
      
    case MSG_ROUTE:
      handle_route(static_cast<MRoute*>(m));
      break;

      // misc
    case CEPH_MSG_MON_GET_MAP:
      handle_mon_get_map(static_cast<MMonGetMap*>(m));
      break;

    case CEPH_MSG_MON_GET_VERSION:
      handle_get_version(static_cast<MMonGetVersion*>(m));
      break;

    case MSG_MON_COMMAND:
      handle_command(static_cast<MMonCommand*>(m));
      break;

    case CEPH_MSG_MON_SUBSCRIBE:
      /* FIXME: check what's being subscribed, filter accordingly */
      handle_subscribe(static_cast<MMonSubscribe*>(m));
      break;

    case MSG_MON_PROBE:
      handle_probe(static_cast<MMonProbe*>(m));
      break;

    // Sync (i.e., the new slurp, but on steroids)
    case MSG_MON_SYNC:
      handle_sync(static_cast<MMonSync*>(m));
      break;

      // OSDs
    case MSG_OSD_MARK_ME_DOWN:
    case MSG_OSD_FAILURE:
    case MSG_OSD_BOOT:
    case MSG_OSD_ALIVE:
    case MSG_OSD_PGTEMP:
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

    case MSG_REMOVE_SNAPS:
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // MDSs
    case MSG_MDS_BEACON:
    case MSG_MDS_OFFLOAD_TARGETS:
      paxos_service[PAXOS_MDSMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // auth
    case MSG_MON_GLOBAL_ID:
    case CEPH_MSG_AUTH:
      /* no need to check caps here */
      paxos_service[PAXOS_AUTH]->dispatch((PaxosServiceMessage*)m);
      break;

      // pg
    case CEPH_MSG_STATFS:
    case MSG_PGSTATS:
    case MSG_GETPOOLSTATS:
      paxos_service[PAXOS_PGMAP]->dispatch((PaxosServiceMessage*)m);
      break;

    case CEPH_MSG_POOLOP:
      paxos_service[PAXOS_OSDMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // log
    case MSG_LOG:
      paxos_service[PAXOS_LOG]->dispatch((PaxosServiceMessage*)m);
      break;

    case MSG_LOGACK:
      clog.handle_log_ack((MLogAck*)m);
      break;

      // monmap
    case MSG_MON_JOIN:
      paxos_service[PAXOS_MONMAP]->dispatch((PaxosServiceMessage*)m);
      break;

      // paxos
    case MSG_MON_PAXOS:
      {
	MMonPaxos *pm = static_cast<MMonPaxos*>(m);
	if (!src_is_mon && 
	    !s->is_capable("mon", MON_CAP_X)) {
	  //can't send these!
	  pm->put();
	  break;
	}

	if (state == STATE_SYNCHRONIZING) {
	  // we are synchronizing. These messages would do us no
	  // good, thus just drop them and ignore them.
	  dout(10) << __func__ << " ignore paxos msg from "
		   << pm->get_source_inst() << dendl;
	  pm->put();
	  break;
	}

	// sanitize
	if (pm->epoch > get_epoch()) {
	  bootstrap();
	  pm->put();
	  break;
	}
	if (pm->epoch != get_epoch()) {
	  pm->put();
	  break;
	}

	paxos->dispatch((PaxosServiceMessage*)m);

	// make sure service finds out about any state changes
	if (paxos->is_active()) {
	  vector<PaxosService*>::iterator service_it = paxos_service.begin();
	  for ( ; service_it != paxos_service.end(); ++service_it)
	    (*service_it)->update_from_paxos();
	}
      }
      break;

      // elector messages
    case MSG_MON_ELECTION:
      //check privileges here for simplicity
      if (s &&
	  !s->is_capable("mon", MON_CAP_X)) {
	dout(0) << "MMonElection received from entity without enough caps!"
		<< s->caps << dendl;
	m->put();
	break;
      }
      if (!is_probing() && !is_synchronizing()) {
	elector.dispatch(m);
      } else {
	m->put();
      }
      break;

    case MSG_FORWARD:
      handle_forward(static_cast<MForward *>(m));
      break;

    case MSG_TIMECHECK:
      handle_timecheck(static_cast<MTimeCheck *>(m));
      break;

    case MSG_MON_HEALTH:
      health_monitor->dispatch(static_cast<MMonHealth *>(m));
      break;

    default:
      ret = false;
    }
  }
  if (s) {
    s->put();
  }

  return ret;
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
    if (curr_time - timecheck_round_start > max) {
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
  timecheck_event = new C_TimeCheck(this);
  timer.add_event_after(g_conf->mon_timecheck_interval, timecheck_event);
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

    for (map<entity_inst_t, double>::iterator it = timecheck_skews.begin(); it != timecheck_skews.end(); ++it) {
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
  double abs_skew = (skew_bound > 0 ? skew_bound : -skew_bound);
  assert(latency >= 0);

  if (abs_skew > g_conf->mon_clock_drift_allowed) {
    status = HEALTH_WARN;
    ss << "clock skew " << abs_skew << "s"
       << " > max " << g_conf->mon_clock_drift_allowed << "s";
  }

  return status;
}

void Monitor::handle_timecheck_leader(MTimeCheck *m)
{
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
    clog.error() << other << " " << ss.str() << "\n";
  else if (status == HEALTH_WARN)
    clog.warn() << other << " " << ss.str() << "\n";

  dout(10) << __func__ << " from " << other << " ts " << m->timestamp
	   << " delta " << delta << " skew_bound " << skew_bound
	   << " latency " << latency << dendl;

  if (timecheck_skews.count(other) == 0) {
    timecheck_skews[other] = skew_bound;
  } else {
    timecheck_skews[other] = (timecheck_skews[other]*0.8)+(skew_bound*0.2);
  }

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

void Monitor::handle_timecheck_peon(MTimeCheck *m)
{
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
  messenger->send_message(reply, m->get_connection());
}

void Monitor::handle_timecheck(MTimeCheck *m)
{
  dout(10) << __func__ << " " << *m << dendl;

  if (is_leader()) {
    if (m->op != MTimeCheck::OP_PONG) {
      dout(1) << __func__ << " drop unexpected msg (not pong)" << dendl;
    } else {
      handle_timecheck_leader(m);
    }
  } else if (is_peon()) {
    if (m->op != MTimeCheck::OP_PING && m->op != MTimeCheck::OP_REPORT) {
      dout(1) << __func__ << " drop unexpected msg (not ping or report)" << dendl;
    } else {
      handle_timecheck_peon(m);
    }
  } else {
    dout(1) << __func__ << " drop unexpected msg" << dendl;
  }
  m->put();
}

void Monitor::handle_subscribe(MMonSubscribe *m)
{
  dout(10) << "handle_subscribe " << *m << dendl;
  
  bool reply = false;

  MonSession *s = static_cast<MonSession *>(m->get_connection()->get_priv());
  if (!s) {
    dout(10) << " no session, dropping" << dendl;
    m->put();
    return;
  }

  s->until = ceph_clock_now(g_ceph_context);
  s->until += g_conf->mon_subscribe_interval;
  for (map<string,ceph_mon_subscribe_item>::iterator p = m->what.begin();
       p != m->what.end();
       ++p) {
    // if there are any non-onetime subscriptions, we need to reply to start the resubscribe timer
    if ((p->second.flags & CEPH_SUBSCRIBE_ONETIME) == 0)
      reply = true;

    session_map.add_update_sub(s, p->first, p->second.start, 
			       p->second.flags & CEPH_SUBSCRIBE_ONETIME,
			       m->get_connection()->has_feature(CEPH_FEATURE_INCSUBOSDMAP));

    if (p->first == "mdsmap") {
      if ((int)s->is_capable("mds", MON_CAP_R)) {
        mdsmon()->check_sub(s->sub_map["mdsmap"]);
      }
    } else if (p->first == "osdmap") {
      if ((int)s->is_capable("osd", MON_CAP_R)) {
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

  // ???

  if (reply)
    messenger->send_message(new MMonSubscribeAck(monmap->get_fsid(), (int)g_conf->mon_subscribe_interval),
			    m->get_source_inst());

  s->put();
  m->put();
}

void Monitor::handle_get_version(MMonGetVersion *m)
{
  dout(10) << "handle_get_version " << *m << dendl;

  MonSession *s = static_cast<MonSession *>(m->get_connection()->get_priv());
  if (!s) {
    dout(10) << " no session, dropping" << dendl;
    m->put();
    return;
  }

  MMonGetVersionReply *reply = new MMonGetVersionReply();
  reply->handle = m->handle;
  if (m->what == "mdsmap") {
    reply->version = mdsmon()->mdsmap.get_epoch();
    reply->oldest_version = mdsmon()->get_first_committed();
  } else if (m->what == "osdmap") {
    reply->version = osdmon()->osdmap.get_epoch();
    reply->oldest_version = osdmon()->get_first_committed();
  } else if (m->what == "monmap") {
    reply->version = monmap->get_epoch();
    reply->oldest_version = monmon()->get_first_committed();
  } else {
    derr << "invalid map type " << m->what << dendl;
  }

  messenger->send_message(reply, m->get_source_inst());

  s->put();
  m->put();
}

bool Monitor::ms_handle_reset(Connection *con)
{
  dout(10) << "ms_handle_reset " << con << " " << con->get_peer_addr() << dendl;

  if (state == STATE_SHUTDOWN)
    return false;

  // ignore lossless monitor sessions
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON)
    return false;

  MonSession *s = static_cast<MonSession *>(con->get_priv());
  if (!s)
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
    send_latest_monmap(sub->session->con);
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
  messenger->send_message(new MMonMap(bl), con);
}

void Monitor::handle_mon_get_map(MMonGetMap *m)
{
  dout(10) << "handle_mon_get_map" << dendl;
  send_latest_monmap(m->get_connection());
  m->put();
}







/************ TICK ***************/

class C_Mon_Tick : public Context {
  Monitor *mon;
public:
  C_Mon_Tick(Monitor *m) : mon(m) {}
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
  }
  
  // trim sessions
  utime_t now = ceph_clock_now(g_ceph_context);
  xlist<MonSession*>::iterator p = session_map.sessions.begin();
  while (!p.end()) {
    MonSession *s = *p;
    ++p;
    
    // don't trim monitors
    if (s->inst.name.is_mon())
      continue; 

    if (!s->until.is_zero() && s->until < now) {
      dout(10) << " trimming session " << s->inst
	       << " (until " << s->until << " < now " << now << ")" << dendl;
      messenger->mark_down(s->inst.addr);
      remove_session(s);
    } else if (!exited_quorum.is_zero()) {
      if (now > (exited_quorum + 2 * g_conf->mon_lease)) {
        // boot the client Session because we've taken too long getting back in
        dout(10) << " trimming session " << s->inst
            << " because we've been out of quorum too long" << dendl;
        messenger->mark_down(s->inst.addr);
        remove_session(s);
      }
    }
  }

  if (!maybe_wait_for_quorum.empty()) {
    finish_contexts(g_ceph_context, maybe_wait_for_quorum);
  }

  new_tick();
}

int Monitor::check_fsid()
{
  ostringstream ss;
  ss << monmap->get_fsid();
  string us = ss.str();
  bufferlist ebl;

  if (!store->exists(MONITOR_NAME, "cluster_uuid"))
    return -ENOENT;

  int r = store->get(MONITOR_NAME, "cluster_uuid", ebl);
  assert(r == 0);

  string es(ebl.c_str(), ebl.length());

  // only keep the first line
  size_t pos = es.find_first_of('\n');
  if (pos != string::npos)
    es.resize(pos);

  dout(10) << "check_fsid cluster_uuid contains '" << es << "'" << dendl;
  if (es.length() < us.length() ||
      strncmp(us.c_str(), es.c_str(), us.length()) != 0) {
    derr << "error: cluster_uuid file exists with value '" << es
	 << "', != our uuid " << monmap->get_fsid() << dendl;
    return -EEXIST;
  }

  return 0;
}

int Monitor::write_fsid()
{
  MonitorDBStore::Transaction t;
  int r = write_fsid(t);
  store->apply_transaction(t);
  return r;
}

int Monitor::write_fsid(MonitorDBStore::Transaction &t)
{
  ostringstream ss;
  ss << monmap->get_fsid() << "\n";
  string us = ss.str();

  bufferlist b;
  b.append(us);

  t.put(MONITOR_NAME, "cluster_uuid", b);
  return 0;
}

/*
 * this is the closest thing to a traditional 'mkfs' for ceph.
 * initialize the monitor state machines to their initial values.
 */
int Monitor::mkfs(bufferlist& osdmapbl)
{
  MonitorDBStore::Transaction t;

  // verify cluster fsid
  int r = check_fsid();
  if (r < 0 && r != -ENOENT)
    return r;

  bufferlist magicbl;
  magicbl.append(CEPH_MON_ONDISK_MAGIC);
  magicbl.append("\n");
  t.put(MONITOR_NAME, "magic", magicbl);


  features = get_supported_features();
  write_features(t);

  // save monmap, osdmap, keyring.
  bufferlist monmapbl;
  monmap->encode(monmapbl, CEPH_FEATURES_ALL);
  monmap->set_epoch(0);     // must be 0 to avoid confusing first MonmapMonitor::update_from_paxos()
  t.put("mkfs", "monmap", monmapbl);

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
    t.put("mkfs", "osdmap", osdmapbl);
  }

  KeyRing keyring;
  string keyring_filename;
  if (!ceph_resolve_file_search(g_conf->keyring, keyring_filename)) {
    derr << "unable to find a keyring file on " << g_conf->keyring << dendl;
    return -ENOENT;
  }

  r = keyring.load(g_ceph_context, keyring_filename);
  if (r < 0) {
    derr << "unable to load initial keyring " << g_conf->keyring << dendl;
    return r;
  }

  // put mon. key in external keyring; seed with everything else.
  extract_save_mon_key(keyring);

  bufferlist keyringbl;
  keyring.encode_plaintext(keyringbl);
  t.put("mkfs", "keyring", keyringbl);
  write_fsid(t);
  store->apply_transaction(t);

  return 0;
}

int Monitor::write_default_keyring(bufferlist& bl)
{
  ostringstream os;
  os << g_conf->mon_data << "/keyring";

  int err = 0;
  int fd = ::open(os.str().c_str(), O_WRONLY|O_CREAT, 0644);
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

  if (state == STATE_SHUTDOWN)
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
    key_server.list_secrets(ss, ds);
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

  if (state == STATE_SHUTDOWN)
    return false;

  if (peer_type == CEPH_ENTITY_TYPE_MON &&
      auth_cluster_required.is_supported_auth(CEPH_AUTH_CEPHX)) {
    // monitor, and cephx is enabled
    isvalid = false;
    if (protocol == CEPH_AUTH_CEPHX) {
      bufferlist::iterator iter = authorizer_data.begin();
      CephXServiceTicketInfo auth_ticket_info;
      
      if (authorizer_data.length()) {
	int ret = cephx_verify_authorizer(g_ceph_context, &keyring, iter,
					  auth_ticket_info, authorizer_reply);
	if (ret >= 0) {
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
};

#undef dout_prefix
#define dout_prefix *_dout

void Monitor::StoreConverter::_convert_finish_features(
    MonitorDBStore::Transaction &t)
{
  dout(20) << __func__ << dendl;

  assert(db->exists(MONITOR_NAME, COMPAT_SET_LOC));
  bufferlist features_bl;
  db->get(MONITOR_NAME, COMPAT_SET_LOC, features_bl);
  assert(features_bl.length());

  CompatSet features;
  bufferlist::iterator p = features_bl.begin();
  features.decode(p);

  assert(features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_GV));
  features.incompat.remove(CEPH_MON_FEATURE_INCOMPAT_GV);
  assert(!features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_GV));

  features.incompat.insert(CEPH_MON_FEATURE_INCOMPAT_SINGLE_PAXOS);
  assert(features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_SINGLE_PAXOS));

  features_bl.clear();
  features.encode(features_bl);

  dout(20) << __func__ << " new features " << features << dendl;
  t.put(MONITOR_NAME, COMPAT_SET_LOC, features_bl);
}


bool Monitor::StoreConverter::_check_gv_store()
{
  dout(20) << __func__ << dendl;
  if (!store->exists_bl_ss(COMPAT_SET_LOC, 0))
    return false;

  bufferlist features_bl;
  store->get_bl_ss_safe(features_bl, COMPAT_SET_LOC, 0);
  if (!features_bl.length()) {
    dout(20) << __func__ << " on-disk features length is zero" << dendl;
    return false;
  }
  CompatSet features;
  bufferlist::iterator p = features_bl.begin();
  features.decode(p);
  return (features.incompat.contains(CEPH_MON_FEATURE_INCOMPAT_GV));
}

int Monitor::StoreConverter::needs_conversion()
{
  int ret = 0;

  dout(10) << __func__ << dendl;
  _init();
  if (db->open(std::cerr) < 0) {
    dout(1) << "unable to open monitor store at " << g_conf->mon_data << dendl;
    dout(1) << "check for old monitor store format" << dendl;
    int err = store->mount();
    if (err < 0) {
      if (err == -ENOENT) {
        derr << "unable to mount monitor store: "
             << cpp_strerror(err) << dendl;
      } else {
        derr << "it appears that another monitor is running: "
             << cpp_strerror(err) << dendl;
      }
      ret = err;
      goto out;
    }
    assert(err == 0);
    bufferlist magicbl;
    if (store->exists_bl_ss("magic", 0)) {
      if (_check_gv_store()) {
	dout(1) << "found old GV monitor store format "
		<< "-- should convert!" << dendl;
	ret = 1;
      } else {
	dout(0) << "Existing monitor store has not been converted "
		<< "to 0.52 (bobtail) format" << dendl;
	assert(0 == "Existing store has not been converted to 0.52 format");
      }
    }
    assert(!store->umount());
  } else {
    if (db->exists("mon_convert", "on_going")) {
      ret = -EEXIST;
      derr << "there is an on-going (maybe aborted?) conversion." << dendl;
      derr << "you should check what happened" << dendl;
    }
  }
out:
  _deinit();
  return ret;
}

int Monitor::StoreConverter::convert()
{
  _init();
  assert(!db->create_and_open(std::cerr));
  assert(!store->mount());
  if (db->exists("mon_convert", "on_going")) {
    dout(0) << __func__ << " found a mon store in mid-convertion; abort!"
      << dendl;
    return -EEXIST;
  }

  _mark_convert_start();
  _convert_monitor();
  _convert_machines();
  _convert_paxos();
  _mark_convert_finish();

  store->umount();
  _deinit();

  dout(0) << __func__ << " finished conversion" << dendl;

  return 0;
}

void Monitor::StoreConverter::_convert_monitor()
{
  dout(10) << __func__ << dendl;

  assert(store->exists_bl_ss("magic"));
  assert(store->exists_bl_ss("keyring"));
  assert(store->exists_bl_ss("feature_set"));
  assert(store->exists_bl_ss("election_epoch"));

  MonitorDBStore::Transaction tx;

  if (store->exists_bl_ss("joined")) {
    version_t joined = store->get_int("joined");
    tx.put(MONITOR_NAME, "joined", joined);
  }

  vector<string> keys;
  keys.push_back("magic");
  keys.push_back("feature_set");
  keys.push_back("cluster_uuid");

  vector<string>::iterator it;
  for (it = keys.begin(); it != keys.end(); ++it) {
    if (!store->exists_bl_ss((*it).c_str()))
      continue;

    bufferlist bl;
    int r = store->get_bl_ss(bl, (*it).c_str(), 0);
    assert(r > 0);
    tx.put(MONITOR_NAME, *it, bl);
  }
  version_t election_epoch = store->get_int("election_epoch");
  tx.put(MONITOR_NAME, "election_epoch", election_epoch);

  assert(!tx.empty());
  db->apply_transaction(tx);
  dout(10) << __func__ << " finished" << dendl;
}

void Monitor::StoreConverter::_convert_machines(string machine)
{
  dout(10) << __func__ << " " << machine << dendl;

  version_t first_committed =
    store->get_int(machine.c_str(), "first_committed");
  version_t last_committed =
    store->get_int(machine.c_str(), "last_committed");

  version_t accepted_pn = store->get_int(machine.c_str(), "accepted_pn");
  version_t last_pn = store->get_int(machine.c_str(), "last_pn");

  if (accepted_pn > highest_accepted_pn)
    highest_accepted_pn = accepted_pn;
  if (last_pn > highest_last_pn)
    highest_last_pn = last_pn;

  string machine_gv(machine);
  machine_gv.append("_gv");
  bool has_gv = true;

  if (!store->exists_bl_ss(machine_gv.c_str())) {
    dout(1) << __func__ << " " << machine
      << " no gv dir '" << machine_gv << "'" << dendl;
    has_gv = false;
  }

  for (version_t ver = first_committed; ver <= last_committed; ver++) {
    if (!store->exists_bl_sn(machine.c_str(), ver)) {
      dout(20) << __func__ << " " << machine
	       << " ver " << ver << " dne" << dendl;
      continue;
    }

    bufferlist bl;
    int r = store->get_bl_sn(bl, machine.c_str(), ver);
    assert(r >= 0);
    dout(20) << __func__ << " " << machine
	     << " ver " << ver << " bl " << bl.length() << dendl;

    MonitorDBStore::Transaction tx;
    tx.put(machine, ver, bl);
    tx.put(machine, "last_committed", ver);

    if (has_gv && store->exists_bl_sn(machine_gv.c_str(), ver)) {
      stringstream s;
      s << ver;
      string ver_str = s.str();

      version_t gv = store->get_int(machine_gv.c_str(), ver_str.c_str());
      dout(20) << __func__ << " " << machine
	       << " ver " << ver << " -> " << gv << dendl;

      MonitorDBStore::Transaction paxos_tx;

      if (gvs.count(gv) == 0) {
        gvs.insert(gv);
      } else {
	dout(0) << __func__ << " " << machine
		<< " gv " << gv << " already exists"
		<< dendl;

        // Duplicates aren't supposed to happen, but an old bug introduced
	// them and the mds state machine wasn't ever trimmed, so many users
	// will see them.  So we'll just merge them all in one
        // single paxos version.
        // We know that they are either from another paxos machine or
        // they are from the same paxos machine but their version is
        // lower than ours -- given that we are iterating all versions
        // from the lowest to the highest, duh!
        // We'll just append our stuff to the existing paxos transaction
        // as if nothing had happened.

        // Just make sure we are correct. This shouldn't take long and
        // should never be triggered!
        set<pair<string,version_t> >& s = gv_map[gv];
        for (set<pair<string,version_t> >::iterator it = s.begin();
             it != s.end(); ++it) {
          if (it->first == machine)
            assert(it->second + 1 == ver);
        }

        bufferlist paxos_bl;
        int r = db->get("paxos", gv, paxos_bl);
        assert(r >= 0);
        paxos_tx.append_from_encoded(paxos_bl);
      }
      gv_map[gv].insert(make_pair(machine,ver));

      bufferlist tx_bl;
      tx.encode(tx_bl);
      paxos_tx.append_from_encoded(tx_bl);
      bufferlist paxos_bl;
      paxos_tx.encode(paxos_bl);
      tx.put("paxos", gv, paxos_bl);
    }
    db->apply_transaction(tx);
  }

  version_t lc = db->get(machine, "last_committed");
  dout(20) << __func__ << " lc " << lc << " last_committed " << last_committed << dendl;
  assert(lc == last_committed);

  MonitorDBStore::Transaction tx;
  tx.put(machine, "first_committed", first_committed);
  tx.put(machine, "last_committed", last_committed);
  tx.put(machine, "conversion_first", first_committed);

  if (store->exists_bl_ss(machine.c_str(), "latest")) {
    bufferlist latest_bl_raw;
    int r = store->get_bl_ss(latest_bl_raw, machine.c_str(), "latest");
    assert(r >= 0);
    if (!latest_bl_raw.length()) {
      dout(20) << __func__ << " machine " << machine
	       << " skip latest with size 0" << dendl;
      goto out;
    }

    tx.put(machine, "latest", latest_bl_raw);

    bufferlist::iterator lbl_it = latest_bl_raw.begin();
    bufferlist latest_bl;
    version_t latest_ver;
    ::decode(latest_ver, lbl_it);
    ::decode(latest_bl, lbl_it);

    dout(20) << __func__ << " machine " << machine
	     << " latest ver " << latest_ver << dendl;

    tx.put(machine, "full_latest", latest_ver);
    stringstream os;
    os << "full_" << latest_ver;
    tx.put(machine, os.str(), latest_bl);
  }
out:
  db->apply_transaction(tx);
  dout(10) << __func__ << " machine " << machine << " finished" << dendl;
}

void Monitor::StoreConverter::_convert_osdmap_full()
{
  dout(10) << __func__ << dendl;
  version_t first_committed =
    store->get_int("osdmap", "first_committed");
  version_t last_committed =
    store->get_int("osdmap", "last_committed");

  int err = 0;
  for (version_t ver = first_committed; ver <= last_committed; ver++) {
    if (!store->exists_bl_sn("osdmap_full", ver)) {
      dout(20) << __func__ << " osdmap_full  ver " << ver << " dne" << dendl;
      err++;
      continue;
    }

    bufferlist bl;
    int r = store->get_bl_sn(bl, "osdmap_full", ver);
    assert(r >= 0);
    dout(20) << __func__ << " osdmap_full ver " << ver
             << " bl " << bl.length() << " bytes" << dendl;

    string full_key = "full_" + stringify(ver);
    MonitorDBStore::Transaction tx;
    tx.put("osdmap", full_key, bl);
    db->apply_transaction(tx);
  }
  dout(10) << __func__ << " found " << err << " conversion errors!" << dendl;
  assert(err == 0);
}

void Monitor::StoreConverter::_convert_paxos()
{
  dout(10) << __func__ << dendl;
  assert(!gvs.empty());

  set<version_t>::reverse_iterator rit = gvs.rbegin();
  version_t highest_gv = *rit;
  version_t last_gv = highest_gv;

  int n = 0;
  int max_versions = (g_conf->paxos_max_join_drift*2);
  for (; (rit != gvs.rend()) && (n < max_versions); ++rit, ++n) {
    version_t gv = *rit;

    if (last_gv == gv)
      continue;
    if ((last_gv - gv) > 1) {
      // we are done; we found a gap and we are only interested in keeping
      // contiguous paxos versions.
      break;
    }
    last_gv = gv;
  }

  // erase all paxos versions between [first, last_gv[, with first being the
  // first gv in the map.
  MonitorDBStore::Transaction tx;
  set<version_t>::iterator it = gvs.begin();
  dout(1) << __func__ << " first gv " << (*it)
	  << " last gv " << last_gv << dendl;
  for (; it != gvs.end() && (*it < last_gv); ++it) {
    tx.erase("paxos", *it);
  }
  tx.put("paxos", "first_committed", last_gv);
  tx.put("paxos", "last_committed", highest_gv);
  tx.put("paxos", "accepted_pn", highest_accepted_pn);
  tx.put("paxos", "last_pn", highest_last_pn);
  tx.put("paxos", "conversion_first", last_gv);
  db->apply_transaction(tx);

  dout(10) << __func__ << " finished" << dendl;
}

void Monitor::StoreConverter::_convert_machines()
{
  dout(10) << __func__ << dendl;
  set<string> machine_names = _get_machines_names();
  set<string>::iterator it = machine_names.begin();

  for (; it != machine_names.end(); ++it) {
    _convert_machines(*it);
  }
  // convert osdmap full versions
  // this stays here as these aren't really an independent paxos
  // machine, but rather machine-specific and don't fit on the
  // _convert_machines(string) function.
  _convert_osdmap_full();

  dout(10) << __func__ << " finished" << dendl;
}
