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

#include "Monitor.h"
#include "common/version.h"

#include "osd/OSDMap.h"

#include "MonitorStore.h"

#include "msg/Messenger.h"

#include "messages/PaxosServiceMessage.h"
#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonProbe.h"
#include "messages/MMonJoin.h"
#include "messages/MMonPaxos.h"
#include "messages/MRoute.h"
#include "messages/MForward.h"

#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"

#include "messages/MAuthReply.h"

#include "messages/MTimeCheck.h"

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

#include "auth/AuthMethodList.h"
#include "auth/KeyRing.h"

#include "common/config.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, const Monitor *mon) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name() << ") e" << mon->monmap->get_epoch() << " ";
}

long parse_pos_long(const char *s, ostream *pss)
{
  if (*s == '-' || *s == '+') {
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


Monitor::Monitor(CephContext* cct_, string nm, MonitorStore *s, Messenger *m, MonMap *map) :
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

  timecheck_round(0),
  timecheck_event(NULL),

  probe_timeout_event(NULL),

  paxos_service(PAXOS_NUM),
  admin_hook(NULL),
  routed_request_tid(0)
{
  rank = -1;

  paxos_service[PAXOS_PGMAP] = new PGMonitor(this, add_paxos(PAXOS_PGMAP));
  paxos_service[PAXOS_OSDMAP] = new OSDMonitor(this, add_paxos(PAXOS_OSDMAP));
  // mdsmap should be added to the paxos vector after the osdmap
  paxos_service[PAXOS_MDSMAP] = new MDSMonitor(this, add_paxos(PAXOS_MDSMAP));
  paxos_service[PAXOS_LOG] = new LogMonitor(this, add_paxos(PAXOS_LOG));
  paxos_service[PAXOS_MONMAP] = new MonmapMonitor(this, add_paxos(PAXOS_MONMAP));
  paxos_service[PAXOS_AUTH] = new AuthMonitor(this, add_paxos(PAXOS_AUTH));

  mon_caps = new MonCaps();
  mon_caps->set_allow_all(true);
  mon_caps->text = "allow *";

  exited_quorum = ceph_clock_now(g_ceph_context);
}

Paxos *Monitor::add_paxos(int type)
{
  Paxos *p = new Paxos(this, type);
  paxos.push_back(p);
  return p;
}

Paxos *Monitor::get_paxos_by_name(const string& name)
{
  for (list<Paxos*>::iterator p = paxos.begin();
       p != paxos.end();
       ++p) {
    if ((*p)->machine_name == name)
      return *p;
  }
  return NULL;
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
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    delete *p;
  for (list<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    delete *p;
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
  else if (command.find("add_bootstrap_peer_hint") == 0)
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
  ceph_mon_feature_incompat.insert(CEPH_MON_FEATURE_INCOMPAT_GV);
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

int Monitor::check_features(MonitorStore *store)
{
  CompatSet required = get_supported_features();
  CompatSet ondisk;

  bufferlist features;
  store->get_bl_ss_safe(features, COMPAT_SET_LOC, 0);
  if (features.length() == 0) {
    generic_dout(0) << "WARNING: mon fs missing feature list.\n"
	    << "Assuming it is old-style and introducing one." << dendl;
    //we only want the baseline ~v.18 features assumed to be on disk.
    //If new features are introduced this code needs to disappear or
    //be made smarter.
    ondisk = get_legacy_features();

    bufferlist bl;
    ondisk.encode(bl);
    store->put_bl_ss(bl, COMPAT_SET_LOC, 0);
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
  store->get_bl_ss_safe(bl, COMPAT_SET_LOC, 0);
  assert(bl.length());

  bufferlist::iterator p = bl.begin();
  ::decode(features, p);
  dout(10) << "features " << features << dendl;
}

void Monitor::write_features()
{
  bufferlist bl;
  features.encode(bl);
  store->put_bl_ss(bl, COMPAT_SET_LOC, 0);
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
  has_ever_joined = store->exists_bl_ss("joined");
  dout(10) << "has_ever_joined = " << (int)has_ever_joined << dendl;

  if (!has_ever_joined) {
    // impose initial quorum restrictions?
    list<string> initial_members;
    get_str_list(g_conf->mon_initial_members, initial_members);

    if (initial_members.size()) {
      dout(1) << " initial_members " << initial_members << ", filtering seed monmap" << dendl;

      monmap->set_initial_members(g_ceph_context, initial_members, name, messenger->get_myaddr(),
				  &extra_probe_peers);

      dout(10) << " monmap is " << *monmap << dendl;
    }
  }

  // init paxos
  for (list<Paxos*>::iterator it = paxos.begin(); it != paxos.end(); ++it) {
    (*it)->init();
    if ((*it)->is_consistent()) {
      int i = (*it)->machine_id;
      paxos_service[i]->update_from_paxos();
    } // else we don't do anything; handle_probe_reply will detect it's slurping
  }

  // we need to bootstrap authentication keys so we can form an
  // initial quorum.
  if (authmon()->paxos->get_version() == 0) {
    dout(10) << "loading initial keyring to bootstrap authentication for mkfs" << dendl;
    bufferlist bl;
    store->get_bl_ss_safe(bl, "mkfs", "keyring");
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
  r = admin_socket->register_command("mon_status", admin_hook,
				     "show current monitor status");
  assert(r == 0);
  r = admin_socket->register_command("quorum_status", admin_hook,
					 "show current quorum status");
  assert(r == 0);
  r = admin_socket->register_command("add_bootstrap_peer_hint", admin_hook,
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
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->shutdown();

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

  // reset
  state = STATE_PROBING;

  reset();

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

  for (list<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->restart();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->restart();
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
  double t = is_probing() ? g_conf->mon_probe_timeout : g_conf->mon_slurp_timeout;
  timer.add_event_after(t, probe_timeout_event);
  dout(10) << "reset_probe_timeout " << probe_timeout_event << " after " << t << " seconds" << dendl;
}

void Monitor::probe_timeout(int r)
{
  dout(4) << "probe_timeout " << probe_timeout_event << dendl;
  assert(is_probing() || is_slurping());
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

  case MMonProbe::OP_SLURP:
    handle_probe_slurp(m);
    break;

  case MMonProbe::OP_SLURP_LATEST:
    handle_probe_slurp_latest(m);
    break;

  case MMonProbe::OP_DATA:
    handle_probe_data(m);
    break;

  default:
    m->put();
  }
}

void Monitor::handle_probe_probe(MMonProbe *m)
{
  dout(10) << "handle_probe_probe " << m->get_source_inst() << *m << dendl;
  MMonProbe *r = new MMonProbe(monmap->fsid, MMonProbe::OP_REPLY, name, has_ever_joined);
  r->name = name;
  r->quorum = quorum;
  monmap->encode(r->monmap_bl, m->get_connection()->get_features());
  for (list<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); ++p)
    r->paxos_versions[(*p)->get_machine_name()] = (*p)->get_version();
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

  // is there an existing quorum?
  if (m->quorum.size()) {
    dout(10) << " existing quorum " << m->quorum << dendl;

    // do i need to catch up?
    bool ok = true;
    for (map<string,version_t>::iterator p = m->paxos_versions.begin();
	 p != m->paxos_versions.end();
	 ++p) {
      Paxos *pax = get_paxos_by_name(p->first);
      if (!pax) {
	dout(0) << " peer has paxos machine " << p->first << " but i don't... weird" << dendl;
	continue;  // weird!
      }
      if (pax->is_slurping()) {
        dout(10) << " My paxos machine " << p->first
                 << " is currently slurping, so that will continue. Peer has v "
                 << p->second << dendl;
        ok = false;
      } else if (pax->get_version() + g_conf->paxos_max_join_drift < p->second) {
	dout(10) << " peer paxos machine " << p->first << " v " << p->second
		 << " vs my v " << pax->get_version()
		 << " (too far ahead)"
		 << dendl;
	ok = false;
      } else {
	dout(10) << " peer paxos machine " << p->first << " v " << p->second
		 << " vs my v " << pax->get_version()
		 << " (ok)"
		 << dendl;
      }
    }
    if (ok) {
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
      slurp_source = m->get_source_inst();
      slurp_versions = m->paxos_versions;
      slurp();
    }
  } else {
    // not part of a quorum
    if (monmap->contains(m->name))
      outside_quorum.insert(m->name);
    else
      dout(10) << " mostly ignoring mon." << m->name << ", not part of monmap" << dendl;

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

/*
 * The whole slurp process is currently a bit of a hack.  Given the
 * current storage model, we should be sharing code with Paxos to make
 * sure we copy the right content.  But that model sucks and will
 * hopefully soon change, and it's less work to kludge around it here
 * than it is to make the current model clean.
 *
 * So: more or less duplicate the work of resyncing each paxos state
 * machine here.  And move the monitor storage refactor stuff up the
 * todo list.
 *
 */

void Monitor::slurp()
{
  dout(10) << "slurp " << slurp_source << " " << slurp_versions << dendl;

  reset_probe_timeout();

  state = STATE_SLURPING;

  map<string,version_t>::iterator p = slurp_versions.begin();
  while (p != slurp_versions.end()) {
    Paxos *pax = get_paxos_by_name(p->first);
    if (!pax) {
      p++;
      continue;
    }

    dout(10) << " " << p->first << " v " << p->second << " vs my " << pax->get_version() << dendl;
    if (p->second > pax->get_version() ||
	pax->get_stashed_version() > pax->get_version()) {
      if (!pax->is_slurping()) {
        pax->start_slurping();
      }
      MMonProbe *m = new MMonProbe(monmap->fsid, MMonProbe::OP_SLURP, name, has_ever_joined);
      m->machine_name = p->first;
      m->oldest_version = pax->get_first_committed();
      m->newest_version = pax->get_version();
      messenger->send_message(m, slurp_source);
      return;
    }

    // latest?
    if (pax->get_first_committed() > 1 &&   // don't need it!
	pax->get_stashed_version() < pax->get_first_committed()) {
      if (!pax->is_slurping()) {
        pax->start_slurping();
      }
      MMonProbe *m = new MMonProbe(monmap->fsid, MMonProbe::OP_SLURP_LATEST, name, has_ever_joined);
      m->machine_name = p->first;
      m->oldest_version = pax->get_first_committed();
      m->newest_version = pax->get_version();
      messenger->send_message(m, slurp_source);
      return;
    }

    PaxosService *paxs = get_paxos_service_by_name(p->first);
    assert(paxs);
    paxs->update_from_paxos();

    pax->end_slurping();

    slurp_versions.erase(p++);
  }

  dout(10) << "done slurping" << dendl;
  bootstrap();
}

MMonProbe *Monitor::fill_probe_data(MMonProbe *m, Paxos *pax)
{
  MMonProbe *r = new MMonProbe(monmap->fsid, MMonProbe::OP_DATA, name, has_ever_joined);
  r->machine_name = m->machine_name;
  r->oldest_version = pax->get_first_committed();
  r->newest_version = pax->get_version();

  version_t v = MAX(pax->get_first_committed(), m->newest_version + 1);
  int len = 0;
  for (; v <= pax->get_version(); v++) {
    store->get_bl_sn_safe(r->paxos_values[m->machine_name][v], m->machine_name.c_str(), v);
    len += r->paxos_values[m->machine_name][v].length();
    for (list<string>::iterator p = pax->extra_state_dirs.begin();
         p != pax->extra_state_dirs.end();
         ++p) {
      store->get_bl_sn_safe(r->paxos_values[*p][v], p->c_str(), v);
      len += r->paxos_values[*p][v].length();
    }
    if (len >= g_conf->mon_slurp_bytes)
      break;
  }

  return r;
}

void Monitor::handle_probe_slurp(MMonProbe *m)
{
  dout(10) << "handle_probe_slurp " << *m << dendl;

  Paxos *pax = get_paxos_by_name(m->machine_name);
  assert(pax);

  MMonProbe *r = fill_probe_data(m, pax);
  messenger->send_message(r, m->get_connection());
  m->put();
}

void Monitor::handle_probe_slurp_latest(MMonProbe *m)
{
  dout(10) << "handle_probe_slurp_latest " << *m << dendl;

  Paxos *pax = get_paxos_by_name(m->machine_name);
  assert(pax);

  MMonProbe *r = fill_probe_data(m, pax);
  r->latest_version = pax->get_stashed(r->latest_value);

  messenger->send_message(r, m->get_connection());
  m->put();
}

void Monitor::handle_probe_data(MMonProbe *m)
{
  dout(10) << "handle_probe_data " << *m << dendl;

  Paxos *pax = get_paxos_by_name(m->machine_name);
  assert(pax);

  // trim old cruft?
  if (m->oldest_version > pax->get_first_committed())
    pax->trim_to(m->oldest_version, true);

  // note new latest version?
  if (slurp_versions.count(m->machine_name))
    slurp_versions[m->machine_name] = m->newest_version;

  // store any new stuff
  if (m->paxos_values.size()) {
    for (map<string, map<version_t, bufferlist> >::iterator p = m->paxos_values.begin();
	 p != m->paxos_values.end();
	 ++p) {
      store->put_bl_sn_map(p->first.c_str(), p->second.begin(), p->second.end());
    }

    pax->last_committed = m->paxos_values.begin()->second.rbegin()->first;
    store->put_int(pax->last_committed, m->machine_name.c_str(),
		   "last_committed");
  }

  // latest?
  if (m->latest_version) {
    pax->stash_latest(m->latest_version, m->latest_value);
  }

  m->put();

  slurp();
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

  for (list<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->leader_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_finished();

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

  for (list<Paxos*>::iterator p = paxos.begin(); p != paxos.end(); p++)
    (*p)->peon_init();
  for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++)
    (*p)->election_finished();

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


bool Monitor::_allowed_command(MonSession *s, const vector<string>& cmd)
{
  for (list<list<string> >::iterator p = s->caps.cmd_allow.begin();
       p != s->caps.cmd_allow.end();
       ++p) {
    list<string>::iterator q;
    unsigned i;
    dout(0) << "cmd " << cmd << " vs " << *p << dendl;
    for (q = p->begin(), i = 0; q != p->end() && i < cmd.size(); ++q, ++i) {
      if (*q == "*")
	continue;
      if (*q == "...") {
	i = cmd.size() - 1;
	continue;
      }	
      if (*q != cmd[i])
	break;
    }
    if (q == p->end() && i == cmd.size())
      return true;   // match
  }

  return false;
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

  if (is_slurping()) {
    jf.dump_stream("slurp_source") << slurp_source;
    jf.open_object_section("slurp_version");
    for (map<string,version_t>::iterator p = slurp_versions.begin(); p != slurp_versions.end(); ++p)
      jf.dump_int(p->first.c_str(), p->second);	  
    jf.close_section();
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
       p++) {
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
      << (timecheck_round%2 ? "on-going" : "finished");
  }

  if (timecheck_skews.size() != 0) {
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
    f->dump_stream("monmap") << *monmap;
    f->dump_stream("election_epoch") << get_epoch();
    f->dump_stream("quorum") << get_quorum();
    f->dump_stream("quorum_names") << get_quorum_names();
    f->dump_stream("osdmap") << osdmon()->osdmap;
    f->dump_stream("pgmap") << pgmon()->pg_map;
    f->dump_stream("mdsmap") << mdsmon()->mdsmap;
    f->close_section();
  } else {
    ss << "   health " << health << "\n";
    ss << "   monmap " << *monmap << ", election epoch " << get_epoch()
      << ", quorum " << get_quorum() << " " << get_quorum_names() << "\n";
    ss << "   osdmap " << osdmon()->osdmap << "\n";
    ss << "    pgmap " << pgmon()->pg_map << "\n";
    ss << "   mdsmap " << mdsmon()->mdsmap << "\n";
  }
}

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

  bool access_cmd = _allowed_command(session, m->cmd);
  bool access_r = (session->caps.check_privileges(PAXOS_MONMAP, MON_CAP_R) ||
		   access_cmd);
  bool access_all = (session->caps.get_allow_all() || access_cmd);

  dout(0) << "handle_command " << *m << dendl;
  bufferlist rdata;
  string rs;
  int r = -EINVAL;
  rs = "unrecognized command";
  if (m->cmd.empty())
    goto out;

  if (m->cmd[0] == "mds") {
    mdsmon()->dispatch(m);
    return;
  }
  if (m->cmd[0] == "osd") {
    osdmon()->dispatch(m);
    return;
  }
  if (m->cmd[0] == "pg") {
    pgmon()->dispatch(m);
    return;
  }
  if (m->cmd[0] == "mon") {
    monmon()->dispatch(m);
    return;
  }
  if (m->cmd[0] == "class") {
    reply_command(m, -EINVAL, "class distribution is no longer handled by the monitor", 0);
    return;
  }
  if (m->cmd[0] == "auth") {
    authmon()->dispatch(m);
    return;
  }

  if (m->cmd[0] == "fsid") {
    stringstream ss;
    ss << monmap->fsid;
    reply_command(m, 0, ss.str(), rdata, 0);
    return;
  }
  if (m->cmd[0] == "log") {
    if (!access_r) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    stringstream ss;
    for (unsigned i=1; i<m->cmd.size(); i++) {
      if (i > 1)
        ss << ' ';
      ss << m->cmd[i];
    }
    clog.info(ss);
    rs = "ok";
    reply_command(m, 0, rs, rdata, 0);
    return;
  }
  if (m->cmd[0] == "stop_cluster") {
    if (!access_all) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    stop_cluster();
    reply_command(m, 0, "initiating cluster shutdown", 0);
    return;
  }

  if (m->cmd[0] == "injectargs") {
    if (!access_all) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    if (m->cmd.size() == 2) {
      dout(0) << "parsing injected options '" << m->cmd[1] << "'" << dendl;
      ostringstream oss;
      g_conf->injectargs(m->cmd[1], &oss);
      derr << "injectargs:" << dendl;
      derr << oss.str() << dendl;
      rs = "parsed options";
      r = 0;
    } else {
      rs = "must supply options to be parsed in a single string";
      r = -EINVAL;
    }
  } else if ((m->cmd[0] == "status") || (m->cmd[0] == "health")) {
    if (!access_r) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }

    vector<const char *> args;
    for (unsigned int i = 0; i < m->cmd.size(); ++i)
      args.push_back(m->cmd[i].c_str());

    string format = "plain";
    JSONFormatter *jf = NULL;
    for (vector<const char*>::iterator i = args.begin(); i != args.end();) {
      string val;
      if (ceph_argparse_witharg(args, i, &val,
            "-f", "--format", (char*)NULL)) {
        format = val;
      } else {
        ++i;
      }
    }

    if (format != "plain") {
      if (format == "json") {
        jf = new JSONFormatter(true);
      } else {
        r = -EINVAL;
        stringstream err_ss;
        err_ss << "unrecognized format '" << format
          << "' (available: plain, json)";
        rs = err_ss.str();
        goto out;
      }
    }

    stringstream ss;
    if (string(args[0]) == "status") {
      get_status(ss, jf);

      if (jf) {
        jf->flush(ss);
        ss << '\n';
      }
    } else if (string(args[0]) == "health") {
      string health_str;
      get_health(health_str, (args.size() > 1) ? &rdata : NULL, jf);
      if (jf) {
        jf->flush(ss);
        ss << '\n';
      } else {
        ss << health_str;
      }
    } else {
      assert(0 == "We should never get here!");
      return;
    }
    rs = ss.str();
    r = 0;
  } else if (m->cmd[0] == "report") {
    if (!access_r) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }

    JSONFormatter jf(true);

    jf.open_object_section("report");
    jf.dump_string("version", ceph_version_to_str());
    jf.dump_string("commit", git_version_to_str());
    jf.dump_stream("timestamp") << ceph_clock_now(NULL);

    string d;
    for (unsigned i = 1; i < m->cmd.size(); i++) {
      if (i > 1)
        d += " ";
      d += m->cmd[i];
    }
    jf.dump_string("tag", d);

    string hs;
    get_health(hs, NULL, &jf);

    monmon()->dump_info(&jf);
    osdmon()->dump_info(&jf);
    mdsmon()->dump_info(&jf);
    pgmon()->dump_info(&jf);

    jf.close_section();
    stringstream ss;
    jf.flush(ss);

    bufferlist bl;
    bl.append("-------- BEGIN REPORT --------\n");
    bl.append(ss);
    ostringstream ss2;
    ss2 << "\n-------- END REPORT " << bl.crc32c(6789) << " --------\n";
    rdata.append(bl);
    rdata.append(ss2.str());
    rs = string();
    r = 0;
  } else if (m->cmd[0] == "quorum_status") {
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
    stringstream ss;
    _quorum_status(ss);
    rs = ss.str();
    r = 0;
  } else if (m->cmd[0] == "mon_status") {
    if (!access_r) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    stringstream ss;
    _mon_status(ss);
    rs = ss.str();
    r = 0;
  } else if (m->cmd[0] == "heap") {
    if (!access_all) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    if (!ceph_using_tcmalloc())
      rs = "tcmalloc not enabled, can't use heap profiler commands\n";
    else {
      ostringstream ss;
      ceph_heap_profiler_handle_command(m->cmd, ss);
      rs = ss.str();
    }
  } else if (m->cmd[0] == "quorum") {
    if (!access_all) {
      r = -EACCES;
      rs = "access denied";
      goto out;
    }
    if (m->cmd[1] == "exit") {
      reset();
      start_election();
      elector.stop_participating();
      rs = "stopped responding to quorum, initiated new election";
      r = 0;
    } else if (m->cmd[1] == "enter") {
      elector.start_participating();
      reset();
      start_election();
      rs = "started responding to quorum, initiated new election";
      r = 0;
    } else {
      rs = "unknown quorum subcommand; use exit or enter";
      r = -EINVAL;
    }
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
    session = (MonSession *)req->get_connection()->get_priv();
  if (req->session_mon >= 0) {
    dout(10) << "forward_request won't double fwd request " << *req << dendl;
    req->put();
  } else if (session && !session->closed) {
    RoutedRequest *rr = new RoutedRequest;
    rr->tid = ++routed_request_tid;
    rr->client = req->get_source_inst();
    encode_message(req, CEPH_FEATURES_ALL, rr->request_bl);   // for my use only; use all features
    rr->session = (MonSession *)session->get();
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
  MonSession *session = (MonSession *)m->get_connection()->get_priv();
  assert(session);

  if (!session->caps.check_privileges(PAXOS_MONMAP, MON_CAP_X)) {
    dout(0) << "forward from entity with insufficient caps! " 
	    << session->caps << dendl;
  } else {
    Connection *c = new Connection;
    MonSession *s = new MonSession(m->msg->get_source_inst(), c);
    c->set_priv(s);
    c->set_peer_addr(m->client.addr);
    c->set_peer_type(m->client.name.type());

    s->caps = m->client_caps;
    s->proxy_con = m->get_connection()->get();
    s->proxy_tid = m->tid;

    PaxosServiceMessage *req = m->msg;
    m->msg = NULL;  // so ~MForward doesn't delete it
    req->set_connection(c);
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
  MonSession *session = (MonSession*)req->get_connection()->get_priv();
  if (!session) {
    dout(2) << "send_reply no session, dropping reply " << *reply
	    << " to " << req << " " << *req << dendl;
    reply->put();
    return;
  }
  if (session->proxy_con) {
    dout(15) << "send_reply routing reply to " << req->get_connection()->get_peer_addr()
	     << " via mon" << req->session_mon
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
  MonSession *session = (MonSession*)req->get_connection()->get_priv();
  if (!session) {
    dout(2) << "no_reply no session, dropping non-reply to " << req << " " << *req << dendl;
    return;
  }
  if (session->proxy_con) {
    if (get_quorum_features() & CEPH_FEATURE_MON_NULLROUTE) {
      dout(10) << "no_reply to " << req->get_source_inst() << " via mon." << req->session_mon
	       << " for request " << *req << dendl;
      messenger->send_message(new MRoute(session->proxy_tid, NULL),
			      session->proxy_con);
    } else {
      dout(10) << "no_reply no quorum nullroute feature for " << req->get_source_inst() << " via mon." << req->session_mon
	       << " for request " << *req << dendl;
    }
  } else {
    dout(10) << "no_reply to " << req->get_source_inst() << " " << *req << dendl;
  }
  session->put();
}

void Monitor::handle_route(MRoute *m)
{
  MonSession *session = (MonSession *)m->get_connection()->get_priv();
  //check privileges
  if (session && !session->caps.check_privileges(PAXOS_MONMAP, MON_CAP_X)) {
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
	messenger->send_message(m->msg, rr->session->inst);
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
  for (map<uint64_t, RoutedRequest*>::iterator p = routed_requests.begin();
       p != routed_requests.end();
       p++) {
    RoutedRequest *rr = p->second;

    bufferlist::iterator q = rr->request_bl.begin();
    PaxosServiceMessage *req = (PaxosServiceMessage *)decode_message(cct, q);

    dout(10) << " resend to mon." << mon << " tid " << rr->tid << " " << *req << dendl;
    MForward *forward = new MForward(rr->tid, req, rr->session->caps);
    forward->client = rr->client;
    forward->set_priority(req->get_priority());
    messenger->send_message(forward, monmap->get_inst(mon));
  }  
}

void Monitor::remove_session(MonSession *s)
{
  dout(10) << "remove_session " << s << " " << s->inst << dendl;
  assert(!s->closed);
  for (set<uint64_t>::iterator p = s->routed_request_tids.begin();
       p != s->routed_request_tids.end();
       p++) {
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


void Monitor::stop_cluster()
{
  dout(0) << "stop_cluster -- initiating shutdown" << dendl;
  mdsmon()->do_stop();
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
  bool reuse_caps = false;
  MonCaps caps;
  EntityName entity_name;
  bool src_is_mon;

  src_is_mon = !connection || (connection->get_peer_type() & CEPH_ENTITY_TYPE_MON);

  if (connection) {
    dout(20) << "have connection" << dendl;
    s = (MonSession *)connection->get_priv();
    if (s && s->closed) {
      caps = s->caps;
      reuse_caps = true;
      s->put();
      s = NULL;
    }
    if (!s) {
      if (!exited_quorum.is_zero()
          && !src_is_mon) {
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
        utime_t too_old = ceph_clock_now(g_ceph_context);
        too_old -= g_ceph_context->_conf->mon_lease;
        if (m->get_recv_stamp() > too_old
            && connection->is_connected()) {
          dout(5) << "waitlisting message " << *m
                  << " until we get in quorum" << dendl;
          maybe_wait_for_quorum.push_back(new C_RetryMessage(this, m));
        } else {
          dout(1) << "discarding message " << *m
                  << " and sending client elsewhere; we are not in quorum"
                  << dendl;
          messenger->mark_down(connection);
          m->put();
        }
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
	if (!s->caps.allow_all) //but no need to repeatedly copy
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

  {
    switch (m->get_type()) {
      
    case MSG_ROUTE:
      handle_route((MRoute*)m);
      break;

      // misc
    case CEPH_MSG_MON_GET_MAP:
      handle_mon_get_map((MMonGetMap*)m);
      break;

    case CEPH_MSG_MON_GET_VERSION:
      handle_get_version((MMonGetVersion*)m);
      break;

    case MSG_MON_COMMAND:
      handle_command((MMonCommand*)m);
      break;

    case CEPH_MSG_MON_SUBSCRIBE:
      /* FIXME: check what's being subscribed, filter accordingly */
      handle_subscribe((MMonSubscribe*)m);
      break;

    case MSG_MON_PROBE:
      handle_probe((MMonProbe*)m);
      break;

      // OSDs
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
	MMonPaxos *pm = (MMonPaxos*)m;
	if (!src_is_mon && 
	    !s->caps.check_privileges(PAXOS_MONMAP, MON_CAP_X)) {
	  //can't send these!
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

	// send it to the right paxos instance
	assert(pm->machine_id < PAXOS_NUM);
	Paxos *p = get_paxos_by_name(get_paxos_name(pm->machine_id));
	p->dispatch((PaxosServiceMessage*)m);

	// make sure service finds out about any state changes
	if (p->is_active())
	  paxos_service[p->machine_id]->update_from_paxos();
      }
      break;

      // elector messages
    case MSG_MON_ELECTION:
      //check privileges here for simplicity
      if (s &&
	  !s->caps.check_privileges(PAXOS_MONMAP, MON_CAP_X)) {
	dout(0) << "MMonElection received from entity without enough caps!"
		<< s->caps << dendl;
	m->put();
	break;
      }
      if (!is_probing() && !is_slurping()) {
	elector.dispatch(m);
      } else {
	m->put();
      }
      break;

    case MSG_FORWARD:
      handle_forward((MForward *)m);
      break;

    case MSG_TIMECHECK:
      handle_timecheck((MTimeCheck *)m);
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
    assert(timecheck_waiting.size() == 0);
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
  timecheck_skews[monmap->get_inst(name)] = 0.0;
  timecheck_latencies[monmap->get_inst(name)] = 0.0;

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
    assert(timecheck_waiting.size() == 0);
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

  MonSession *s = (MonSession *)m->get_connection()->get_priv();
  if (!s) {
    dout(10) << " no session, dropping" << dendl;
    m->put();
    return;
  }

  s->until = ceph_clock_now(g_ceph_context);
  s->until += g_conf->mon_subscribe_interval;
  for (map<string,ceph_mon_subscribe_item>::iterator p = m->what.begin();
       p != m->what.end();
       p++) {
    // if there are any non-onetime subscriptions, we need to reply to start the resubscribe timer
    if ((p->second.flags & CEPH_SUBSCRIBE_ONETIME) == 0)
      reply = true;

    session_map.add_update_sub(s, p->first, p->second.start, 
			       p->second.flags & CEPH_SUBSCRIBE_ONETIME,
			       m->get_connection()->has_feature(CEPH_FEATURE_INCSUBOSDMAP));

    if (p->first == "mdsmap") {
      if ((int)s->caps.check_privileges(PAXOS_MDSMAP, MON_CAP_R)) {
        mdsmon()->check_sub(s->sub_map["mdsmap"]);
      }
    } else if (p->first == "osdmap") {
      if ((int)s->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_R)) {
        osdmon()->check_sub(s->sub_map["osdmap"]);
      }
    } else if (p->first == "osd_pg_creates") {
      if ((int)s->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W)) {
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

  MonSession *s = (MonSession *)m->get_connection()->get_priv();
  if (!s) {
    dout(10) << " no session, dropping" << dendl;
    m->put();
    return;
  }

  MMonGetVersionReply *reply = new MMonGetVersionReply();
  reply->handle = m->handle;
  if (m->what == "mdsmap") {
    reply->version = mdsmon()->mdsmap.get_epoch();
    reply->oldest_version = mdsmon()->paxos->get_first_committed();
  } else if (m->what == "osdmap") {
    reply->version = osdmon()->osdmap.get_epoch();
    reply->oldest_version = osdmon()->paxos->get_first_committed();
  } else if (m->what == "monmap") {
    reply->version = monmap->get_epoch();
    reply->oldest_version = monmon()->paxos->get_first_committed();
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

  MonSession *s = (MonSession *)con->get_priv();
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
  
  if (!is_slurping()) {
    for (vector<PaxosService*>::iterator p = paxos_service.begin(); p != paxos_service.end(); p++) {
      (*p)->tick();
    }
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
  int r = store->get_bl_ss(ebl, "cluster_uuid", 0);
  if (r < 0)
    return r;

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
  ostringstream ss;
  ss << monmap->get_fsid() << "\n";
  string us = ss.str();

  bufferlist b;
  b.append(us);
  store->put_bl_ss(b, "cluster_uuid", 0);
  return 0;
}

/*
 * this is the closest thing to a traditional 'mkfs' for ceph.
 * initialize the monitor state machines to their initial values.
 */
int Monitor::mkfs(bufferlist& osdmapbl)
{
  // create it
  int err = store->mkfs();
  if (err) {
    derr << "store->mkfs failed with: " << cpp_strerror(err) << dendl;
    return err;
  }

  // verify cluster fsid
  int r = check_fsid();
  if (r < 0 && r != -ENOENT)
    return r;

  bufferlist magicbl;
  magicbl.append(CEPH_MON_ONDISK_MAGIC);
  magicbl.append("\n");
  store->put_bl_ss(magicbl, "magic", 0);


  features = get_supported_features();
  write_features();

  // save monmap, osdmap, keyring.
  bufferlist monmapbl;
  monmap->encode(monmapbl, CEPH_FEATURES_ALL);
  monmap->set_epoch(0);     // must be 0 to avoid confusing first MonmapMonitor::update_from_paxos()
  store->put_bl_ss(monmapbl, "mkfs", "monmap");

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
    store->put_bl_ss(osdmapbl, "mkfs", "osdmap");
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
  store->put_bl_ss(keyringbl, "mkfs", "keyring");

  // sync and write out fsid to indicate completion.
  store->sync();
  r = write_fsid();
  if (r < 0)
    return r;

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
    stringstream ss;
    key_server.list_secrets(ss);
    dout(0) << ss.str() << dendl;
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
