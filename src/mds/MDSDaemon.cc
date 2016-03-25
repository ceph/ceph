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

#include <unistd.h>

#include "global/signal_handler.h"

#include "include/types.h"
#include "include/str_list.h"
#include "common/entity_name.h"
#include "common/Clock.h"
#include "common/signal.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"

#include "msg/Messenger.h"
#include "mon/MonClient.h"

#include "osdc/Objecter.h"

#include "MDSMap.h"

#include "MDSDaemon.h"
#include "Server.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "MDBalancer.h"
#include "Migrator.h"

#include "SnapServer.h"
#include "SnapClient.h"

#include "InoTable.h"

#include "common/HeartbeatMap.h"

#include "common/perf_counters.h"

#include "common/Timer.h"

#include "events/ESession.h"
#include "events/ESubtreeMap.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"

#include "messages/MGenericMessage.h"

#include "messages/MMonCommand.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "auth/AuthAuthorizeHandler.h"
#include "auth/KeyRing.h"

#include "common/config.h"

#include "perfglue/cpu_profiler.h"
#include "perfglue/heap_profiler.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << name << ' '

/**
 * Helper for simple callbacks that call a void fn with no args.
 */
class C_VoidFn : public Context
{
  typedef void (MDSDaemon::*fn_ptr)();
  protected:
   MDSDaemon *mds;
   fn_ptr fn;
  public:
  C_VoidFn(MDSDaemon *mds_, fn_ptr fn_)
    : mds(mds_), fn(fn_)
  {
    assert(mds_);
    assert(fn_);
  }

  void finish(int r)
  {
    (mds->*fn)();
  }
};

// cons/des
MDSDaemon::MDSDaemon(const std::string &n, Messenger *m, MonClient *mc) :
  Dispatcher(m->cct),
  mds_lock("MDSDaemon::mds_lock"),
  stopping(false),
  timer(m->cct, mds_lock),
  beacon(m->cct, mc, n),
  authorize_handler_cluster_registry(new AuthAuthorizeHandlerRegistry(m->cct,
								      m->cct->_conf->auth_supported.empty() ?
								      m->cct->_conf->auth_cluster_required :
								      m->cct->_conf->auth_supported)),
  authorize_handler_service_registry(new AuthAuthorizeHandlerRegistry(m->cct,
								      m->cct->_conf->auth_supported.empty() ?
								      m->cct->_conf->auth_service_required :
								      m->cct->_conf->auth_supported)),
  name(n),
  messenger(m),
  monc(mc),
  objecter(new Objecter(m->cct, m, mc, NULL, 0, 0)),
  log_client(m->cct, messenger, &mc->monmap, LogClient::NO_FLAGS),
  mds_rank(NULL),
  tick_event(0),
  standby_for_rank(MDSMap::MDS_NO_STANDBY_PREF),
  standby_type(MDSMap::STATE_NULL),
  asok_hook(NULL)
{
  orig_argc = 0;
  orig_argv = NULL;

  clog = log_client.create_channel();

  monc->set_messenger(messenger);

  mdsmap = new MDSMap;

  objecter->unset_honor_osdmap_full();
}

MDSDaemon::~MDSDaemon() {
  Mutex::Locker lock(mds_lock);

  delete mds_rank;
  mds_rank = NULL;
  delete objecter;
  objecter = NULL;
  delete mdsmap;
  mdsmap = NULL;

  delete authorize_handler_service_registry;
  delete authorize_handler_cluster_registry;
}

class MDSSocketHook : public AdminSocketHook {
  MDSDaemon *mds;
public:
  explicit MDSSocketHook(MDSDaemon *m) : mds(m) {}
  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {
    stringstream ss;
    bool r = mds->asok_command(command, cmdmap, format, ss);
    out.append(ss);
    return r;
  }
};

bool MDSDaemon::asok_command(string command, cmdmap_t& cmdmap, string format,
		    ostream& ss)
{
  dout(1) << "asok_command: " << command << " (starting...)" << dendl;

  Formatter *f = Formatter::create(format, "json-pretty", "json-pretty");
  bool handled = false;
  if (command == "status") {
    dump_status(f);
    handled = true;
  } else {
    if (mds_rank == NULL) {
      dout(1) << "Can't run that command on an inactive MDS!" << dendl;
      f->dump_string("error", "mds_not_active");
    } else {
      handled = mds_rank->handle_asok_command(command, cmdmap, f, ss);
    }
  }
  f->flush(ss);
  delete f;

  dout(1) << "asok_command: " << command << " (complete)" << dendl;

  return handled;
}

void MDSDaemon::dump_status(Formatter *f)
{
  const epoch_t osd_epoch = objecter->with_osdmap(
    std::mem_fn(&OSDMap::get_epoch));

  f->open_object_section("status");
  f->dump_stream("cluster_fsid") << monc->get_fsid();
  if (mds_rank) {
    f->dump_unsigned("whoami", mds_rank->get_nodeid());
  } else {
    f->dump_unsigned("whoami", MDS_RANK_NONE);
  }

  f->dump_string("want_state", ceph_mds_state_name(beacon.get_want_state()));
  f->dump_string("state", ceph_mds_state_name(mdsmap->get_state_gid(mds_gid_t(
	    monc->get_global_id()))));
  if (mds_rank) {
    Mutex::Locker l(mds_lock);
    mds_rank->dump_status(f);
  }

  f->dump_unsigned("mdsmap_epoch", mdsmap->get_epoch());
  f->dump_unsigned("osdmap_epoch", osd_epoch);
  if (mds_rank) {
    f->dump_unsigned("osdmap_epoch_barrier", mds_rank->get_osd_epoch_barrier());
  } else {
    f->dump_unsigned("osdmap_epoch_barrier", 0);
  }
  f->close_section(); // status
}

void MDSDaemon::set_up_admin_socket()
{
  int r;
  AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
  assert(asok_hook == nullptr);
  asok_hook = new MDSSocketHook(this);
  r = admin_socket->register_command("status", "status", asok_hook,
				     "high-level status of MDS");
  assert(r == 0);
  r = admin_socket->register_command("dump_ops_in_flight",
				     "dump_ops_in_flight", asok_hook,
				     "show the ops currently in flight");
  assert(r == 0);
  r = admin_socket->register_command("ops",
				     "ops", asok_hook,
				     "show the ops currently in flight");
  assert(r == 0);
  r = admin_socket->register_command("dump_blocked_ops", "dump_blocked_ops",
      asok_hook,
      "show the blocked ops currently in flight");
  assert(r == 0);
  r = admin_socket->register_command("dump_historic_ops", "dump_historic_ops",
				     asok_hook,
				     "show slowest recent ops");
  assert(r == 0);
  r = admin_socket->register_command("scrub_path",
				     "scrub_path name=path,type=CephString "
				     "name=scrubops,type=CephChoices,"
				     "strings=force|recursive|repair,n=N,req=false",
                                     asok_hook,
                                     "scrub an inode and output results");
  assert(r == 0);
  r = admin_socket->register_command("tag path",
                                     "tag path name=path,type=CephString"
                                     " name=tag,type=CephString",
                                     asok_hook,
                                     "Apply scrub tag recursively");
   assert(r == 0);
  r = admin_socket->register_command("flush_path",
                                     "flush_path name=path,type=CephString",
                                     asok_hook,
                                     "flush an inode (and its dirfrags)");
  assert(r == 0);
  r = admin_socket->register_command("export dir",
                                     "export dir "
                                     "name=path,type=CephString "
                                     "name=rank,type=CephInt",
                                     asok_hook,
                                     "migrate a subtree to named MDS");
  assert(r == 0);
  r = admin_socket->register_command("dump cache",
                                     "dump cache name=path,type=CephString,req=false",
                                     asok_hook,
                                     "dump metadata cache (optionally to a file)");
  assert(r == 0);
  r = admin_socket->register_command("session evict",
				     "session evict name=client_id,type=CephString",
				     asok_hook,
				     "Evict a CephFS client");
  assert(r == 0);
  r = admin_socket->register_command("osdmap barrier",
				     "osdmap barrier name=target_epoch,type=CephInt",
				     asok_hook,
				     "Wait until the MDS has this OSD map epoch");
  assert(r == 0);
  r = admin_socket->register_command("session ls",
				     "session ls",
				     asok_hook,
				     "Enumerate connected CephFS clients");
  assert(r == 0);
  r = admin_socket->register_command("flush journal",
				     "flush journal",
				     asok_hook,
				     "Flush the journal to the backing store");
  assert(r == 0);
  r = admin_socket->register_command("force_readonly",
				     "force_readonly",
				     asok_hook,
				     "Force MDS to read-only mode");
  assert(r == 0);
  r = admin_socket->register_command("get subtrees",
				     "get subtrees",
				     asok_hook,
				     "Return the subtree map");
  assert(r == 0);
  r = admin_socket->register_command("dirfrag split",
				     "dirfrag split "
                                     "name=path,type=CephString,req=true "
                                     "name=frag,type=CephString,req=true "
                                     "name=bits,type=CephInt,req=true ",
				     asok_hook,
				     "Fragment directory by path");
  assert(r == 0);
  r = admin_socket->register_command("dirfrag merge",
				     "dirfrag merge "
                                     "name=path,type=CephString,req=true "
                                     "name=frag,type=CephString,req=true",
				     asok_hook,
				     "De-fragment directory by path");
  assert(r == 0);
  r = admin_socket->register_command("dirfrag ls",
				     "dirfrag ls "
                                     "name=path,type=CephString,req=true",
				     asok_hook,
				     "List fragments in directory");
  assert(r == 0);
}

void MDSDaemon::clean_up_admin_socket()
{
  AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
  admin_socket->unregister_command("status");
  admin_socket->unregister_command("dump_ops_in_flight");
  admin_socket->unregister_command("ops");
  admin_socket->unregister_command("dump_blocked_ops");
  admin_socket->unregister_command("dump_historic_ops");
  admin_socket->unregister_command("scrub_path");
  admin_socket->unregister_command("flush_path");
  admin_socket->unregister_command("session evict");
  admin_socket->unregister_command("session ls");
  admin_socket->unregister_command("flush journal");
  admin_socket->unregister_command("force_readonly");
  delete asok_hook;
  asok_hook = NULL;
}

const char** MDSDaemon::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "mds_op_complaint_time", "mds_op_log_threshold",
    "mds_op_history_size", "mds_op_history_duration",
    "mds_enable_op_tracker",
    "mds_log_pause",
    // clog & admin clog
    "clog_to_monitors",
    "clog_to_syslog",
    "clog_to_syslog_facility",
    "clog_to_syslog_level",
    // StrayManager
    "mds_max_purge_ops",
    "mds_max_purge_ops_per_pg",
    "clog_to_graylog",
    "clog_to_graylog_host",
    "clog_to_graylog_port",
    "host",
    "fsid",
    NULL
  };
  return KEYS;
}

void MDSDaemon::handle_conf_change(const struct md_config_t *conf,
			     const std::set <std::string> &changed)
{
  // We may be called within mds_lock (via `tell`) or outwith the
  // lock (via admin socket `config set`), so handle either case.
  const bool initially_locked = mds_lock.is_locked_by_me();
  if (!initially_locked) {
    mds_lock.Lock();
  }

  if (changed.count("mds_op_complaint_time") ||
      changed.count("mds_op_log_threshold")) {
    if (mds_rank) {
      mds_rank->op_tracker.set_complaint_and_threshold(conf->mds_op_complaint_time,
                                             conf->mds_op_log_threshold);
    }
  }
  if (changed.count("mds_op_history_size") ||
      changed.count("mds_op_history_duration")) {
    if (mds_rank) {
      mds_rank->op_tracker.set_history_size_and_duration(conf->mds_op_history_size,
                                               conf->mds_op_history_duration);
    }
  }
  if (changed.count("mds_enable_op_tracker")) {
    if (mds_rank) {
      mds_rank->op_tracker.set_tracking(conf->mds_enable_op_tracker);
    }
  }
  if (changed.count("clog_to_monitors") ||
      changed.count("clog_to_syslog") ||
      changed.count("clog_to_syslog_level") ||
      changed.count("clog_to_syslog_facility") ||
      changed.count("clog_to_graylog") ||
      changed.count("clog_to_graylog_host") ||
      changed.count("clog_to_graylog_port") ||
      changed.count("host") ||
      changed.count("fsid")) {
    if (mds_rank) {
      mds_rank->update_log_config();
    }
  }

  if (!g_conf->mds_log_pause && changed.count("mds_log_pause")) {
    if (mds_rank) {
      mds_rank->mdlog->kick_submitter();
    }
  }

  if (mds_rank) {
    mds_rank->mdcache->handle_conf_change(conf, changed);
  }

  if (!initially_locked) {
    mds_lock.Unlock();
  }
}


int MDSDaemon::init(MDSMap::DaemonState wanted_state)
{
  dout(10) << sizeof(MDSCacheObject) << "\tMDSCacheObject" << dendl;
  dout(10) << sizeof(CInode) << "\tCInode" << dendl;
  dout(10) << sizeof(elist<void*>::item) << "\t elist<>::item   *7=" << 7*sizeof(elist<void*>::item) << dendl;
  dout(10) << sizeof(inode_t) << "\t inode_t " << dendl;
  dout(10) << sizeof(nest_info_t) << "\t  nest_info_t " << dendl;
  dout(10) << sizeof(frag_info_t) << "\t  frag_info_t " << dendl;
  dout(10) << sizeof(SimpleLock) << "\t SimpleLock   *5=" << 5*sizeof(SimpleLock) << dendl;
  dout(10) << sizeof(ScatterLock) << "\t ScatterLock  *3=" << 3*sizeof(ScatterLock) << dendl;
  dout(10) << sizeof(CDentry) << "\tCDentry" << dendl;
  dout(10) << sizeof(elist<void*>::item) << "\t elist<>::item" << dendl;
  dout(10) << sizeof(SimpleLock) << "\t SimpleLock" << dendl;
  dout(10) << sizeof(CDir) << "\tCDir " << dendl;
  dout(10) << sizeof(elist<void*>::item) << "\t elist<>::item   *2=" << 2*sizeof(elist<void*>::item) << dendl;
  dout(10) << sizeof(fnode_t) << "\t fnode_t " << dendl;
  dout(10) << sizeof(nest_info_t) << "\t  nest_info_t *2" << dendl;
  dout(10) << sizeof(frag_info_t) << "\t  frag_info_t *2" << dendl;
  dout(10) << sizeof(Capability) << "\tCapability " << dendl;
  dout(10) << sizeof(xlist<void*>::item) << "\t xlist<>::item   *2=" << 2*sizeof(xlist<void*>::item) << dendl;

  objecter->init();
  messenger->add_dispatcher_tail(objecter);

  messenger->add_dispatcher_tail(&beacon);
  messenger->add_dispatcher_tail(this);

  // get monmap
  monc->set_messenger(messenger);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD | CEPH_ENTITY_TYPE_MDS);
  monc->init();

  // tell monc about log_client so it will know about mon session resets
  monc->set_log_client(&log_client);

  int r = monc->authenticate();
  if (r < 0) {
    derr << "ERROR: failed to authenticate: " << cpp_strerror(-r) << dendl;
    mds_lock.Lock();
    suicide();
    mds_lock.Unlock();
    return r;
  }
  while (monc->wait_auth_rotating(30.0) < 0) {
    derr << "unable to obtain rotating service keys; retrying" << dendl;
  }

  objecter->start();

  mds_lock.Lock();
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE) {
    dout(4) << __func__ << ": terminated already, dropping out" << dendl;
    mds_lock.Unlock();
    return 0;
  }

  monc->sub_want("mdsmap", 0, 0);
  monc->renew_subs();

  mds_lock.Unlock();

  // verify that osds support tmap2omap
  while (true) {
    objecter->maybe_request_map();
    objecter->wait_for_osd_map();
    if (objecter->with_osdmap([&](const OSDMap& o) {
	  uint64_t osd_features = o.get_up_osd_features();
	  if (osd_features & CEPH_FEATURE_OSD_TMAP2OMAP)
	    return true;
	  if (o.get_num_up_osds() > 0) {
	    derr << "*** one or more OSDs do not support TMAP2OMAP; upgrade "
		 << "OSDs before starting MDS (or downgrade MDS) ***" << dendl;
	  } else {
	    derr << "*** no OSDs are up as of epoch " << o.get_epoch()
		 << ", waiting" << dendl;
	  }
	  return false;
	}))
      break;
    sleep(10);
  }

  // Set up admin socket before taking mds_lock, so that ordering
  // is consistent (later we take mds_lock within asok callbacks)
  set_up_admin_socket();
  g_conf->add_observer(this);
  mds_lock.Lock();
  if (beacon.get_want_state() == MDSMap::STATE_DNE) {
    suicide();  // we could do something more graceful here
  }

  timer.init();

  if (wanted_state==MDSMap::STATE_BOOT && g_conf->mds_standby_replay) {
    wanted_state = MDSMap::STATE_STANDBY_REPLAY;
  }

  // starting beacon.  this will induce an MDSMap from the monitor
  if (wanted_state==MDSMap::STATE_STANDBY_REPLAY ||
      wanted_state==MDSMap::STATE_ONESHOT_REPLAY) {
    g_conf->set_val_or_die("mds_standby_replay", "true");
    g_conf->apply_changes(NULL);
    if ( wanted_state == MDSMap::STATE_ONESHOT_REPLAY &&
        (g_conf->mds_standby_for_rank == -1) &&
        g_conf->mds_standby_for_name.empty()) {
      // uh-oh, must specify one or the other!
      dout(0) << "Specified oneshot replay mode but not an MDS!" << dendl;
      suicide();
    }
    standby_type = wanted_state;
    wanted_state = MDSMap::STATE_BOOT;
  }

  standby_for_rank = mds_rank_t(g_conf->mds_standby_for_rank);
  standby_for_name.assign(g_conf->mds_standby_for_name);

  if (standby_type == MDSMap::STATE_STANDBY_REPLAY &&
      standby_for_rank == -1) {
    if (standby_for_name.empty())
      standby_for_rank = MDSMap::MDS_STANDBY_ANY;
    else
      standby_for_rank = MDSMap::MDS_STANDBY_NAME;
  } else if (standby_type == MDSMap::STATE_NULL && !standby_for_name.empty())
    standby_for_rank = MDSMap::MDS_MATCHED_ACTIVE;

  if (wanted_state == MDSMap::STATE_NULL) {
    wanted_state = MDSMap::STATE_BOOT;
  }
  beacon.init(mdsmap, wanted_state,
    standby_for_rank, standby_for_name,
    fs_cluster_id_t(g_conf->mds_standby_for_fscid));
  messenger->set_myname(entity_name_t::MDS(MDS_RANK_NONE));

  // schedule tick
  reset_tick();
  g_conf->add_observer(this);
  mds_lock.Unlock();

  return 0;
}

void MDSDaemon::reset_tick()
{
  // cancel old
  if (tick_event) timer.cancel_event(tick_event);

  // schedule
  tick_event = new C_MDS_Tick(this);
  timer.add_event_after(g_conf->mds_tick_interval, tick_event);
}

void MDSDaemon::tick()
{
  tick_event = 0;

  // reschedule
  reset_tick();

  // Call through to subsystems' tick functions
  if (mds_rank) {
    mds_rank->tick();
  }
}

/* This function DOES put the passed message before returning*/
void MDSDaemon::handle_command(MCommand *m)
{
  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
  assert(session != NULL);

  int r = 0;
  cmdmap_t cmdmap;
  std::stringstream ss;
  std::string outs;
  bufferlist outbl;
  Context *run_after = NULL;


  if (!session->auth_caps.allow_all()) {
    dout(1) << __func__
      << ": received command from client without `tell` capability: "
      << m->get_connection()->peer_addr << dendl;

    ss << "permission denied";
    r = -EPERM;
  } else if (m->cmd.empty()) {
    ss << "no command given";
    outs = ss.str();
  } else if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    r = -EINVAL;
    outs = ss.str();
  } else {
    r = _handle_command(cmdmap, m->get_data(), &outbl, &outs, &run_after);
  }

  // If someone is using a closed session for sending commands (e.g.
  // the ceph CLI) then we should feel free to clean up this connection
  // as soon as we've sent them a response.
  const bool live_session = mds_rank &&
    mds_rank->sessionmap.get_session(session->info.inst.name) != nullptr
    && session->get_state_seq() > 0;

  if (!live_session) {
    // This session only existed to issue commands, so terminate it
    // as soon as we can.
    assert(session->is_closed());
    session->connection->mark_disposable();
    session->put();
  }

  MCommandReply *reply = new MCommandReply(r, outs);
  reply->set_tid(m->get_tid());
  reply->set_data(outbl);
  m->get_connection()->send_message(reply);

  if (run_after) {
    run_after->complete(0);
  }

  m->put();
}


struct MDSCommand {
  string cmdstring;
  string helpstring;
  string module;
  string perm;
  string availability;
} mds_commands[] = {

#define COMMAND(parsesig, helptext, module, perm, availability) \
  {parsesig, helptext, module, perm, availability},

COMMAND("injectargs " \
	"name=injected_args,type=CephString,n=N",
	"inject configuration arguments into running MDS",
	"mds", "*", "cli,rest")
COMMAND("exit",
	"Terminate this MDS",
	"mds", "*", "cli,rest")
COMMAND("respawn",
	"Restart this MDS",
	"mds", "*", "cli,rest")
COMMAND("session kill " \
        "name=session_id,type=CephInt",
	"End a client session",
	"mds", "*", "cli,rest")
COMMAND("cpu_profiler " \
	"name=arg,type=CephChoices,strings=status|flush",
	"run cpu profiling on daemon", "mds", "rw", "cli,rest")
COMMAND("session ls " \
	"name=filters,type=CephString,n=N,req=false",
	"List client sessions", "mds", "r", "cli,rest")
COMMAND("session evict " \
	"name=filters,type=CephString,n=N,req=false",
	"Evict client session(s)", "mds", "rw", "cli,rest")
COMMAND("damage ls",
	"List detected metadata damage", "mds", "r", "cli,rest")
COMMAND("damage rm name=damage_id,type=CephInt",
	"Remove a damage table entry", "mds", "rw", "cli,rest")
COMMAND("heap " \
	"name=heapcmd,type=CephChoices,strings=dump|start_profiler|stop_profiler|release|stats", \
	"show heap usage info (available only if compiled with tcmalloc)", \
	"mds", "*", "cli,rest")
};

// FIXME: reinstate issue_caps, try_eval,
//  *if* it makes sense to do so (or should these be admin socket things?)

/* This function DOES put the passed message before returning*/
void MDSDaemon::handle_command(MMonCommand *m)
{
  bufferlist outbl;
  _handle_command_legacy(m->cmd);
  m->put();
}

int MDSDaemon::_handle_command(
    const cmdmap_t &cmdmap,
    bufferlist const &inbl,
    bufferlist *outbl,
    std::string *outs,
    Context **run_later)
{
  assert(outbl != NULL);
  assert(outs != NULL);

  class SuicideLater : public Context
  {
    MDSDaemon *mds;

    public:
    explicit SuicideLater(MDSDaemon *mds_) : mds(mds_) {}
    void finish(int r) {
      // Wait a little to improve chances of caller getting
      // our response before seeing us disappear from mdsmap
      sleep(1);

      mds->suicide();
    }
  };


  class RespawnLater : public Context
  {
    MDSDaemon *mds;

    public:

    explicit RespawnLater(MDSDaemon *mds_) : mds(mds_) {}
    void finish(int r) {
      // Wait a little to improve chances of caller getting
      // our response before seeing us disappear from mdsmap
      sleep(1);

      mds->respawn();
    }
  };

  std::stringstream ds;
  std::stringstream ss;
  std::string prefix;
  cmd_getval(cct, cmdmap, "prefix", prefix);

  int r = 0;

  if (prefix == "get_command_descriptions") {
    int cmdnum = 0;
    JSONFormatter *f = new JSONFormatter();
    f->open_object_section("command_descriptions");
    for (MDSCommand *cp = mds_commands;
	 cp < &mds_commands[ARRAY_SIZE(mds_commands)]; cp++) {

      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmddesc_to_json(f, secname.str(), cp->cmdstring, cp->helpstring,
			   cp->module, cp->perm, cp->availability);
      cmdnum++;
    }
    f->close_section();	// command_descriptions

    f->flush(ds);
    delete f;
  } else if (prefix == "injectargs") {
    vector<string> argsvec;
    cmd_getval(cct, cmdmap, "injected_args", argsvec);

    if (argsvec.empty()) {
      r = -EINVAL;
      ss << "ignoring empty injectargs";
      goto out;
    }
    string args = argsvec.front();
    for (vector<string>::iterator a = ++argsvec.begin(); a != argsvec.end(); ++a)
      args += " " + *a;
    cct->_conf->injectargs(args, &ss);
  } else if (prefix == "exit") {
    // We will send response before executing
    ss << "Exiting...";
    *run_later = new SuicideLater(this);
  }
  else if (prefix == "respawn") {
    // We will send response before executing
    ss << "Respawning...";
    *run_later = new RespawnLater(this);
  } else if (prefix == "session kill") {
    if (mds_rank == NULL) {
      r = -EINVAL;
      ss << "MDS not active";
    }
    // FIXME harmonize `session kill` with admin socket session evict
    int64_t session_id = 0;
    bool got = cmd_getval(cct, cmdmap, "session_id", session_id);
    assert(got);
    const bool killed = mds_rank->kill_session(session_id);
    if (!killed) {
      r = -ENOENT;
      ss << "session '" << session_id << "' not found";
    }
  } else if (prefix == "heap") {
    if (!ceph_using_tcmalloc()) {
      r = -EOPNOTSUPP;
      ss << "could not issue heap profiler command -- not using tcmalloc!";
    } else {
      string heapcmd;
      cmd_getval(cct, cmdmap, "heapcmd", heapcmd);
      vector<string> heapcmd_vec;
      get_str_vec(heapcmd, heapcmd_vec);
      ceph_heap_profiler_handle_command(heapcmd_vec, ds);
    }
  } else if (prefix == "cpu_profiler") {
    string arg;
    cmd_getval(cct, cmdmap, "arg", arg);
    vector<string> argvec;
    get_str_vec(arg, argvec);
    cpu_profiler_handle_command(argvec, ds);
  } else {
    // Give MDSRank a shot at the command
    if (mds_rank) {
      bool handled = mds_rank->handle_command(cmdmap, inbl, &r, &ds, &ss);
      if (handled) {
        goto out;
      }
    }

    // Neither MDSDaemon nor MDSRank know this command
    std::ostringstream ss;
    ss << "unrecognized command! " << prefix;
    r = -EINVAL;
  }

out:
  *outs = ss.str();
  outbl->append(ds);
  return r;
}

/**
 * Legacy "mds tell", takes a simple array of args
 */
int MDSDaemon::_handle_command_legacy(std::vector<std::string> args)
{
  dout(10) << "handle_command args: " << args << dendl;
  if (args[0] == "injectargs") {
    if (args.size() < 2) {
      derr << "Ignoring empty injectargs!" << dendl;
    }
    else {
      std::ostringstream oss;
      mds_lock.Unlock();
      g_conf->injectargs(args[1], &oss);
      mds_lock.Lock();
      derr << "injectargs:" << dendl;
      derr << oss.str() << dendl;
    }
  }
  else if (args[0] == "exit") {
    suicide();
  }
  else if (args[0] == "respawn") {
    respawn();
  }
  else if (args[0] == "cpu_profiler") {
    ostringstream ss;
    cpu_profiler_handle_command(args, ss);
    clog->info() << ss.str();
  }
  else if (args[0] == "heap") {
    if (!ceph_using_tcmalloc())
      clog->info() << "tcmalloc not enabled, can't use heap profiler commands\n";
    else {
      ostringstream ss;
      vector<std::string> cmdargs;
      cmdargs.insert(cmdargs.begin(), args.begin()+1, args.end());
      ceph_heap_profiler_handle_command(cmdargs, ss);
      clog->info() << ss.str();
    }
  } else {
    if (!(mds_rank && mds_rank->handle_command_legacy(args))) {
      dout(0) << "unrecognized command! " << args << dendl;
    }
  }

  return 0;
}

/* This function deletes the passed message before returning. */

void MDSDaemon::handle_mds_map(MMDSMap *m)
{
  version_t epoch = m->get_epoch();
  dout(5) << "handle_mds_map epoch " << epoch << " from " << m->get_source() << dendl;

  // is it new?
  if (epoch <= mdsmap->get_epoch()) {
    dout(5) << " old map epoch " << epoch << " <= " << mdsmap->get_epoch()
	    << ", discarding" << dendl;
    m->put();
    return;
  }

  entity_addr_t addr;

  // keep old map, for a moment
  MDSMap *oldmap = mdsmap;

  // decode and process
  mdsmap = new MDSMap;
  mdsmap->decode(m->get_encoded());
  const MDSMap::DaemonState new_state = mdsmap->get_state_gid(mds_gid_t(monc->get_global_id()));
  const int incarnation = mdsmap->get_inc_gid(mds_gid_t(monc->get_global_id()));

  monc->sub_got("mdsmap", mdsmap->get_epoch());

  // Calculate my effective rank (either my owned rank or my
  // standby_for_rank if in standby replay)
  mds_rank_t whoami = mdsmap->get_rank_gid(mds_gid_t(monc->get_global_id()));

  // verify compatset
  CompatSet mdsmap_compat(get_mdsmap_compat_set_all());
  dout(10) << "     my compat " << mdsmap_compat << dendl;
  dout(10) << " mdsmap compat " << mdsmap->compat << dendl;
  if (!mdsmap_compat.writeable(mdsmap->compat)) {
    dout(0) << "handle_mds_map mdsmap compatset " << mdsmap->compat
	    << " not writeable with daemon features " << mdsmap_compat
	    << ", killing myself" << dendl;
    suicide();
    goto out;
  }

  // mark down any failed peers
  for (map<mds_gid_t,MDSMap::mds_info_t>::const_iterator p = oldmap->get_mds_info().begin();
       p != oldmap->get_mds_info().end();
       ++p) {
    if (mdsmap->get_mds_info().count(p->first) == 0) {
      dout(10) << " peer mds gid " << p->first << " removed from map" << dendl;
      messenger->mark_down(p->second.addr);
    }
  }

  // If I was put into standby replay, but I am configured for a different standby
  // type, ignore the map's state and request my standby type (only used
  // for oneshot replay?)
  if (new_state == MDSMap::STATE_STANDBY_REPLAY) {
    if (standby_type != MDSMap::STATE_NULL && standby_type != MDSMap::STATE_STANDBY_REPLAY) {
      beacon.set_want_state(mdsmap, standby_type);
      beacon.send();
      goto out;
    }
  }

  if (whoami == MDS_RANK_NONE && (
      new_state == MDSMap::STATE_STANDBY_REPLAY || new_state == MDSMap::STATE_ONESHOT_REPLAY)) {
    whoami = mdsmap->get_mds_info_gid(mds_gid_t(monc->get_global_id())).standby_for_rank;
  }

  // see who i am
  addr = messenger->get_myaddr();
  dout(10) << "map says i am " << addr << " mds." << whoami << "." << incarnation
	   << " state " << ceph_mds_state_name(new_state) << dendl;

  if (whoami == MDS_RANK_NONE) {
    if (mds_rank != NULL) {
      // We have entered a rank-holding state, we shouldn't be back
      // here!
      if (g_conf->mds_enforce_unique_name) {
        if (mds_gid_t existing = mdsmap->find_mds_gid_by_name(name)) {
          const MDSMap::mds_info_t& i = mdsmap->get_info_gid(existing);
          if (i.global_id > monc->get_global_id()) {
            dout(1) << "handle_mds_map i (" << addr
                    << ") dne in the mdsmap, new instance has larger gid " << i.global_id
                    << ", suicide" << dendl;
            // Call suicide() rather than respawn() because if someone else
            // has taken our ID, we don't want to keep restarting and
            // fighting them for the ID.
            suicide();
            return;
          }
        }
      }

      dout(1) << "handle_mds_map i (" << addr
          << ") dne in the mdsmap, respawning myself" << dendl;
      respawn();
    }
    // MDSRank not active: process the map here to see if we have
    // been assigned a rank.
    dout(10) <<  __func__ << ": handling map in rankless mode" << dendl;
    _handle_mds_map(oldmap);
  } else {

    // Did we already hold a different rank?  MDSMonitor shouldn't try
    // to change that out from under me!
    if (mds_rank && whoami != mds_rank->get_nodeid()) {
      derr << "Invalid rank transition " << mds_rank->get_nodeid() << "->"
           << whoami << dendl;
      respawn();
    }

    // Did I previously not hold a rank?  Initialize!
    if (mds_rank == NULL) {
      mds_rank = new MDSRankDispatcher(whoami, mds_lock, clog,
          timer, beacon, mdsmap, messenger, monc, objecter,
          new C_VoidFn(this, &MDSDaemon::respawn),
          new C_VoidFn(this, &MDSDaemon::suicide));
      dout(10) <<  __func__ << ": initializing MDS rank "
               << mds_rank->get_nodeid() << dendl;
      mds_rank->init();
    }

    // MDSRank is active: let him process the map, we have no say.
    dout(10) <<  __func__ << ": handling map as rank "
             << mds_rank->get_nodeid() << dendl;
    mds_rank->handle_mds_map(m, oldmap);
  }

out:
  beacon.notify_mdsmap(mdsmap);
  m->put();
  delete oldmap;
}

void MDSDaemon::_handle_mds_map(MDSMap *oldmap)
{
  MDSMap::DaemonState new_state = mdsmap->get_state_gid(mds_gid_t(monc->get_global_id()));

  // Normal rankless case, we're marked as standby
  if (new_state == MDSMap::STATE_STANDBY) {
    beacon.set_want_state(mdsmap, new_state);
    dout(1) << "handle_mds_map standby" << dendl;

    if (standby_type != MDSMap::STATE_NULL) {// we want to be in standby_replay or oneshot_replay!
      beacon.set_want_state(mdsmap, standby_type);
      beacon.send();
    }
    return;
  }

  // Case where we thought we were standby, but MDSMap disagrees
  if (beacon.get_want_state() == MDSMap::STATE_STANDBY) {
    dout(10) << "dropped out of mdsmap, try to re-add myself" << dendl;
    new_state = MDSMap::STATE_BOOT;
    beacon.set_want_state(mdsmap, new_state);
    return;
  }

  // Case where we have sent a boot beacon that isn't reflected yet
  if (beacon.get_want_state() == MDSMap::STATE_BOOT) {
    dout(10) << "not in map yet" << dendl;
  }
}

void MDSDaemon::handle_signal(int signum)
{
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** got signal " << sig_str(signum) << " ***" << dendl;
  {
    Mutex::Locker l(mds_lock);
    if (stopping) {
      return;
    }
    suicide();
  }
}

void MDSDaemon::suicide()
{
  assert(mds_lock.is_locked());

  dout(1) << "suicide.  wanted state "
          << ceph_mds_state_name(beacon.get_want_state()) << dendl;

  if (tick_event) {
    timer.cancel_event(tick_event);
    tick_event = 0;
  }

  //because add_observer is called after set_up_admin_socket
  //so we can use asok_hook to avoid assert in the remove_observer
  if (asok_hook != NULL)
    g_conf->remove_observer(this);

  clean_up_admin_socket();

  // Inform MDS we are going away, then shut down beacon
  beacon.set_want_state(mdsmap, MDSMap::STATE_DNE);
  if (!mdsmap->is_dne_gid(mds_gid_t(monc->get_global_id()))) {
    // Notify the MDSMonitor that we're dying, so that it doesn't have to
    // wait for us to go laggy.  Only do this if we're actually in the
    // MDSMap, because otherwise the MDSMonitor will drop our message.
    beacon.send_and_wait(1);
  }
  beacon.shutdown();

  if (mds_rank) {
    mds_rank->shutdown();
  } else {

    if (objecter->initialized.read()) {
      objecter->shutdown();
    }
    timer.shutdown();

    monc->shutdown();
    messenger->shutdown();
  }
}

void MDSDaemon::respawn()
{
  dout(1) << "respawn" << dendl;

  char *new_argv[orig_argc+1];
  dout(1) << " e: '" << orig_argv[0] << "'" << dendl;
  for (int i=0; i<orig_argc; i++) {
    new_argv[i] = (char *)orig_argv[i];
    dout(1) << " " << i << ": '" << orig_argv[i] << "'" << dendl;
  }
  new_argv[orig_argc] = NULL;

  /* Determine the path to our executable, try to read
   * linux-specific /proc/ path first */
  char exe_path[PATH_MAX];
  ssize_t exe_path_bytes = readlink("/proc/self/exe", exe_path,
				    sizeof(exe_path) - 1);
  if (exe_path_bytes < 0) {
    /* Print CWD for the user's interest */
    char buf[PATH_MAX];
    char *cwd = getcwd(buf, sizeof(buf));
    assert(cwd);
    dout(1) << " cwd " << cwd << dendl;

    /* Fall back to a best-effort: just running in our CWD */
    strncpy(exe_path, orig_argv[0], sizeof(exe_path) - 1);
  } else {
    exe_path[exe_path_bytes] = '\0';
  }

  dout(1) << " exe_path " << exe_path << dendl;

  unblock_all_signals(NULL);
  execv(exe_path, new_argv);

  dout(0) << "respawn execv " << orig_argv[0]
	  << " failed with " << cpp_strerror(errno) << dendl;

  // We have to assert out here, because suicide() returns, and callers
  // to respawn expect it never to return.
  assert(0);
}



bool MDSDaemon::ms_dispatch(Message *m)
{
  Mutex::Locker l(mds_lock);
  if (stopping) {
    return false;
  }

  // Drop out early if shutting down
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE) {
    dout(10) << " stopping, discarding " << *m << dendl;
    m->put();
    return true;
  }

  // First see if it's a daemon message
  const bool handled_core = handle_core_message(m);
  if (handled_core) {
    return true;
  }

  // Not core, try it as a rank message
  if (mds_rank) {
    return mds_rank->ms_dispatch(m);
  } else {
    return false;
  }
}

bool MDSDaemon::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new)
{
  dout(10) << "MDSDaemon::ms_get_authorizer type=" << ceph_entity_type_name(dest_type) << dendl;

  /* monitor authorization is being handled on different layer */
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}


/*
 * high priority messages we always process
 */
bool MDSDaemon::handle_core_message(Message *m)
{
  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);
    m->put();
    break;

    // MDS
  case CEPH_MSG_MDS_MAP:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_MDS);
    handle_mds_map(static_cast<MMDSMap*>(m));
    break;

    // misc
  case MSG_MON_COMMAND:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);
    handle_command(static_cast<MMonCommand*>(m));
    break;

    // OSD
  case MSG_COMMAND:
    handle_command(static_cast<MCommand*>(m));
    break;
  case CEPH_MSG_OSD_MAP:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);

    if (mds_rank) {
      mds_rank->handle_osd_map();
    }
    break;

  default:
    return false;
  }
  return true;
}

void MDSDaemon::ms_handle_connect(Connection *con)
{
}

bool MDSDaemon::ms_handle_reset(Connection *con)
{
  if (con->get_peer_type() != CEPH_ENTITY_TYPE_CLIENT)
    return false;

  Mutex::Locker l(mds_lock);
  if (stopping) {
    return false;
  }
  dout(5) << "ms_handle_reset on " << con->get_peer_addr() << dendl;
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE)
    return false;

  Session *session = static_cast<Session *>(con->get_priv());
  if (session) {
    if (session->is_closed()) {
      dout(3) << "ms_handle_reset closing connection for session " << session->info.inst << dendl;
      con->mark_down();
      con->set_priv(NULL);
    }
    session->put();
  } else {
    con->mark_down();
  }
  return false;
}


void MDSDaemon::ms_handle_remote_reset(Connection *con)
{
  if (con->get_peer_type() != CEPH_ENTITY_TYPE_CLIENT)
    return;

  Mutex::Locker l(mds_lock);
  if (stopping) {
    return;
  }

  dout(5) << "ms_handle_remote_reset on " << con->get_peer_addr() << dendl;
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE)
    return;

  Session *session = static_cast<Session *>(con->get_priv());
  if (session) {
    if (session->is_closed()) {
      dout(3) << "ms_handle_remote_reset closing connection for session " << session->info.inst << dendl;
      con->mark_down();
      con->set_priv(NULL);
    }
    session->put();
  }
}

bool MDSDaemon::ms_verify_authorizer(Connection *con, int peer_type,
			       int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			       bool& is_valid, CryptoKey& session_key)
{
  Mutex::Locker l(mds_lock);
  if (stopping) {
    return false;
  }
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE)
    return false;

  AuthAuthorizeHandler *authorize_handler = 0;
  switch (peer_type) {
  case CEPH_ENTITY_TYPE_MDS:
    authorize_handler = authorize_handler_cluster_registry->get_handler(protocol);
    break;
  default:
    authorize_handler = authorize_handler_service_registry->get_handler(protocol);
  }
  if (!authorize_handler) {
    dout(0) << "No AuthAuthorizeHandler found for protocol " << protocol << dendl;
    is_valid = false;
    return true;
  }

  AuthCapsInfo caps_info;
  EntityName name;
  uint64_t global_id;

  is_valid = authorize_handler->verify_authorizer(cct, monc->rotating_secrets,
						  authorizer_data, authorizer_reply, name, global_id, caps_info, session_key);

  if (is_valid) {
    entity_name_t n(con->get_peer_type(), global_id);

    // We allow connections and assign Session instances to connections
    // even if we have not been assigned a rank, because clients with
    // "allow *" are allowed to connect and do 'tell' operations before
    // we have a rank.
    Session *s = NULL;
    if (mds_rank) {
      // If we do hold a rank, see if this is an existing client establishing
      // a new connection, rather than a new client
      s = mds_rank->sessionmap.get_session(n);
    }

    // Wire up a Session* to this connection
    // It doesn't go into a SessionMap instance until it sends an explicit
    // request to open a session (initial state of Session is `closed`)
    if (!s) {
      s = new Session;
      s->info.auth_name = name;
      s->info.inst.addr = con->get_peer_addr();
      s->info.inst.name = n;
      dout(10) << " new session " << s << " for " << s->info.inst << " con " << con << dendl;
      con->set_priv(s);
      s->connection = con;
    } else {
      dout(10) << " existing session " << s << " for " << s->info.inst << " existing con " << s->connection
	       << ", new/authorizing con " << con << dendl;
      con->set_priv(s->get());



      // Wait until we fully accept the connection before setting
      // s->connection.  In particular, if there are multiple incoming
      // connection attempts, they will all get their authorizer
      // validated, but some of them may "lose the race" and get
      // dropped.  We only want to consider the winner(s).  See
      // ms_handle_accept().  This is important for Sessions we replay
      // from the journal on recovery that don't have established
      // messenger state; we want the con from only the winning
      // connect attempt(s).  (Normal reconnects that don't follow MDS
      // recovery are reconnected to the existing con by the
      // messenger.)
    }

    if (caps_info.allow_all) {
      // Flag for auth providers that don't provide cap strings
      s->auth_caps.set_allow_all();
    }

    bufferlist::iterator p = caps_info.caps.begin();
    string auth_cap_str;
    try {
      ::decode(auth_cap_str, p);

      dout(10) << __func__ << ": parsing auth_cap_str='" << auth_cap_str << "'" << dendl;
      std::ostringstream errstr;
      if (!s->auth_caps.parse(g_ceph_context, auth_cap_str, &errstr)) {
        dout(1) << __func__ << ": auth cap parse error: " << errstr.str()
		<< " parsing '" << auth_cap_str << "'" << dendl;
	clog->warn() << name << " mds cap '" << auth_cap_str
		     << "' does not parse: " << errstr.str() << "\n";
      }
    } catch (buffer::error& e) {
      // Assume legacy auth, defaults to:
      //  * permit all filesystem ops
      //  * permit no `tell` ops
      dout(1) << __func__ << ": cannot decode auth caps bl of length " << caps_info.caps.length() << dendl;
    }
  }

  return true;  // we made a decision (see is_valid)
}


void MDSDaemon::ms_handle_accept(Connection *con)
{
  Mutex::Locker l(mds_lock);
  if (stopping) {
    return;
  }

  Session *s = static_cast<Session *>(con->get_priv());
  dout(10) << "ms_handle_accept " << con->get_peer_addr() << " con " << con << " session " << s << dendl;
  if (s) {
    if (s->connection != con) {
      dout(10) << " session connection " << s->connection << " -> " << con << dendl;
      s->connection = con;

      // send out any queued messages
      while (!s->preopen_out_queue.empty()) {
	con->send_message(s->preopen_out_queue.front());
	s->preopen_out_queue.pop_front();
      }
    }
    s->put();
  }
}

bool MDSDaemon::is_clean_shutdown()
{
  if (mds_rank) {
    return mds_rank->is_stopped();
  } else {
    return true;
  }
}
