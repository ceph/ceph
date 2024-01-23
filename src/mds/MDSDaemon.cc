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

#include "include/compat.h"
#include "include/types.h"
#include "include/str_list.h"

#include "common/Clock.h"
#include "common/HeartbeatMap.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/entity_name.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/signal.h"
#include "common/version.h"

#include "global/signal_handler.h"

#include "msg/Messenger.h"
#include "mon/MonClient.h"

#include "osdc/Objecter.h"

#include "MDSMap.h"

#include "MDSDaemon.h"
#include "Server.h"
#include "Locker.h"

#include "SnapServer.h"
#include "SnapClient.h"

#include "events/ESession.h"
#include "events/ESubtreeMap.h"

#include "auth/AuthAuthorizeHandler.h"
#include "auth/RotatingKeyRing.h"
#include "auth/KeyRing.h"

#include "perfglue/cpu_profiler.h"
#include "perfglue/heap_profiler.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << name << ' '

using std::string;
using std::vector;
using TOPNSPC::common::cmd_getval;

// cons/des
MDSDaemon::MDSDaemon(std::string_view n, Messenger *m, MonClient *mc,
		     boost::asio::io_context& ioctx) :
  Dispatcher(m->cct),
  timer(m->cct, mds_lock),
  gss_ktfile_client(m->cct->_conf.get_val<std::string>("gss_ktab_client_file")),
  beacon(m->cct, mc, n),
  name(n),
  messenger(m),
  monc(mc),
  ioctx(ioctx),
  mgrc(m->cct, m, &mc->monmap),
  log_client(m->cct, messenger, &mc->monmap, LogClient::NO_FLAGS),
  starttime(mono_clock::now())
{
  orig_argc = 0;
  orig_argv = NULL;

  clog = log_client.create_channel();
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

  mdsmap.reset(new MDSMap);
}

MDSDaemon::~MDSDaemon() {
  std::lock_guard lock(mds_lock);

  delete mds_rank;
  mds_rank = NULL;
}

class MDSSocketHook : public AdminSocketHook {
  MDSDaemon *mds;
public:
  explicit MDSSocketHook(MDSDaemon *m) : mds(m) {}
  int call(
    std::string_view command,
    const cmdmap_t& cmdmap,
    const bufferlist&,
    Formatter *f,
    std::ostream& errss,
    ceph::buffer::list& out) override {
    ceph_abort("should go to call_async");
  }
  void call_async(
    std::string_view command,
    const cmdmap_t& cmdmap,
    Formatter *f,
    const bufferlist& inbl,
    std::function<void(int,const std::string&,bufferlist&)> on_finish) override {
    mds->asok_command(command, cmdmap, f, inbl, on_finish);
  }
};

void MDSDaemon::asok_command(
  std::string_view command,
  const cmdmap_t& cmdmap,
  Formatter *f,
  const bufferlist& inbl,
  std::function<void(int,const std::string&,bufferlist&)> on_finish)
{
  dout(1) << "asok_command: " << command << " " << cmdmap
	  << " (starting...)" << dendl;

  int r = -CEPHFS_ENOSYS;
  bufferlist outbl;
  CachedStackStringStream css;
  auto& ss = *css;
  if (command == "status") {
    dump_status(f);
    r = 0;
  } else if (command == "exit") {
    outbl.append("Exiting...\n");
    r = 0;
    std::thread t([this](){
		    // Wait a little to improve chances of caller getting
		    // our response before seeing us disappear from mdsmap
		    sleep(1);
		    std::lock_guard l(mds_lock);
                    derr << "Exiting due to admin socket command" << dendl;
		    suicide();
		  });
    t.detach();
  } else if (command == "respawn") {
    outbl.append("Respawning...\n");
    r = 0;
    std::thread t([this](){
		    // Wait a little to improve chances of caller getting
		    // our response before seeing us disappear from mdsmap
		    sleep(1);
		    std::lock_guard l(mds_lock);
		    respawn();
		  });
    t.detach();
  } else if (command == "heap") {
    if (!ceph_using_tcmalloc()) {
      ss << "not using tcmalloc";
      r = -CEPHFS_EOPNOTSUPP;
    } else {
      string heapcmd;
      cmd_getval(cmdmap, "heapcmd", heapcmd);
      vector<string> heapcmd_vec;
      get_str_vec(heapcmd, heapcmd_vec);
      string value;
      if (cmd_getval(cmdmap, "value", value)) {
	heapcmd_vec.push_back(value);
      }
      std::stringstream outss;
      ceph_heap_profiler_handle_command(heapcmd_vec, outss);
      outbl.append(outss);
      r = 0;
    }
  } else if (command == "cpu_profiler") {
    string arg;
    cmd_getval(cmdmap, "arg", arg);
    vector<string> argvec;
    get_str_vec(arg, argvec);
    cpu_profiler_handle_command(argvec, ss);
    r = 0;
  } else {
    if (mds_rank == NULL) {
      dout(1) << "Can't run that command on an inactive MDS!" << dendl;
      f->dump_string("error", "mds_not_active");
    } else {
      try {
	mds_rank->handle_asok_command(command, cmdmap, f, inbl, on_finish);
	return;
      } catch (const TOPNSPC::common::bad_cmd_get& e) {
	ss << e.what();
	r = -CEPHFS_EINVAL;
      }
    }
  }
  on_finish(r, ss.str(), outbl);
}

void MDSDaemon::dump_status(Formatter *f)
{
  f->open_object_section("status");
  f->dump_stream("cluster_fsid") << monc->get_fsid();
  if (mds_rank) {
    f->dump_int("whoami", mds_rank->get_nodeid());
  } else {
    f->dump_int("whoami", MDS_RANK_NONE);
  }

  f->dump_int("id", monc->get_global_id());
  f->dump_string("want_state", ceph_mds_state_name(beacon.get_want_state()));
  f->dump_string("state", ceph_mds_state_name(mdsmap->get_state_gid(mds_gid_t(
	    monc->get_global_id()))));
  if (mds_rank) {
    std::lock_guard l(mds_lock);
    mds_rank->dump_status(f);
  }

  f->dump_unsigned("mdsmap_epoch", mdsmap->get_epoch());
  if (mds_rank) {
    f->dump_unsigned("osdmap_epoch", mds_rank->get_osd_epoch());
    f->dump_unsigned("osdmap_epoch_barrier", mds_rank->get_osd_epoch_barrier());
  } else {
    f->dump_unsigned("osdmap_epoch", 0);
    f->dump_unsigned("osdmap_epoch_barrier", 0);
  }

  f->dump_float("uptime", get_uptime().count());

  f->close_section(); // status
}

void MDSDaemon::set_up_admin_socket()
{
  int r;
  AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
  ceph_assert(asok_hook == nullptr);
  asok_hook = new MDSSocketHook(this);
  r = admin_socket->register_command("status", asok_hook,
				     "high-level status of MDS");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dump_ops_in_flight", asok_hook,
				     "show the ops currently in flight");
  ceph_assert(r == 0);
  r = admin_socket->register_command("ops", asok_hook,
				     "show the ops currently in flight");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dump_blocked_ops",
      asok_hook,
      "show the blocked ops currently in flight");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dump_historic_ops",
				     asok_hook,
				     "show recent ops");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dump_historic_ops_by_duration",
				     asok_hook,
				     "show recent ops, sorted by op duration");
  ceph_assert(r == 0);
  r = admin_socket->register_command("scrub_path name=path,type=CephString "
				     "name=scrubops,type=CephChoices,"
				     "strings=force|recursive|repair,n=N,req=false "
				     "name=tag,type=CephString,req=false",
                                     asok_hook,
                                     "scrub an inode and output results");
  ceph_assert(r == 0);
  r = admin_socket->register_command("scrub start "
				     "name=path,type=CephString "
				     "name=scrubops,type=CephChoices,strings=force|recursive|repair|scrub_mdsdir,n=N,req=false "
				     "name=tag,type=CephString,req=false",
				     asok_hook,
				     "scrub and inode and output results");
  ceph_assert(r == 0);
  r = admin_socket->register_command("scrub abort",
                                     asok_hook,
                                     "Abort in progress scrub operations(s)");
  ceph_assert(r == 0);
  r = admin_socket->register_command("scrub pause",
                                     asok_hook,
                                     "Pause in progress scrub operations(s)");
  ceph_assert(r == 0);
  r = admin_socket->register_command("scrub resume",
                                     asok_hook,
                                     "Resume paused scrub operations(s)");
  ceph_assert(r == 0);
  r = admin_socket->register_command("scrub status",
                                     asok_hook,
                                     "Status of scrub operations(s)");
  ceph_assert(r == 0);
  r = admin_socket->register_command("tag path name=path,type=CephString"
                                     " name=tag,type=CephString",
                                     asok_hook,
                                     "Apply scrub tag recursively");
   ceph_assert(r == 0);
  r = admin_socket->register_command("flush_path name=path,type=CephString",
                                     asok_hook,
                                     "flush an inode (and its dirfrags)");
  ceph_assert(r == 0);
  r = admin_socket->register_command("export dir "
                                     "name=path,type=CephString "
                                     "name=rank,type=CephInt",
                                     asok_hook,
                                     "migrate a subtree to named MDS");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dump cache "
				     "name=path,type=CephString,req=false "
				     "name=timeout,type=CephInt,range=0,req=false",
                                     asok_hook,
                                     "dump metadata cache (optionally to a file)");
  ceph_assert(r == 0);
  r = admin_socket->register_command("cache drop "
				     "name=timeout,type=CephInt,range=0,req=false",
				     asok_hook,
				     "trim cache and optionally request client to release all caps and flush the journal");
  ceph_assert(r == 0);
  r = admin_socket->register_command("cache status",
                                     asok_hook,
                                     "show cache status");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dump tree "
				     "name=root,type=CephString,req=true "
				     "name=depth,type=CephInt,req=false ",
				     asok_hook,
				     "dump metadata cache for subtree");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dump loads",
                                     asok_hook,
                                     "dump metadata loads");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dump snaps name=server,type=CephChoices,strings=--server,req=false",
                                     asok_hook,
                                     "dump snapshots");
  ceph_assert(r == 0);
  r = admin_socket->register_command("session ls "
				     "name=cap_dump,type=CephBool,req=false "
		                     "name=filters,type=CephString,n=N,req=false ",
				     asok_hook,
				     "List client sessions based on a filter");
  ceph_assert(r == 0);
  r = admin_socket->register_command("client ls "
				     "name=cap_dump,type=CephBool,req=false "
		                     "name=filters,type=CephString,n=N,req=false ",
				     asok_hook,
				     "List client sessions based on a filter");
  ceph_assert(r == 0);
  r = admin_socket->register_command("session evict name=filters,type=CephString,n=N,req=false",
				     asok_hook,
				     "Evict client session(s) based on a filter");
  ceph_assert(r == 0);
  r = admin_socket->register_command("client evict name=filters,type=CephString,n=N,req=false",
				     asok_hook,
				     "Evict client session(s) based on a filter");
  ceph_assert(r == 0);
  r = admin_socket->register_command("session kill name=client_id,type=CephString",
				     asok_hook,
				     "Evict a client session by id");
  ceph_assert(r == 0);
  r = admin_socket->register_command("session config "
				     "name=client_id,type=CephInt,req=true "
				     "name=option,type=CephString,req=true "
				     "name=value,type=CephString,req=false ",
				     asok_hook,
				     "Config a CephFS client session");
  ceph_assert(r == 0);
  r = admin_socket->register_command("client config "
				     "name=client_id,type=CephInt,req=true "
				     "name=option,type=CephString,req=true "
				     "name=value,type=CephString,req=false ",
				     asok_hook,
				     "Config a CephFS client session");
  ceph_assert(r == 0);
  r = admin_socket->register_command("damage ls",
				     asok_hook,
				     "List detected metadata damage");
  ceph_assert(r == 0);
  r = admin_socket->register_command("damage rm "
				     "name=damage_id,type=CephInt",
				     asok_hook,
				     "Remove a damage table entry");
  ceph_assert(r == 0);
  r = admin_socket->register_command("osdmap barrier name=target_epoch,type=CephInt",
				     asok_hook,
				     "Wait until the MDS has this OSD map epoch");
  ceph_assert(r == 0);
  r = admin_socket->register_command("flush journal",
				     asok_hook,
				     "Flush the journal to the backing store");
  ceph_assert(r == 0);
  r = admin_socket->register_command("force_readonly",
				     asok_hook,
				     "Force MDS to read-only mode");
  ceph_assert(r == 0);
  r = admin_socket->register_command("get subtrees",
				     asok_hook,
				     "Return the subtree map");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dirfrag split "
                                     "name=path,type=CephString,req=true "
                                     "name=frag,type=CephString,req=true "
                                     "name=bits,type=CephInt,req=true ",
				     asok_hook,
				     "Fragment directory by path");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dirfrag merge "
                                     "name=path,type=CephString,req=true "
                                     "name=frag,type=CephString,req=true",
				     asok_hook,
				     "De-fragment directory by path");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dirfrag ls "
                                     "name=path,type=CephString,req=true",
				     asok_hook,
				     "List fragments in directory");
  ceph_assert(r == 0);
  r = admin_socket->register_command("openfiles ls",
                                     asok_hook,
                                     "List the opening files and their caps");
  ceph_assert(r == 0);
  r = admin_socket->register_command("dump inode "
                                     "name=number,type=CephInt,req=true",
				     asok_hook,
				     "dump inode by inode number");
  ceph_assert(r == 0);
  r = admin_socket->register_command("exit",
				     asok_hook,
				     "Terminate this MDS");
  r = admin_socket->register_command("respawn",
				     asok_hook,
				     "Respawn this MDS");
  ceph_assert(r == 0);
  r = admin_socket->register_command(
    "heap " \
    "name=heapcmd,type=CephChoices,strings="				\
    "dump|start_profiler|stop_profiler|release|get_release_rate|set_release_rate|stats " \
    "name=value,type=CephString,req=false",
    asok_hook,
    "show heap usage info (available only if compiled with tcmalloc)");
  ceph_assert(r == 0);
  r = admin_socket->register_command(
    "cpu_profiler " \
    "name=arg,type=CephChoices,strings=status|flush",
    asok_hook,
    "run cpu profiling on daemon");
  ceph_assert(r == 0);
}

void MDSDaemon::clean_up_admin_socket()
{
  g_ceph_context->get_admin_socket()->unregister_commands(asok_hook);
  delete asok_hook;
  asok_hook = NULL;
}

int MDSDaemon::init()
{
#ifdef _WIN32
  // Some file related flags and types are stubbed on Windows. In order to avoid
  // incorrect behavior, we're going to prevent the MDS from running on Windows
  // until those limitations are addressed. MDS clients, however, are allowed
  // to run on Windows.
  derr << "The Ceph MDS does not support running on Windows at the moment."
       << dendl;
  return -CEPHFS_ENOSYS;
#endif // _WIN32

  dout(10) << "Dumping misc struct sizes:" << dendl;
  dout(10) << sizeof(MDSCacheObject) << "\tMDSCacheObject" << dendl;
  dout(10) << sizeof(CInode) << "\tCInode" << dendl;
  dout(10) << sizeof(elist<void*>::item) << "\telist<>::item" << dendl;
  dout(10) << sizeof(CInode::mempool_inode) << "\tinode" << dendl;
  dout(10) << sizeof(CInode::mempool_old_inode) << "\told_inode" << dendl;
  dout(10) << sizeof(nest_info_t) << "\tnest_info_t" << dendl;
  dout(10) << sizeof(frag_info_t) << "\tfrag_info_t" << dendl;
  dout(10) << sizeof(SimpleLock) << "\tSimpleLock" << dendl;
  dout(10) << sizeof(ScatterLock) << "\tScatterLock" << dendl;
  dout(10) << sizeof(CDentry) << "\tCDentry" << dendl;
  dout(10) << sizeof(elist<void*>::item) << "\telist<>::item" << dendl;
  dout(10) << sizeof(SimpleLock) << "\tSimpleLock" << dendl;
  dout(10) << sizeof(CDir) << "\tCDir" << dendl;
  dout(10) << sizeof(elist<void*>::item) << "\telist<>::item" << dendl;
  dout(10) << sizeof(fnode_t) << "\tfnode_t" << dendl;
  dout(10) << sizeof(nest_info_t) << "\tnest_info_t" << dendl;
  dout(10) << sizeof(frag_info_t) << "\tfrag_info_t" << dendl;
  dout(10) << sizeof(Capability) << "\tCapability" << dendl;
  dout(10) << sizeof(xlist<void*>::item) << "\txlist<>::item" << dendl;

  messenger->add_dispatcher_tail(&beacon);
  messenger->add_dispatcher_tail(this);

  // init monc
  monc->set_messenger(messenger);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD |
                      CEPH_ENTITY_TYPE_MDS | CEPH_ENTITY_TYPE_MGR);
  int r = 0;
  r = monc->init();
  if (r < 0) {
    derr << "ERROR: failed to init monc: " << cpp_strerror(-r) << dendl;
    mds_lock.lock();
    suicide();
    mds_lock.unlock();
    return r;
  }

  messenger->set_auth_client(monc);
  messenger->set_auth_server(monc);
  monc->set_handle_authentication_dispatcher(this);

  // tell monc about log_client so it will know about mon session resets
  monc->set_log_client(&log_client);

  r = monc->authenticate();
  if (r < 0) {
    derr << "ERROR: failed to authenticate: " << cpp_strerror(-r) << dendl;
    mds_lock.lock();
    suicide();
    mds_lock.unlock();
    return r;
  }

  int rotating_auth_attempts = 0;
  auto rotating_auth_timeout =
    g_conf().get_val<int64_t>("rotating_keys_bootstrap_timeout");
  while (monc->wait_auth_rotating(rotating_auth_timeout) < 0) {
    if (++rotating_auth_attempts <= g_conf()->max_rotating_auth_attempts) {
      derr << "unable to obtain rotating service keys; retrying" << dendl;
      continue;
    }
    derr << "ERROR: failed to refresh rotating keys, "
         << "maximum retry time reached."
	 << " Maybe I have a clock skew against the monitors?" << dendl;
    std::lock_guard locker{mds_lock};
    suicide();
    return -CEPHFS_ETIMEDOUT;
  }

  mds_lock.lock();
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE) {
    dout(4) << __func__ << ": terminated already, dropping out" << dendl;
    mds_lock.unlock();
    return 0;
  }

  monc->sub_want("mdsmap", 0, 0);
  monc->renew_subs();

  mds_lock.unlock();

  // Set up admin socket before taking mds_lock, so that ordering
  // is consistent (later we take mds_lock within asok callbacks)
  set_up_admin_socket();
  std::lock_guard locker{mds_lock};
  if (beacon.get_want_state() == MDSMap::STATE_DNE) {
    suicide();  // we could do something more graceful here
    dout(4) << __func__ << ": terminated already, dropping out" << dendl;
    return 0; 
  }

  timer.init();

  beacon.init(*mdsmap);
  messenger->set_myname(entity_name_t::MDS(MDS_RANK_NONE));

  // schedule tick
  reset_tick();
  return 0;
}

void MDSDaemon::reset_tick()
{
  // cancel old
  if (tick_event) timer.cancel_event(tick_event);

  // schedule
  tick_event = timer.add_event_after(
    g_conf()->mds_tick_interval,
    new LambdaContext([this](int) {
	ceph_assert(ceph_mutex_is_locked_by_me(mds_lock));
	tick();
      }));
}

void MDSDaemon::tick()
{
  // reschedule
  reset_tick();

  // Call through to subsystems' tick functions
  if (mds_rank) {
    mds_rank->tick();
  }
}

void MDSDaemon::handle_command(const cref_t<MCommand> &m)
{
  auto priv = m->get_connection()->get_priv();
  auto session = static_cast<Session *>(priv.get());
  ceph_assert(session != NULL);

  int r = 0;
  cmdmap_t cmdmap;
  CachedStackStringStream css;
  auto& ss = *css;
  bufferlist outbl;

  // If someone is using a closed session for sending commands (e.g.
  // the ceph CLI) then we should feel free to clean up this connection
  // as soon as we've sent them a response.
  const bool live_session =
    session->get_state_seq() > 0 &&
    mds_rank &&
    mds_rank->sessionmap.get_session(session->info.inst.name);

  if (!live_session) {
    // This session only existed to issue commands, so terminate it
    // as soon as we can.
    ceph_assert(session->is_closed());
    session->get_connection()->mark_disposable();
  }
  priv.reset();

  if (!session->auth_caps.allow_all()) {
    dout(1) << __func__
      << ": received command from client without `tell` capability: "
      << *m->get_connection()->peer_addrs << dendl;

    ss << "permission denied";
    r = -CEPHFS_EACCES;
  } else if (m->cmd.empty()) {
    r = -CEPHFS_EINVAL;
    ss << "no command given";
  } else if (!TOPNSPC::common::cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    r = -CEPHFS_EINVAL;
  } else {
    cct->get_admin_socket()->queue_tell_command(m);
    return;
  }

  auto reply = make_message<MCommandReply>(r, ss.str());
  reply->set_tid(m->get_tid());
  reply->set_data(outbl);
  m->get_connection()->send_message2(reply);
}

void MDSDaemon::handle_mds_map(const cref_t<MMDSMap> &m)
{
  version_t epoch = m->get_epoch();

  // is it new?
  if (epoch <= mdsmap->get_epoch()) {
    dout(5) << "handle_mds_map old map epoch " << epoch << " <= "
            << mdsmap->get_epoch() << ", discarding" << dendl;
    return;
  }

  dout(1) << "Updating MDS map to version " << epoch << " from " << m->get_source() << dendl;

  // keep old map, for a moment
  std::unique_ptr<MDSMap> oldmap;
  oldmap.swap(mdsmap);

  // decode and process
  mdsmap.reset(new MDSMap);
  mdsmap->decode(m->get_encoded());

  monc->sub_got("mdsmap", mdsmap->get_epoch());

  // verify compatset
  CompatSet mdsmap_compat(MDSMap::get_compat_set_all());
  dout(10) << "     my compat " << mdsmap_compat << dendl;
  dout(10) << " mdsmap compat " << mdsmap->compat << dendl;
  if (!mdsmap_compat.writeable(mdsmap->compat)) {
    dout(0) << "handle_mds_map mdsmap compatset " << mdsmap->compat
	    << " not writeable with daemon features " << mdsmap_compat
	    << ", killing myself" << dendl;
    suicide();
    return;
  }

  // Calculate my effective rank (either my owned rank or the rank I'm following if STATE_STANDBY_REPLAY
  const auto addrs = messenger->get_myaddrs();
  const auto myid = monc->get_global_id();
  const auto mygid = mds_gid_t(myid);
  const auto whoami = mdsmap->get_rank_gid(mygid);
  const auto old_state = oldmap->get_state_gid(mygid);
  const auto new_state = mdsmap->get_state_gid(mygid);
  const auto incarnation = mdsmap->get_inc_gid(mygid);
  dout(10) << "my gid is " << myid << dendl;
  dout(10) << "map says I am mds." << whoami << "." << incarnation
	   << " state " << ceph_mds_state_name(new_state) << dendl;
  dout(10) << "msgr says I am " << addrs << dendl;

  // If we're removed from the MDSMap, stop all processing.
  using DS = MDSMap::DaemonState;
  if (old_state != DS::STATE_NULL && new_state == DS::STATE_NULL) {
    const auto& oldinfo = oldmap->get_info_gid(mygid);
    dout(1) << "Map removed me " << oldinfo
            << " from cluster; respawning! See cluster/monitor logs for details." << dendl;
    respawn();
  }

  if (old_state == DS::STATE_NULL && new_state != DS::STATE_NULL) {
    /* The MDS has been added to the FSMap, now we can init the MgrClient */
    mgrc.init();
    messenger->add_dispatcher_tail(&mgrc);
    monc->sub_want("mgrmap", 0, 0);
    monc->renew_subs(); /* MgrMap receipt drives connection to ceph-mgr */
  }

  // mark down any failed peers
  for (const auto& [gid, info] : oldmap->get_mds_info()) {
    if (mdsmap->get_mds_info().count(gid) == 0) {
      dout(10) << " peer mds gid " << gid << " removed from map" << dendl;
      messenger->mark_down_addrs(info.addrs);
    }
  }

  if (whoami == MDS_RANK_NONE) {
    // We do not hold a rank:
    dout(10) <<  __func__ << ": handling map in rankless mode" << dendl;

    if (new_state == DS::STATE_STANDBY) {
      /* Note: STATE_BOOT is never an actual state in the FSMap. The Monitors
       * generally mark a new MDS as STANDBY (although it's possible to
       * immediately be assigned a rank).
       */
      if (old_state == DS::STATE_NULL) {
        dout(1) << "Monitors have assigned me to become a standby." << dendl;
        beacon.set_want_state(*mdsmap, new_state);
      } else if (old_state == DS::STATE_STANDBY) {
        dout(5) << "I am still standby" << dendl;
      }
    } else if (new_state == DS::STATE_NULL) {
      /* We are not in the MDSMap yet! Keep waiting: */
      ceph_assert(beacon.get_want_state() == DS::STATE_BOOT);
      dout(10) << "not in map yet" << dendl;
    } else {
      /* We moved to standby somehow from another state */
      ceph_abort("invalid transition to standby");
    }
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
          timer, beacon, mdsmap, messenger, monc, &mgrc,
          new LambdaContext([this](int r){respawn();}),
          new LambdaContext([this](int r){suicide();}),
	  ioctx);
      dout(10) <<  __func__ << ": initializing MDS rank "
               << mds_rank->get_nodeid() << dendl;
      mds_rank->init();
    }

    // MDSRank is active: let him process the map, we have no say.
    dout(10) <<  __func__ << ": handling map as rank "
             << mds_rank->get_nodeid() << dendl;
    mds_rank->handle_mds_map(m, *oldmap);
  }

  beacon.notify_mdsmap(*mdsmap);
}

void MDSDaemon::handle_signal(int signum)
{
  ceph_assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** got signal " << sig_str(signum) << " ***" << dendl;
  {
    std::lock_guard l(mds_lock);
    if (stopping) {
      return;
    }
    suicide();
  }
}

void MDSDaemon::suicide()
{
  ceph_assert(ceph_mutex_is_locked(mds_lock));
  
  // make sure we don't suicide twice
  ceph_assert(stopping == false);
  stopping = true;

  dout(1) << "suicide! Wanted state "
          << ceph_mds_state_name(beacon.get_want_state()) << dendl;

  if (tick_event) {
    timer.cancel_event(tick_event);
    tick_event = 0;
  }

  clean_up_admin_socket();

  // Notify the Monitors (MDSMonitor) that we're dying, so that it doesn't have
  // to wait for us to go laggy. Only do this if we're actually in the MDSMap,
  // because otherwise the MDSMonitor will drop our message.
  beacon.set_want_state(*mdsmap, MDSMap::STATE_DNE);
  if (!mdsmap->is_dne_gid(mds_gid_t(monc->get_global_id()))) {
    beacon.send_and_wait(1);
  }
  beacon.shutdown();

  if (mgrc.is_initialized())
    mgrc.shutdown();

  if (mds_rank) {
    mds_rank->shutdown();
  } else {
    timer.shutdown();

    monc->shutdown();
    messenger->shutdown();
  }
}

void MDSDaemon::respawn()
{
  // --- WARNING TO FUTURE COPY/PASTERS ---
  // You must also add a call like
  //
  //   ceph_pthread_setname(pthread_self(), "ceph-mds");
  //
  // to main() so that /proc/$pid/stat field 2 contains "(ceph-mds)"
  // instead of "(exe)", so that killall (and log rotation) will work.

  dout(1) << "respawn!" << dendl;

  /* Dump recent in case the MDS was stuck doing something which caused it to
   * be removed from the MDSMap leading to respawn. */
  g_ceph_context->_log->dump_recent();

  /* valgrind can't handle execve; just exit and let QA infra restart */
  if (g_conf().get_val<bool>("mds_valgrind_exit")) {
    _exit(0);
  }

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



bool MDSDaemon::ms_dispatch2(const ref_t<Message> &m)
{
  std::lock_guard l(mds_lock);
  if (stopping) {
    return false;
  }

  // Drop out early if shutting down
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE) {
    dout(10) << " stopping, discarding " << *m << dendl;
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

/*
 * high priority messages we always process
 */

#define ALLOW_MESSAGES_FROM(peers)                                      \
  do {                                                                  \
    if (m->get_connection() && (m->get_connection()->get_peer_type() & (peers)) == 0) { \
      dout(0) << __FILE__ << "." << __LINE__ << ": filtered out request, peer=" \
              << m->get_connection()->get_peer_type() << " allowing="   \
              << #peers << " message=" << *m << dendl;                  \
      return true;                                                      \
    }                                                                   \
  } while (0)

bool MDSDaemon::handle_core_message(const cref_t<Message> &m)
{
  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);
    break;

    // MDS
  case CEPH_MSG_MDS_MAP:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_MDS);
    handle_mds_map(ref_cast<MMDSMap>(m));
    break;

  case MSG_REMOVE_SNAPS:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);
    mds_rank->snapserver->handle_remove_snaps(ref_cast<MRemoveSnaps>(m));
    break;

    // OSD
  case MSG_COMMAND:
    handle_command(ref_cast<MCommand>(m));
    break;
  case CEPH_MSG_OSD_MAP:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);

    if (mds_rank) {
      mds_rank->handle_osd_map();
    }
    break;

  case MSG_MON_COMMAND:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);
    clog->warn() << "dropping `mds tell` command from legacy monitor";
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

  std::lock_guard l(mds_lock);
  if (stopping) {
    return false;
  }
  dout(5) << "ms_handle_reset on " << con->get_peer_socket_addr() << dendl;
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE)
    return false;

  auto priv = con->get_priv();
  if (auto session = static_cast<Session *>(priv.get()); session) {
    if (session->is_closed()) {
      dout(3) << "ms_handle_reset closing connection for session " << session->info.inst << dendl;
      con->mark_down();
      con->set_priv(nullptr);
    }
  } else {
    con->mark_down();
  }
  return false;
}


void MDSDaemon::ms_handle_remote_reset(Connection *con)
{
  if (con->get_peer_type() != CEPH_ENTITY_TYPE_CLIENT)
    return;

  std::lock_guard l(mds_lock);
  if (stopping) {
    return;
  }

  dout(5) << "ms_handle_remote_reset on " << con->get_peer_socket_addr() << dendl;
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE)
    return;

  auto priv = con->get_priv();
  if (auto session = static_cast<Session *>(priv.get()); session) {
    if (session->is_closed()) {
      dout(3) << "ms_handle_remote_reset closing connection for session " << session->info.inst << dendl;
      con->mark_down();
      con->set_priv(nullptr);
    }
  }
}

bool MDSDaemon::ms_handle_refused(Connection *con)
{
  // do nothing for now
  return false;
}

bool MDSDaemon::parse_caps(const AuthCapsInfo& info, MDSAuthCaps& caps)
{
  caps.clear();
  if (info.allow_all) {
    caps.set_allow_all();
    return true;
  } else {
    auto it = info.caps.begin();
    string auth_cap_str;
    try {
      decode(auth_cap_str, it);
    } catch (const buffer::error& e) {
      dout(1) << __func__ << ": cannot decode auth caps buffer of length " << info.caps.length() << dendl;
      return false;
    }

    dout(10) << __func__ << ": parsing auth_cap_str='" << auth_cap_str << "'" << dendl;
    CachedStackStringStream cs;
    if (caps.parse(auth_cap_str, cs.get())) {
      return true;
    } else {
      dout(1) << __func__ << ": auth cap parse error: " << cs->strv() << " parsing '" << auth_cap_str << "'" << dendl;
      return false;
    }
  }
}

int MDSDaemon::ms_handle_fast_authentication(Connection *con)
{
  /* N.B. without mds_lock! */
  MDSAuthCaps caps;
  return parse_caps(con->get_peer_caps_info(), caps) ? 0 : -1;
}

void MDSDaemon::ms_handle_accept(Connection *con)
{
  entity_name_t n(con->get_peer_type(), con->get_peer_global_id());
  std::lock_guard l(mds_lock);
  if (stopping) {
    return;
  }

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
    s = new Session(con);
    dout(10) << " new session " << s << " for " << s->info.inst
	     << " con " << con << dendl;
    con->set_priv(RefCountedPtr{s, false});
    if (mds_rank) {
      mds_rank->kick_waiters_for_any_client_connection();
    }
  } else {
    dout(10) << " existing session " << s << " for " << s->info.inst
	     << " existing con " << s->get_connection()
	     << ", new/authorizing con " << con << dendl;
    con->set_priv(RefCountedPtr{s});
  }

  parse_caps(con->get_peer_caps_info(), s->auth_caps);

  dout(10) << "ms_handle_accept " << con->get_peer_socket_addr() << " con " << con << " session " << s << dendl;
  if (s) {
    if (s->get_connection() != con) {
      dout(10) << " session connection " << s->get_connection()
	       << " -> " << con << dendl;
      s->set_connection(con);

      // send out any queued messages
      while (!s->preopen_out_queue.empty()) {
	con->send_message2(s->preopen_out_queue.front());
	s->preopen_out_queue.pop_front();
      }
    }
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
