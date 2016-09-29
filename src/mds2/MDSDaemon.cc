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
#include "SessionMap.h"

#include "common/HeartbeatMap.h"
#include "common/perf_counters.h"
#include "common/Timer.h"


#include "messages/MMDSMap.h"
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
  log_client(m->cct, messenger, &mc->monmap, LogClient::NO_FLAGS),
  mds_rank(NULL),
  tick_event(0),
  asok_hook(NULL)
{
  orig_argc = 0;
  orig_argv = NULL;

  clog = log_client.create_channel();

  monc->set_messenger(messenger);

  mdsmap = new MDSMap;
}

MDSDaemon::~MDSDaemon() {
  Mutex::Locker lock(mds_lock);

  delete mds_rank;
  mds_rank = NULL;
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
  f->open_object_section("status");
  f->dump_stream("cluster_fsid") << monc->get_fsid();
  if (mds_rank) {
    f->dump_unsigned("whoami", mds_rank->get_nodeid());
  } else {
    f->dump_unsigned("whoami", MDS_RANK_NONE);
  }

  f->dump_string("want_state", ceph_mds_state_name(beacon.get_want_state()));
  f->dump_string("state", ceph_mds_state_name(mdsmap->get_state_gid(mds_gid_t(monc->get_global_id()))));
  if (mds_rank) {
    Mutex::Locker l(mds_lock);
    mds_rank->dump_status(f);
  }

  f->dump_unsigned("mdsmap_epoch", mdsmap->get_epoch());
  f->close_section(); // status
}

void MDSDaemon::set_up_admin_socket()
{
  AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
  assert(asok_hook == nullptr);
  asok_hook = new MDSSocketHook(this);

  int r;
  r = admin_socket->register_command("dump cache",
      "dump cache name=path,type=CephString,req=false",
      asok_hook,
      "dump metadata cache (optionally to a file)");
  assert(r == 0);
}

void MDSDaemon::clean_up_admin_socket()
{
  AdminSocket *admin_socket = g_ceph_context->get_admin_socket();

  admin_socket->unregister_command("dump cache");
  delete asok_hook;
  asok_hook = NULL;
}

const char** MDSDaemon::get_tracked_conf_keys() const
{
  static const char* keys[] = {
    "fsid",
    NULL
  };
  return keys;
}

void MDSDaemon::handle_conf_change(const struct md_config_t *conf,
			     const std::set <std::string> &changed)
{
}


int MDSDaemon::init()
{
  messenger->add_dispatcher_tail(&beacon);
  messenger->add_dispatcher_tail(this);

  // get monmap
  monc->set_messenger(messenger);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD | CEPH_ENTITY_TYPE_MDS);
  int r = 0;
  r = monc->init();
  if (r < 0) {
    derr << "ERROR: failed to get monmap: " << cpp_strerror(-r) << dendl;
    mds_lock.Lock();
    suicide();
    mds_lock.Unlock();
    return r;
  }

  // tell monc about log_client so it will know about mon session resets
  monc->set_log_client(&log_client);

  r = monc->authenticate();
  if (r < 0) {
    derr << "ERROR: failed to authenticate: " << cpp_strerror(-r) << dendl;
    mds_lock.Lock();
    suicide();
    mds_lock.Unlock();
    return r;
  }

  int rotating_auth_attempts = 0;
  const int max_rotating_auth_attempts = 10;

  while (monc->wait_auth_rotating(30.0) < 0) {
    if (++rotating_auth_attempts <= max_rotating_auth_attempts) {
      derr << "unable to obtain rotating service keys; retrying" << dendl;
      continue;
    }
    derr << "ERROR: failed to refresh rotating keys, "
         << "maximum retry time reached." << dendl;
    mds_lock.Lock();
    suicide();
    mds_lock.Unlock();
    return -ETIMEDOUT;
  }

  mds_lock.Lock();
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE) {
    dout(4) << __func__ << ": terminated already, dropping out" << dendl;
    mds_lock.Unlock();
    return 0;
  }

  monc->sub_want("mdsmap", 0, 0);
  monc->renew_subs();

  mds_lock.Unlock();

  // Set up admin socket before taking mds_lock, so that ordering
  // is consistent (later we take mds_lock within asok callbacks)
  set_up_admin_socket();
  g_conf->add_observer(this);
  mds_lock.Lock();
  if (beacon.get_want_state() == MDSMap::STATE_DNE) {
    suicide();  // we could do something more graceful here
    dout(4) << __func__ << ": terminated already, dropping out" << dendl;
    mds_lock.Unlock();
    return 0; 
  }

  timer.init();

  beacon.init(mdsmap);
  messenger->set_myname(entity_name_t::MDS(MDS_RANK_NONE));

  // schedule tick
  reset_tick();
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

void MDSDaemon::send_command_reply(MCommand *m, MDSRank *mds_rank,
				   int r, bufferlist outbl,
				   const std::string& outs)
{
  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
  assert(session != NULL);

  MCommandReply *reply = new MCommandReply(r, outs);
  reply->set_tid(m->get_tid());
  reply->set_data(outbl);
  m->get_connection()->send_message(reply);
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
  bool need_reply = true;

  if (!session->auth_caps.allow_all()) {
    dout(1) << __func__
      << ": received command from client without `tell` capability: "
      << m->get_connection()->peer_addr << dendl;

    ss << "permission denied";
    r = -EPERM;
  } else if (m->cmd.empty()) {
    r = -EINVAL;
    ss << "no command given";
    outs = ss.str();
  } else if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    r = -EINVAL;
    outs = ss.str();
  } else {
    r = _handle_command(cmdmap, m, &outbl, &outs, &run_after, &need_reply);
  }

  if (need_reply) {
    send_command_reply(m, mds_rank, r, outbl, outs);
  }

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
};

int MDSDaemon::_handle_command(
    const cmdmap_t &cmdmap,
    MCommand *m,
    bufferlist *outbl,
    std::string *outs,
    Context **run_later,
    bool *need_reply)
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
    r = cct->_conf->injectargs(args, &ss);
  } else if (prefix == "exit") {
    // We will send response before executing
    ss << "Exiting...";
    *run_later = new SuicideLater(this);
  }
  else if (prefix == "respawn") {
    // We will send response before executing
    ss << "Respawning...";
    *run_later = new RespawnLater(this);
  } else {
    // Give MDSRank a shot at the command
    if (mds_rank) {
      bool handled = mds_rank->handle_command(cmdmap, m, &r, &ds, &ss,
					      need_reply);
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

  if (whoami == MDS_RANK_NONE && 
      new_state == MDSMap::STATE_STANDBY_REPLAY) {
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
            m->put();
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
          timer, beacon, mdsmap, messenger, monc,
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
  
  // make sure we don't suicide twice
  assert(stopping == false);
  stopping = true;

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


bool MDSDaemon::ms_can_fast_dispatch(Message *m) const
{
  return mds_rank && mds_rank->is_deferrable_message(m);
}

void MDSDaemon::ms_fast_dispatch(Message *m)
{
  mds_rank->ms_dispatch(m, true);
}

bool MDSDaemon::ms_dispatch(Message *m)
{
  /*
  if (stopping) {
    return false;
  }

  // Drop out early if shutting down
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE) {
    dout(10) << " stopping, discarding " << *m << dendl;
    m->put();
    return true;
  }
  */

  // First see if it's a daemon message
  if (is_core_message(m)) {
    Mutex::Locker l(mds_lock);
    handle_core_message(m);
    return true;
  }

  // Not core, try it as a rank message
  if (mds_rank) {
    return mds_rank->ms_dispatch(m, false);
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

bool MDSDaemon::is_core_message(Message *m)
{
  switch (m->get_type()) {
    case CEPH_MSG_MON_MAP:
    case CEPH_MSG_MDS_MAP:
    case CEPH_MSG_OSD_MAP:
    case MSG_COMMAND:
    case MSG_MON_COMMAND:
      return true;
  }
  return false;
}

/*
 * high priority messages we always process
 */
void MDSDaemon::handle_core_message(Message *m)
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

  case MSG_COMMAND:
    handle_command(static_cast<MCommand*>(m));
    break;
  default:
    derr << "mds daemon unknown message " << m->get_type() << dendl;
    m->put();
  }
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

bool MDSDaemon::ms_handle_refused(Connection *con)
{
  // do nothing for now
  return false;
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
      
      mds_rank->sessionmap->mutex_lock();
      s = mds_rank->sessionmap->get_session(n);
      mds_rank->sessionmap->mutex_unlock();
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
