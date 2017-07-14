// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <Python.h>

#include "common/errno.h"
#include "common/signal.h"
#include "include/compat.h"

#include "include/stringify.h"
#include "global/global_context.h"
#include "global/signal_handler.h"

#include "mgr/MgrContext.h"

#include "messages/MMgrBeacon.h"
#include "messages/MMgrMap.h"
#include "Mgr.h"

#include "MgrStandby.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "


MgrStandby::MgrStandby(int argc, const char **argv) :
  Dispatcher(g_ceph_context),
  monc{g_ceph_context},
  client_messenger(Messenger::create_client_messenger(g_ceph_context, "mgr")),
  objecter{g_ceph_context, client_messenger.get(), &monc, NULL, 0, 0},
  client{client_messenger.get(), &monc, &objecter},
  log_client(g_ceph_context, client_messenger.get(), &monc.monmap, LogClient::NO_FLAGS),
  clog(log_client.create_channel(CLOG_CHANNEL_CLUSTER)),
  audit_clog(log_client.create_channel(CLOG_CHANNEL_AUDIT)),
  lock("MgrStandby::lock"),
  timer(g_ceph_context, lock),
  active_mgr(nullptr),
  orig_argc(argc),
  orig_argv(argv)
{
}

MgrStandby::~MgrStandby() = default;

const char** MgrStandby::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    // clog & admin clog
    "clog_to_monitors",
    "clog_to_syslog",
    "clog_to_syslog_facility",
    "clog_to_syslog_level",
    "osd_objectstore_fuse",
    "clog_to_graylog",
    "clog_to_graylog_host",
    "clog_to_graylog_port",
    "host",
    "fsid",
    NULL
  };
  return KEYS;
}

void MgrStandby::handle_conf_change(
  const struct md_config_t *conf,
  const std::set <std::string> &changed)
{
  if (changed.count("clog_to_monitors") ||
      changed.count("clog_to_syslog") ||
      changed.count("clog_to_syslog_level") ||
      changed.count("clog_to_syslog_facility") ||
      changed.count("clog_to_graylog") ||
      changed.count("clog_to_graylog_host") ||
      changed.count("clog_to_graylog_port") ||
      changed.count("host") ||
      changed.count("fsid")) {
    _update_log_config();
  }
}

int MgrStandby::init()
{
  Mutex::Locker l(lock);

  // Initialize Messenger
  client_messenger->add_dispatcher_tail(this);
  client_messenger->add_dispatcher_head(&objecter);
  client_messenger->add_dispatcher_tail(&client);
  client_messenger->start();

  // Initialize MonClient
  if (monc.build_initial_monmap() < 0) {
    client_messenger->shutdown();
    client_messenger->wait();
    return -1;
  }

  monc.sub_want("mgrmap", 0, 0);

  monc.set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD
      |CEPH_ENTITY_TYPE_MDS|CEPH_ENTITY_TYPE_MGR);
  monc.set_messenger(client_messenger.get());
  int r = monc.init();
  if (r < 0) {
    monc.shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }
  r = monc.authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify a mgr ID with a valid keyring?" << dendl;
    monc.shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }

  client_t whoami = monc.get_global_id();
  client_messenger->set_myname(entity_name_t::CLIENT(whoami.v));
  monc.set_log_client(&log_client);
  _update_log_config();
  objecter.set_client_incarnation(0);
  objecter.init();
  objecter.start();
  client.init();
  timer.init();

  tick();

  dout(4) << "Complete." << dendl;
  return 0;
}

void MgrStandby::send_beacon()
{
  assert(lock.is_locked_by_me());
  dout(1) << state_str() << dendl;

  set<string> modules;
  PyModules::list_modules(&modules);
  bool available = active_mgr != nullptr && active_mgr->is_initialized();
  auto addr = available ? active_mgr->get_server_addr() : entity_addr_t();
  dout(10) << "sending beacon as gid " << monc.get_global_id()
	   << " modules " << modules << dendl;

  MMgrBeacon *m = new MMgrBeacon(monc.get_fsid(),
				 monc.get_global_id(),
                                 g_conf->name.get_id(),
                                 addr,
                                 available,
				 modules);
  monc.send_mon_message(m);
}

void MgrStandby::tick()
{
  dout(10) << __func__ << dendl;
  send_beacon();

  if (active_mgr) {
    active_mgr->tick();
  }

  timer.add_event_after(g_conf->mgr_tick_period, new FunctionContext(
        [this](int r){
          tick();
        }
  )); 
}

void MgrStandby::handle_signal(int signum)
{
  Mutex::Locker l(lock);
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got signal " << sig_str(signum) << " ***" << dendl;
  shutdown();
}

void MgrStandby::shutdown()
{
  // Expect already to be locked as we're called from signal handler
  assert(lock.is_locked_by_me());

  // stop sending beacon first, i use monc to talk with monitors
  timer.shutdown();
  // client uses monc and objecter
  client.shutdown();
  // stop monc, so mon won't be able to instruct me to shutdown/activate after
  // the active_mgr is stopped
  monc.shutdown();
  if (active_mgr) {
    active_mgr->shutdown();
  }
  // objecter is used by monc and active_mgr
  objecter.shutdown();
  // client_messenger is used by all of them, so stop it in the end
  client_messenger->shutdown();
}

void MgrStandby::respawn()
{
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
  if (readlink(PROCPREFIX "/proc/self/exe", exe_path, PATH_MAX-1) == -1) {
    /* Print CWD for the user's interest */
    char buf[PATH_MAX];
    char *cwd = getcwd(buf, sizeof(buf));
    assert(cwd);
    dout(1) << " cwd " << cwd << dendl;

    /* Fall back to a best-effort: just running in our CWD */
    strncpy(exe_path, orig_argv[0], PATH_MAX-1);
  } else {
    dout(1) << "respawning with exe " << exe_path << dendl;
    strcpy(exe_path, PROCPREFIX "/proc/self/exe");
  }

  dout(1) << " exe_path " << exe_path << dendl;

  unblock_all_signals(NULL);
  execv(exe_path, new_argv);

  derr << "respawn execv " << orig_argv[0]
       << " failed with " << cpp_strerror(errno) << dendl;
  ceph_abort();
}

void MgrStandby::_update_log_config()
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

  if (parse_log_client_options(cct, log_to_monitors, log_to_syslog,
			       log_channel, log_prio, log_to_graylog,
			       log_to_graylog_host, log_to_graylog_port,
			       fsid, host) == 0) {
    clog->update_config(log_to_monitors, log_to_syslog,
			log_channel, log_prio, log_to_graylog,
			log_to_graylog_host, log_to_graylog_port,
			fsid, host);
    audit_clog->update_config(log_to_monitors, log_to_syslog,
			      log_channel, log_prio, log_to_graylog,
			      log_to_graylog_host, log_to_graylog_port,
			      fsid, host);
  }
}

void MgrStandby::handle_mgr_map(MMgrMap* mmap)
{
  auto map = mmap->get_map();
  dout(4) << "received map epoch " << map.get_epoch() << dendl;
  const bool active_in_map = map.active_gid == monc.get_global_id();
  dout(4) << "active in map: " << active_in_map
          << " active is " << map.active_gid << dendl;
  if (active_in_map) {
    if (!active_mgr) {
      dout(1) << "Activating!" << dendl;
      active_mgr.reset(new Mgr(&monc, map, client_messenger.get(), &objecter,
			       &client, clog, audit_clog));
      active_mgr->background_init(new FunctionContext(
            [this](int r){
              // Advertise our active-ness ASAP instead of waiting for
              // next tick.
              Mutex::Locker l(lock);
              send_beacon();
            }));
      dout(1) << "I am now activating" << dendl;
    } else {
      dout(10) << "I was already active" << dendl;
      bool need_respawn = active_mgr->got_mgr_map(map);
      if (need_respawn) {
	respawn();
      }
    }
  } else {
    if (active_mgr != nullptr) {
      derr << "I was active but no longer am" << dendl;
      respawn();
    }
  }

  mmap->put();
}

bool MgrStandby::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);
  dout(4) << state_str() << " " << *m << dendl;

  if (m->get_type() == MSG_MGR_MAP) {
    handle_mgr_map(static_cast<MMgrMap*>(m));
    return true;
  } else if (active_mgr) {
    auto am = active_mgr;
    lock.Unlock();
    bool handled = am->ms_dispatch(m);
    lock.Lock();
    return handled;
  } else {
    return false;
  }
}


bool MgrStandby::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new)
{
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    if (monc.wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc.build_authorizer(dest_type);
  return *authorizer != NULL;
}

bool MgrStandby::ms_handle_refused(Connection *con)
{
  // do nothing for now
  return false;
}

// A reference for use by the signal handler
static MgrStandby *signal_mgr = nullptr;

static void handle_mgr_signal(int signum)
{
  if (signal_mgr) {
    signal_mgr->handle_signal(signum);
  }
}

int MgrStandby::main(vector<const char *> args)
{
  // Enable signal handlers
  signal_mgr = this;
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_mgr_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_mgr_signal);

  client_messenger->wait();

  // Disable signal handlers
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_mgr_signal);
  unregister_async_signal_handler(SIGTERM, handle_mgr_signal);
  shutdown_async_signal_handler();
  signal_mgr = nullptr;

  return 0;
}


std::string MgrStandby::state_str()
{
  return active_mgr == nullptr ? "standby" : "active";
}

