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
#include <boost/algorithm/string/replace.hpp>

#include "common/errno.h"
#include "common/signal.h"
#include "include/compat.h"

#include "include/stringify.h"
#include "global/global_context.h"
#include "global/signal_handler.h"

#include "mgr/MgrContext.h"
#include "mgr/mgr_commands.h"
#include "mgr/mgr_perf_counters.h"

#include "messages/MMgrBeacon.h"
#include "messages/MMgrMap.h"
#include "Mgr.h"

#include "MgrStandby.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

using std::map;
using std::string;
using std::vector;

MgrStandby::MgrStandby(int argc, const char **argv) :
  Dispatcher(g_ceph_context),
  monc{g_ceph_context, poolctx},
  client_messenger(Messenger::create(
		     g_ceph_context,
		     cct->_conf.get_val<std::string>("ms_public_type").empty() ?
			cct->_conf.get_val<std::string>("ms_type") : cct->_conf.get_val<std::string>("ms_public_type"),
		     entity_name_t::MGR(),
		     "mgr",
		     Messenger::get_random_nonce())),
  objecter{g_ceph_context, client_messenger.get(), &monc, poolctx},
  client{client_messenger.get(), &monc, &objecter},
  mgrc(g_ceph_context, client_messenger.get(), &monc.monmap),
  log_client(g_ceph_context, client_messenger.get(), &monc.monmap, LogClient::NO_FLAGS),
  clog(log_client.create_channel(CLOG_CHANNEL_CLUSTER)),
  audit_clog(log_client.create_channel(CLOG_CHANNEL_AUDIT)),
  finisher(g_ceph_context, "MgrStandby", "mgrsb-fin"),
  timer(g_ceph_context, lock),
  py_module_registry(clog),
  active_mgr(nullptr),
  orig_argc(argc),
  orig_argv(argv),
  available_in_map(false)
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
    "clog_to_graylog",
    "clog_to_graylog_host",
    "clog_to_graylog_port",
    "mgr_standby_modules",
    "host",
    "fsid",
    NULL
  };
  return KEYS;
}

void MgrStandby::handle_conf_change(
  const ConfigProxy& conf,
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
  if (changed.count("mgr_standby_modules") && !active_mgr) {
    if (g_conf().get_val<bool>("mgr_standby_modules") != py_module_registry.have_standby_modules()) {
      dout(1) << "mgr_standby_modules now "
	      << (int)g_conf().get_val<bool>("mgr_standby_modules")
	      << ", standby modules are "
	      << (py_module_registry.have_standby_modules() ? "":"not ")
	      << "active, respawning"
	      << dendl;
      respawn();
    }
  }
}

int MgrStandby::init()
{
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);

  cct->_conf.add_observer(this);

  std::lock_guard l(lock);

  // Start finisher
  finisher.start();

  // Initialize Messenger
  client_messenger->add_dispatcher_tail(this);
  client_messenger->add_dispatcher_head(&objecter);
  client_messenger->add_dispatcher_tail(&client);
  client_messenger->start();

  poolctx.start(2);

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

  // We must register our config callback before calling init(), so
  // that we see the initial configuration message
  monc.register_config_callback([this](const std::string &k, const std::string &v){
      // removing value to hide sensitive data going into mgr logs
      // leaving this for debugging purposes
      // dout(10) << "config_callback: " << k << " : " << v << dendl;
      dout(10) << "config_callback: " << k << " : " << dendl;
      if (k.substr(0, 4) == "mgr/") {
        py_module_registry.handle_config(k, v);
	return true;
      }
      return false;
    });
  monc.register_config_notify_callback([this]() {
      py_module_registry.handle_config_notify();
    });
  dout(4) << "Registered monc callback" << dendl;

  int r = monc.init();
  if (r < 0) {
    monc.shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }
  mgrc.init();
  client_messenger->add_dispatcher_tail(&mgrc);

  r = monc.authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify a mgr ID with a valid keyring?" << dendl;
    monc.shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }
  // only forward monmap updates after authentication finishes, otherwise
  // monc.authenticate() will be waiting for MgrStandy::ms_dispatch()
  // to acquire the lock forever, as it is already locked in the beginning of
  // this method.
  monc.set_passthrough_monmap();

  client_t whoami = monc.get_global_id();
  client_messenger->set_myname(entity_name_t::MGR(whoami.v));
  monc.set_log_client(&log_client);
  _update_log_config();
  objecter.set_client_incarnation(0);
  objecter.init();
  objecter.start();
  client.init();
  timer.init();

  py_module_registry.init();
  mgr_perf_start(g_ceph_context);


  tick();

  dout(4) << "Complete." << dendl;
  return 0;
}

void MgrStandby::send_beacon()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));
  dout(20) << state_str() << dendl;

  auto modules = py_module_registry.get_modules();

  // Construct a list of the info about each loaded module
  // which we will transmit to the monitor.
  std::vector<MgrMap::ModuleInfo> module_info;
  for (const auto &module : modules) {
    MgrMap::ModuleInfo info;
    info.name = module->get_name();
    info.error_string = module->get_error_string();
    info.can_run = module->get_can_run();
    info.module_options = module->get_options();
    module_info.push_back(std::move(info));
  }

  auto clients = py_module_registry.get_clients();
  for (const auto& client : clients) {
    dout(15) << "noting RADOS client for blocklist: " << client << dendl;
  }

  // Whether I think I am available (request MgrMonitor to set me
  // as available in the map)
  bool available = active_mgr != nullptr && active_mgr->is_initialized();

  auto addrs = available ? active_mgr->get_server_addrs() : entity_addrvec_t();
  dout(10) << "sending beacon as gid " << monc.get_global_id() << dendl;

  map<string,string> metadata;
  metadata["addr"] = client_messenger->get_myaddr_legacy().ip_only_to_str();
  metadata["addrs"] = stringify(client_messenger->get_myaddrs());
  collect_sys_info(&metadata, g_ceph_context);

  auto m = ceph::make_message<MMgrBeacon>(monc.get_fsid(),
				 monc.get_global_id(),
                                 g_conf()->name.get_id(),
                                 addrs,
                                 available,
				 std::move(module_info),
				 std::move(metadata),
                                 std::move(clients),
				 CEPH_FEATURES_ALL);

  if (available) {
    if (!available_in_map) {
      // We are informing the mon that we are done initializing: inform
      // it of our command set.  This has to happen after init() because
      // it needs the python modules to have loaded.
      std::vector<MonCommand> commands = mgr_commands;
      std::vector<MonCommand> py_commands = py_module_registry.get_commands();
      commands.insert(commands.end(), py_commands.begin(), py_commands.end());
      if (monc.monmap.min_mon_release < ceph_release_t::quincy) {
	dout(10) << " stripping out positional=false quincy-ism" << dendl;
	for (auto& i : commands) {
	  boost::replace_all(i.cmdstring, ",positional=false", "");
	}
      }
      m->set_command_descs(commands);
      dout(4) << "going active, including " << m->get_command_descs().size()
              << " commands in beacon" << dendl;
    }

    m->set_services(active_mgr->get_services());
  }

  monc.send_mon_message(std::move(m));
}

void MgrStandby::tick()
{
  dout(10) << __func__ << dendl;
  send_beacon();

  timer.add_event_after(
      g_conf().get_val<std::chrono::seconds>("mgr_tick_period").count(),
      new LambdaContext([this](int r){
          tick();
      }
  ));
}

void MgrStandby::respawn()
{
  // --- WARNING TO FUTURE COPY/PASTERS ---
  // You must also add a call like
  //
  //   ceph_pthread_setname(pthread_self(), "ceph-mgr");
  //
  // to main() so that /proc/$pid/stat field 2 contains "(ceph-mgr)"
  // instead of "(exe)", so that killall (and log rotation) will work.

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
    ceph_assert(cwd);
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
  clog->parse_client_options(cct);
  audit_clog->parse_client_options(cct);
}

void MgrStandby::handle_mgr_map(ref_t<MMgrMap> mmap)
{
  auto &map = mmap->get_map();
  dout(4) << "received map epoch " << map.get_epoch() << dendl;
  const bool active_in_map = map.active_gid == monc.get_global_id();
  dout(4) << "active in map: " << active_in_map
          << " active is " << map.active_gid << dendl;

  // PyModuleRegistry may ask us to respawn if it sees that
  // this MgrMap is changing its set of enabled modules
  bool need_respawn = py_module_registry.handle_mgr_map(map);
  if (need_respawn) {
    dout(1) << "respawning because set of enabled modules changed!" << dendl;
    respawn();
  }

  if (active_in_map) {
    if (!active_mgr) {
      dout(1) << "Activating!" << dendl;
      active_mgr.reset(new Mgr(&monc, map, &py_module_registry,
                               client_messenger.get(), &objecter,
			       &client, clog, audit_clog));
      active_mgr->background_init(new LambdaContext(
            [this](int r){
              // Advertise our active-ness ASAP instead of waiting for
              // next tick.
              std::lock_guard l(lock);
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

    if (!available_in_map && map.get_available()) {
      dout(4) << "Map now says I am available" << dendl;
      available_in_map = true;
    }
  } else if (active_mgr != nullptr) {
    derr << "I was active but no longer am" << dendl;
    respawn();
  } else {
    if (map.active_gid != 0 && map.active_name != g_conf()->name.get_id()) {
      // I am the standby and someone else is active, start modules
      // in standby mode to do redirects if needed
      if (!py_module_registry.is_standby_running() &&
	  g_conf().get_val<bool>("mgr_standby_modules")) {
        py_module_registry.standby_start(monc, finisher);
      }
    }
  }
}

bool MgrStandby::ms_dispatch2(const ref_t<Message>& m)
{
  std::lock_guard l(lock);
  dout(10) << state_str() << " " << *m << dendl;

  if (m->get_type() == MSG_MGR_MAP) {
    handle_mgr_map(ref_cast<MMgrMap>(m));
  }
  bool handled = false;
  if (active_mgr) {
    auto am = active_mgr;
    lock.unlock();
    handled = am->ms_dispatch2(m);
    lock.lock();
  }
  if (m->get_type() == MSG_MGR_MAP) {
    // let this pass through for mgrc
    handled = false;
  }
  return handled;
}


bool MgrStandby::ms_handle_refused(Connection *con)
{
  // do nothing for now
  return false;
}

int MgrStandby::main(vector<const char *> args)
{
  client_messenger->wait();

  // Disable signal handlers
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  shutdown_async_signal_handler();

  return 0;
}


std::string MgrStandby::state_str()
{
  if (active_mgr == nullptr) {
    return "standby";
  } else if (active_mgr->is_initialized()) {
    return "active";
  } else {
    return "active (starting)";
  }
}
