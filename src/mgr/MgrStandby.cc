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

#include "common/errno.h"
#include "mon/MonClient.h"
#include "include/stringify.h"
#include "global/global_context.h"
#include "global/signal_handler.h"

#include "mgr/MgrContext.h"

#include "messages/MMgrBeacon.h"
#include "messages/MMgrMap.h"
#include "Mgr.h"

#include "MgrStandby.h"

#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "


MgrStandby::MgrStandby() :
  Dispatcher(g_ceph_context),
  monc(new MonClient(g_ceph_context)),
  lock("MgrStandby::lock"),
  timer(g_ceph_context, lock),
  active_mgr(nullptr)
{
  client_messenger = Messenger::create_client_messenger(g_ceph_context, "mgr");
}


MgrStandby::~MgrStandby()
{
  delete monc;
  delete client_messenger;
  delete active_mgr;
}


int MgrStandby::init()
{
  Mutex::Locker l(lock);

  // Initialize Messenger
  client_messenger->add_dispatcher_tail(this);
  client_messenger->start();

  // Initialize MonClient
  if (monc->build_initial_monmap() < 0) {
    client_messenger->shutdown();
    client_messenger->wait();
    return -1;
  }

  monc->sub_want("mgrmap", 0, 0);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD
      |CEPH_ENTITY_TYPE_MDS|CEPH_ENTITY_TYPE_MGR);
  monc->set_messenger(client_messenger);
  monc->init();
  int r = monc->authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify a mgr ID with a valid keyring?" << dendl;
    monc->shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }

  client_t whoami = monc->get_global_id();
  client_messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  timer.init();
  send_beacon();

  dout(4) << "Complete." << dendl;
  return 0;
}

void MgrStandby::send_beacon()
{
  assert(lock.is_locked_by_me());
  dout(1) << state_str() << dendl;
  dout(10) << "sending beacon as gid " << monc->get_global_id() << dendl;

  bool available = active_mgr != nullptr && active_mgr->is_initialized();
  auto addr = available ? active_mgr->get_server_addr() : entity_addr_t();
  MMgrBeacon *m = new MMgrBeacon(monc->get_global_id(),
                                 g_conf->name.get_id(),
                                 addr,
                                 available);
                                 
  monc->send_mon_message(m);
  // TODO configure period
  timer.add_event_after(5, new C_StdFunction(
        [this](){
          send_beacon();
        }
  )); 
}

void MgrStandby::handle_signal(int signum)
{
  Mutex::Locker l(lock);
  assert(signum == SIGINT || signum == SIGTERM);
  shutdown();
}

void MgrStandby::shutdown()
{
  // Expect already to be locked as we're called from signal handler
  assert(lock.is_locked_by_me());

  if (active_mgr) {
    active_mgr->shutdown();
  }

  timer.shutdown();

  monc->shutdown();
  client_messenger->shutdown();
}

bool MgrStandby::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);
  dout(4) << state_str() << " " << *m << dendl;

  switch (m->get_type()) {
    case MSG_MGR_MAP:
      {
        auto mmap = static_cast<MMgrMap*>(m);
        auto map = mmap->get_map();
        dout(4) << "received map epoch " << map.get_epoch() << dendl;
        const bool active_in_map = map.active_gid == monc->get_global_id();
        dout(4) << "active in map: " << active_in_map
                << " active is " << map.active_gid << dendl;
        if (active_in_map) {
          if (active_mgr == nullptr) {
            dout(1) << "Activating!" << dendl;
            active_mgr = new Mgr(monc, client_messenger);
            active_mgr->background_init();
            dout(1) << "I am now active" << dendl;
          } else {
            dout(10) << "I was already active" << dendl;
          }
        } else {
          if (active_mgr != nullptr) {
            derr << "I was active but no longer am" << dendl;
            shutdown();
            // FIXME: should politely go back to standby (or respawn
            // process) instead of stopping entirely.
          }
        }
      }
      break;

    default:
      if (active_mgr) {
        return active_mgr->ms_dispatch(m);
      } else {
        return false;
      }
  }
  return true;
}


bool MgrStandby::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new)
{
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}

// A reference for use by the signal handler
MgrStandby *signal_mgr = nullptr;

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
  register_async_signal_handler_oneshot(SIGINT, handle_mgr_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_mgr_signal);

  client_messenger->wait();

  // Disable signal handlers
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

