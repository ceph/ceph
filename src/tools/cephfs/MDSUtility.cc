// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "MDSUtility.h"
#include "mon/MonClient.h"

#define dout_subsys ceph_subsys_mds


MDSUtility::MDSUtility() :
  Dispatcher(g_ceph_context),
  objecter(NULL),
  lock("MDSUtility::lock"),
  timer(g_ceph_context, lock),
  finisher(g_ceph_context, "MDSUtility", "fn_mds_utility"),
  waiting_for_mds_map(NULL)
{
  monc = new MonClient(g_ceph_context);
  messenger = Messenger::create_client_messenger(g_ceph_context, "mds");
  fsmap = new FSMap();
  objecter = new Objecter(g_ceph_context, messenger, monc, NULL, 0, 0);
}


MDSUtility::~MDSUtility()
{
  delete objecter;
  delete monc;
  delete messenger;
  delete fsmap;
  assert(waiting_for_mds_map == NULL);
}


int MDSUtility::init()
{
  // Initialize Messenger
  int r = messenger->bind(g_conf->public_addr);
  if (r < 0)
    return r;

  messenger->start();

  objecter->set_client_incarnation(0);
  objecter->init();

  // Connect dispatchers before starting objecter
  messenger->add_dispatcher_tail(objecter);
  messenger->add_dispatcher_tail(this);

  // Initialize MonClient
  if (monc->build_initial_monmap() < 0) {
    objecter->shutdown();
    messenger->shutdown();
    messenger->wait();
    return -1;
  }

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD|CEPH_ENTITY_TYPE_MDS);
  monc->set_messenger(messenger);
  monc->init();
  r = monc->authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify an MDS ID with a valid keyring?" << dendl;
    monc->shutdown();
    objecter->shutdown();
    messenger->shutdown();
    messenger->wait();
    return r;
  }

  client_t whoami = monc->get_global_id();
  messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  // Start Objecter and wait for OSD map
  objecter->start();
  objecter->wait_for_osd_map();
  timer.init();

  // Prepare to receive MDS map and request it
  Mutex init_lock("MDSUtility:init");
  Cond cond;
  bool done = false;
  assert(!fsmap->get_epoch());
  lock.Lock();
  waiting_for_mds_map = new C_SafeCond(&init_lock, &cond, &done, NULL);
  lock.Unlock();
  monc->sub_want("fsmap", 0, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();

  // Wait for MDS map
  dout(4) << "waiting for MDS map..." << dendl;
  init_lock.Lock();
  while (!done)
    cond.Wait(init_lock);
  init_lock.Unlock();
  dout(4) << "Got MDS map " << fsmap->get_epoch() << dendl;

  finisher.start();

  return 0;
}


void MDSUtility::shutdown()
{
  finisher.stop();

  lock.Lock();
  timer.shutdown();
  objecter->shutdown();
  lock.Unlock();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
}


bool MDSUtility::ms_dispatch(Message *m)
{
   Mutex::Locker locker(lock);
   switch (m->get_type()) {
   case CEPH_MSG_FS_MAP:
     handle_fs_map((MFSMap*)m);
     break;
   case CEPH_MSG_OSD_MAP:
     break;
   default:
     return false;
   }
   return true;
}


void MDSUtility::handle_fs_map(MFSMap* m)
{
  *fsmap = m->get_fsmap();
  if (waiting_for_mds_map) {
    waiting_for_mds_map->complete(0);
    waiting_for_mds_map = NULL;
  }
}


bool MDSUtility::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
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
