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

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds


MDSUtility::MDSUtility() :
  Dispatcher(g_ceph_context),
  objecter(NULL),
  finisher(g_ceph_context, "MDSUtility", "fn_mds_utility"),
  waiting_for_mds_map(NULL),
  inited(false)
{
  monc = new MonClient(g_ceph_context, poolctx);
  messenger = Messenger::create_client_messenger(g_ceph_context, "mds");
  fsmap = new FSMap();
  objecter = new Objecter(g_ceph_context, messenger, monc, poolctx, 0, 0);
}


MDSUtility::~MDSUtility()
{
  if (inited) {
    shutdown();
  }
  delete objecter;
  delete monc;
  delete messenger;
  delete fsmap;
  ceph_assert(waiting_for_mds_map == NULL);
}


int MDSUtility::init()
{
  // Initialize Messenger
  poolctx.start(1);
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
  int r = monc->authenticate();
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

  // Prepare to receive MDS map and request it
  ceph::mutex init_lock = ceph::make_mutex("MDSUtility:init");
  ceph::condition_variable cond;
  bool done = false;
  ceph_assert(!fsmap->get_epoch());
  lock.lock();
  waiting_for_mds_map = new C_SafeCond(init_lock, cond, &done, NULL);
  lock.unlock();
  monc->sub_want("fsmap", 0, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();

  // Wait for MDS map
  dout(4) << "waiting for MDS map..." << dendl;
  {
    std::unique_lock locker{init_lock};
    cond.wait(locker, [&done] { return done; });
  }
  dout(4) << "Got MDS map " << fsmap->get_epoch() << dendl;

  finisher.start();

  inited = true;
  return 0;
}


void MDSUtility::shutdown()
{
  finisher.stop();

  lock.lock();
  objecter->shutdown();
  lock.unlock();
  monc->shutdown();
  messenger->shutdown();
  messenger->wait();
  poolctx.finish();
}


bool MDSUtility::ms_dispatch(Message *m)
{
  std::lock_guard locker{lock};
   switch (m->get_type()) {
   case CEPH_MSG_FS_MAP:
     handle_fs_map((MFSMap*)m);
     break;
   case CEPH_MSG_OSD_MAP:
     break;
   default:
     return false;
   }
   m->put();
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


