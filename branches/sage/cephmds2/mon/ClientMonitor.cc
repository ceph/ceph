// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#include "ClientMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"
#include "OSDMonitor.h"

#include "messages/MClientMount.h"
#include "messages/MClientUnmount.h"

#include "common/Timer.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".client "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".client "




void ClientMonitor::dispatch(Message *m)
{
  switch (m->get_type()) {

  case MSG_CLIENT_MOUNT:
    handle_client_mount((MClientMount*)m);
    break;

  case MSG_CLIENT_UNMOUNT:
    handle_client_unmount((MClientUnmount*)m);
    break;
    
       
  default:
    assert(0);
  }  
}

void ClientMonitor::handle_client_mount(MClientMount *m)
{
  dout(7) << "client_mount from " << m->get_source_inst() << endl;
  assert(m->get_source().is_client());
  int from = m->get_source().num();
  
  // choose a client id
  if (from < 0 || 
      (client_map.count(from) && 
       client_map[from] != m->get_source_addr())) {
    from = num_clients++;
    dout(10) << "client_mount assigned client" << from << endl;
  }
  
  client_map[from] = m->get_source_addr();
  
  // reply with latest mds map
  entity_inst_t to = m->get_source_inst();
  to.name = MSG_ADDR_CLIENT(from);
  mon->mdsmon->send_latest(to);
  mon->osdmon->send_latest(to);
  delete m;
}

void ClientMonitor::handle_client_unmount(MClientUnmount *m)
{
  dout(7) << "client_unmount from " << m->get_source()
	  << " at " << m->get_source_inst() << endl;
  assert(m->get_source().is_client());
  int from = m->get_source().num();

  if (client_map.count(from)) {
    client_map.erase(from);

    if (client_map.empty()) {
      dout(1) << "last client unmounted" << endl;
      if (g_conf.mds_shutdown_on_last_unmount) 
	mon->do_stop();
    }
  }

  // reply with (same) unmount message to ack
  mon->messenger->send_message(m, m->get_source_inst());
}



/*
void ClientMonitor::handle_mds_shutdown(Message *m)
{
  assert(m->get_source().is_mds());
  int from = m->get_source().num();

  mdsmap.mds_inst.erase(from);
  mdsmap.all_mds.erase(from);

  dout(7) << "mds_shutdown from " << m->get_source() 
	  << ", still have " << mdsmap.all_mds
	  << endl;
  
  // tell someone?
  // fixme
  
  delete m;
}

*/

/*
void ClientMonitor::bcast_latest_mds()
{
  dout(10) << "bcast_latest_mds " << mdsmap.get_epoch() << endl;
  
  // tell mds
  for (set<int>::iterator p = mdsmap.get_mds().begin();
       p != mdsmap.get_mds().end();
       p++) {
    if (mdsmap.is_down(*p)) continue;
    send_full(MSG_ADDR_MDS(*p), mdsmap.get_inst(*p));
  }
}

*/
