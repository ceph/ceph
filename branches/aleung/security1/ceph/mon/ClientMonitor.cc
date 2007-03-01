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

#include "messages/MClientBoot.h"
#include "messages/MClientAuthUser.h"
#include "messages/MClientAuthUserAck.h"
#include "messages/MMDSMap.h"
//#include "messages/MMDSFailure.h"

#include "common/Timer.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".client "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".client "




void ClientMonitor::dispatch(Message *m)
{
  switch (m->get_type()) {

  case MSG_CLIENT_BOOT:
    handle_client_boot((MClientBoot*)m);
    break;
  case MSG_CLIENT_AUTH_USER:
    handle_client_auth_user((MClientAuthUser*)m);
    break;
    
    /*
      case MSG_client_FAILURE:
      handle_client_failure((MClientFailure*)m);
      break;
    */
        
  default:
    assert(0);
  }  
}

void ClientMonitor::handle_client_boot(MClientBoot *m)
{
  dout(7) << "client_boot from " << m->get_source() << " at " << m->get_source_inst() << endl;
  assert(m->get_source().is_client());
  int from = m->get_source().num();
  
  // choose a client id
  if (from < 0 || 
      (client_map.count(m->get_source()) && client_map[m->get_source()] != m->get_source_addr())) {
    from = ++num_clients;
    dout(10) << "client_boot assigned client" << from << endl;
  }

  client_map[MSG_ADDR_CLIENT(from)] = m->get_source_addr();

  // reply with latest mds map
  entity_inst_t to = m->get_source_inst();
  to.name = MSG_ADDR_CLIENT(from);
  mon->mdsmon->send_latest(to);
  delete m;
}

void ClientMonitor::handle_client_auth_user(MClientAuthUser *m)
{
  dout(7) << "client_auth_user from " << m->get_source() << " at " << m->get_source_inst() << endl;
  assert(m->get_source().is_client());
  //int from = m->get_source().num();

  Ticket *userTicket;
  
  // grab information
  uid_t uid = m->get_uid();
  // do we have a ticket already?
  // user should be able to make new ticket eventually
  if (user_tickets.count(uid) == 0) {
    gid_t gid = m->get_gid();
    
    // create iv
    byte iv[RJBLOCKSIZE];
    // set generic IV
    memset(iv, 0x01, RJBLOCKSIZE);
    
    // create a ticket
    userTicket = new Ticket(uid, gid, iv, m->get_key());
    
    // sign the ticket
    userTicket->sign_ticket(mon->myPrivKey);
    
    // cache the ticket
    user_tickets[uid] = userTicket;
  }
  else
    userTicket = user_tickets[uid];
  // reply to auth_user
  messenger->send_message(new MClientAuthUserAck(userTicket),
			  m->get_source_inst());
  
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
