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


#include "ClientMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"
#include "OSDMonitor.h"
#include "MonitorStore.h"

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
  case MSG_CLIENT_UNMOUNT:
    handle_query(m);
    break;
       
  default:
    assert(0);
  }  
}


void ClientMonitor::handle_query(Message *m)
{
  dout(10) << "handle_query " << *m << " from " << m->get_source_inst() << endl;
  
  // make sure our map is readable and up to date
  if (!paxos->is_readable() ||
      !update_from_paxos()) {
    dout(10) << " waiting for paxos -> readable" << endl;
    paxos->wait_for_readable(new C_RetryMessage(this, m));
    return;
  }

  // preprocess
  if (preprocess_update(m)) 
    return;  // easy!

  // leader?
  if (!mon->is_leader()) {
    // fw to leader
    dout(10) << " fw to leader mon" << mon->get_leader() << endl;
    mon->messenger->send_message(m, mon->monmap->get_inst(mon->get_leader()));
    return;
  }
  
  // writeable?
  if (!paxos->is_writeable()) {
    dout(10) << " waiting for paxos -> writeable" << endl;
    paxos->wait_for_writeable(new C_RetryMessage(this, m));
    return;
  }

  prepare_update(m);

  // do it now (for now!) ***
  propose_pending();
}

bool ClientMonitor::update_from_paxos()
{
  assert(paxos->is_active());
  version_t paxosv = paxos->get_version();
  dout(10) << "update_from_paxos paxosv " << paxosv 
	   << ", my v " << client_map.version << endl;

  assert(paxosv >= client_map.version);
  while (paxosv > client_map.version) {
    bufferlist bl;
    bool success = paxos->read(client_map.version+1, bl);
    if (success) {
      dout(10) << "update_from_paxos  applying incremental " << client_map.version+1 << endl;
      Incremental inc;
      int off = 0;
      inc._decode(bl, off);
      client_map.apply_incremental(inc);

    } else {
      dout(10) << "update_from_paxos  couldn't read incremental " << client_map.version+1 << endl;
      return false;
    }

    // save latest
    bl.clear();
    client_map._encode(bl);
    mon->store->put_bl_ss(bl, "clientmap", "latest");

    // prepare next inc
    prepare_pending();
  }

  return true;
}

void ClientMonitor::prepare_pending()
{
  pending_inc = Incremental();
  pending_inc.version = client_map.version + 1;
  pending_inc.next_client = client_map.next_client;
  dout(10) << "prepare_pending v " << pending_inc.version
	   << ", next is " << pending_inc.next_client
	   << endl;
}

void ClientMonitor::propose_pending()
{
  dout(10) << "propose_pending v " << pending_inc.version 
	   << ", next is " << pending_inc.next_client
	   << endl;
  
  // apply to paxos
  assert(paxos->get_version() + 1 == pending_inc.version);
  bufferlist bl;
  pending_inc._encode(bl);
  paxos->propose_new_value(bl, new C_Commit(this));
}


// -------


bool ClientMonitor::preprocess_update(Message *m)
{
  dout(10) << "preprocess_update " << *m << " from " << m->get_source_inst() << endl;

  switch (m->get_type()) {
  case MSG_CLIENT_MOUNT:
    {
      // already mounted?
      entity_addr_t addr = m->get_source_addr();
      if (client_map.addr_client.count(addr)) {
	int client = client_map.addr_client[addr];
	dout(7) << " client" << client << " already mounted" << endl;
	_mounted(client, m);
	return true;
      }
    }
    return false;
    
  case MSG_CLIENT_UNMOUNT:
    {
      // already unmounted?
      int client = m->get_source().num();
      if (client_map.client_addr.count(client) == 0) {
	dout(7) << " client" << client << " not mounted" << endl;
	_unmounted(m);
	return true;
      }
    }
    return false;
    

  default:
    assert(0);
    delete m;
    return true;
  }
}

void ClientMonitor::prepare_update(Message *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_source_inst() << endl;

  int client = m->get_source().num();
  entity_addr_t addr = m->get_source_addr();

  switch (m->get_type()) {
  case MSG_CLIENT_MOUNT:
    {
      // choose a client id
      if (client < 0 || 
	  (client_map.client_addr.count(client) && 
	   client_map.client_addr[client] != addr)) {
	client = pending_inc.next_client;
	dout(10) << "mount: assigned client" << client << " to " << addr << endl;
      } else {
	dout(10) << "mount: client" << client << " requested by " << addr << endl;
      }
      
      pending_inc.add_mount(client, addr);
      pending_commit.push_back(new C_Mounted(this, client, m));
    }
    break;

  case MSG_CLIENT_UNMOUNT:
    {
      assert(client_map.client_addr.count(client));
      
      pending_inc.add_unmount(client);
      pending_commit.push_back(new C_Unmounted(this, m));
    }
    break;

  default:
    assert(0);
    delete m;
  }
}


// MOUNT


void ClientMonitor::_mounted(int client, Message *m)
{
  entity_inst_t to = m->get_source_inst();
  to.name = MSG_ADDR_CLIENT(client);

  dout(10) << "_mounted client" << client << " at " << to << endl;
  
  // reply with latest mds, osd maps
  mon->mdsmon->send_latest(to);
  mon->osdmon->send_latest(to);

  delete m;
}

void ClientMonitor::_unmounted(Message *m)
{
  dout(10) << "_unmounted " << m->get_source() << endl;
  
  // reply with (same) unmount message
  mon->messenger->send_message(m, m->get_source_inst());

  // auto-shutdown?
  if (update_from_paxos() &&
      mon->is_leader() &&
      client_map.version > 1 &&
      client_map.client_addr.empty() && 
      g_conf.mds_shutdown_on_last_unmount) {
    dout(1) << "last client unmounted" << endl;
    mon->do_stop();
  }
}


void ClientMonitor::_commit(int r)
{
  if (r >= 0) {
    dout(10) << "_commit success" << endl;
    finish_contexts(pending_commit);
  } else {
    dout(10) << "_commit failed" << endl;
  }

  finish_contexts(pending_commit, r);
}

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


void ClientMonitor::create_initial()
{
  dout(10) << "create_initial" << endl;

  if (!mon->is_leader()) return;
  if (paxos->get_version() > 0) return;

  if (paxos->is_writeable()) {
    dout(1) << "create_initial -- creating initial map" << endl;
    prepare_pending();
    propose_pending();
  } else {
    dout(1) << "create_initial -- waiting for writeable" << endl;
    paxos->wait_for_writeable(new C_CreateInitial(this));
  }
}


void ClientMonitor::election_finished()
{
 
  if (mon->is_leader() && g_conf.mkfs)
    create_initial();
}
