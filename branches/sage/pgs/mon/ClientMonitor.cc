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



bool ClientMonitor::update_from_paxos()
{
  assert(paxos->is_active());
  
  version_t paxosv = paxos->get_version();
  dout(10) << "update_from_paxos paxosv " << paxosv 
	   << ", my v " << client_map.version << endl;

  if (paxosv == client_map.version) return true;
  assert(paxosv >= client_map.version);

  if (client_map.version == 0 && paxosv > 1 &&
      mon->store->exists_bl_ss("clientmap","latest")) {
    // starting up: load latest
    dout(7) << "update_from_paxos startup: loading latest full clientmap" << endl;
    bufferlist bl;
    mon->store->get_bl_ss(bl, "clientmap", "latest");
    int off = 0;
    client_map._decode(bl, off);
  } 

  // walk through incrementals
  while (paxosv > client_map.version) {
    bufferlist bl;
    bool success = paxos->read(client_map.version+1, bl);
    if (success) {
      dout(7) << "update_from_paxos  applying incremental " << client_map.version+1 << endl;
      Incremental inc;
      int off = 0;
      inc._decode(bl, off);
      client_map.apply_incremental(inc);
      
    } else {
      dout(7) << "update_from_paxos  couldn't read incremental " << client_map.version+1 << endl;
      return false;
    }
  }

  // save latest
  bufferlist bl;
  client_map._encode(bl);
  mon->store->put_bl_ss(bl, "clientmap", "latest");

  return true;
}

void ClientMonitor::create_pending()
{
  assert(mon->is_leader());
  pending_inc = Incremental();
  pending_inc.version = client_map.version + 1;
  pending_inc.next_client = client_map.next_client;
  dout(10) << "create_pending v " << pending_inc.version
	   << ", next is " << pending_inc.next_client
	   << endl;
}

void ClientMonitor::create_initial()
{
  dout(1) << "create_initial -- creating initial map" << endl;
}



void ClientMonitor::encode_pending(bufferlist &bl)
{
  assert(mon->is_leader());
  dout(10) << "encode_pending v " << pending_inc.version 
	   << ", next is " << pending_inc.next_client
	   << endl;
  
  assert(paxos->get_version() + 1 == pending_inc.version);
  pending_inc._encode(bl);
}


// -------


bool ClientMonitor::preprocess_query(Message *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_source_inst() << endl;

  switch (m->get_type()) {
  case MSG_CLIENT_MOUNT:
    {
      // already mounted?
      entity_addr_t addr = m->get_source_addr();
      if (client_map.addr_client.count(addr)) {
	int client = client_map.addr_client[addr];
	dout(7) << " client" << client << " already mounted" << endl;
	_mounted(client, (MClientMount*)m);
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
	_unmounted((MClientUnmount*)m);
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

bool ClientMonitor::prepare_update(Message *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_source_inst() << endl;
  
  switch (m->get_type()) {
  case MSG_CLIENT_MOUNT:
    {
      MClientMount *mount = (MClientMount*)m;
      entity_addr_t addr = mount->addr;
      int client = -1;
      if (mount->get_source().is_client())
	client = mount->get_source().num();

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
      paxos->wait_for_commit(new C_Mounted(this, client, mount));
    }
    return true;

  case MSG_CLIENT_UNMOUNT:
    {
      MClientUnmount *unmount = (MClientUnmount*)m;
      assert(unmount->inst.name.is_client());
      int client = unmount->inst.name.num();

      assert(client_map.client_addr.count(client));
      
      pending_inc.add_unmount(client);
      paxos->wait_for_commit(new C_Unmounted(this, unmount));
    }
    return true;
  
  default:
    assert(0);
    delete m;
    return false;
  }

}


// MOUNT


void ClientMonitor::_mounted(int client, MClientMount *m)
{
  entity_inst_t to;
  to.addr = m->addr;
  to.name = MSG_ADDR_CLIENT(client);

  dout(10) << "_mounted client" << client << " at " << to << endl;
  
  // reply with latest mds, osd maps
  mon->mdsmon->send_latest(to);
  mon->osdmon->send_latest(to);

  delete m;
}

void ClientMonitor::_unmounted(MClientUnmount *m)
{
  dout(10) << "_unmounted " << m->inst << endl;
  
  // reply with (same) unmount message
  mon->messenger->send_message(m, m->inst);

  // auto-shutdown?
  // (hack for fakesyn/newsyn, mostly)
  if (mon->is_leader() &&
      client_map.version > 1 &&
      client_map.client_addr.empty() && 
      g_conf.mon_stop_on_last_unmount) {
    dout(1) << "last client unmounted" << endl;
    mon->do_stop();
  }
}


