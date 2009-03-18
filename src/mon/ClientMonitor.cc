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

#include "messages/MMonMap.h"
#include "messages/MClientMount.h"
#include "messages/MClientUnmount.h"
#include "messages/MMonCommand.h"

#include "common/Timer.h"

#include "auth/ExportControl.h"

#include <sstream>

#include "config.h"

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(mon, client_map)
static ostream& _prefix(Monitor *mon, ClientMap& client_map) {
  return *_dout << dbeginl
		<< "mon" << mon->whoami
		<< (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)")))
		<< ".client v" << client_map.version << " ";
}


bool ClientMonitor::update_from_paxos()
{
  assert(paxos->is_active());
  
  version_t paxosv = paxos->get_version();
  if (paxosv == client_map.version) return true;
  assert(paxosv >= client_map.version);

  dout(10) << "update_from_paxos paxosv " << paxosv 
	   << ", my v " << client_map.version << dendl;


  if (client_map.version == 0 && paxosv > 1) {
    // starting up: load latest
    bufferlist latest;
    version_t v = paxos->get_latest(latest);
    if (v) {
      dout(7) << "update_from_paxos startup: loaded latest full clientmap" << dendl;
      bufferlist::iterator p = latest.begin();
      client_map.decode(p);
    }
  } 

  // walk through incrementals
  while (paxosv > client_map.version) {
    bufferlist bl;
    bool success = paxos->read(client_map.version+1, bl);
    assert(success);

    dout(7) << "update_from_paxos  applying incremental " << client_map.version+1 << dendl;
    ClientMap::Incremental inc;
    bufferlist::iterator p = bl.begin();
    inc.decode(p);
    client_map.apply_incremental(inc);
    
    dout(1) << client_map.client_info.size() << " clients (+" 
	    << inc.mount.size() << " -" << inc.unmount.size() << ")" 
	    << dendl;
  }

  assert(paxosv == client_map.version);

  // save latest
  bufferlist bl;
  client_map.encode(bl);
  paxos->stash_latest(paxosv, bl);
  mon->store->put_int(paxosv, "clientmap", "last_consumed");

  return true;
}

void ClientMonitor::create_pending()
{
  pending_inc = ClientMap::Incremental();
  pending_inc.version = client_map.version + 1;
  pending_inc.next_client = client_map.next_client;
  dout(10) << "create_pending v " << pending_inc.version
	   << ", next is " << pending_inc.next_client
	   << dendl;
}

void ClientMonitor::create_initial(bufferlist& bl)
{
  dout(10) << "create_initial -- creating initial map" << dendl;
}

void ClientMonitor::committed()
{

}


void ClientMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending v " << pending_inc.version 
	   << ", next is " << pending_inc.next_client
	   << dendl;
  assert(paxos->get_version() + 1 == pending_inc.version);
  pending_inc.encode(bl);
}


// -------

bool ClientMonitor::check_mount(MClientMount *m)
{
    // already mounted?
    entity_addr_t addr = m->get_orig_source_addr();
    ExportControl *ec = conf_get_export_control();
    if (ec && (!ec->is_authorized(&addr, "/"))) {
      dout(0) << "client is not authorized to mount" << dendl;
      return true;
    }
    if (client_map.addr_client.count(addr)) {
	int client = client_map.addr_client[addr];
	dout(7) << " client" << client << " already mounted" << dendl;
	_mounted(client, m);
	return true;
    }

    return false;
}

bool ClientMonitor::preprocess_query(Message *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_MOUNT:
	return check_mount((MClientMount *)m);
    
  case CEPH_MSG_CLIENT_UNMOUNT:
    {
      // already unmounted?
      int client = m->get_orig_source().num();
      if (client_map.client_info.count(client) == 0) {
	dout(7) << " client" << client << " not mounted" << dendl;
	_unmounted((MClientUnmount*)m);
	return true;
      }
    }
    return false;
    
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

  default:
    assert(0);
    delete m;
    return true;
  }
}

bool ClientMonitor::prepare_update(Message *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  
  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_MOUNT:
    {
      entity_addr_t addr = m->get_orig_source_addr();
      int client = -1;

      if (m->get_orig_source().is_client())
	client = m->get_orig_source().num();

      // choose a client id
      if (client < 0) {
	client = pending_inc.next_client;
	dout(10) << "mount: assigned client" << client << " to " << addr << dendl;
      } else {
	dout(10) << "mount: client" << client << " requested by " << addr << dendl;
	if (client_map.client_info.count(client)) {
	  assert(client_map.client_info[client].addr != addr);
	  dout(0) << "mount: WARNING: client" << client << " requested by " << addr
		  << ", which used to be "  << client_map.client_info[client].addr << dendl;
	}
      }
      
      client_info_t info;
      info.addr = addr;
      info.mount_time = g_clock.now();
      pending_inc.add_mount(client, info);
      paxos->wait_for_commit(new C_Mounted(this, client, (MClientMount*)m));
    }
    return true;

  case CEPH_MSG_CLIENT_UNMOUNT:
    {
      assert(m->get_orig_source().is_client());
      int client = m->get_orig_source().num();

      assert(client_map.client_info.count(client));
      
      pending_inc.add_unmount(client);
      paxos->wait_for_commit(new C_Unmounted(this, (MClientUnmount*)m));
    }
    return true;
  

  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);

  default:
    assert(0);
    delete m;
    return false;
  }

}


// COMMAND

bool ClientMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stat") {
      ss << client_map;
      r = 0;
    }
    else if (m->cmd[1] == "getmap") {
      client_map.encode(rdata);
      ss << "got clientmap version " << client_map.version;
      r = 0;
    }
    else if (m->cmd[1] == "dump") {
      ss << "version " << client_map.version << std::endl;
      ss << "next_client " << client_map.next_client << std::endl;
      for (map<uint32_t, client_info_t>::iterator p = client_map.client_info.begin();
	   p != client_map.client_info.end();
	   p++) {
	ss << "client" << p->first
	   << "\t" << p->second.addr
	   << "\t" << p->second.mount_time
	   << std::endl;
      }
      while (!ss.eof()) {
	string s;
	getline(ss, s);
	rdata.append(s.c_str(), s.length());
	rdata.append("\n", 1);
      }
      ss << "ok";
      r = 0;
    }
  }

  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata);
    return true;
  } else
    return false;
}


bool ClientMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  string rs;
  int err = -EINVAL;

  // nothing here yet
  ss << "unrecognized command";

  getline(ss, rs);
  mon->reply_command(m, err, rs);
  return false;
}


// MOUNT


void ClientMonitor::_mounted(int client, MClientMount *m)
{
  entity_inst_t to;
  to.addr = m->get_orig_source_addr();
  to.name = entity_name_t::CLIENT(client);

  dout(10) << "_mounted client" << client << " at " << to << dendl;
  
  // reply with latest mon, mds, osd maps
  bufferlist bl;
  mon->monmap->encode(bl);
  mon->messenger->send_message(new MMonMap(bl), to);

  mon->mdsmon()->send_latest(to);
  mon->osdmon()->send_latest(to);

  delete m;
}

void ClientMonitor::_unmounted(MClientUnmount *m)
{
  dout(10) << "_unmounted " << m->get_orig_source_inst() << dendl;
  
  // reply with (same) unmount message
  mon->messenger->send_message(m, m->get_orig_source_inst());

  // auto-shutdown?
  // (hack for fakesyn/newsyn, mostly)
  if (mon->is_leader() &&
      client_map.version > 1 &&
      client_map.client_info.empty() && 
      g_conf.mon_stop_on_last_unmount &&
      !mon->is_stopping()) {
    dout(1) << "last client unmounted" << dendl;
    mon->stop_cluster();
  }
}

void ClientMonitor::tick()
{
  if (!paxos->is_active()) return;

  update_from_paxos();
  dout(10) << client_map << dendl;

  if (!mon->is_leader()) return;

  // ...
}
