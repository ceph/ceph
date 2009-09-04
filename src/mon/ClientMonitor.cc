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
#include "messages/MClientMountAck.h"
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
  version_t paxosv = paxos->get_version();
  if (paxosv == client_map.version) return true;
  assert(paxosv >= client_map.version);

  dout(10) << "update_from_paxos paxosv " << paxosv 
	   << ", my v " << client_map.version << dendl;

  bufferlist bl;
  bool success = paxos->read(paxosv, bl);
  assert(success);
 
  bufferlist::iterator p = bl.begin();
  client_map.decode(p);

  assert(paxosv == client_map.version);

  paxos->stash_latest(paxosv, bl);

  if (next_client < 0) {
    dout(10) << "in-core next_client reset to " << client_map.next_client << dendl;
    next_client = client_map.next_client;
  }

  return true;
}

void ClientMonitor::on_election_start()
{
  dout(10) << "in-core next_client cleared" << dendl;
  next_client = -1;
}

void ClientMonitor::create_pending()
{
  pending_map = client_map;
  pending_map.version = client_map.version + 1;
  dout(10) << "create_pending" << dendl;
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
  dout(10) << "encode_pending v " << pending_map.version 
	   << ", next is " << pending_map.next_client
	   << dendl;
  assert(paxos->get_version() + 1 == pending_map.version);
  pending_map.encode(bl);
}


// -------

bool ClientMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_MOUNT:
    return preprocess_mount((MClientMount *)m);
    
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

  default:
    assert(0);
    delete m;
    return true;
  }
}

bool ClientMonitor::prepare_update(PaxosServiceMessage *m)
{
  stringstream ss;
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  
  switch (m->get_type()) {
  case CEPH_MSG_CLIENT_MOUNT:
    return prepare_mount((MClientMount*)m);

  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);

  default:
    assert(0);
    delete m;
    return false;
  }

}


// MOUNT

bool ClientMonitor::preprocess_mount(MClientMount *m)
{
  stringstream ss;
  
  // allowed?
  entity_addr_t addr = m->get_orig_source_addr();
  ExportControl *ec = conf_get_export_control();
  if (ec && (!ec->is_authorized(&addr, "/"))) {
    dout(0) << "client is not authorized to mount" << dendl;
    ss << "client " << addr << " is not authorized to mount";
    mon->get_logclient()->log(LOG_SEC, ss);
    
    string s;
    getline(ss, s);
    mon->messenger->send_message(new MClientMountAck(-1, -EPERM, s.c_str()),
				 m->get_orig_source_inst());
    return true;
  }
  
  // fast mount?
  if (mon->is_leader() &&
      next_client < client_map.next_client) {
    client_t client = next_client;
    next_client.v++;

    dout(10) << "preprocess_mount fast mount client" << client << ", in-core next is " << next_client
	     << ", paxos next is " << pending_map.next_client << dendl;
    _mounted(client, m);
    
    if (next_client == pending_map.next_client) {
      pending_map.next_client.v += g_conf.mon_clientid_prealloc;
      propose_pending();
    }

    return true;
  }      
  
  return false;
}

bool ClientMonitor::prepare_mount(MClientMount *m)
{
  stringstream ss;
  entity_addr_t addr = m->get_orig_source_addr();

  assert(next_client <= client_map.next_client);

  client_t client = next_client;
  next_client.v++;
  
  pending_map.next_client.v = next_client.v + g_conf.mon_clientid_prealloc;
  
  dout(10) << "mount: assigned client" << client << " to " << addr << dendl;
  
  paxos->wait_for_commit(new C_Mounted(this, client, (MClientMount*)m));
  ss << "client" << client << " " << addr << " mounted";
  mon->get_logclient()->log(LOG_INFO, ss);
  return true;
}

void ClientMonitor::_mounted(client_t client, MClientMount *m)
{
  entity_inst_t to;
  to.addr = m->get_orig_source_addr();
  to.name = entity_name_t::CLIENT(client.v);

  dout(10) << "_mounted client" << client << " at " << to << dendl;
  
  // reply with client ticket
  MClientMountAck *ack = new MClientMountAck;
  ack->client = client;
  mon->monmap->encode(ack->monmap_bl);

  mon->send_reply(m, ack, to);

  // also send latest mds and osd maps
  //mon->mdsmon()->send_latest(to);
  //mon->osdmon()->send_latest(to);

  delete m;
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
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
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
  mon->reply_command(m, err, rs, paxos->get_version());
  return false;
}


bool ClientMonitor::should_propose(double& delay)
{
  return true;  // never delay!  we want fast mounts!
}

void ClientMonitor::tick()
{
  if (!paxos->is_active()) return;

  update_from_paxos();
  dout(10) << client_map << dendl;

  if (!mon->is_leader()) return;

  // ...
}
