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



#include "TCPDirectory.h"

#include "messages/MNSConnect.h"
#include "messages/MNSConnectAck.h"
#include "messages/MNSRegister.h"
#include "messages/MNSRegisterAck.h"
#include "messages/MNSLookup.h"
#include "messages/MNSLookupReply.h"
//#include "messages/MNSUnregister.h"

#include "config.h"
#undef dout
#define dout(x)   if (x <= g_conf.debug || x <= g_conf.debug_ns) cout << "nameserver: " 

void tcp_open(int rank);


void TCPDirectory::handle_connect(MNSConnect *m)
{
  int rank = nrank++;
  dout(2) << "connect from new rank " << rank << " " << m->get_addr() << endl;

  dir[MSG_ADDR_RANK(rank)] = rank;
  messenger->map_entity_rank(MSG_ADDR_RANK(rank), rank);

  rank_addr[rank] = m->get_addr();
  messenger->map_rank_addr(rank, m->get_addr());

  messenger->send_message(new MNSConnectAck(rank),
                          MSG_ADDR_RANK(rank));
  delete m;
}



void TCPDirectory::handle_register(MNSRegister *m)
{
  dout(10) << "register from rank " << m->get_rank() << " addr " << MSG_ADDR_NICE(m->get_entity()) << endl;
  
  // pick id
  int rank = m->get_rank();
  entity_name_t entity = m->get_entity();

  if (entity.is_new()) {
    // make up a new address!
    switch (entity.type()) {
      
    case MSG_ADDR_RANK_BASE:         // stupid client should be able to figure this out
      entity = MSG_ADDR_RANK(rank);
      break;
      
    case MSG_ADDR_MDS_BASE:
      entity = MSG_ADDR_MDS(nmds++);
      break;
      
    case MSG_ADDR_OSD_BASE:
      entity = MSG_ADDR_OSD(nosd++);
      break;
      
    case MSG_ADDR_CLIENT_BASE:
      entity = MSG_ADDR_CLIENT(nclient++);
      break;
      
    default:
      assert(0);
    }
  } else {
    // specific address!
    assert(dir.count(entity) == 0);  // make sure it doesn't exist yet.
  }

  dout(2) << "registered " << MSG_ADDR_NICE(entity) << endl;

  // register
  dir[entity] = rank;
  
  if (entity == MSG_ADDR_RANK(rank))   // map this locally now so we can reply
    messenger->map_entity_rank(entity, rank);  // otherwise wait until they send STARTED msg

  hold.insert(entity);

  ++version;
  update_log[version] = entity;

  // reply w/ new id
  messenger->send_message(new MNSRegisterAck(m->get_tid(), entity), 
                          MSG_ADDR_RANK(rank));
  delete m;
}

void TCPDirectory::handle_started(Message *m)
{
  entity_name_t entity = m->get_source();

  dout(3) << "start signal from " << MSG_ADDR_NICE(entity) << endl;
  hold.erase(entity);
  messenger->map_entity_rank(entity, dir[entity]);

  // waiters?
  if (waiting.count(entity)) {
    list<Message*> ls;
    ls.splice(ls.begin(), waiting[entity]);
    waiting.erase(entity);

    dout(10) << "doing waiter on " << MSG_ADDR_NICE(entity) << endl;
    for (list<Message*>::iterator it = ls.begin();
         it != ls.end();
         it++) {
      dispatch(*it);
    }
  }
}

void TCPDirectory::handle_unregister(Message *m)
{
  entity_name_t who = m->get_source();
  dout(2) << "unregister from entity " << MSG_ADDR_NICE(who) << endl;
  
  assert(dir.count(who));
  dir.erase(who);
  
  // shutdown?
  if (dir.size() <= 2) {
    dout(2) << "dir is empty except for me, shutting down" << endl;
    tcpmessenger_stop_nameserver();
  }
  else {
    if (0) {
      dout(10) << "dir size now " << dir.size() << endl;
      for (hash_map<entity_name_t, int>::iterator it = dir.begin();
           it != dir.end();
           it++) {
        dout(10) << " dir: " << MSG_ADDR_NICE(it->first) << " on rank " << it->second << endl;
      }
    }
  }

}


void TCPDirectory::handle_lookup(MNSLookup *m) 
{
  // have it?
  if (dir.count(m->get_entity()) == 0 ||
      hold.count(m->get_entity())) {
    dout(2) << MSG_ADDR_NICE(m->get_source()) << " lookup '" << MSG_ADDR_NICE(m->get_entity()) << "' -> dne or on hold" << endl;
    waiting[m->get_entity()].push_back(m);
    return;
  }

  // look it up!  
  MNSLookupReply *reply = new MNSLookupReply(m);

  int rank = dir[m->get_entity()];
  reply->entity_map[m->get_entity()] = rank;
  reply->rank_addr[rank] = rank_addr[rank];

  dout(2) << MSG_ADDR_NICE(m->get_source()) << " lookup '" << MSG_ADDR_NICE(m->get_entity()) << "' -> rank " << rank << endl;

  messenger->send_message(reply,
                          m->get_source(), m->get_source_port());
  delete m;
}
