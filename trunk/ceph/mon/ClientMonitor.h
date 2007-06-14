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

#ifndef __CLIENTMONITOR_H
#define __CLIENTMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "mds/MDSMap.h"

#include "PaxosService.h"

class Monitor;
class Paxos;

class ClientMonitor : public PaxosService {
public:

  struct Incremental {
    version_t version;
    uint32_t next_client;
    map<int32_t, entity_addr_t> mount;
    set<int32_t> unmount;
    
    Incremental() : version(0), next_client() {}

    bool is_empty() { return mount.empty() && unmount.empty(); }
    void add_mount(uint32_t client, entity_addr_t addr) {
      next_client = MAX(next_client, client+1);
      mount[client] = addr;
    }
    void add_unmount(uint32_t client) {
      assert(client < next_client);
      if (mount.count(client))
	mount.erase(client);
      else
	unmount.insert(client);
    }
    
    void _encode(bufferlist &bl) {
      ::_encode(version, bl);
      ::_encode(next_client, bl);
      ::_encode(mount, bl);
      ::_encode(unmount, bl);
    }
    void _decode(bufferlist &bl, int& off) {
      ::_decode(version, bl, off);
      ::_decode(next_client, bl, off);
      ::_decode(mount, bl, off);
      ::_decode(unmount, bl, off);
    }
  };

  struct Map {
    version_t version;
    uint32_t next_client;
    map<uint32_t,entity_addr_t> client_addr;
    hash_map<entity_addr_t,uint32_t> addr_client;

    Map() : version(0), next_client(0) {}

    void reverse() {
      addr_client.clear();
      for (map<uint32_t,entity_addr_t>::iterator p = client_addr.begin();
	   p != client_addr.end();
	   ++p) {
	addr_client[p->second] = p->first;
      }
    }
    void apply_incremental(Incremental &inc) {
      assert(inc.version == version+1);
      version = inc.version;
      next_client = inc.next_client;
      for (map<int32_t, entity_addr_t>::iterator p = inc.mount.begin();
	   p != inc.mount.end();
	   ++p) {
	client_addr[p->first] = p->second;
	addr_client[p->second] = p->first;
      }
	
      for (set<int32_t>::iterator p = inc.unmount.begin();
	   p != inc.unmount.end();
	   ++p) {
	assert(client_addr.count(*p));
	addr_client.erase(client_addr[*p]);
	client_addr.erase(*p);
      }
    }

    void _encode(bufferlist &bl) {
      ::_encode(version, bl);
      ::_encode(next_client, bl);
      ::_encode(client_addr, bl);
    }
    void _decode(bufferlist &bl, int& off) {
      ::_decode(version, bl, off);
      ::_decode(next_client, bl, off);
      ::_decode(client_addr, bl, off);
      reverse();
    }
  };

  class C_CreateInitial : public Context {
    ClientMonitor *cmon;
  public:
    C_CreateInitial(ClientMonitor *cm) : cmon(cm) {}
    void finish(int r) {
      cmon->create_initial();
    }
  };

  class C_Mounted : public Context {
    ClientMonitor *cmon;
    int client;
    Message *m;
  public:
    C_Mounted(ClientMonitor *cm, int c, Message *m_) : 
      cmon(cm), client(c), m(m_) {}
    void finish(int r) {
      if (r >= 0)
	cmon->_mounted(client, m);
      else
	cmon->dispatch(m);
    }
  };

  class C_Unmounted : public Context {
    ClientMonitor *cmon;
    Message *m;
  public:
    C_Unmounted(ClientMonitor *cm, Message *m_) : 
      cmon(cm), m(m_) {}
    void finish(int r) {
      if (r >= 0)
	cmon->_unmounted(m);
      else
	cmon->dispatch(m);
    }
  };

  class C_Commit : public Context {
    ClientMonitor *cmon;
  public:
    C_Commit(ClientMonitor *cm) :
      cmon(cm) {}
    void finish(int r) {
      cmon->_commit(r);
    }
  };

private:
  Map client_map;
  list<Message*> waiting_for_active;

  // leader
  Incremental pending_inc;
  list<Context*> pending_commit;   // contributers to pending_inc

  void create_initial();
  bool update_from_paxos();
  void prepare_pending();  // prepare a new pending
  void propose_pending();  // propose pending update to peers

  void _mounted(int c, Message *m);
  void _unmounted(Message *m);
  void _commit(int r);
 
  void handle_query(Message *m);
  bool preprocess_update(Message *m);  // true if processed.
  void prepare_update(Message *m);

  
 public:
  ClientMonitor(Monitor *mn, Paxos *p) : PaxosService(mn, p) { }
  
  //void tick();  // check state, take actions

};

#endif
