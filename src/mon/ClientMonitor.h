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
 
/*
 * The Client Monitor is used for traking the filesystem's clients.
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
#include "ClientMap.h"

class Monitor;
class Paxos;
class MClientMount;
class MClientUnmount;
class MMonCommand;


class ClientMonitor : public PaxosService {
public:

  class C_Mounted : public Context {
    ClientMonitor *cmon;
    client_t client;
    MClientMount *m;
  public:
    C_Mounted(ClientMonitor *cm, client_t c, MClientMount *m_) : 
      cmon(cm), client(c), m(m_) {}
    void finish(int r) {
      if (r >= 0)
	cmon->_mounted(client, m);
      else
	cmon->dispatch((PaxosServiceMessage*)m);
    }
  };

  ClientMap client_map, pending_map;

private:
  // leader

  void create_initial(bufferlist& bl);
  bool update_from_paxos();
  void create_pending();  // prepare a new pending
  void encode_pending(bufferlist &bl);  // propose pending update to peers

  void committed();

  bool check_mount(MClientMount *m);
  void _mounted(client_t c, MClientMount *m);
 
  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);

  bool preprocess_command(MMonCommand *m);  // true if processed.
  bool prepare_command(MMonCommand *m);

  bool should_propose(double& delay);

 public:
  ClientMonitor(Monitor *mn, Paxos *p) : PaxosService(mn, p) { }
  
  void tick();  // check state, take actions

};

#endif
