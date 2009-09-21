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

#ifndef __AUTHMONITOR_H
#define __AUTHMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"
#include "PaxosService.h"
#include "mon/Monitor.h"

#include "include/AuthLibrary.h"
#include "auth/KeysServer.h"

class MMonCommand;
class MAuthMon;
class MAuthRotating;

class AuthMonitor : public PaxosService {
  void auth_usage(stringstream& ss);
  vector<AuthLibIncremental> pending_auth;
  KeysServer keys_server;
  version_t last_rotating_ver;

  void on_active();

  void create_initial(bufferlist& bl);
  bool update_from_paxos();
  void create_pending();  // prepare a new pending
  void encode_pending(bufferlist &bl);  // propose pending update to peers

  void committed();

  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);

  bool preprocess_auth(MAuthMon *m);
  bool prepare_auth(MAuthMon *m);
  void _updated_auth(MAuthMon *m, entity_inst_t who);

  struct C_Auth : public Context {
    AuthMonitor *authmon;
    MAuthMon *ack;
    entity_inst_t who;
    C_Auth(AuthMonitor *p, MAuthMon *a, entity_inst_t w) : authmon(p), ack(a), who(w) {}
    void finish(int r) {
      authmon->_updated_auth(ack, who);
    }    
  };

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);
  bool store_entry(AuthLibEntry& entry);

  void check_rotate();
 public:
  AuthMonitor(Monitor *mn, Paxos *p) : PaxosService(mn, p), last_rotating_ver(0) { }
  void handle_request(MAuthMon *m);
  void handle_request(MAuthRotating *m);
  
  void tick();  // check state, take actions
};

#endif
