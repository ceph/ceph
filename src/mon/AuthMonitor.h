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

#include "messages/MMonGlobalID.h"
#include "messages/MMonGlobalIDReply.h"

class MMonCommand;
class MAuth;
class MAuthMon;

#define MIN_GLOBAL_ID 0x1000

class AuthMonitor : public PaxosService {
  void auth_usage(stringstream& ss);
  enum IncType {
    GLOBAL_ID,
    AUTH_DATA,
  };
public:
  struct Incremental {
    IncType inc_type;
    uint64_t max_global_id;
    uint32_t auth_type;
    bufferlist auth_data;

    void encode(bufferlist& bl) const {
      __u32 _type = (__u32)inc_type;
      ::encode(_type, bl);
      if (_type == GLOBAL_ID) {
	::encode(max_global_id, bl);
      } else {
	::encode(auth_type, bl);
	::encode(auth_data, bl);
      }
    }
    void decode(bufferlist::iterator& bl) {
      __u32 _type;
      ::decode(_type, bl);
      inc_type = (IncType)_type;
      assert(inc_type >= GLOBAL_ID && inc_type <= AUTH_DATA);
      if (_type == GLOBAL_ID) {
	::decode(max_global_id, bl);
      } else {
	::decode(auth_type, bl);
	::decode(auth_data, bl);
      }
    }
  };

private:
  vector<Incremental> pending_auth;
  version_t last_rotating_ver;
  uint64_t max_global_id;
  uint64_t last_allocated_id;

  void push_cephx_inc(KeyServerData::Incremental& auth_inc) {
    Incremental inc;
    inc.inc_type = AUTH_DATA;
    ::encode(auth_inc, inc.auth_data);
    inc.auth_type = CEPH_AUTH_CEPHX;
    pending_auth.push_back(inc);
  }

  void on_active();
  void election_finished();

  void create_initial(bufferlist& bl);
  bool update_from_paxos();
  void create_pending();  // prepare a new pending
  bool prepare_global_id(MMonGlobalID *m);
  void increase_max_global_id();
  uint64_t assign_global_id(MAuth *m);
  void encode_pending(bufferlist &bl);  // propose pending update to peers

  void committed();

  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);

  bool preprocess_auth(MAuth *m);

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  uint64_t assign_next_global_id();

  void check_rotate();
 public:
  AuthMonitor(Monitor *mn, Paxos *p) : PaxosService(mn, p), last_rotating_ver(0), max_global_id(-1), last_allocated_id(-1) {}
  void pre_auth(MAuth *m);
  
  void tick();  // check state, take actions

  void init();
};


WRITE_CLASS_ENCODER(AuthMonitor::Incremental);

#endif
