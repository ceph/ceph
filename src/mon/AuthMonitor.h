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

#ifndef CEPH_AUTHMONITOR_H
#define CEPH_AUTHMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/ceph_features.h"
#include "include/types.h"
#include "msg/Messenger.h"
#include "mon/PaxosService.h"
#include "mon/Monitor.h"
#include "mon/MonitorDBStore.h"

class MMonCommand;
struct MAuth;
class MAuthMon;
struct MMonGlobalID;
class KeyRing;

#define MIN_GLOBAL_ID 0x1000

class AuthMonitor : public PaxosService {
public:
  enum IncType {
    GLOBAL_ID,
    AUTH_DATA,
  };
  struct Incremental {
    IncType inc_type;
    uint64_t max_global_id;
    uint32_t auth_type;
    bufferlist auth_data;

    Incremental() : inc_type(GLOBAL_ID), max_global_id(0), auth_type(0) {}

    void encode(bufferlist& bl, uint64_t features=-1) const {
      if ((features & CEPH_FEATURE_MONENC) == 0) {
	__u8 v = 1;
	::encode(v, bl);
	__u32 _type = (__u32)inc_type;
	::encode(_type, bl);
	if (_type == GLOBAL_ID) {
	  ::encode(max_global_id, bl);
	} else {
	  ::encode(auth_type, bl);
	  ::encode(auth_data, bl);
	}
	return;
      } 
      ENCODE_START(2, 2, bl);
      __u32 _type = (__u32)inc_type;
      ::encode(_type, bl);
      if (_type == GLOBAL_ID) {
	::encode(max_global_id, bl);
      } else {
	::encode(auth_type, bl);
	::encode(auth_data, bl);
      }
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& bl) {
      DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
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
      DECODE_FINISH(bl);
    }
    void dump(Formatter *f) const {
      f->dump_int("type", inc_type);
      f->dump_int("max_global_id", max_global_id);
      f->dump_int("auth_type", auth_type);
      f->dump_int("auth_data_len", auth_data.length());
    }
    static void generate_test_instances(list<Incremental*>& ls) {
      ls.push_back(new Incremental);
      ls.push_back(new Incremental);
      ls.back()->inc_type = GLOBAL_ID;
      ls.back()->max_global_id = 1234;
      ls.push_back(new Incremental);
      ls.back()->inc_type = AUTH_DATA;
      ls.back()->auth_type = 12;
      ls.back()->auth_data.append("foo");
    }
  };

private:
  vector<Incremental> pending_auth;
  version_t last_rotating_ver;
  uint64_t max_global_id;
  uint64_t last_allocated_id;

  void upgrade_format();

  void export_keyring(KeyRing& keyring);
  void import_keyring(KeyRing& keyring);

  void push_cephx_inc(KeyServerData::Incremental& auth_inc) {
    Incremental inc;
    inc.inc_type = AUTH_DATA;
    ::encode(auth_inc, inc.auth_data);
    inc.auth_type = CEPH_AUTH_CEPHX;
    pending_auth.push_back(inc);
  }

  /* validate mon caps ; don't care about caps for other services as
   * we don't know how to validate them */
  bool valid_caps(const vector<string>& caps, ostream *out) {
    for (vector<string>::const_iterator p = caps.begin();
         p != caps.end(); p += 2) {
      if (!p->empty() && *p != "mon")
        continue;
      MonCap tmp;
      if (!tmp.parse(*(p+1), out))
        return false;
    }
    return true;
  }

  void on_active();
  bool should_propose(double& delay);
  void create_initial();
  void update_from_paxos(bool *need_bootstrap);
  void create_pending();  // prepare a new pending
  bool prepare_global_id(MMonGlobalID *m);
  void increase_max_global_id();
  uint64_t assign_global_id(MAuth *m, bool should_increase_max);
  // propose pending update to peers
  void encode_pending(MonitorDBStore::TransactionRef t);
  virtual void encode_full(MonitorDBStore::TransactionRef t);
  version_t get_trim_to();

  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);

  bool prep_auth(MAuth *m, bool paxos_writable);

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  bool check_rotate();
 public:
  AuthMonitor(Monitor *mn, Paxos *p, const string& service_name)
    : PaxosService(mn, p, service_name),
      last_rotating_ver(0),
      max_global_id(0),
      last_allocated_id(0)
  {}

  void pre_auth(MAuth *m);
  
  void tick();  // check state, take actions

  void dump_info(Formatter *f);
};


WRITE_CLASS_ENCODER_FEATURES(AuthMonitor::Incremental)

#endif
