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

#ifndef __CEPH_MONCLIENT_H
#define __CEPH_MONCLIENT_H

#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "MonMap.h"

#include "common/Timer.h"

#include "auth/AuthClientHandler.h"

#include "messages/MMonSubscribe.h"

class MonMap;
class MMonMap;
class MClientMountAck;
class MMonSubscribeAck;
class MAuthReply;
class MAuthRotating;


enum MonClientState {
  MC_STATE_NONE,
  MC_STATE_NEGOTIATING,
  MC_STATE_AUTHENTICATING,
  MC_STATE_HAVE_SESSION,
};

class MonClient : public Dispatcher {
public:
  MonMap monmap;
private:
  MonClientState state;

  Messenger *messenger;

  int cur_mon;

  EntityName entity_name;

  entity_addr_t my_addr;

  Mutex monc_lock;
  SafeTimer timer;

  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con) {}

  void handle_monmap(MMonMap *m);

  void handle_auth(MAuthReply *m);


  // monitor session
  bool hunting;

  struct C_Tick : public Context {
    MonClient *monc;
    C_Tick(MonClient *m) : monc(m) {}
    void finish(int r) {
      monc->tick();
    }
  };
  void tick();

  Cond auth_cond;

  void handle_auth_rotating_response(MAuthRotating *m);
  // monclient
  bool want_monmap;

  uint32_t want_keys;

  // mount
private:
  client_t clientid;
  int mounting;
  int mount_err;
  Cond mount_cond, map_cond;
  Cond authenticate_cond;
  utime_t mount_started;

  list<Message*> waiting_for_session;

  void _finish_hunting();
  void _reopen_session();
  void _pick_new_mon();
  void _send_mon_message(Message *m, bool force=false);
  void _send_mount();
  void handle_mount_ack(MClientMountAck* m);

public:
  void set_entity_name(EntityName name) { entity_name = name; }

  int _start_auth_rotating();
  int wait_auth_rotating(double timeout);

  int mount(double mount_timeout);
 
  int wait_authenticate(double timeout);

  // mon subscriptions
private:
  map<nstring,ceph_mon_subscribe_item> sub_have;  // my subs, and current versions
  utime_t sub_renew_sent, sub_renew_after;

  void _renew_subs();
  void handle_subscribe_ack(MMonSubscribeAck* m);

  void _sub_want(nstring what, version_t have) {
    sub_have[what].have = have;
    sub_have[what].onetime = false;
  }
  bool _sub_want_onetime(nstring what, version_t have) {
    if (sub_have.count(what) == 0) {
      sub_have[what].have = have;
      sub_have[what].onetime = true;
      return true;
    } else
      sub_have[what].have = have;
    return false;
  }
  void _sub_got(nstring what, version_t have) {
    if (sub_have.count(what)) {
      if (sub_have[what].onetime)
	sub_have.erase(what);
      else
	sub_have[what].have = have;
    }
  }

  // auth tickets
public:
  AuthClientHandler *auth;
public:
  void renew_subs() {
    Mutex::Locker l(monc_lock);
    _renew_subs();
  }
  void sub_want(nstring what, version_t have) {
    Mutex::Locker l(monc_lock);
    _sub_want(what, have);
  }
  bool sub_want_onetime(nstring what, version_t have) {
    Mutex::Locker l(monc_lock);
    return _sub_want_onetime(what, have);
  }
  void sub_got(nstring what, version_t have) {
    Mutex::Locker l(monc_lock);
    _sub_got(what, have);
  }
  
 public:
  MonClient() : state(MC_STATE_NONE),
                messenger(NULL), cur_mon(-1),
		monc_lock("MonClient::monc_lock"),
		timer(monc_lock),
		hunting(false),
		want_monmap(false),
		want_keys(0),
		mounting(0), mount_err(0),
		auth(NULL) { }
  ~MonClient() {
    timer.cancel_all_events();
  }

  void init();
  void shutdown();

  int build_initial_monmap();
  int get_monmap();
  int get_monmap_privately();

  void send_mon_message(Message *m) {
    Mutex::Locker l(monc_lock);
    _send_mon_message(m);
  }
  void reopen_session() {
    Mutex::Locker l(monc_lock);
    _reopen_session();
  }

  entity_addr_t get_my_addr() { return my_addr; }

  const ceph_fsid_t& get_fsid() {
    return monmap.fsid;
  }

  entity_addr_t get_mon_addr(unsigned i) {
    Mutex::Locker l(monc_lock);
    if (i < monmap.size())
      return monmap.mon_inst[i].addr;
    return entity_addr_t();
  }
  entity_inst_t get_mon_inst(unsigned i) {
    Mutex::Locker l(monc_lock);
    if (i < monmap.size())
      return monmap.mon_inst[i];
    return entity_inst_t();
  }
  int get_num_mon() {
    Mutex::Locker l(monc_lock);
    return monmap.size();
  }

  void set_messenger(Messenger *m) { messenger = m; }

  void send_auth_message(Message *m) {
    _send_mon_message(m, true);
  }

  void set_want_keys(uint32_t want) {
    want_keys = want;
    if (auth)
      auth->set_want_keys(want | CEPH_ENTITY_TYPE_MON);
  }

  void add_want_keys(uint32_t want) {
    want_keys |= want;
    if (auth)
      auth->add_want_keys(want);
  }
};

#endif
