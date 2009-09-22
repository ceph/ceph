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

#include "auth/AuthClient.h"
#include "auth/AuthClientHandler.h"
#include "auth/KeyRing.h"

#include "messages/MMonSubscribe.h"

class MonMap;
class MMonMap;
class MClientMountAck;
class MMonSubscribeAck;
class MAuthRotating;


enum MonClientState {
  MC_STATE_NONE,
  MC_STATE_AUTHENTICATING,
  MC_STATE_AUTHENTICATED,
};

class MonClient : public Dispatcher, public AuthClient {
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
  bool ms_handle_reset(Connection *con, const entity_addr_t& peer);

  void ms_handle_failure(Connection *con, Message *m, const entity_addr_t& peer) { }
  void ms_handle_remote_reset(Connection *con, const entity_addr_t& peer) {}

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

  Context *auth_timeout_event;
  bool auth_got_timeout;
  Cond auth_cond;
  KeyRing *keyring;

  class C_AuthRotatingTimeout : public Context {
  protected:
    MonClient *client;
    double timeout;
  public:
    C_AuthRotatingTimeout(MonClient *c, double to) :
                                        client(c), timeout(to) {
    }
    void finish(int r) {
      if (r >= 0) client->_auth_rotating_timeout(timeout);
    }
  };

  void handle_auth_rotating_response(MAuthRotating *m);
  // monclient
  bool want_monmap;

  // mount
private:
  client_t clientid;
  int mounting;
  int mount_err;
  Cond mount_cond, map_cond;
  utime_t mount_started;

  void _finish_hunting();
  void _reopen_session();
  void _pick_new_mon();
  void _send_mon_message(Message *m);
  void _send_mount();
  void handle_mount_ack(MClientMountAck* m);

public:
  void set_entity_name(EntityName name) { entity_name = name; }
  void _auth_rotating_timeout(double timeout) {
    auth_got_timeout = true;
    auth_cond.Signal();
  }

  int _start_auth_rotating(double timeout);

  int mount(double mount_timeout);
  int authenticate(double timeout);
  int authorize(double timeout);

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
  void _sub_want_onetime(nstring what, version_t have) {
    if (sub_have.count(what) == 0) {
      sub_have[what].have = have;
      sub_have[what].onetime = true;
    }
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
  AuthClientHandler auth;
  AuthClientAuthenticateHandler auth_handler;
  double auth_timeout;
public:
  void renew_subs() {
    Mutex::Locker l(monc_lock);
    _renew_subs();
  }
  void sub_want(nstring what, version_t have) {
    Mutex::Locker l(monc_lock);
    _sub_want(what, have);
  }
  void sub_want_onetime(nstring what, version_t have) {
    Mutex::Locker l(monc_lock);
    _sub_want_onetime(what, have);
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
                auth_timeout_event(NULL),
                auth_got_timeout(false),
                keyring(NULL),
		mounting(0), mount_err(0),
                auth_handler(&auth, 0, 0) { }
  ~MonClient() {
    timer.cancel_all_events();
  }

  void init();
  void shutdown();

  int build_initial_monmap();
  int get_monmap();
  int get_monmap_privately();

  void set_keyring(KeyRing *ring) { keyring = ring; }

  void send_mon_message(Message *m) {
    Mutex::Locker l(monc_lock);
    _send_mon_message(m);
  }
  void note_mon_leader(int m) {
    Mutex::Locker l(monc_lock);
    monmap.last_mon = m;
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

  void send_message(Message *m);
};

#endif
