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

class MonClient : public Dispatcher {
public:
  MonMap monmap;
private:
  Messenger *messenger;

  bufferlist tgt;
  entity_addr_t my_addr;

  Context *mount_timeout_event;

  Mutex monc_lock;
  Mutex auth_lock;
  bool mounted;
  int mounters;
  Cond mount_cond, map_cond;
  AuthClientHandler auth_client_handler;

  bool ms_dispatch(Message *m);
  void handle_monmap(MMonMap *m);

  void ms_handle_remote_reset(const entity_addr_t& peer);

 protected:
  class C_MountTimeout : public Context {
    MonClient *client;
    double timeout;
  public:
    C_MountTimeout(MonClient *c, double to) : client(c), timeout(to) { }
    void finish(int r) {
      if (r >= 0) client->_mount_timeout(timeout);
    }
  };

  class MonClientOpHandler {
  protected:
    MonClient *client;
    Mutex op_lock;
    SafeTimer timer;
  public:
    bool done;
    int num_waiters;
    Cond cond;
    Context *timeout_event;

    MonClientOpHandler(MonClient *c) : client(c),
                op_lock("MonClientOpHandler::op_lock"),
		timer(op_lock) {
      done = false;
      num_waiters = 0;
      timeout_event = NULL;
    }

    void _op_timeout(double timeout);
    void _try_do_op(double timeout);
    int do_op(double timeout);

    virtual ~MonClientOpHandler() {}

    virtual Message *build_request() = 0;
    virtual void handle_response(Message *response) = 0;
    virtual bool got_response() = 0;
  };
  
  class C_OpTimeout : public Context {
  protected:
    MonClientOpHandler *op_handler;
    double timeout;
  public:
    C_OpTimeout(MonClientOpHandler *oph, double to) :
                                        op_handler(oph), timeout(to) {
    }
    void finish(int r) {
      if (r >= 0) op_handler->_op_timeout(timeout);
    }
  };

  class MonClientMountHandler : public MonClientOpHandler {
    bool response_flag;
  public:
    MonClientMountHandler(MonClient *c) : MonClientOpHandler(c) { response_flag = false; }
    ~MonClientMountHandler() {}

    Message *build_request();
    void handle_response(Message *response);
    bool got_response() { return response_flag; }
  };

  class MonClientUnmountHandler : public MonClientOpHandler {
    bool got_ack;
  public:
    MonClientUnmountHandler(MonClient *c) : MonClientOpHandler(c),
                            got_ack(false) {}
    ~MonClientUnmountHandler() {}

    Message *build_request();
    void handle_response(Message *response);
    bool got_response() { return got_ack; }
  };

  class MonClientAuthHandler : public MonClientOpHandler {
    bool has_data;
    int last_result;
  public:
    MonClientAuthHandler(MonClient *c) : MonClientOpHandler(c) {
      last_result = 0;
    }
    ~MonClientAuthHandler() {}

    Message *build_request();
    void handle_response(Message *response);
    bool got_response() { return !client->auth_client_handler.request_pending(); }
    int get_result() { return last_result; }
  };

  MonClientMountHandler mount_handler;
  MonClientUnmountHandler unmount_handler;

  MonClientAuthHandler *cur_auth_handler;

  void _try_mount(double timeout);
  void _mount_timeout(double timeout);
  void handle_mount_ack(MClientMountAck* m);

  // mon subscriptions
private:
  map<nstring,ceph_mon_subscribe_item> sub_have;  // my subs, and current versions
  utime_t sub_renew_sent, sub_renew_after;

public:
  void renew_subs();
  void sub_want(nstring what, version_t have) {
    sub_have[what].have = have;
    sub_have[what].onetime = false;
  }
  void sub_want_onetime(nstring what, version_t have) {
    if (sub_have.count(what) == 0) {
      sub_have[what].have = have;
      sub_have[what].onetime = true;
      renew_subs();
    }
  }
  void sub_got(nstring what, version_t have) {
    if (sub_have.count(what)) {
      if (sub_have[what].onetime)
	sub_have.erase(what);
      else
	sub_have[what].have = have;
    }
  }
  void handle_subscribe_ack(MMonSubscribeAck* m);

 public:
  MonClient() : messenger(NULL),
		mount_timeout_event(NULL),
		monc_lock("MonClient::monc_lock"),
		auth_lock("MonClient::auth_lock"),
                mount_handler(this),
                unmount_handler(this) {
    //            auth_handler(this) {
    mounted = false;
    mounters = 0;
  }

  int build_initial_monmap();
  int get_monmap();

  int mount(double mount_timeout);
  int authorize(uint32_t want_keys, double timeout);

  void tick();

  void send_mon_message(Message *m, bool new_mon=false);
  void note_mon_leader(int m) {
    monmap.last_mon = m;
  }
  void pick_new_mon();

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
  class MonClientOpCtx {
  public:
    bool done;
    int num_waiters;
    Cond cond;
    Context *timeout_event;

    MonClientOpCtx() {
      done = false;
      num_waiters = 0;
    }
  };
  

    Mutex::Locker l(monc_lock);
    return monmap.size();
  }

  void set_messenger(Messenger *m) { messenger = m; }

};

#endif
