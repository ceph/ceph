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
#include "auth/ClientTicket.h"

#include "common/Timer.h"

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

  entity_addr_t my_addr;

  Context *mount_timeout_event;

  Mutex monc_lock;
  SafeTimer timer;
  bool mounted;
  int mounters;
  Cond mount_cond, map_cond;

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
		timer(monc_lock) {
    mounted = false;
    mounters = 0;
  }

  int build_initial_monmap();
  int get_monmap();

  int mount(double mount_timeout);

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
    Mutex::Locker l(monc_lock);
    return monmap.size();
  }

  void set_messenger(Messenger *m) { messenger = m; }


};

#endif
