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


class MonMap;
class MMonMap;
class MClientMountAck;

class MonClient : public Dispatcher {
public:
  MonMap monmap;
private:
  Messenger *messenger;

  entity_addr_t my_addr;

  ClientTicket ticket;
  bufferlist signed_ticket;

  Context *mount_timeout_event;

  Mutex monc_lock;
  SafeTimer timer;
  bool mounted;
  int mounters;
  bool unmounting;
  Cond mount_cond, map_cond;


  bool dispatch_impl(Message *m);
  void handle_monmap(MMonMap *m);

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
  void handle_unmount(Message* m);
 public:
  MonClient() : messenger(NULL),
		monc_lock("MonClient::monc_lock"),
		timer(monc_lock) {
    mounted = false;
    mounters = 0;
    mount_timeout_event = 0;
    unmounting = false;
  }

  int build_initial_monmap();
  int get_monmap();

  int mount(double mount_timeout);
  int unmount();

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

  bufferlist& get_signed_ticket() { return signed_ticket; }
  ClientTicket& get_ticket() { return ticket; }

};

#endif
