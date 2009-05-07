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

#include "common/Timer.h"

class MonMap;
class MMonMap;
class MClientMountAck;

class MonClient : public Dispatcher {
  MonMap *pmonmap;
  Context *mount_timeout_event;
  Messenger *messenger;

  Mutex monc_lock;
  SafeTimer timer;
  bool mounted;
  int mounters;
  bool unmounting;
  Cond mount_cond;

  ceph_client_ticket ticket;
  bufferlist signed_ticket;

  int probe_mon(MonMap *pmonmap);
  void handle_monmap(MMonMap *m);
  bool dispatch_impl(Message *m);


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
  MonClient(MonMap *pmm, Messenger *m) : pmonmap(pmm), messenger(m),
					 monc_lock("mon_client"), timer(monc_lock) {
    mounted = false;
    mounters = 0;
    mount_timeout_event = 0;
    unmounting = false;
  }

  MonMap *get_monmap();

  int mount(double mount_timeout);
  int unmount();

  void set_messenger(Messenger *m) { messenger = m; }

  bufferlist& get_signed_ticket() { return signed_ticket; }
  ceph_client_ticket get_ticket() { return ticket; }

};

#endif
