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

#ifndef CEPH_MSG_DIRECTMESSENGER_H
#define CEPH_MSG_DIRECTMESSENGER_H

#include "msg/SimplePolicyMessenger.h"
#include "common/Semaphore.h"


class DispatchStrategy;

/**
 * DirectMessenger provides a direct path between two messengers
 * within a process. A pair of DirectMessengers share their
 * DispatchStrategy with each other, and calls to send_message()
 * forward the message directly to the other.
 */
class DirectMessenger : public SimplePolicyMessenger {
private:
  // semaphore for signalling wait() from shutdown()
  Semaphore sem;

  DispatchStrategy *my_dispatchers;

  entity_inst_t peer_inst;
  DispatchStrategy *peer_dispatchers;

  ConnectionRef connection;
  ConnectionRef loopback_connection;

public:
  DirectMessenger(CephContext *cct, entity_name_t name,
                  string mname, uint64_t nonce,
		  DispatchStrategy *my_disapatchers);
  ~DirectMessenger();

  // DirectMessenger interface
  void set_direct_peer(DirectMessenger *peer);
  DispatchStrategy* get_direct_dispatcher() { return my_dispatchers; }

  // Messenger interface
  int bind(const entity_addr_t& bind_addr);
  int start();
  int shutdown();
  void wait();

  ConnectionRef get_connection(const entity_inst_t& dst);
  ConnectionRef get_loopback_connection();

  int send_message(Message *m, const entity_inst_t& dst);
  int send_message(Message *m, Connection *con);

  // unimplemented Messenger interface
  void set_addr_unknowns(entity_addr_t &addr) {}
  int get_dispatch_queue_len() { return 0; }
  double get_dispatch_queue_max_age(utime_t now) { return 0; }
  ceph::timespan get_dispatch_queue_max_age() {
    return ceph::timespan(0); }
  void set_cluster_protocol(int p) {}
  int lazy_send_message(Message *m, const entity_inst_t& dst) { return EINVAL; }
  int lazy_send_message(Message *m, Connection *con) { return EINVAL; }
  int send_keepalive(Connection *con) { return EINVAL; }
  void mark_down(const entity_addr_t& a) {}
  void mark_down(Connection *con) {}
  void mark_down_all() {}
  void mark_down_on_empty(Connection *con) {}
  void mark_disposable(Connection *con) {}
};

#endif
