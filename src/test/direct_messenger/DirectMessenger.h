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
 *
 * This is for testing and i/o injection only, and cannot be used
 * for normal messengers with ms_type.
 */
class DirectMessenger : public SimplePolicyMessenger {
 private:
  /// strategy for local dispatch
  std::unique_ptr<DispatchStrategy> dispatchers;
  /// peer instance for comparison in get_connection()
  entity_inst_t peer_inst;
  /// connection that sends to the peer's dispatchers
  ConnectionRef peer_connection;
  /// connection that sends to my own dispatchers
  ConnectionRef loopback_connection;
  /// semaphore for signalling wait() from shutdown()
  Semaphore sem;

 public:
  DirectMessenger(CephContext *cct, entity_name_t name,
                  string mname, uint64_t nonce,
                  DispatchStrategy *dispatchers);
  ~DirectMessenger();

  /// attach to a peer messenger. must be called before start()
  int set_direct_peer(DirectMessenger *peer);


  // Messenger interface

  /// sets the addr. must not be called after set_direct_peer() or start()
  int bind(const entity_addr_t& bind_addr) override;

  /// sets the addr. must not be called after set_direct_peer() or start()
  int client_bind(const entity_addr_t& bind_addr) override;

  /// starts dispatchers
  int start() override;

  /// breaks connections, stops dispatchers, and unblocks callers of wait()
  int shutdown() override;

  /// blocks until shutdown() completes
  void wait() override;

  /// returns a connection to the peer instance, a loopback connection to our
  /// own instance, or null if not connected
  ConnectionRef get_connection(const entity_inst_t& dst) override;

  /// returns a loopback connection that dispatches to this messenger
  ConnectionRef get_loopback_connection() override;

  /// dispatches a message to the peer instance if connected
  int send_message(Message *m, const entity_inst_t& dst) override;

  /// mark down the connection for the given address
  void mark_down(const entity_addr_t& a) override;

  /// mark down all connections
  void mark_down_all() override;


  // unimplemented Messenger interface
  void set_addr_unknowns(const entity_addr_t &addr) override {}
  void set_addr(const entity_addr_t &addr) override {}
  int get_dispatch_queue_len() override { return 0; }
  double get_dispatch_queue_max_age(utime_t now) override { return 0; }
  void set_cluster_protocol(int p) override {}
};

#endif
