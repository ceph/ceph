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

#include "DirectMessenger.h"
#include "msg/DispatchStrategy.h"


class DirectConnection : public Connection {
  /// sent messages are dispatched here
  DispatchStrategy *const dispatchers;

  /// the connection that will be attached to outgoing messages, so that replies
  /// can be dispatched back to the sender. the pointer is atomic for
  /// thread-safety between mark_down() and send_message(). no reference is held
  /// on this Connection to avoid cyclical refs. we don't need a reference
  /// because its owning DirectMessenger will mark both connections down (and
  /// clear this pointer) before dropping its own reference
  std::atomic<Connection*> reply_connection{nullptr};

 public:
  DirectConnection(CephContext *cct, DirectMessenger *m,
                   DispatchStrategy *dispatchers)
    : Connection(cct, m),
      dispatchers(dispatchers)
  {}

  /// sets the Connection that will receive replies to outgoing messages
  void set_direct_reply_connection(ConnectionRef conn);

  /// return true if a peer connection exists
  bool is_connected() override;

  /// pass the given message directly to our dispatchers
  int send_message(Message *m) override;

  /// release our pointer to the peer connection. later calls to is_connected()
  /// will return false, and send_message() will fail with -ENOTCONN
  void mark_down() override;

  /// noop - keepalive messages are not needed within a process
  void send_keepalive() override {}

  /// noop - reconnect/recovery semantics are not needed within a process
  void mark_disposable() override {}
};

void DirectConnection::set_direct_reply_connection(ConnectionRef conn)
{
  reply_connection.store(conn.get());
}

bool DirectConnection::is_connected()
{
  // true between calls to set_direct_reply_connection() and mark_down()
  return reply_connection.load() != nullptr;
}

int DirectConnection::send_message(Message *m)
{
  // read reply_connection atomically and take a reference
  ConnectionRef conn = reply_connection.load();
  if (!conn) {
    m->put();
    return -ENOTCONN;
  }
  // attach reply_connection to the Message, so that calls to
  // m->get_connection()->send_message() can be dispatched back to the sender
  m->set_connection(conn);

  dispatchers->ds_dispatch(m);
  return 0;
}

void DirectConnection::mark_down()
{
  Connection *conn = reply_connection.load();
  if (!conn) {
    return; // already marked down
  }
  if (!reply_connection.compare_exchange_weak(conn, nullptr)) {
    return; // lost the race to mark down
  }
  // called only once to avoid loops
  conn->mark_down();
}


static ConnectionRef create_loopback(DirectMessenger *m,
                                     entity_name_t name,
                                     DispatchStrategy *dispatchers)
{
  auto loopback = boost::intrusive_ptr<DirectConnection>(
      new DirectConnection(m->cct, m, dispatchers));
  // loopback replies go to itself
  loopback->set_direct_reply_connection(loopback);
  loopback->set_peer_type(name.type());
  loopback->set_features(CEPH_FEATURES_ALL);
  return loopback;
}

DirectMessenger::DirectMessenger(CephContext *cct, entity_name_t name,
                                 string mname, uint64_t nonce,
                                 DispatchStrategy *dispatchers)
  : SimplePolicyMessenger(cct, name, mname, nonce),
    dispatchers(dispatchers),
    loopback_connection(create_loopback(this, name, dispatchers))
{
  dispatchers->set_messenger(this);
}

DirectMessenger::~DirectMessenger()
{
}

int DirectMessenger::set_direct_peer(DirectMessenger *peer)
{
  if (get_myinst() == peer->get_myinst()) {
    return -EADDRINUSE; // must have a different entity instance
  }
  peer_inst = peer->get_myinst();

  // allocate a Connection that dispatches to the peer messenger
  auto direct_connection = boost::intrusive_ptr<DirectConnection>(
      new DirectConnection(cct, peer, peer->dispatchers.get()));

  direct_connection->set_peer_addr(peer_inst.addr);
  direct_connection->set_peer_type(peer_inst.name.type());
  direct_connection->set_features(CEPH_FEATURES_ALL);

  // if set_direct_peer() was already called on the peer messenger, we can
  // finish by attaching their connections. if not, the later call to
  // peer->set_direct_peer() will attach their connection to ours
  auto connection = peer->get_connection(get_myinst());
  if (connection) {
    auto p = static_cast<DirectConnection*>(connection.get());

    p->set_direct_reply_connection(direct_connection);
    direct_connection->set_direct_reply_connection(p);
  }

  peer_connection = std::move(direct_connection);
  return 0;
}

int DirectMessenger::bind(const entity_addr_t &bind_addr)
{
  if (peer_connection) {
    return -EINVAL; // can't change address after sharing it with the peer
  }
  set_myaddr(bind_addr);
  loopback_connection->set_peer_addr(bind_addr);
  return 0;
}

int DirectMessenger::client_bind(const entity_addr_t &bind_addr)
{
  // same as bind
  return bind(bind_addr);
}

int DirectMessenger::start()
{
  if (!peer_connection) {
    return -EINVAL; // did not connect to a peer
  }
  if (started) {
    return -EINVAL; // already started
  }

  dispatchers->start();
  return SimplePolicyMessenger::start();
}

int DirectMessenger::shutdown()
{
  if (!started) {
    return -EINVAL; // not started
  }

  mark_down_all();
  peer_connection.reset();
  loopback_connection.reset();

  dispatchers->shutdown();
  SimplePolicyMessenger::shutdown();
  sem.Put(); // signal wait()
  return 0;
}

void DirectMessenger::wait()
{
  sem.Get(); // wait on signal from shutdown()
  dispatchers->wait();
}

ConnectionRef DirectMessenger::get_connection(const entity_inst_t& dst)
{
  if (dst == peer_inst) {
    return peer_connection;
  }
  if (dst == get_myinst()) {
    return loopback_connection;
  }
  return nullptr;
}

ConnectionRef DirectMessenger::get_loopback_connection()
{
  return loopback_connection;
}

int DirectMessenger::send_message(Message *m, const entity_inst_t& dst)
{
  auto conn = get_connection(dst);
  if (!conn) {
    m->put();
    return -ENOTCONN;
  }
  return conn->send_message(m);
}

void DirectMessenger::mark_down(const entity_addr_t& addr)
{
  ConnectionRef conn;
  if (addr == peer_inst.addr) {
    conn = peer_connection;
  } else if (addr == get_myaddr_legacy()) {
    conn = loopback_connection;
  }
  if (conn) {
    conn->mark_down();
  }
}

void DirectMessenger::mark_down_all()
{
  if (peer_connection) {
    peer_connection->mark_down();
  }
  loopback_connection->mark_down();
}
