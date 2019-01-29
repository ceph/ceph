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

#ifndef CEPH_CONNECTION_H
#define CEPH_CONNECTION_H

#include <stdlib.h>
#include <ostream>

#include <boost/intrusive_ptr.hpp>

#include "auth/Auth.h"
#include "common/RefCountedObj.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/Mutex.h"
#include "include/ceph_assert.h" // Because intusive_ptr clobbers our assert...
#include "include/buffer.h"
#include "include/types.h"
#include "common/item_history.h"
#include "msg/MessageRef.h"


// ======================================================

// abstract Connection, for keeping per-connection state

class Messenger;

struct Connection : public RefCountedObject {
  mutable Mutex lock;
  Messenger *msgr;
  RefCountedPtr priv;
  int peer_type;
  int64_t peer_id = -1;  // [msgr2 only] the 0 of osd.0, 4567 or client.4567
  safe_item_history<entity_addrvec_t> peer_addrs;
  utime_t last_keepalive, last_keepalive_ack;
private:
  uint64_t features;
public:
  bool failed; // true if we are a lossy connection that has failed.

  int rx_buffers_version;
  map<ceph_tid_t,pair<bufferlist,int> > rx_buffers;

  // authentication state
  // FIXME make these private after ms_handle_authorizer is removed
public:
  AuthCapsInfo peer_caps_info;
  EntityName peer_name;
  uint64_t peer_global_id = 0;

  friend class boost::intrusive_ptr<Connection>;
  friend class PipeConnection;

public:
  Connection(CephContext *cct, Messenger *m)
    // we are managed exclusively by ConnectionRef; make it so you can
    //   ConnectionRef foo = new Connection;
    : RefCountedObject(cct, 0),
      lock("Connection::lock"),
      msgr(m),
      peer_type(-1),
      features(0),
      failed(false),
      rx_buffers_version(0) {
  }

  ~Connection() override {
    //generic_dout(0) << "~Connection " << this << dendl;
  }

  void set_priv(const RefCountedPtr& o) {
    Mutex::Locker l(lock);
    priv = o;
  }

  RefCountedPtr get_priv() {
    Mutex::Locker l(lock);
    return priv;
  }

  /**
   * Used to judge whether this connection is ready to send. Usually, the
   * implementation need to build a own shakehand or sesson then it can be
   * ready to send.
   *
   * @return true if ready to send, or false otherwise
   */
  virtual bool is_connected() = 0;

  Messenger *get_messenger() {
    return msgr;
  }

  /**
   * Queue the given Message to send out on the given Connection.
   * Success in this function does not guarantee Message delivery, only
   * success in queueing the Message. Other guarantees may be provided based
   * on the Connection policy.
   *
   * @param m The Message to send. The Messenger consumes a single reference
   * when you pass it in.
   *
   * @return 0 on success, or -errno on failure.
   */
  virtual int send_message(Message *m) = 0;

  virtual int send_message2(MessageRef m)
  {
    return send_message(m.detach()); /* send_message(Message *m) consumes a reference */
  }

  /**
   * Send a "keepalive" ping along the given Connection, if it's working.
   * If the underlying connection has broken, this function does nothing.
   *
   * @return 0, or implementation-defined error numbers.
   */
  virtual void send_keepalive() = 0;
  /**
   * Mark down the given Connection.
   *
   * This will cause us to discard its outgoing queue, and if reset
   * detection is enabled in the policy and the endpoint tries to
   * reconnect they will discard their queue when we inform them of
   * the session reset.
   *
   * It does not generate any notifications to the Dispatcher.
   */
  virtual void mark_down() = 0;

  /**
   * Mark a Connection as "disposable", setting it to lossy
   * (regardless of initial Policy).  This does not immediately close
   * the Connection once Messages have been delivered, so as long as
   * there are no errors you can continue to receive responses; but it
   * will not attempt to reconnect for message delivery or preserve
   * your old delivery semantics, either.
   *
   * TODO: There's some odd stuff going on in our SimpleMessenger
   * implementation during connect that looks unused; is there
   * more of a contract that that's enforcing?
   */
  virtual void mark_disposable() = 0;

  // WARNING / FIXME: this is not populated for loopback connections
  AuthCapsInfo& get_peer_caps_info() {
    return peer_caps_info;
  }
  const EntityName& get_peer_entity_name() {
    return peer_name;
  }
  uint64_t get_peer_global_id() {
    return peer_global_id;
  }

  int get_peer_type() const { return peer_type; }
  void set_peer_type(int t) { peer_type = t; }

  // peer_id is only defined for msgr2
  int64_t get_peer_id() const { return peer_id; }
  void set_peer_id(int64_t t) { peer_id = t; }

  bool peer_is_mon() const { return peer_type == CEPH_ENTITY_TYPE_MON; }
  bool peer_is_mgr() const { return peer_type == CEPH_ENTITY_TYPE_MGR; }
  bool peer_is_mds() const { return peer_type == CEPH_ENTITY_TYPE_MDS; }
  bool peer_is_osd() const { return peer_type == CEPH_ENTITY_TYPE_OSD; }
  bool peer_is_client() const { return peer_type == CEPH_ENTITY_TYPE_CLIENT; }

  /// which of the peer's addrs is actually in use for this connection
  virtual entity_addr_t get_peer_socket_addr() const = 0;

  entity_addr_t get_peer_addr() const {
    return peer_addrs->front();
  }
  const entity_addrvec_t& get_peer_addrs() const {
    return *peer_addrs;
  }
  void set_peer_addr(const entity_addr_t& a) {
    peer_addrs = entity_addrvec_t(a);
  }
  void set_peer_addrs(const entity_addrvec_t& av) { peer_addrs = av; }

  uint64_t get_features() const { return features; }
  bool has_feature(uint64_t f) const { return features & f; }
  bool has_features(uint64_t f) const {
    return (features & f) == f;
  }
  void set_features(uint64_t f) { features = f; }
  void set_feature(uint64_t f) { features |= f; }

  void post_rx_buffer(ceph_tid_t tid, bufferlist& bl) {
    Mutex::Locker l(lock);
    ++rx_buffers_version;
    rx_buffers[tid] = pair<bufferlist,int>(bl, rx_buffers_version);
  }

  void revoke_rx_buffer(ceph_tid_t tid) {
    Mutex::Locker l(lock);
    rx_buffers.erase(tid);
  }

  utime_t get_last_keepalive() const {
    Mutex::Locker l(lock);
    return last_keepalive;
  }
  void set_last_keepalive(utime_t t) {
    Mutex::Locker l(lock);
    last_keepalive = t;
  }
  utime_t get_last_keepalive_ack() const {
    Mutex::Locker l(lock);
    return last_keepalive_ack;
  }
  void set_last_keepalive_ack(utime_t t) {
    Mutex::Locker l(lock);
    last_keepalive_ack = t;
  }

};

typedef boost::intrusive_ptr<Connection> ConnectionRef;


#endif /* CEPH_CONNECTION_H */
