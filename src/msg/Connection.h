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
// Because intusive_ptr clobbers our assert...
#include "include/assert.h"

#include "include/types.h"
#include "include/buffer.h"

#include "common/RefCountedObj.h"

#include "common/debug.h"
#include "common/config.h"


// ======================================================

// abstract Connection, for keeping per-connection state

class Messenger;

struct Connection : public RefCountedObject {
  Mutex lock;
  Messenger *msgr;
  RefCountedObject *priv;
  int peer_type;
  entity_addr_t peer_addr;
  utime_t last_keepalive_ack;
private:
  uint64_t features;
public:
  bool failed; // true if we are a lossy connection that has failed.

  int rx_buffers_version;
  map<ceph_tid_t,pair<bufferlist,int> > rx_buffers;

  friend class boost::intrusive_ptr<Connection>;
  friend class PipeConnection;

public:
  Connection(Messenger *m)
    : lock("Connection::lock"),
      msgr(m),
      priv(NULL),
      peer_type(-1),
      features(0),
      failed(false),
      rx_buffers_version(0) {
    // we are managed exlusively by ConnectionRef; make it so you can
    //   ConnectionRef foo = new Connection;
    nref.set(0);
  }

  ~Connection() {
    //generic_dout(0) << "~Connection " << this << dendl;
    if (priv) {
      //generic_dout(0) << "~Connection " << this << " dropping priv " << priv << dendl;
      priv->put();
    }
  }

  void set_priv(RefCountedObject *o) {
    Mutex::Locker l(lock);
    if (priv)
      priv->put();
    priv = o;
  }

  RefCountedObject *get_priv() {
    Mutex::Locker l(lock);
    if (priv)
      return priv->get();
    return NULL;
  }

  virtual bool is_connected() = 0;

  Messenger *get_messenger() {
    return msgr;
  }

  int get_peer_type() { return peer_type; }
  void set_peer_type(int t) { peer_type = t; }

  bool peer_is_mon() { return peer_type == CEPH_ENTITY_TYPE_MON; }
  bool peer_is_mds() { return peer_type == CEPH_ENTITY_TYPE_MDS; }
  bool peer_is_osd() { return peer_type == CEPH_ENTITY_TYPE_OSD; }
  bool peer_is_client() { return peer_type == CEPH_ENTITY_TYPE_CLIENT; }

  const entity_addr_t& get_peer_addr() { return peer_addr; }
  void set_peer_addr(const entity_addr_t& a) { peer_addr = a; }

  uint64_t get_features() const { return features; }
  bool has_feature(uint64_t f) const { return features & f; }
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

  utime_t get_last_keepalive_ack() const {
    return last_keepalive_ack;
  }
};

typedef boost::intrusive_ptr<Connection> ConnectionRef;


#endif /* CEPH_CONNECTION_H */
