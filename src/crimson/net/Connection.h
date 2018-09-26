// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <queue>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "Fwd.h"

namespace ceph::net {

enum class state_t;

class Connection : public boost::intrusive_ref_counter<Connection,
						       boost::thread_unsafe_counter> {
 protected:
  Messenger *const messenger;

 public:
  Connection(Messenger *messenger)
    : messenger(messenger) {}
  virtual ~Connection() {}

  Messenger* get_messenger() const { return messenger; }

  virtual const entity_addr_t& get_my_addr() const = 0;
  virtual const entity_addr_t& get_peer_addr() const = 0;
  virtual int get_peer_type() const = 0;

  /// true if the handshake has completed and no errors have been encountered
  virtual bool is_connected() = 0;

  /// complete a handshake from the client's perspective
  virtual void protocol_connect(entity_type_t, entity_type_t) = 0;

  /// complete a handshake from the server's perspective
  virtual void protocol_accept() = 0;

  /// read a message from a connection that has completed its handshake
  virtual seastar::future<MessageRef> read_message() = 0;

  /// send a message over a connection that has completed its handshake
  virtual seastar::future<> send(MessageRef msg) = 0;

  /// send a keepalive message over a connection that has completed its
  /// handshake
  virtual seastar::future<> keepalive() = 0;

  /// close the connection and cancel any any pending futures from read/send
  virtual seastar::future<> close() = 0;

  /// move all messages in the sent list back into the queue
  virtual void requeue_sent() = 0;

  /// get all messages in the out queue
  virtual std::tuple<seq_num_t, std::queue<MessageRef>> get_out_queue() = 0;

public:
  /// the number of connections initiated in this session, increment when a
  /// new connection is established
  virtual uint32_t connect_seq() const = 0;

  /// the client side should connect us with a gseq. it will be reset with a
  /// the one of exsting connection if it's greater.
  virtual uint32_t peer_global_seq() const = 0;

  virtual seq_num_t rx_seq_num() const = 0;

  /// current state of connection
  virtual state_t get_state() const = 0;
  virtual bool is_server_side() const = 0;
  virtual bool is_lossy() const = 0;
};

} // namespace ceph::net
