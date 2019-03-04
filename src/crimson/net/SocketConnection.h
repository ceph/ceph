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

#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sharded.hh>

#include "msg/Policy.h"
#include "Connection.h"
#include "Socket.h"
#include "crimson/thread/Throttle.h"

class AuthAuthorizer;
class AuthSessionHandler;

namespace ceph::net {

using stop_t = seastar::stop_iteration;

class SocketMessenger;
class SocketConnection;
using SocketConnectionRef = seastar::shared_ptr<SocketConnection>;

class SocketConnection : public Connection {
  SocketMessenger& messenger;
  seastar::foreign_ptr<std::unique_ptr<Socket>> socket;
  Dispatcher& dispatcher;
  seastar::gate pending_dispatch;

  // if acceptor side, socket_port is different from peer_addr.get_port();
  // if connector side, socket_port is different from my_addr.get_port().
  enum class side_t {
    none,
    acceptor,
    connector
  };
  side_t side = side_t::none;
  uint16_t socket_port = 0;

  enum class state_t {
    none,
    accepting,
    connecting,
    open,
    standby,
    wait,
    closing
  };
  state_t state = state_t::none;

  /// become valid only when state is state_t::closing
  seastar::shared_future<> close_ready;

  /// state for handshake
  struct Handshake {
    ceph_msg_connect connect;
    ceph_msg_connect_reply reply;
    AuthAuthorizer* authorizer = nullptr;
    std::chrono::milliseconds backoff;
    uint32_t connect_seq = 0;
    uint32_t peer_global_seq = 0;
    uint32_t global_seq;
    seastar::promise<> promise;
  } h;

  /// server side of handshake negotiation
  seastar::future<stop_t> repeat_handle_connect();
  seastar::future<stop_t> handle_connect_with_existing(SocketConnectionRef existing,
                                                        bufferlist&& authorizer_reply);
  seastar::future<stop_t> replace_existing(SocketConnectionRef existing,
                                            bufferlist&& authorizer_reply,
                                            bool is_reset_from_peer = false);
  seastar::future<stop_t> send_connect_reply(ceph::net::msgr_tag_t tag,
                                              bufferlist&& authorizer_reply = {});
  seastar::future<stop_t> send_connect_reply_ready(ceph::net::msgr_tag_t tag,
                                                    bufferlist&& authorizer_reply);

  seastar::future<> handle_keepalive2();
  seastar::future<> handle_keepalive2_ack();

  bool require_auth_feature() const;
  uint32_t get_proto_version(entity_type_t peer_type, bool connec) const;
  /// client side of handshake negotiation
  seastar::future<stop_t> repeat_connect();
  seastar::future<stop_t> handle_connect_reply(ceph::net::msgr_tag_t tag);
  void reset_session();

  /// state for an incoming message
  struct MessageReader {
    ceph_msg_header header;
    ceph_msg_footer footer;
    bufferlist front;
    bufferlist middle;
    bufferlist data;
  } m;

  seastar::future<> maybe_throttle();
  seastar::future<> handle_tags();
  seastar::future<> handle_ack();

  /// becomes available when handshake completes, and when all previous messages
  /// have been sent to the output stream. send() chains new messages as
  /// continuations to this future to act as a queue
  seastar::future<> send_ready;

  /// encode/write a message
  seastar::future<> write_message(MessageRef msg);

  ceph::net::Policy<ceph::thread::Throttle> policy;
  uint64_t features;
  void set_features(uint64_t new_features) {
    features = new_features;
  }

  /// the seq num of the last transmitted message
  seq_num_t out_seq = 0;
  /// the seq num of the last received message
  seq_num_t in_seq = 0;
  /// update the seq num of last received message
  /// @returns true if the @c seq is valid, and @c in_seq is updated,
  ///          false otherwise.
  bool update_rx_seq(seq_num_t seq);

  seastar::future<> read_message();

  std::unique_ptr<AuthSessionHandler> session_security;

  // messages to be resent after connection gets reset
  std::queue<MessageRef> out_q;
  // messages sent, but not yet acked by peer
  std::queue<MessageRef> sent;
  static void discard_up_to(std::queue<MessageRef>*, seq_num_t);

  struct Keepalive {
    struct {
      const char tag = CEPH_MSGR_TAG_KEEPALIVE2;
      ceph_timespec stamp;
    } __attribute__((packed)) req;
    struct {
      const char tag = CEPH_MSGR_TAG_KEEPALIVE2_ACK;
      ceph_timespec stamp;
    } __attribute__((packed)) ack;
    ceph_timespec ack_stamp;
  } k;

  seastar::future<> fault();

  void execute_open();

  seastar::future<> do_send(MessageRef msg);
  seastar::future<> do_keepalive();
  seastar::future<> do_close();

 public:
  SocketConnection(SocketMessenger& messenger,
                   Dispatcher& dispatcher);
  ~SocketConnection();

  Messenger* get_messenger() const override;

  int get_peer_type() const override {
    return peer_type;
  }

  seastar::future<bool> is_connected() override;

  seastar::future<> send(MessageRef msg) override;

  seastar::future<> keepalive() override;

  seastar::future<> close() override;

  seastar::shard_id shard_id() const override;

  void print(ostream& out) const override;

 public:
  /// start a handshake from the client's perspective,
  /// only call when SocketConnection first construct
  void start_connect(const entity_addr_t& peer_addr,
                     const entity_type_t& peer_type);
  /// start a handshake from the server's perspective,
  /// only call when SocketConnection first construct
  void start_accept(seastar::foreign_ptr<std::unique_ptr<Socket>>&& socket,
                    const entity_addr_t& peer_addr);

  /// the number of connections initiated in this session, increment when a
  /// new connection is established
  uint32_t connect_seq() const {
    return h.connect_seq;
  }

  /// the client side should connect us with a gseq. it will be reset with
  /// the one of exsting connection if it's greater.
  uint32_t peer_global_seq() const {
    return h.peer_global_seq;
  }
  seq_num_t rx_seq_num() const {
    return in_seq;
  }

  /// current state of connection
  state_t get_state() const {
    return state;
  }
  bool is_server_side() const {
    return policy.server;
  }
  bool is_lossy() const {
    return policy.lossy;
  }

  /// move all messages in the sent list back into the queue
  void requeue_sent();

  std::tuple<seq_num_t, std::queue<MessageRef>> get_out_queue() {
    return {out_seq, std::move(out_q)};
  }
};

} // namespace ceph::net
