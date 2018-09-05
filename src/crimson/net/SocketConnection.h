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

#include <seastar/core/reactor.hh>
#include <seastar/core/shared_future.hh>

#include "msg/Policy.h"
#include "Connection.h"
#include "crimson/thread/Throttle.h"

class AuthSessionHandler;

namespace ceph::net {

class SocketConnection : public Connection {
  seastar::connected_socket socket;
  seastar::input_stream<char> in;
  seastar::output_stream<char> out;

  state_t state = state_t::none;

  /// become valid only when state is state_t::closed
  seastar::shared_future<> close_ready;

  /// buffer state for read()
  struct Reader {
    bufferlist buffer;
    size_t remaining;
  } r;

  /// read the requested number of bytes into a bufferlist
  seastar::future<bufferlist> read(size_t bytes);

  /// state for handshake
  struct Handshake {
    ceph_msg_connect connect;
    ceph_msg_connect_reply reply;
    bool got_bad_auth = false;
    std::unique_ptr<AuthAuthorizer> authorizer;
    peer_type_t peer_type;
    std::chrono::milliseconds backoff;
    uint32_t connect_seq = 0;
    uint32_t peer_global_seq = 0;
    uint32_t global_seq;
    seastar::promise<> promise;
  } h;

  /// server side of handshake negotiation
  seastar::future<> handle_connect();
  seastar::future<> handle_connect_with_existing(ConnectionRef existing,
						 bufferlist&& authorizer_reply);
  seastar::future<> replace_existing(ConnectionRef existing,
				     bufferlist&& authorizer_reply,
				     bool is_reset_from_peer = false);
  seastar::future<> send_connect_reply(ceph::net::msgr_tag_t tag,
				       bufferlist&& authorizer_reply = {});
  seastar::future<> send_connect_reply_ready(ceph::net::msgr_tag_t tag,
					     bufferlist&& authorizer_reply);

  seastar::future<> handle_keepalive2();
  seastar::future<> handle_keepalive2_ack();

  bool require_auth_feature() const;
  int get_peer_type() const override {
    return h.connect.host_type;
  }
  uint32_t get_proto_version(entity_type_t peer_type, bool connec) const;
  /// client side of handshake negotiation
  seastar::future<> connect(entity_type_t peer_type, entity_type_t host_type);
  seastar::future<> handle_connect_reply(ceph::net::msgr_tag_t tag);
  void reset_session();

  /// state for an incoming message
  struct MessageReader {
    ceph_msg_header header;
    ceph_msg_footer footer;
    bufferlist front;
    bufferlist middle;
    bufferlist data;
  } m;

  /// satisfied when a CEPH_MSGR_TAG_MSG is read, indicating that a message
  /// header will follow
  seastar::promise<> on_message;

  seastar::future<> maybe_throttle();
  void read_tags_until_next_message();
  seastar::future<seastar::stop_iteration> handle_ack();

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

 public:
  SocketConnection(Messenger *messenger,
                   const entity_addr_t& my_addr,
                   const entity_addr_t& peer_addr,
                   seastar::connected_socket&& socket);
  ~SocketConnection();

  bool is_connected() override;

  seastar::future<> client_handshake(entity_type_t peer_type,
				     entity_type_t host_type) override;

  seastar::future<> server_handshake() override;

  seastar::future<MessageRef> read_message() override;

  seastar::future<> send(MessageRef msg) override;

  seastar::future<> keepalive() override;

  seastar::future<> close() override;

  uint32_t connect_seq() const override {
    return h.connect_seq;
  }
  uint32_t peer_global_seq() const override {
    return h.peer_global_seq;
  }
  seq_num_t rx_seq_num() const {
    return in_seq;
  }
  state_t get_state() const override {
    return state;
  }
  bool is_server_side() const override {
    return policy.server;
  }
  bool is_lossy() const override {
    return policy.lossy;
  }

private:
  void requeue_sent() override;
  std::tuple<seq_num_t, std::queue<MessageRef>> get_out_queue() override {
    return {out_seq, std::move(out_q)};
  }

};

} // namespace ceph::net
