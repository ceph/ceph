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

#include <seastar/core/shared_future.hh>

#include "Connection.h"
#include "Protocol.h"
#include "Session.h"

namespace ceph::net {

class SocketConnection : public Connection {
  Session _s;
  Session * const s = &_s;

  state_t state = state_t::none;

  /// become valid only when state is state_t::closed
  seastar::shared_future<> close_ready;

  /// state for handshake
  struct Handshake {
    ceph_msg_connect connect;
    ceph_msg_connect_reply reply;
    bool got_bad_auth = false;
    std::unique_ptr<AuthAuthorizer> authorizer;
  } h;

  /// server side of handshake negotiation
  seastar::future<seastar::stop_iteration> handle_connect();
  seastar::future<seastar::stop_iteration> handle_connect_with_existing(ConnectionRef existing,
                                                                        bufferlist&& authorizer_reply);
  seastar::future<seastar::stop_iteration> replace_existing(ConnectionRef existing,
				                            bufferlist&& authorizer_reply,
				                            bool is_reset_from_peer = false);
  seastar::future<seastar::stop_iteration> send_connect_reply(ceph::net::msgr_tag_t tag,
				                              bufferlist&& authorizer_reply = {});
  seastar::future<seastar::stop_iteration> send_connect_reply_ready(ceph::net::msgr_tag_t tag,
					                            bufferlist&& authorizer_reply);

  seastar::future<> handle_keepalive2();
  seastar::future<> handle_keepalive2_ack();

  bool require_auth_feature() const;
  int get_peer_type() const override {
    return _s.peer_type;
  }
  uint32_t get_proto_version(entity_type_t peer_type, bool connec) const;
  /// client side of handshake negotiation
  seastar::future<seastar::stop_iteration> connect(entity_type_t peer_type, entity_type_t host_type);
  seastar::future<seastar::stop_iteration> handle_connect_reply(ceph::net::msgr_tag_t tag);
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
  seastar::future<> send_ready = _s.send_promise.get_future();

  /// encode/write a message
  seastar::future<> write_message(MessageRef msg);

  void set_features(uint64_t new_features) {
    _s.features = new_features;
  }

  /// update the seq num of last received message
  /// @returns true if the @c seq is valid, and @c in_seq is updated,
  ///          false otherwise.
  bool update_rx_seq(seq_num_t seq);

  seastar::future<MessageRef> do_read_message();

  struct Keepalive {
    struct {
      const char tag = CEPH_MSGR_TAG_KEEPALIVE2;
      ceph_timespec stamp;
    } __attribute__((packed)) req;
    struct {
      const char tag = CEPH_MSGR_TAG_KEEPALIVE2_ACK;
      ceph_timespec stamp;
    } __attribute__((packed)) ack;
  } k;

  seastar::future<> fault();

  // prototols specific
  Dispatcher *dispatcher;

  void protocol_open();

 public:
  SocketConnection(Messenger *messenger,
                   Dispatcher *disp,
                   const entity_addr_t& my_addr,
                   const entity_addr_t& peer_addr);
  SocketConnection(Messenger *messenger,
                   Dispatcher *disp,
                   const entity_addr_t& my_addr,
                   const entity_addr_t& peer_addr,
                   seastar::connected_socket&& socket);
  ~SocketConnection();

  const entity_addr_t& get_my_addr() const { return _s.my_addr; };
  const entity_addr_t& get_peer_addr() const { return _s.peer_addr; };

  bool is_connected() override;

  void protocol_connect(entity_type_t, entity_type_t) override;

  void protocol_accept() override;

  seastar::future<MessageRef> read_message() override;

  seastar::future<> send(MessageRef msg) override;

  seastar::future<> keepalive() override;

  seastar::future<> close() override;

  uint32_t connect_seq() const override {
    return _s.connect_seq;
  }
  uint32_t peer_global_seq() const override {
    return _s.peer_global_seq;
  }
  seq_num_t rx_seq_num() const {
    return _s.in_seq;
  }
  state_t get_state() const override {
    return state;
  }
  bool is_server_side() const override {
    return _s.policy.server;
  }
  bool is_lossy() const override {
    return _s.policy.lossy;
  }

private:
  void requeue_sent() override;
  std::tuple<seq_num_t, std::queue<MessageRef>> get_out_queue() override {
    return {_s.out_seq, std::move(_s.out_q)};
  }

};

} // namespace ceph::net
