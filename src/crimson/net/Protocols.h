// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel Corp.
 *
 * Author: Yingxin Cheng <yingxincheng@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "Protocol.h"

namespace ceph::net {

class NoneProtocol : public Protocol
{
  static constexpr state_t my_state = state_t::none;
  seastar::future<> do_execute() override { return seastar::now(); }

 public:
  NoneProtocol(Protocol *const *protos,
               Session *_s,
               Connection *conn,
               Dispatcher *disp,
               Messenger *msgr)
    : Protocol(protos, _s, conn, disp, msgr) {};

  const state_t& state() const override { return my_state; }

  seastar::future<seastar::stop_iteration>
  send(MessageRef msg) override {
    /// none-state connection should not be registered.
    ceph_assert(false);
  }

  seastar::future<seastar::stop_iteration>
  keepalive() override {
    /// none-state connection should not be registered.
    ceph_assert(false);
  }

  bool replace(const Protocol *newp) override {
    /// none-state connection should not be registered.
    ceph_assert(false);
  }

  void start_accept();
  void start_connect(entity_type_t peer_type);
};

class AcceptProtocol : public Protocol
{
  static constexpr state_t my_state = state_t::accept;
  seastar::future<> do_execute() override;

  ceph_msg_connect h_connect;
  ceph_msg_connect_reply h_reply;
  seastar::future<seastar::stop_iteration> repeat_accept();
  seastar::future<seastar::stop_iteration> send_connect_reply(
      msgr_tag_t tag,
      bufferlist&& authorizer_reply = {});
  seastar::future<seastar::stop_iteration> send_connect_reply_ready(
      msgr_tag_t tag,
      bufferlist&& authorizer_reply);
  seastar::future<seastar::stop_iteration> handle_connect_with_existing(
      ConnectionRef existing,
      bufferlist&& authorizer_reply);
  seastar::future<seastar::stop_iteration> replace_existing(
      ConnectionRef existing,
      bufferlist&& authorizer_reply,
      bool is_reset_from_peer = false);
  bool require_auth_feature() const;

 public:
  AcceptProtocol(Protocol *const *protos,
                 Session *_s,
                 Connection *conn,
                 Dispatcher *disp,
                 Messenger *msgr)
    : Protocol(protos, _s, conn, disp, msgr) {};

  const state_t& state() const override { return my_state; }

  seastar::future<seastar::stop_iteration> send(MessageRef msg) override {
    /// accepting connection should not be registered.
    ceph_assert(false);
  }

  seastar::future<seastar::stop_iteration> keepalive() override {
    /// accepting connection should not be registered.
    ceph_assert(false);
  }

  bool replace(const Protocol *newp) override {
    /// accepting connection should not be registered.
    ceph_assert(false);
  }
};

class ConnectProtocol : public Protocol
{
  static constexpr state_t my_state = state_t::connect;
  seastar::future<> do_execute() override;

  ceph_msg_connect h_connect;
  ceph_msg_connect_reply h_reply;
  bool got_bad_auth = false;
  std::unique_ptr<AuthAuthorizer> authorizer;
  seastar::future<seastar::stop_iteration> repeat_connect();

 public:
  ConnectProtocol(Protocol *const *protos,
                  Session *_s,
                  Connection *conn,
                  Dispatcher *disp,
                  Messenger *msgr)
    : Protocol(protos, _s, conn, disp, msgr) {};

  const state_t& state() const override { return my_state; }

  seastar::future<seastar::stop_iteration> send(MessageRef msg) override {
    return wait().then([] {
        return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::no);
      });
  }

  seastar::future<seastar::stop_iteration> keepalive() override {
    return wait().then([] {
        return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::no);
      });
  }

  bool replace(const Protocol *newp) override;
};

class OpenProtocol : public Protocol
{
  static constexpr state_t my_state = state_t::open;
  seastar::future<> do_execute() override;

  /// state for an incoming message
  struct MessageReader {
    ceph_msg_header header;
    ceph_msg_footer footer;
    bufferlist front;
    bufferlist middle;
    bufferlist data;
  } m;

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

  /// satisfied when a CEPH_MSGR_TAG_MSG is read, indicating that a message
  /// header will follow
  seastar::promise<> on_message;

  seastar::future<seastar::stop_iteration> handle_ack();
  seastar::future<> handle_keepalive2();
  seastar::future<> handle_keepalive2_ack();
  void read_tags_until_next_message();
  seastar::future<> maybe_throttle();
  seastar::future<MessageRef> do_read_message();
  bool update_rx_seq(seq_num_t seq);
  seastar::future<MessageRef> read_message();
  seastar::future<> write_message(MessageRef msg);

 public:
  OpenProtocol(Protocol *const *protos,
               Session *_s,
               Connection *conn,
               Dispatcher *disp,
               Messenger *msgr)
    : Protocol(protos, _s, conn, disp, msgr) {};

  const state_t& state() const override { return my_state; }

  seastar::future<seastar::stop_iteration> send(MessageRef msg) override;

  seastar::future<seastar::stop_iteration> keepalive() override;

  bool replace(const Protocol *newp) override;
};

class CloseProtocol : public Protocol
{
  static constexpr state_t my_state = state_t::close;
  seastar::future<> do_execute() override;

 public:
  CloseProtocol(Protocol *const *protos,
           Session *_s,
           Connection *conn,
           Dispatcher *disp,
           Messenger *msgr)
    : Protocol(protos, _s, conn, disp, msgr) {};

  const state_t& state() const override { return my_state; }

  seastar::future<seastar::stop_iteration> send(MessageRef msg) override {
    return seastar::make_ready_future<seastar::stop_iteration>(
        seastar::stop_iteration::yes);
  }

  seastar::future<seastar::stop_iteration> keepalive() override {
    return seastar::make_ready_future<seastar::stop_iteration>(
        seastar::stop_iteration::yes);
  }

  bool replace(const Protocol *newp) override {
    /// closed connection should be already unregistered.
    ceph_assert(false);
  }
};

} // namespace ceph::net
