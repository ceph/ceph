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
#include "auth/AuthSessionHandler.h"

namespace ceph::net {

class ProtocolV1 : public Protocol
{
 private:
  ////////////////////////// ProtocolV1:States //////////////////////////
  class StateNone : public Protocol::State
  {
    ProtocolV1 &proto;

    seastar::future<> do_execute() override { return seastar::now(); }
    void trigger_close() override { ceph_assert(false); }
    bool is_connected() const override { ceph_assert(false); }
    seastar::future<seastop> send(MessageRef msg) override { ceph_assert(false); }
    seastar::future<seastop> keepalive() override { ceph_assert(false); }

   public:
    StateNone(ProtocolV1 &);
    void start_accept();
    void start_connect();
  };

  class StateAccept : public Protocol::State
  {
    ProtocolV1 &proto;

    seastar::future<> do_execute() override;
    void trigger_close() override;
    bool is_connected() const override { ceph_assert(false); }
    seastar::future<seastop> send(MessageRef msg) override;
    seastar::future<seastop> keepalive() override;

    seastar::future<seastop> repeat_accept();
    seastar::future<seastop> send_connect_reply(
        msgr_tag_t tag,
        bufferlist&& authorizer_reply = {});
    seastar::future<seastop> send_connect_reply_ready(
        msgr_tag_t tag,
        bufferlist&& authorizer_reply);
    seastar::future<seastop> handle_connect_with_existing(
        SocketConnectionRef existing,
        bufferlist&& authorizer_reply);
    seastar::future<seastop> replace_existing(
        SocketConnectionRef existing,
        bufferlist&& authorizer_reply,
        bool is_reset_from_peer = false);
    bool require_auth_feature() const;

   public:
    StateAccept(ProtocolV1 &);
  };

  class StateConnect : public Protocol::State
  {
    ProtocolV1 &proto;

    seastar::future<> do_execute() override;
    void trigger_close() override;
    bool is_connected() const override { return false; }
    seastar::future<seastop> send(MessageRef msg) override;
    seastar::future<seastop> keepalive() override;

    bool got_bad_auth = false;
    std::unique_ptr<AuthAuthorizer> authorizer;
    seastar::future<seastop> repeat_connect();

   public:
    StateConnect(ProtocolV1 &);
  };

  class StateOpen: public Protocol::State
  {
    ProtocolV1 &proto;

    seastar::future<> do_execute() override;
    void trigger_close() override;
    bool is_connected() const override { return true; }
    seastar::future<seastop> send(MessageRef msg) override;
    seastar::future<seastop> keepalive() override;

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

    seastar::future<seastop> handle_ack();
    seastar::future<> handle_keepalive2();
    seastar::future<> handle_keepalive2_ack();
    void read_tags_until_next_message();
    seastar::future<> maybe_throttle();
    seastar::future<MessageRef> do_read_message();
    bool update_rx_seq(seq_num_t seq);
    seastar::future<MessageRef> read_message();
    seastar::future<> write_message(MessageRef msg);

   public:
    StateOpen(ProtocolV1 &);
  };

  class StateClose : public Protocol::State
  {
    ProtocolV1 &proto;

    seastar::future<> do_execute() override;
    void trigger_close() override;
    bool is_connected() const override { return false; }
    seastar::future<seastop> send(MessageRef msg) override;
    seastar::future<seastop> keepalive() override;

   public:
    StateClose(ProtocolV1 &);
  };

  ////////////////////////////// ProtocolV1 //////////////////////////////
 public:
  ProtocolV1(Session &_s,
             SocketConnection &_conn,
             Dispatcher &_disp,
             SocketMessenger &_msgr)
    : Protocol(_s, _conn, _disp, _msgr, &state_none),
      state_none(*this),
      state_accept(*this),
      state_connect(*this),
      state_open(*this),
      state_close(*this) { }

 private:
  StateNone state_none;
  StateAccept state_accept;
  StateConnect state_connect;
  StateOpen state_open;
  StateClose state_close;

  void do_start_accept() override {
    state_none.start_accept();
  }
  void do_start_connect() override {
    state_none.start_connect();
  }

 public:
  ////////////////// ProtocolV1 defined data structures //////////////////
  ceph_msg_connect h_connect;
  ceph_msg_connect_reply h_reply;
};

} // namespace ceph::net
