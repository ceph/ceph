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

#include <core/reactor.hh>

#include "Connection.h"

namespace ceph {
namespace net {

class SocketConnection : public Connection {
  seastar::connected_socket socket;
  seastar::input_stream<char> in;
  seastar::output_stream<char> out;

  /// buffer state for read()
  struct Reader {
    bufferlist buffer;
    size_t remaining;
  } r;

  /// read the requested number of bytes into a bufferlist
  seastar::future<bufferlist> read(size_t bytes);

  /// state for an incoming message
  struct MessageReader {
    ceph_msg_header header;
    ceph_msg_footer footer;
    bufferlist front;
    bufferlist middle;
    bufferlist data;
  } m;

  /// becomes available when handshake completes, and when all previous messages
  /// have been sent to the output stream. send() chains new messages as
  /// continuations to this future to act as a queue
  seastar::future<> send_ready;

  /// encode/write a message
  seastar::future<> write_message(MessageRef msg);

 public:
  SocketConnection(Messenger *messenger,
                   const entity_addr_t& my_addr,
                   const entity_addr_t& peer_addr,
                   seastar::connected_socket&& socket);
  ~SocketConnection();

  bool is_connected() override;

  seastar::future<> client_handshake() override;

  seastar::future<> server_handshake() override;

  seastar::future<ceph_msg_header> read_header() override;

  seastar::future<MessageRef> read_message() override;

  seastar::future<> send(MessageRef msg) override;

  seastar::future<> close() override;
};

} // namespace net
} // namespace ceph
