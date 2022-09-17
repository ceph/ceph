// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <chrono>
#include <string_view>

#include <h3/types.h>

namespace rgw::h3 {

/// Observer interface for logging.
class Observer {
 public:
  virtual ~Observer() {}

  /// Report an error from recvmmsg().
  virtual void on_listener_recvmmsg_error(error_code ec)
  {}
  /// Report an error from sendto().
  virtual void on_listener_sendto_error(const ip::udp::endpoint& peer,
                                        error_code ec)
  {}
  /// Report an error from quiche_header_info().
  virtual void on_listener_header_info_error(error_code ec)
  {}
  /// Listener received a packet.
  virtual void on_listener_packet_received(
      uint8_t type, size_t bytes, const ip::udp::endpoint& peer,
      const connection_id& scid, const connection_id& dcid,
      const address_validation_token& token)
  {}
  /// Report an error from quiche_negotiate_version().
  virtual void on_listener_negotiate_version_error(
      const ip::udp::endpoint& peer, error_code ec)
  {}
  /// Listener replied with a version negotiation packet.
  virtual void on_listener_negotiate_version(const ip::udp::endpoint& peer,
                                             size_t bytes, uint32_t version)
  {}
  /// Report an error from quiche_retry().
  virtual void on_listener_stateless_retry_error(
      const ip::udp::endpoint& peer, error_code ec)
  {}
  /// Listener replied with a stateless retry packet.
  virtual void on_listener_stateless_retry(
      const ip::udp::endpoint& peer, size_t bytes,
      const address_validation_token& token, const connection_id& cid)
  {}
  /// Listener received an invalid address validation token.
  virtual void on_listener_token_validation_error(
      const ip::udp::endpoint& peer,
      const address_validation_token& token)
  {}
  /// Report an error from quiche_accept().
  virtual void on_listener_accept_error(const ip::udp::endpoint& peer)
  {}
  /// Listener stopped receiving packets.
  virtual void on_listener_closed(error_code ec)
  {}

  /// New connection successfully negotiated.
  virtual void on_conn_accept(const connection_id& cid)
  {}
  /// Connection closed by peer.
  virtual void on_conn_close_peer(const connection_id& cid,
                                  std::string_view reason,
                                  uint64_t code, bool is_app)
  {}
  /// Connection closed due to a local error.
  virtual void on_conn_close_local(const connection_id& cid,
                                   std::string_view reason,
                                   uint64_t code, bool is_app)
  {}
  /// Connection reached idle timeout.
  virtual void on_conn_timed_out(const connection_id& cid)
  {}
  /// Connection handle destroyed after last use.
  virtual void on_conn_destroy(const connection_id& cid)
  {}
  /// Connection timer rescheduled.
  virtual void on_conn_schedule_timeout(const connection_id& cid,
                                        std::chrono::nanoseconds ns)
  {}
  /// Connection writes delayed for congestion control.
  virtual void on_conn_pacing_delay(const connection_id& cid,
                                    std::chrono::nanoseconds ns)
  {}
  /// Report an error from quiche_conn_send().
  virtual void on_conn_send_error(const connection_id& cid, error_code ec)
  {}
  /// Report an error from quiche_conn_recv().
  virtual void on_conn_recv_error(const connection_id& cid, error_code ec)
  {}
  /// Report an error from sendmmsg().
  virtual void on_conn_sendmmsg_error(const connection_id& cid, error_code ec)
  {}
  /// Report an error from quiche_h3_conn_poll().
  virtual void on_conn_h3_poll_error(const connection_id& cid,
                                     uint64_t stream_id, error_code ec)
  {}

  /// Received a HEADERS frame for a new stream.
  virtual void on_stream_accept(const connection_id& cid,
                                uint64_t stream_id)
  {}
  /// Report an error from quiche_h3_recv_body().
  virtual void on_stream_recv_body_error(const connection_id& cid,
                                         uint64_t stream_id,
                                         error_code ec)
  {}
  /// Successfully read bytes from a stream's request body.
  virtual void on_stream_recv_body(const connection_id& cid,
                                   uint64_t stream_id, size_t bytes)
  {}
  /// Report an error from quiche_h3_send_body().
  virtual void on_stream_send_body_error(const connection_id& cid,
                                         uint64_t stream_id,
                                         error_code ec)
  {}
  /// Successfully wrote bytes of a stream's response body.
  virtual void on_stream_send_body(const connection_id& cid,
                                   uint64_t stream_id, size_t bytes)
  {}
  /// Report an error from quiche_h3_send_response().
  virtual void on_stream_send_response_error(const connection_id& cid,
                                             uint64_t stream_id,
                                             error_code ec)
  {}
  /// Successfully wrote the HEADERS frame for a stream's response.
  virtual void on_stream_send_response(const connection_id& cid,
                                       uint64_t stream_id)
  {}
};

} // namespace rgw::h3
