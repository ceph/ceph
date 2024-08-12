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

#include <random>
#include <span>

#include <boost/asio/detached.hpp>
#include <boost/asio/ssl.hpp>
#include <quiche.h>

#include <h3/h3.h>
#include "connection_set.h"
#include "ssl.h"

namespace rgw::h3 {

struct message;

/// Listener reads packets from a bound udp socket, manages the set of accepted
/// connections, and delivers their incoming packets.
class ListenerImpl : public Listener {
 public:
  /// Construct a listener on the given socket.
  explicit ListenerImpl(Observer& observer, ConfigImpl& config,
                        executor_type ex, udp_socket socket,
                        StreamHandler& on_new_stream);

  executor_type get_executor() const override { return ex; }

  /// Start reading packets from the socket, accepting new connections and
  /// their streams, and dispatching requests to the stream handler.
  void async_listen() override
  {
    // spawn listen() on the listener's executor and enable its
    // cancellation via 'cancel_listen'
    auto ex = get_executor();
    asio::co_spawn(ex, listen(),
        bind_cancellation_slot(cancel_listen.slot(),
            bind_executor(ex, asio::detached)));
  }

  /// Close the socket and its associated connections/streams
  void close() override;

 private:
  Observer& observer;
  boost::intrusive_ptr<SSL_CTX> ssl_context;
  quiche_config* config;
  quiche_h3_config* h3config;
  executor_type ex;
  udp_socket socket;
  StreamHandler& on_new_stream;

  asio::cancellation_signal cancel_listen;
  connection_set connections_by_id;

  using use_awaitable_t = asio::use_awaitable_t<executor_type>;
  static constexpr use_awaitable_t use_awaitable{};

  auto listen()
      -> asio::awaitable<void, executor_type>;

  // parse an incoming packet. attempt to negotiate new connections for
  // unrecognized destination connection ids. return a reference to
  // ConnectionImpl for recognized connections
  auto parse_packet(std::default_random_engine& rng,
                    const ip::udp::endpoint& self,
                    const ip::udp::endpoint& peer,
                    std::span<const uint8_t> data)
      -> std::pair<boost::intrusive_ptr<ConnectionImpl>, error_code>;
};

} // namespace rgw::h3

extern "C" {

/// Create a Listener on the given udp socket.
auto create_h3_listener(rgw::h3::Observer& observer,
                        rgw::h3::Config& config,
                        rgw::h3::Listener::executor_type ex,
                        rgw::h3::udp_socket socket,
                        rgw::h3::StreamHandler& on_new_stream)
    -> std::unique_ptr<rgw::h3::Listener>;

} // extern "C"
