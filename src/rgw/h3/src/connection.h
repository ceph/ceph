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

#include <memory>
#include <optional>
#include <span>
#include <tuple>

#include <boost/asio/append.hpp>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <quiche.h>

#include "common/ceph_time.h"

#include <h3/h3.h>

#include "message.h"

namespace rgw::h3 {

struct conn_deleter {
  void operator()(quiche_conn* conn) { ::quiche_conn_free(conn); }
};
using conn_ptr = std::unique_ptr<quiche_conn, conn_deleter>;

struct h3_conn_deleter {
  void operator()(quiche_h3_conn* h3) { ::quiche_h3_conn_free(h3); }
};
using h3_conn_ptr = std::unique_ptr<quiche_h3_conn, h3_conn_deleter>;


struct streamid_key {
  using type = uint64_t;
  type operator()(const StreamIO& s) { return s.id; }
};

struct streamid_less {
  using K = streamid_key::type;
  using V = const StreamIO&;
  bool operator()(V lhs, V rhs) const { return lhs.id < rhs.id; }
  bool operator()(K lhs, V rhs) const { return lhs < rhs.id; }
  bool operator()(V lhs, K rhs) const { return lhs.id < rhs; }
  bool operator()(K lhs, K rhs) const { return lhs < rhs; }
};

namespace bi = boost::intrusive;

using streamio_reader_set = bi::set<StreamIO,
      bi::member_hook<StreamIO, bi::set_member_hook<>, &StreamIO::read_hook>,
      bi::key_of_value<streamid_key>,
      bi::compare<streamid_less>>;

using streamio_writer_set = bi::set<StreamIO,
      bi::member_hook<StreamIO, bi::set_member_hook<>, &StreamIO::write_hook>,
      bi::key_of_value<streamid_key>,
      bi::compare<streamid_less>>;


/// A server-side h3 connection accepted by a Listener. When incoming packets
/// indicate new client-initiated streams, the stream handler is called to
/// process their requests. When the connection has packets to send, they're
/// written directly to the udp socket instead of going through Listener.
class ConnectionImpl : public Connection,
                       public boost::intrusive::set_base_hook<> {
 public:
  ConnectionImpl(Observer& observer, quiche_h3_config* h3config,
                 ip::udp::socket& socket, StreamHandler& on_new_stream,
                 conn_ptr conn, connection_id cid);
  ~ConnectionImpl();

  executor_type get_executor() const override { return ex; }

  /// Return the negotiated connection id.
  connection_id get_cid() const override { return cid; }

  /// Accept and service a new connection on its own executor. The completion
  /// handler does not complete until the connection closes.
  template <typename CompletionToken>
  auto async_accept(CompletionToken&& token)
  {
    using Signature = void(error_code);
    auto ref = boost::intrusive_ptr{this};
    return asio::async_initiate<CompletionToken, Signature>(
        [this, ref=std::move(ref)] (auto&& handler) mutable {
          // spawn accept() on the connection's executor and enable its
          // cancellation via 'cancel_accept'
          auto ex = get_executor();
          asio::co_spawn(ex, accept(),
              bind_cancellation_slot(cancel_accept.slot(),
                  bind_executor(ex,
                      [ref=std::move(ref), h=std::move(handler)]
                      (std::exception_ptr eptr, error_code ec) mutable {
                        if (eptr) {
                          std::rethrow_exception(std::move(eptr));
                        } else {
                          asio::dispatch(asio::append(std::move(h), ec));
                        }
                      })));
        }, token);
  }

  /// process input packets on the connection's executor
  template <typename CompletionToken>
  auto async_handle_packets(boost::intrusive::list<message> messages,
                            ip::udp::endpoint self,
                            CompletionToken&& token)
  {
    using Signature = void(error_code);
    auto ref = boost::intrusive_ptr{this};
    return asio::async_initiate<CompletionToken, Signature>(
        [this, ref=std::move(ref), messages=std::move(messages),
         self=std::move(self)] (auto&& handler) mutable {
          asio::dispatch(get_executor(),
              [this, ref=std::move(ref), messages=std::move(messages),
               self=std::move(self), h=std::move(handler)] () mutable {
                auto ec = handle_packets(std::move(messages), self);
                asio::dispatch(asio::append(std::move(h), ec));
              });
        }, token);
  }

  /// receive body bytes from the given stream
  auto read_body(StreamIO& stream, std::span<uint8_t> data)
      -> asio::awaitable<size_t, executor_type> override;

  /// write response headers to the given stream
  auto write_response(StreamIO& stream, const http::fields& response, bool fin)
      -> asio::awaitable<void, executor_type> override;

  /// write response body to the given stream
  auto write_body(StreamIO& stream, std::span<const uint8_t> data, bool fin)
      -> asio::awaitable<size_t, executor_type> override;

  void cancel();

 public:
  Observer& observer;
  quiche_h3_config* h3config;
  executor_type ex;
  // socket shared with Listener, but Connection only uses it to write packets
  ip::udp::socket& socket;
  StreamHandler& on_new_stream;

  // cancellation signal attached to async_accept()
  asio::cancellation_signal cancel_accept;

  template <typename Clock>
  using timer_type = asio::basic_waitable_timer<Clock,
      asio::wait_traits<Clock>, executor_type>;

  timer_type<ceph::coarse_mono_clock> timeout_timer;

  using use_awaitable_t = asio::use_awaitable_t<executor_type>;
  static constexpr use_awaitable_t use_awaitable{};

  using WriterSignature = void(std::exception_ptr, error_code);
  using WriterHandler = typename asio::async_result<
      use_awaitable_t, WriterSignature>::handler_type;
  std::optional<WriterHandler> writer_handler;

  using pacing_clock = ceph::mono_clock;
  timer_type<pacing_clock> pacing_timer;
  // we get pacing hints with a higher resolution than our timer. ignore
  // pacing delays until they're large enough to wait on
  static constexpr auto pacing_threshold = std::chrono::milliseconds(10);

  conn_ptr conn;
  h3_conn_ptr h3conn;
  connection_id cid;

  // streams waiting on reads/writes
  streamio_reader_set readers;
  streamio_writer_set writers;

  // long-lived coroutine that completes when the connection closes
  asio::awaitable<error_code, executor_type> accept();

  // arm the timeout timer with quiche_conn_timeout_as_nanos()
  void reset_timeout();
  // timeout handler
  void on_timeout(error_code ec);

  // send a batch of outgoing packets
  asio::awaitable<error_code, executor_type> flush_some();
  // flush all outgoing packets
  asio::awaitable<error_code, executor_type> flush();
  // continuous writer that wakes up to flush when writer_wait() is called
  asio::awaitable<error_code, executor_type> writer();
  // completes when writer_wake() is called
  asio::awaitable<error_code, executor_type> writer_wait();
  // wake writer() to flush data
  void writer_wake(error_code ec = {});

  // poll the connection for request headers on a new stream
  error_code poll_events(ip::udp::endpoint peer, ip::udp::endpoint self);

  // handle an incoming packet from the Listener
  error_code handle_packet(std::span<uint8_t> data,
                           ip::udp::endpoint peer,
                           ip::udp::endpoint self);

  error_code handle_packets(boost::intrusive::list<message> messages,
                            const ip::udp::endpoint& self);

  error_code streamio_read(StreamIO& stream);

  error_code streamio_write(StreamIO& stream, bool fin);

  void streamio_wake(std::optional<StreamIO::Handler>& handler, error_code ec);

  // add a stream to the given set and wait for its io to be ready
  auto streamio_wait(std::optional<StreamIO::Handler>& handler)
      -> asio::awaitable<error_code, executor_type>;

  // cancel blocked streams with the given error
  void streamio_reset(error_code ec);

  // return an error code that best describes why the connection was closed
  error_code on_closed();
};

} // namespace rgw::h3
