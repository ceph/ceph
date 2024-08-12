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

#include <boost/asio/redirect_error.hpp>
#include <boost/container/small_vector.hpp>
#include <h3/observer.h>
#include "connection.h"
#include "error.h"
#include "message.h"

namespace rgw::h3 {

ConnectionImpl::ConnectionImpl(Observer& observer, quiche_h3_config* h3config,
                               udp_socket& socket, StreamHandler& on_new_stream,
                               conn_ptr _conn, connection_id cid)
    : observer(observer), h3config(h3config),
      ex(asio::make_strand(socket.get_executor())),
      socket(socket), on_new_stream(on_new_stream),
      timeout_timer(ex), pacing_timer(ex),
      conn(std::move(_conn)), cid(std::move(cid))
{
}

ConnectionImpl::~ConnectionImpl()
{
  observer.on_conn_destroy(cid);
}

auto ConnectionImpl::accept()
    -> asio::awaitable<error_code, executor_type>
{
  observer.on_conn_accept(cid);

  reset_timeout(); // schedule the initial timeout

  co_return co_await writer();
}

void ConnectionImpl::reset_timeout()
{
  const uint64_t value = ::quiche_conn_timeout_as_nanos(conn.get());
  if (value == std::numeric_limits<uint64_t>::max()) {
    timeout_timer.cancel();
    return;
  }
  auto ns = std::chrono::nanoseconds{value};
  observer.on_conn_schedule_timeout(cid, ns);
  timeout_timer.expires_after(ns);
  timeout_timer.async_wait([this] (error_code ec) { on_timeout(ec); });
}

void ConnectionImpl::on_timeout(error_code ec)
{
  if (ec) {
    return;
  }

  ::quiche_conn_on_timeout(conn.get());

  if (::quiche_conn_is_closed(conn.get())) {
    ec = on_closed();
    streamio_reset(ec);
  }

  writer_wake(ec);
}

auto ConnectionImpl::flush_some()
    -> asio::awaitable<error_code, executor_type>
{
  // send up to 8 packets at a time with sendmmsg()
  static constexpr size_t max_mmsg = 8;
  std::array<message, max_mmsg> messages;
  std::array<iovec, max_mmsg> iovs;
  std::array<mmsghdr, max_mmsg> headers;
  std::array<quiche_send_info, max_mmsg> sendinfos;

  error_code ec;
  size_t count = 0;
  for (; count < max_mmsg; count++) {
    auto& m = messages[count];
    auto& sendinfo = sendinfos[count];

    // serialize the next packet for sending
    const ssize_t bytes = ::quiche_conn_send(
        conn.get(), m.buffer.data(), m.buffer.max_size(), &sendinfo);

    if (bytes < 0) {
      ec.assign(bytes, quic_category());
      if (ec == quic_errc::done) {
        if (count == 0) {
          co_return ec;
        }
        break; // send the packets we've already prepared
      }
      observer.on_conn_send_error(cid, ec);
      co_return ec;
    }

    auto& iov = iovs[count];
    iov = iovec{m.buffer.data(), static_cast<size_t>(bytes)};

    auto& h = headers[count];
    h.msg_hdr.msg_name = &sendinfo.to;
    h.msg_hdr.msg_namelen = sendinfo.to_len;
    h.msg_hdr.msg_iov = &iov;
    h.msg_hdr.msg_iovlen = 1;
    h.msg_hdr.msg_control = nullptr;
    h.msg_hdr.msg_controllen = 0;
    h.msg_hdr.msg_flags = 0;
    h.msg_len = 0;
  }

  // apply the pacing delay from the first packet
  const auto& first = sendinfos[0];
  const auto send_at = pacing_clock::time_point{
      std::chrono::seconds(first.at.tv_sec) +
      std::chrono::nanoseconds(first.at.tv_nsec)};

  if (send_at > pacing_clock::zero()) {
    const auto now = pacing_clock::now();
    if (send_at > now) {
      const auto delay = send_at - now;
      if (delay >= pacing_threshold) {
        observer.on_conn_pacing_delay(cid, delay);

        pacing_timer.expires_after(delay);
        co_await pacing_timer.async_wait(
            asio::redirect_error(use_awaitable, ec));
        if (ec) {
          co_return ec;
        }
      }
    }
  }

  int sent = ::sendmmsg(socket.native_handle(), headers.data(), count, 0);
  if (sent == -1) {
    ec.assign(errno, boost::system::system_category());
    observer.on_conn_sendmmsg_error(cid, ec);
  }
  co_return ec;
}

auto ConnectionImpl::flush()
    -> asio::awaitable<error_code, executor_type>
{
  error_code ec;
  while (!ec) {
    ec = co_await flush_some();
  }
  co_return ec;
}

auto ConnectionImpl::writer()
    -> asio::awaitable<error_code, executor_type>
{
  for (;;) {
    error_code ec = co_await writer_wait();
    if (ec) {
      co_return ec;
    }

    ec = co_await flush();
    if (ec != quic_errc::done) {
      co_return ec;
    }

    if (::quiche_conn_is_closed(conn.get())) {
      ec = on_closed();
      streamio_reset(ec);
      co_return ec;
    }
    reset_timeout(); // reschedule the connection timeout
  }
  // unreachable
}

auto ConnectionImpl::writer_wait()
    -> asio::awaitable<error_code, executor_type>
{
  if (writer_handler) {
    throw std::runtime_error("ConnectionImpl::writer_wait() only "
                             "supports a single writer");
  }

  auto token = use_awaitable;
  return asio::async_initiate<use_awaitable_t, WriterSignature>(
      [this] (WriterHandler h) {
        writer_handler.emplace(std::move(h));
      }, token);
}

void ConnectionImpl::writer_wake(error_code ec)
{
  if (!writer_handler) {
    return;
  }
  auto c = asio::append(std::move(*writer_handler), nullptr, ec);
  writer_handler.reset();

  asio::post(std::move(c));
}

struct h3event_deleter {
  void operator()(quiche_h3_event* ev) { ::quiche_h3_event_free(ev); }
};
using h3event_ptr = std::unique_ptr<quiche_h3_event, h3event_deleter>;


int header_cb(uint8_t *name, size_t name_len,
              uint8_t *value, size_t value_len,
              void *argp)
{
  auto& headers = *reinterpret_cast<http::fields*>(argp);
  headers.insert({reinterpret_cast<char*>(name), name_len},
                 {reinterpret_cast<char*>(value), value_len});
  return 0;
}

error_code ConnectionImpl::poll_events(ip::udp::endpoint peer,
                                       ip::udp::endpoint self)
{
  for (;;) {
    quiche_h3_event* pevent = nullptr;
    int64_t stream_id = ::quiche_h3_conn_poll(h3conn.get(), conn.get(), &pevent);
    if (stream_id < 0) {
      break;
    }
    auto ev = h3event_ptr{pevent};
    const auto type = ::quiche_h3_event_type(ev.get());
    if (type == QUICHE_H3_EVENT_HEADERS) {
      // read request headers
      http::fields headers;
      int r = ::quiche_h3_event_for_each_header(ev.get(), header_cb, &headers);
      if (r < 0) {
        auto ec = error_code{r, h3_category()};
        observer.on_conn_h3_poll_error(cid, stream_id, ec);
        return ec;
      }

      // call the stream handler
      on_new_stream(boost::intrusive_ptr{this}, stream_id, std::move(headers),
                    std::move(self), std::move(peer));
    } else if (type == QUICHE_H3_EVENT_DATA) {
      if (auto r = readers.find(stream_id); r != readers.end()) {
        auto ec = streamio_read(*r);
        if (ec == h3_errc::done) {
          // wait for the next event
        } else if (ec) {
          return ec;
        } else if (r->read_data.empty()) {
          // wake the reader
          auto& handler = r->read_handler;
          readers.erase(r);
          streamio_wake(handler, ec);
        }
      }
    }
    // TODO: do we care about any other h3 events?
  }
  return {};
}

error_code ConnectionImpl::handle_packet(std::span<uint8_t> data,
                                         ip::udp::endpoint peer,
                                         ip::udp::endpoint self)
{
  error_code ec;

  const auto recvinfo = quiche_recv_info{
    peer.data(), static_cast<socklen_t>(peer.size()),
    self.data(), static_cast<socklen_t>(self.size())
  };

  const ssize_t bytes = ::quiche_conn_recv(
      conn.get(), data.data(), data.size(), &recvinfo);
  if (bytes < 0) {
    ec.assign(bytes, quic_category());
    observer.on_conn_recv_error(cid, ec);
    return ec;
  }

  if (::quiche_conn_is_established(conn.get())) {
    if (!h3conn) {
      // create the quiche_h3_conn() on first use
      h3conn.reset(::quiche_h3_conn_new_with_transport(conn.get(), h3config));
    }

    ec = poll_events(std::move(peer), std::move(self));
    if (ec) {
      return ec;
    }
  }

  return error_code{};
}

error_code ConnectionImpl::handle_packets(boost::intrusive::list<message> messages,
                                          const ip::udp::endpoint& self)
{
  error_code ec;
  for (auto& m : messages) {
    ec = handle_packet(m.buffer, m.peer, self);
    if (ec) {
      return ec;
    }
  }

  // try to wake writers
  auto w = writers.begin();
  while (w != writers.end()) {
    // try to write some more
    auto ec = streamio_write(*w, w->write_fin);
    if (ec == h3_errc::done) {
      ++w; // wait for more packets
    } else if (ec) {
      return ec;
    } else if (w->write_data.empty()) {
      auto& handler = w->write_handler;
      w = writers.erase(w);
      streamio_wake(handler, ec);
    } else {
      ++w; // wait for more packets
    }
  }

  writer_wake();

  return {};
}

auto ConnectionImpl::streamio_wait(std::optional<StreamIO::Handler>& handler)
    -> asio::awaitable<error_code, executor_type>
{
  writer_wake();

  if (handler) {
    throw std::runtime_error("ConnectionImpl::streamio_wait() only "
                             "supports a single reader and writer");
  }

  auto token = use_awaitable;
  return asio::async_initiate<use_awaitable_t, StreamIO::Signature>(
      [&handler] (StreamIO::Handler h) {
        handler.emplace(std::move(h));
      }, token);
}

void ConnectionImpl::streamio_reset(error_code ec)
{
  // cancel any readers/writers
  auto r = readers.begin();
  while (r != readers.end()) {
    auto& handler = r->read_handler;
    r = readers.erase(r);
    streamio_wake(handler, ec);
  }
  auto w = writers.begin();
  while (w != writers.end()) {
    auto& handler = w->write_handler;
    w = writers.erase(w);
    streamio_wake(handler, ec);
  }
}

error_code ConnectionImpl::streamio_read(StreamIO& stream)
{
  auto& data = stream.read_data;
  const ssize_t bytes = ::quiche_h3_recv_body(
        h3conn.get(), conn.get(), stream.id,
        data.data(), data.size());

  error_code ec;
  if (bytes < 0) {
    ec.assign(static_cast<int>(bytes), h3_category());
  } else {
    data = data.subspan(bytes);
  }
  return ec;
}

error_code ConnectionImpl::streamio_write(StreamIO& stream, bool fin)
{
  auto& data = stream.write_data;
  const ssize_t bytes = ::quiche_h3_send_body(
      h3conn.get(), conn.get(), stream.id,
      data.data(), data.size(), fin);

  error_code ec;
  if (bytes < 0) {
    ec.assign(static_cast<int>(bytes), h3_category());
  } else {
    data = data.subspan(bytes);
  }
  return ec;
}

void ConnectionImpl::streamio_wake(std::optional<StreamIO::Handler>& handler,
                                   error_code ec)
{
  if (!handler) {
    throw std::runtime_error("ConnectionImpl::streamio_wake() "
                             "called without a waiter");
  }

  // bind arguments to the handler for dispatch
  auto c = asio::append(std::move(*handler), nullptr, ec);
  handler.reset();

  asio::post(std::move(c));
}

auto ConnectionImpl::read_body(StreamIO& stream, std::span<uint8_t> data)
    -> asio::awaitable<size_t, executor_type>
{
  stream.read_data = data;

  while (!stream.read_data.empty()) {
    auto ec = streamio_read(stream);
    if (ec == h3_errc::done) {
      // no bytes buffered, wait for more packets
      readers.push_back(stream);
      ec = co_await streamio_wait(stream.read_handler);
      if (ec) {
        throw boost::system::system_error(ec);
      }
    } else if (ec) {
      observer.on_stream_recv_body_error(cid, stream.id, ec);
      throw boost::system::system_error(ec);
    }
  }

  observer.on_stream_recv_body(cid, stream.id, data.size());
  co_return data.size() - stream.read_data.size();
}

auto ConnectionImpl::write_response(StreamIO& stream,
                                    const http::fields& response,
                                    bool fin)
    -> asio::awaitable<void, executor_type>
{
  static constexpr size_t static_count = 32;
  using vector_type = boost::container::small_vector<
      quiche_h3_header, static_count>;
  vector_type headers;

  for (const auto& f : response) {
    auto& h = headers.emplace_back();
    const auto name = f.name_string();
    h.name = reinterpret_cast<const uint8_t*>(name.data());
    h.name_len = name.size();
    const auto value = f.value();
    h.value = reinterpret_cast<const uint8_t*>(value.data());
    h.value_len = value.size();
  }

  for (;;) { // retry on h3_errc::done
    const int result = ::quiche_h3_send_response(
        h3conn.get(), conn.get(), stream.id,
        headers.data(), headers.size(), fin);

    if (result < 0) {
      auto ec = error_code{result, h3_category()};
      observer.on_stream_send_response_error(cid, stream.id, ec);

      if (ec == h3_errc::done) {
        // unable to buffer more bytes, wait for flow control window and retry
        writers.push_back(stream);
        ec = co_await streamio_wait(stream.write_handler);
        if (ec) {
          throw boost::system::system_error(ec);
        }
        continue;
      }
      throw boost::system::system_error(ec);
    }

    observer.on_stream_send_response(cid, stream.id);
    writer_wake();
    co_return;
  }
  // unreachable
}

auto ConnectionImpl::write_body(StreamIO& stream, std::span<const uint8_t> data,
                                bool fin)
    -> asio::awaitable<size_t, executor_type>
{
  stream.write_data = data;
  stream.write_fin = fin;

  // if fin is set, call quiche_h3_send_body() even if there's no data
  bool fin_flag = fin;

  while (!stream.write_data.empty() || fin_flag) {
    auto ec = streamio_write(stream, fin);
    if (ec == h3_errc::done) {
      // unable to buffer more bytes, wait for flow control window
      writers.push_back(stream);
      ec = co_await streamio_wait(stream.write_handler);
      if (ec) {
        throw boost::system::system_error(ec);
      }
    } else if (ec) {
      observer.on_stream_send_body_error(cid, stream.id, ec);
      throw boost::system::system_error(ec);
    } else {
      fin_flag = false;
    }
  }

  observer.on_stream_send_body(cid, stream.id, data.size());
  writer_wake();
  co_return data.size() - stream.write_data.size();
}

error_code ConnectionImpl::on_closed()
{
  bool is_app;
  uint64_t code;
  const uint8_t* reason_buf;
  size_t reason_len;
  if (::quiche_conn_peer_error(conn.get(), &is_app, &code,
                               &reason_buf, &reason_len)) {
    auto reason = std::string_view{
      reinterpret_cast<const char*>(reason_buf), reason_len};
    observer.on_conn_close_peer(cid, reason, code, is_app);
    return make_error_code(std::errc::connection_reset);
  }
  if (::quiche_conn_local_error(conn.get(), &is_app, &code,
                                &reason_buf, &reason_len)) {
    auto reason = std::string_view{
      reinterpret_cast<const char*>(reason_buf), reason_len};
    observer.on_conn_close_local(cid, reason, code, is_app);
    return make_error_code(std::errc::connection_reset);
  }
  if (::quiche_conn_is_timed_out(conn.get())) {
    observer.on_conn_timed_out(cid);
    return make_error_code(std::errc::timed_out);
  }
  return error_code{};
}

void ConnectionImpl::cancel()
{
  auto self = boost::intrusive_ptr{this};
  asio::dispatch(get_executor(), [this, self=std::move(self)] {
      // cancel accept() so it returns control to Listener for removal from its
      // connection set
      cancel_accept.emit(asio::cancellation_type::terminal);

      // cancel any pending stream io to unblock process_request()
      const auto ec = make_error_code(asio::error::operation_aborted);
      streamio_reset(ec);

      // cancel timers
      timeout_timer.cancel();
      writer_wake(ec);
      pacing_timer.cancel();
    });
}

} // namespace rgw::h3
