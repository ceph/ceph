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

#include <sys/socket.h>
#include <sys/uio.h>
#include <array>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/intrusive_ptr.hpp>
#include <h3/observer.h>
#include "address_validation.h"
#include "config.h"
#include "error.h"
#include "listener.h"
#include "message.h"

namespace rgw::h3 {

ListenerImpl::ListenerImpl(Observer& observer, ConfigImpl& cfg,
                           executor_type ex, udp_socket socket,
                           StreamHandler& on_new_stream)
    : observer(observer), ssl_context(cfg.get_ssl_context()),
      config(cfg.get_config()), h3config(cfg.get_h3_config()),
      ex(ex), socket(std::move(socket)),
      on_new_stream(on_new_stream)
{
}

// read and dispatch packets until the socket closes
auto ListenerImpl::listen() -> asio::awaitable<void, executor_type>
{
  // generator for random connection ids
  std::default_random_engine rng{std::random_device{}()};

  // receive up to 16 packets at a time with recvmmsg()
  static constexpr size_t max_mmsg = 16;
  std::array<message, max_mmsg> messages;
  std::array<iovec, max_mmsg> iovs;
  std::array<mmsghdr, max_mmsg> headers;

  for (size_t i = 0; i < max_mmsg; i++) {
    auto& m = messages[i];
    iovs[i] = iovec{m.buffer.data(), m.buffer.max_size()};
    auto& h = headers[i];
    h.msg_hdr.msg_name = m.peer.data();
    h.msg_hdr.msg_namelen = m.peer.size();
    h.msg_hdr.msg_iov = &iovs[i];
    h.msg_hdr.msg_iovlen = 1;
    h.msg_hdr.msg_control = nullptr;
    h.msg_hdr.msg_controllen = 0;
    h.msg_hdr.msg_flags = 0;
  }

  error_code ec;
  while (!ec) {
    for (size_t i = 0; i < max_mmsg; i++) {
      messages[i].buffer.resize(messages[i].buffer.max_size(),
                                boost::container::default_init);
      headers[i].msg_len = 0;
    }

    const int count = ::recvmmsg(socket.native_handle(), headers.data(),
                                 headers.size(), 0, nullptr);
    if (count == -1) {
      auto ec = error_code{errno, boost::system::system_category()};

      if (ec == std::errc::operation_would_block ||
          ec == std::errc::resource_unavailable_try_again) {
        // wait until the socket is readable
        co_await socket.async_wait(udp_socket::wait_read,
            asio::redirect_error(use_awaitable, ec));
        continue;
      }

      observer.on_listener_recvmmsg_error(ec);
      break;
    }

    auto self = socket.local_endpoint(ec);
    if (ec) {
      break;
    }

    auto m = messages.begin();
    auto end = std::next(m, count);
    for (auto h = headers.begin(); m != end; ++m, ++h) {
      // set the message size to match the bytes received
      m->buffer.resize(h->msg_len, boost::container::default_init);

      ec = co_await on_packet(rng, &*m, self);
      if (ec) {
        break;
      }
    }
  }

  observer.on_listener_closed(ec);
  co_return;
}

auto ListenerImpl::on_packet(std::default_random_engine& rng,
                             message* packet, ip::udp::endpoint self)
  -> asio::awaitable<error_code, executor_type>
{
  auto data = std::span(packet->buffer);
  ip::udp::endpoint& peer = packet->peer;

  // parse the packet header
  uint32_t version = 0;
  uint8_t type = 0;
  auto scid = connection_id{connection_id::max_size(),
                            boost::container::default_init};
  auto dcid = connection_id{connection_id::max_size(),
                            boost::container::default_init};
  size_t scid_len = scid.max_size();
  size_t dcid_len = dcid.max_size();
  auto token = address_validation_token{address_validation_token::max_size(),
                                        boost::container::default_init};
  size_t token_len = token.max_size();

  int rc = ::quiche_header_info(
      data.data(), data.size(),
      QUICHE_MAX_CONN_ID_LEN, &version, &type,
      scid.data(), &scid_len,
      dcid.data(), &dcid_len,
      token.data(), &token_len);
  if (rc < 0) {
    auto ec = error_code{rc, quic_category()};
    observer.on_listener_header_info_error(ec);
    co_return error_code{}; // not fatal
  }
  scid.resize(scid_len);
  dcid.resize(dcid_len);
  token.resize(token_len);

  observer.on_listener_packet_received(type, data.size(), peer,
                                       scid, dcid, token);

  // look up connection by dcid
  boost::intrusive_ptr<ConnectionImpl> connection;
  {
    connection_set::insert_commit_data commit_data;
    auto insert = connections_by_id.insert_check(dcid, commit_data);

    if (!insert.second) {
      // connection existed, take a reference while we deliver the packet
      connection = boost::intrusive_ptr<ConnectionImpl>(&*insert.first);
    } else {
      // dcid not found, can we accept the connection?

      if (!::quiche_version_is_supported(version)) {
        // send a version negotiation packet
        std::array<uint8_t, 2048> outbuf;
        ssize_t bytes = ::quiche_negotiate_version(
            scid.data(), scid.size(),
            dcid.data(), dcid.size(),
            outbuf.data(), outbuf.size());
        if (bytes <= 0) {
          auto ec = error_code{static_cast<int>(bytes), quic_category()};
          observer.on_listener_negotiate_version_error(peer, ec);
          co_return error_code{}; // not fatal
        }

        error_code ec;
        size_t sent = socket.send_to(asio::buffer(outbuf.data(), bytes),
                                     peer, 0, ec);
        if (ec == std::errc::operation_would_block ||
            ec == std::errc::resource_unavailable_try_again) {
          ec.clear(); // don't block the recvmmsg() loop
        } else if (ec) {
          observer.on_listener_sendto_error(peer, ec);
        } else {
          observer.on_listener_negotiate_version(peer, sent, version);
        }
        co_return ec;
      }

      if (token_len == 0) {
        // stateless retry
        token_len = write_token(dcid, peer, token);

        // generate a random cid
        connection_id cid;
        cid.resize(QUICHE_MAX_CONN_ID_LEN);
        std::generate(cid.begin(), cid.end(), rng);

        std::array<uint8_t, 2048> outbuf;
        ssize_t bytes = ::quiche_retry(scid.data(), scid.size(),
                                       dcid.data(), dcid.size(),
                                       cid.data(), cid.size(),
                                       token.data(), token_len, version,
                                       outbuf.data(), outbuf.size());
        if (bytes <= 0) {
          auto ec = error_code{static_cast<int>(bytes), quic_category()};
          observer.on_listener_stateless_retry_error(peer, ec);
          co_return error_code{}; // not fatal
        }

        error_code ec;
        size_t sent = socket.send_to(asio::buffer(outbuf.data(), bytes),
                                     peer, 0, ec);
        if (ec == std::errc::operation_would_block ||
            ec == std::errc::resource_unavailable_try_again) {
          ec.clear(); // don't block the recvmmsg() loop
        } else if (ec) {
          observer.on_listener_sendto_error(peer, ec);
        } else {
          observer.on_listener_stateless_retry(peer, sent, token, cid);
        }
        co_return ec;
      }

      // token validation
      connection_id odcid;
      const size_t odcid_len = validate_token(token, peer, odcid);
      if (odcid_len == 0) {
        observer.on_listener_token_validation_error(peer, token);
        co_return error_code{}; // not fatal
      }

      auto ssl = ::SSL_new(ssl_context.get());
      constexpr bool is_server = true;

      auto conn = conn_ptr{::quiche_conn_new_with_tls(
              dcid.data(), dcid.size(),
              odcid.data(), odcid.size(),
              self.data(), self.size(),
              peer.data(), peer.size(),
              config, ssl, is_server)};
      if (!conn) {
        observer.on_listener_accept_error(peer);
        co_return error_code{}; // not fatal
      }

      // allocate the Connection and commit its set insertion
      connection = new ConnectionImpl(observer, h3config, socket, on_new_stream,
                                      std::move(conn), std::move(dcid));
      insert.first = connections_by_id.insert_commit(*connection, commit_data);

      // accept the connection for processing. once the connection closes,
      // remove it from the connection set. this completion handler holds a
      // reference to the Connection while it's in the set
      connection->async_accept(
          asio::bind_executor(get_executor(),
              [this, connection] (error_code ec) {
                if (connection->is_linked()) {
                  auto c = connections_by_id.iterator_to(*connection);
                  connections_by_id.erase(c);
                }
              }));
    }
  }

  // handle the packet under Connection's executor
  error_code ec;
  co_await connection->async_handle_packet(data, peer, self,
      asio::redirect_error(use_awaitable, ec));

  co_return error_code{};
}

void ListenerImpl::close()
{
  asio::dispatch(get_executor(), [this] {
      // cancel the connections and remove them from the set
      auto c = connections_by_id.begin();
      while (c != connections_by_id.end()) {
        c->cancel();
        c = connections_by_id.erase(c);
      }
    });

  // cancel listen()
  cancel_listen.emit(asio::cancellation_type::terminal);

  // close the socket
  error_code ec;
  socket.close(ec);
}

} // namespace rgw::h3


extern "C" {

/// Create a Listener on the given udp socket.
auto create_h3_listener(rgw::h3::Observer& observer,
                        rgw::h3::Config& config,
                        rgw::h3::Listener::executor_type ex,
                        rgw::h3::udp_socket socket,
                        rgw::h3::StreamHandler& on_new_stream)
    -> std::unique_ptr<rgw::h3::Listener>
{
  auto& cfg = static_cast<rgw::h3::ConfigImpl&>(config);
  return std::make_unique<rgw::h3::ListenerImpl>(
      observer, cfg, ex, std::move(socket), on_new_stream);
}

} // extern "C"
