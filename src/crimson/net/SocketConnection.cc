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

#include <algorithm>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/packet.hh>

#include "Config.h"
#include "Dispatcher.h"
#include "Messenger.h"
#include "SocketConnection.h"

#include "include/msgr.h"
#include "auth/Auth.h"
#include "auth/AuthSessionHandler.h"
#include "msg/Message.h"

#include "crimson/common/log.h"

using namespace ceph::net;

template <typename T>
seastar::net::packet make_static_packet(const T& value) {
    return { reinterpret_cast<const char*>(&value), sizeof(value) };
}

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_ms);
  }
}

SocketConnection::SocketConnection(Messenger *messenger,
                                   Dispatcher *disp,
                                   const entity_addr_t& my_addr,
                                   const entity_addr_t& peer_addr)
  : Connection(messenger),
    _s(my_addr, peer_addr),
    workspace(&_s, this, disp, messenger),
    dispatcher(disp)
{
}

SocketConnection::SocketConnection(Messenger *messenger,
                                   Dispatcher *disp,
                                   const entity_addr_t& my_addr,
                                   const entity_addr_t& peer_addr,
                                   seastar::connected_socket&& fd)
  : Connection(messenger),
    _s(my_addr, peer_addr, std::forward<seastar::connected_socket>(fd)),
    workspace(&_s, this, disp, messenger),
    dispatcher(disp)
{
}

SocketConnection::~SocketConnection()
{
  // errors were reported to callers of send()
  ceph_assert(send_ready.available());
  send_ready.ignore_ready_future();
}

bool SocketConnection::is_connected()
{
  return !send_ready.failed();
}

void SocketConnection::read_tags_until_next_message()
{
  seastar::repeat([this] {
      // read the next tag
      return s->socket->read_exactly(1)
        .then([this] (auto buf) {
          if (buf.empty()) {
            throw std::system_error(make_error_code(error::read_eof));
          }
          switch (buf[0]) {
          case CEPH_MSGR_TAG_MSG:
            // stop looping and notify read_header()
            return seastar::make_ready_future<seastar::stop_iteration>(
                seastar::stop_iteration::yes);
          case CEPH_MSGR_TAG_ACK:
            return handle_ack();
          case CEPH_MSGR_TAG_KEEPALIVE:
            break;
          case CEPH_MSGR_TAG_KEEPALIVE2:
            return handle_keepalive2()
              .then([this] { return seastar::stop_iteration::no; });
          case CEPH_MSGR_TAG_KEEPALIVE2_ACK:
            return handle_keepalive2_ack()
              .then([this] { return seastar::stop_iteration::no; });
          case CEPH_MSGR_TAG_CLOSE:
            std::cout << "close" << std::endl;
            break;
          }
          return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::no);
        });
    }).handle_exception_type([this] (const std::system_error& e) {
      if (e.code() == error::read_eof) {
        close();
      }
      throw e;
    }).then_wrapped([this] (auto fut) {
      // satisfy the message promise
      fut.forward_to(std::move(on_message));
    });
}

seastar::future<seastar::stop_iteration> SocketConnection::handle_ack()
{
  return s->socket->read_exactly(sizeof(ceph_le64))
    .then([this] (auto buf) {
      auto seq = reinterpret_cast<const ceph_le64*>(buf.get());
      s->discard_sent_up_to(*seq);
      return seastar::stop_iteration::no;
    });
}

void SocketConnection::requeue_sent()
{
  s->out_seq -= s->sent.size();
  while (!s->sent.empty()) {
    auto m = s->sent.front();
    s->sent.pop();
    s->out_q.push(std::move(m));
  }
}

seastar::future<> SocketConnection::maybe_throttle()
{
  if (!s->policy.throttler_bytes) {
    return seastar::now();
  }
  const auto to_read = (m.header.front_len +
                        m.header.middle_len +
                        m.header.data_len);
  return s->policy.throttler_bytes->get(to_read);
}

seastar::future<MessageRef> SocketConnection::do_read_message()
{
  return on_message.get_future()
    .then([this] {
      on_message = seastar::promise<>{};
      // read header
      return s->socket->read(sizeof(m.header));
    }).then([this] (bufferlist bl) {
      // throttle the traffic, maybe
      auto p = bl.cbegin();
      ::decode(m.header, p);
      return maybe_throttle();
    }).then([this] {
      // read front
      return s->socket->read(m.header.front_len);
    }).then([this] (bufferlist bl) {
      m.front = std::move(bl);
      // read middle
      return s->socket->read(m.header.middle_len);
    }).then([this] (bufferlist bl) {
      m.middle = std::move(bl);
      // read data
      return s->socket->read(m.header.data_len);
    }).then([this] (bufferlist bl) {
      m.data = std::move(bl);
      // read footer
      return s->socket->read(sizeof(m.footer));
    }).then([this] (bufferlist bl) {
      // resume background processing of tags
      read_tags_until_next_message();

      auto p = bl.cbegin();
      ::decode(m.footer, p);
      auto msg = ::decode_message(nullptr, 0, m.header, m.footer,
                                  m.front, m.middle, m.data, nullptr);
      // TODO: set time stamps
      msg->set_byte_throttler(s->policy.throttler_bytes);
      constexpr bool add_ref = false; // Message starts with 1 ref
      return MessageRef{msg, add_ref};
    });
}

seastar::future<MessageRef> SocketConnection::read_message()
{
  namespace stdx = std::experimental;

  return seastar::repeat_until_value([this] {
      return do_read_message()
        .then([this] (MessageRef msg) -> stdx::optional<MessageRef> {
          if (!update_rx_seq(msg->get_seq())) {
            // skip this request and read the next
            return stdx::nullopt;
          }
          return msg;
        });
    });
}

bool SocketConnection::update_rx_seq(seq_num_t seq)
{
  if (seq <= s->in_seq) {
    if (HAVE_FEATURE(s->features, RECONNECT_SEQ) &&
        conf.ms_die_on_old_message) {
      ceph_abort_msg("old msgs despite reconnect_seq feature");
    }
    return false;
  } else if (seq > s->in_seq + 1) {
    if (conf.ms_die_on_skipped_message) {
      ceph_abort_msg("skipped incoming seq");
    }
    return false;
  } else {
    s->in_seq = seq;
    return true;
  }
}

seastar::future<> SocketConnection::write_message(MessageRef msg)
{
  msg->set_seq(++s->out_seq);
  msg->encode(s->features, get_messenger()->get_crc_flags());
  bufferlist bl;
  bl.append(CEPH_MSGR_TAG_MSG);
  auto& header = msg->get_header();
  bl.append((const char*)&header, sizeof(header));
  bl.append(msg->get_payload());
  bl.append(msg->get_middle());
  bl.append(msg->get_data());
  auto& footer = msg->get_footer();
  if (HAVE_FEATURE(s->features, MSG_AUTH)) {
    bl.append((const char*)&footer, sizeof(footer));
  } else {
    ceph_msg_footer_old old_footer;
    if (get_messenger()->get_crc_flags() & MSG_CRC_HEADER) {
      old_footer.front_crc = footer.front_crc;
      old_footer.middle_crc = footer.middle_crc;
    } else {
      old_footer.front_crc = old_footer.middle_crc = 0;
    }
    if (get_messenger()->get_crc_flags() & MSG_CRC_DATA) {
      old_footer.data_crc = footer.data_crc;
    } else {
      old_footer.data_crc = 0;
    }
    old_footer.flags = footer.flags;
    bl.append((const char*)&old_footer, sizeof(old_footer));
  }
  // write as a seastar::net::packet
  return s->socket->write_flush(std::move(bl))
    .then([this, msg = std::move(msg)] {
      if (!s->policy.lossy) {
        s->sent.push(std::move(msg));
      }
    });
}

seastar::future<> SocketConnection::send(MessageRef msg)
{
  // chain the message after the last message is sent
  seastar::shared_future<> f = send_ready.then(
    [this, msg = std::move(msg)] {
      return write_message(std::move(msg));
    });

  // chain any later messages after this one completes
  send_ready = f.get_future();
  // allow the caller to wait on the same future
  return f.get_future();
}

seastar::future<> SocketConnection::keepalive()
{
  seastar::shared_future<> f = send_ready.then([this] {
      k.req.stamp = ceph::coarse_real_clock::to_ceph_timespec(
        ceph::coarse_real_clock::now());
      return s->socket->write_flush(make_static_packet(k.req));
    });
  send_ready = f.get_future();
  return f.get_future();
}

seastar::future<> SocketConnection::close()
{
  if (state == state_t::close) {
    // already closing
    assert(close_ready.valid());
    return close_ready.get_future();
  }

  // unregister_conn() drops a reference, so hold another until completion
  auto cleanup = [conn = ConnectionRef(this)] {};

  if (state == state_t::accept) {
    get_messenger()->unaccept_conn(this);
  }
  if (state >= state_t::connect) {
    get_messenger()->unregister_conn(this);
  }

  state = state_t::close;

  // close_ready become valid only after state is state_t::close
  assert(!close_ready.valid());
  close_ready = s->socket->close()
    .then([this] {
      // closing connections will unblock any dispatchers that were waiting to
      // send(). wait for any pending calls to finish
      return s->dispatch_gate.close();
    }).finally(std::move(cleanup));
  return close_ready.get_future();
}

// handshake

/// store the banner in a non-const string for buffer::create_static()
static char banner[] = CEPH_BANNER;
constexpr size_t banner_size = sizeof(CEPH_BANNER)-1;

constexpr size_t client_header_size = banner_size + sizeof(ceph_entity_addr);
constexpr size_t server_header_size = banner_size + 2 * sizeof(ceph_entity_addr);

WRITE_RAW_ENCODER(ceph_msg_connect);
WRITE_RAW_ENCODER(ceph_msg_connect_reply);

// check that the buffer starts with a valid banner without requiring it to
// be contiguous in memory
static void validate_banner(bufferlist::const_iterator& p)
{
  auto b = std::cbegin(banner);
  auto end = b + banner_size;
  while (b != end) {
    const char *buf{nullptr};
    auto remaining = std::distance(b, end);
    auto len = p.get_ptr_and_advance(remaining, &buf);
    if (!std::equal(buf, buf + len, b)) {
      throw std::system_error(make_error_code(error::bad_connect_banner));
    }
    b += len;
  }
}

// make sure that we agree with the peer about its address
static void validate_peer_addr(const entity_addr_t& addr,
                               const entity_addr_t& expected)
{
  if (addr == expected) {
    return;
  }
  // ok if server bound anonymously, as long as port/nonce match
  if (addr.is_blank_ip() &&
      addr.get_port() == expected.get_port() &&
      addr.get_nonce() == expected.get_nonce()) {
    return;
  } else {
    throw std::system_error(make_error_code(error::bad_peer_address));
  }
}

/// return a static bufferptr to the given object
template <typename T>
bufferptr create_static(T& obj)
{
  return buffer::create_static(sizeof(obj), reinterpret_cast<char*>(&obj));
}

bool SocketConnection::require_auth_feature() const
{
  if (h.connect.authorizer_protocol != CEPH_AUTH_CEPHX) {
    return false;
  }
  if (conf.cephx_require_signatures) {
    return true;
  }
  if (h.connect.host_type == CEPH_ENTITY_TYPE_OSD ||
      h.connect.host_type == CEPH_ENTITY_TYPE_MDS) {
    return conf.cephx_cluster_require_signatures;
  } else {
    return conf.cephx_service_require_signatures;
  }
}

uint32_t SocketConnection::get_proto_version(entity_type_t peer_type, bool connect) const
{
  // TODO check msgr type instead
  constexpr entity_type_t my_type = CEPH_ENTITY_TYPE_OSD;
  // see also OSD.h, unlike other connection of simple/async messenger,
  // crimson msgr is only used by osd
  constexpr uint32_t CEPH_OSD_PROTOCOL = 10;
  if (peer_type == my_type) {
    // internal
    return CEPH_OSD_PROTOCOL;
  } else {
    // public
    switch (connect ? peer_type : my_type) {
      case CEPH_ENTITY_TYPE_OSD: return CEPH_OSDC_PROTOCOL;
      case CEPH_ENTITY_TYPE_MDS: return CEPH_MDSC_PROTOCOL;
      case CEPH_ENTITY_TYPE_MON: return CEPH_MONC_PROTOCOL;
      default: return 0;
    }
  }
}

seastar::future<seastar::stop_iteration>
SocketConnection::handle_connect()
{
  return s->socket->read(sizeof(h.connect))
    .then([this](bufferlist bl) {
      auto p = bl.cbegin();
      ::decode(h.connect, p);
      return s->socket->read(h.connect.authorizer_len);
    }).then([this] (bufferlist authorizer) {
      if (h.connect.protocol_version != get_proto_version(h.connect.host_type, false)) {
      	return seastar::make_ready_future<msgr_tag_t, bufferlist>(
            CEPH_MSGR_TAG_BADPROTOVER, bufferlist{});
      }
      if (require_auth_feature()) {
        s->policy.features_required |= CEPH_FEATURE_MSG_AUTH;
      }
      if (auto feat_missing = s->policy.features_required & ~(uint64_t)h.connect.features;
      	  feat_missing != 0) {
        return seastar::make_ready_future<msgr_tag_t, bufferlist>(
            CEPH_MSGR_TAG_FEATURES, bufferlist{});
      }
      return get_messenger()->verify_authorizer(s->peer_type,
      						h.connect.authorizer_protocol,
      						authorizer);
    }).then([this] (ceph::net::msgr_tag_t tag, bufferlist&& authorizer_reply) {
      memset(&h.reply, 0, sizeof(h.reply));
      if (tag) {
	return send_connect_reply(tag, std::move(authorizer_reply));
      }
      if (auto existing = get_messenger()->lookup_conn(s->peer_addr); existing) {
	return handle_connect_with_existing(existing, std::move(authorizer_reply));
      } else if (h.connect.connect_seq > 0) {
	return send_connect_reply(CEPH_MSGR_TAG_RESETSESSION,
				  std::move(authorizer_reply));
      }
      s->connect_seq = h.connect.connect_seq + 1;
      s->peer_global_seq = h.connect.global_seq;
      set_features((uint64_t)s->policy.features_supported & (uint64_t)h.connect.features);
      // TODO: cct
      return send_connect_reply_ready(CEPH_MSGR_TAG_READY, std::move(authorizer_reply));
    });
}

seastar::future<seastar::stop_iteration>
SocketConnection::send_connect_reply(msgr_tag_t tag,
                                     bufferlist&& authorizer_reply)
{
  h.reply.tag = tag;
  h.reply.features = static_cast<uint64_t>((h.connect.features &
					    s->policy.features_supported) |
					    s->policy.features_required);
  h.reply.authorizer_len = authorizer_reply.length();
  return s->socket->write(make_static_packet(h.reply))
    .then([this, reply=std::move(authorizer_reply)]() mutable {
      return s->socket->write_flush(std::move(reply));
    }).then([] {
      return seastar::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::no);
    });
}

seastar::future<seastar::stop_iteration>
SocketConnection::send_connect_reply_ready(msgr_tag_t tag,
                                           bufferlist&& authorizer_reply)
{
  s->global_seq = get_messenger()->get_global_seq();
  h.reply.tag = tag;
  h.reply.features = s->policy.features_supported;
  h.reply.global_seq = s->global_seq;
  h.reply.connect_seq = s->connect_seq;
  h.reply.flags = 0;
  if (s->policy.lossy) {
    h.reply.flags = h.reply.flags | CEPH_MSG_CONNECT_LOSSY;
  }
  h.reply.authorizer_len = authorizer_reply.length();
  return s->socket->write(make_static_packet(h.reply))
    .then([this, reply=std::move(authorizer_reply)]() mutable {
      if (reply.length()) {
        return s->socket->write(std::move(reply));
      } else {
        return seastar::now();
      }
    }).then([this] {
      if (h.reply.tag == CEPH_MSGR_TAG_SEQ) {
        return s->socket->write_flush(make_static_packet(s->in_seq))
          .then([this] {
            return s->socket->read_exactly(sizeof(seq_num_t));
          }).then([this] (auto buf) {
            auto acked_seq = reinterpret_cast<const seq_num_t*>(buf.get());
            s->discard_requeued_up_to(*acked_seq);
          });
      } else {
        return s->socket->flush();
      }
    }).then([this] {
      return seastar::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::yes);
    });
}

seastar::future<>
SocketConnection::handle_keepalive2()
{
  return s->socket->read_exactly(sizeof(ceph_timespec))
    .then([this] (auto buf) {
      k.ack.stamp = *reinterpret_cast<const ceph_timespec*>(buf.get());
      std::cout << "keepalive2 " << k.ack.stamp.tv_sec << std::endl;
      return s->socket->write_flush(make_static_packet(k.ack));
    });
}

seastar::future<>
SocketConnection::handle_keepalive2_ack()
{
  return s->socket->read_exactly(sizeof(ceph_timespec))
    .then([this] (auto buf) {
      auto t = reinterpret_cast<const ceph_timespec*>(buf.get());
      s->last_keepalive_ack = *t;
      std::cout << "keepalive2 ack " << t->tv_sec << std::endl;
    });
}

seastar::future<seastar::stop_iteration>
SocketConnection::handle_connect_with_existing(ConnectionRef existing, bufferlist&& authorizer_reply)
{
  if (h.connect.global_seq < existing->peer_global_seq()) {
    h.reply.global_seq = existing->peer_global_seq();
    return send_connect_reply(CEPH_MSGR_TAG_RETRY_GLOBAL);
  } else if (existing->is_lossy()) {
    return replace_existing(existing, std::move(authorizer_reply));
  } else if (h.connect.connect_seq == 0 && existing->connect_seq() > 0) {
    return replace_existing(existing, std::move(authorizer_reply), true);
  } else if (h.connect.connect_seq < existing->connect_seq()) {
    // old attempt, or we sent READY but they didn't get it.
    h.reply.connect_seq = existing->connect_seq() + 1;
    return send_connect_reply(CEPH_MSGR_TAG_RETRY_SESSION);
  } else if (h.connect.connect_seq == existing->connect_seq()) {
    // if the existing connection successfully opened, and/or
    // subsequently went to standby, then the peer should bump
    // their connect_seq and retry: this is not a connection race
    // we need to resolve here.
    if (existing->get_state() == state_t::open/* ||
	existing->get_state() == state_t::standby*/) {
      if (s->policy.resetcheck && existing->connect_seq() == 0) {
	return replace_existing(existing, std::move(authorizer_reply));
      } else {
	h.reply.connect_seq = existing->connect_seq() + 1;
	return send_connect_reply(CEPH_MSGR_TAG_RETRY_SESSION);
      }
    } else if (s->peer_addr < s->my_addr ||
	       existing->is_server_side()) {
      // incoming wins
      return replace_existing(existing, std::move(authorizer_reply));
    } else {
      return send_connect_reply(CEPH_MSGR_TAG_WAIT);
    }
  } else if (s->policy.resetcheck &&
	     existing->connect_seq() == 0) {
    return send_connect_reply(CEPH_MSGR_TAG_RESETSESSION);
  } else {
    return replace_existing(existing, std::move(authorizer_reply));
  }
}

// TODO: change to reuse_existing
seastar::future<seastar::stop_iteration>
SocketConnection::replace_existing(ConnectionRef existing,
                                   bufferlist&& authorizer_reply,
                                   bool is_reset_from_peer)
{
  msgr_tag_t reply_tag;
  if (HAVE_FEATURE(h.connect.features, RECONNECT_SEQ) &&
      !is_reset_from_peer) {
    reply_tag = CEPH_MSGR_TAG_SEQ;
  } else {
    reply_tag = CEPH_MSGR_TAG_READY;
  }
  get_messenger()->unregister_conn(existing);
  if (!existing->is_lossy()) {
    // reset the in_seq if this is a hard reset from peer,
    // otherwise we respect our original connection's value
    s->in_seq = is_reset_from_peer ? 0 : existing->rx_seq_num();
    // steal outgoing queue and out_seq
    existing->requeue_sent();
    std::tie(s->out_seq, s->out_q) = existing->get_out_queue();
  }
  return send_connect_reply_ready(reply_tag, std::move(authorizer_reply));
}

seastar::future<seastar::stop_iteration>
SocketConnection::handle_connect_reply(msgr_tag_t tag)
{
  switch (tag) {
  case CEPH_MSGR_TAG_FEATURES:
    logger().error("{} connect protocol feature mispatch", __func__);
    throw std::system_error(make_error_code(error::negotiation_failure));
  case CEPH_MSGR_TAG_BADPROTOVER:
    logger().error("{} connect protocol version mispatch", __func__);
    throw std::system_error(make_error_code(error::negotiation_failure));
  case CEPH_MSGR_TAG_BADAUTHORIZER:
    if (h.got_bad_auth) {
      logger().error("{} got bad authorizer", __func__);
      throw std::system_error(make_error_code(error::negotiation_failure));
    }
    h.got_bad_auth = true;
    // try harder
    return get_messenger()->get_authorizer(s->peer_type, true)
      .then([this](auto&& auth) {
        h.authorizer = std::move(auth);
        return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::no);
      });
  case CEPH_MSGR_TAG_RESETSESSION:
    reset_session();
    return seastar::make_ready_future<seastar::stop_iteration>(
        seastar::stop_iteration::no);
  case CEPH_MSGR_TAG_RETRY_GLOBAL:
    s->global_seq = get_messenger()->get_global_seq(h.reply.global_seq);
    return seastar::make_ready_future<seastar::stop_iteration>(
        seastar::stop_iteration::no);
  case CEPH_MSGR_TAG_RETRY_SESSION:
    ceph_assert(h.reply.connect_seq > s->connect_seq);
    s->connect_seq = h.reply.connect_seq;
    return seastar::make_ready_future<seastar::stop_iteration>(
        seastar::stop_iteration::no);
  case CEPH_MSGR_TAG_WAIT:
    logger().error("{} connect got WAIT (connection race)", __func__);
    // TODO protocol wait
    throw std::system_error(make_error_code(error::negotiation_failure));
    //return seastar::make_ready_future<seastar::stop_iteration>(
    //    seastar::stop_iteration::yes);
  case CEPH_MSGR_TAG_SEQ:
    break;
  case CEPH_MSGR_TAG_READY:
    break;
  }
  if (auto missing = (s->policy.features_required & ~(uint64_t)h.reply.features);
      missing) {
    logger().error("{} missing required features", __func__);
    throw std::system_error(make_error_code(error::negotiation_failure));
  }
  if (tag == CEPH_MSGR_TAG_SEQ) {
    return s->socket->read_exactly(sizeof(seq_num_t))
      .then([this] (auto buf) {
        auto acked_seq = reinterpret_cast<const seq_num_t*>(buf.get());
        s->discard_requeued_up_to(*acked_seq);
        return s->socket->write_flush(make_static_packet(s->in_seq));
      }).then([this] {
        return handle_connect_reply(CEPH_MSGR_TAG_READY);
      });
  }
  if (tag == CEPH_MSGR_TAG_READY) {
    // hooray!
    s->peer_global_seq = h.reply.global_seq;
    s->policy.lossy = h.reply.flags & CEPH_MSG_CONNECT_LOSSY;
    s->connect_seq++;
    s->backoff = 0ms;
    set_features(h.reply.features & h.connect.features);
    if (h.authorizer) {
      s->session_security.reset(
          get_auth_session_handler(nullptr,
                                   h.authorizer->protocol,
                                   h.authorizer->session_key,
                                   s->features));
    }
    h.authorizer.reset();
    return seastar::make_ready_future<seastar::stop_iteration>(
        seastar::stop_iteration::yes);
  } else {
    // unknown tag
    logger().error("{} got unknown tag", __func__, int(tag));
    throw std::system_error(make_error_code(error::negotiation_failure));
  }
}

void SocketConnection::reset_session()
{
  seastar::with_gate(s->dispatch_gate, [this] {
    return dispatcher->ms_handle_remote_reset(this);
  });
  s->reset();
}

seastar::future<seastar::stop_iteration>
SocketConnection::connect(entity_type_t peer_type,
                          entity_type_t host_type)
{
  // encode ceph_msg_connect
  s->peer_type = peer_type;
  memset(&h.connect, 0, sizeof(h.connect));
  h.connect.features = s->policy.features_supported;
  h.connect.host_type = host_type;
  h.connect.global_seq = s->global_seq;
  h.connect.connect_seq = s->connect_seq;
  h.connect.protocol_version = get_proto_version(peer_type, true);
  // this is fyi, actually, server decides!
  h.connect.flags = s->policy.lossy ? CEPH_MSG_CONNECT_LOSSY : 0;

  return get_messenger()->get_authorizer(peer_type, false)
    .then([this](auto&& auth) {
      h.authorizer = std::move(auth);
      bufferlist bl;
      if (h.authorizer) {
        h.connect.authorizer_protocol = h.authorizer->protocol;
        h.connect.authorizer_len = h.authorizer->bl.length();
        bl.append(create_static(h.connect));
        bl.append(h.authorizer->bl);
      } else {
        h.connect.authorizer_protocol = 0;
        h.connect.authorizer_len = 0;
        bl.append(create_static(h.connect));
      };
      return s->socket->write_flush(std::move(bl));
    }).then([this] {
     // read the reply
      return s->socket->read(sizeof(h.reply));
    }).then([this] (bufferlist bl) {
      auto p = bl.cbegin();
      ::decode(h.reply, p);
      ceph_assert(p.end());
      return s->socket->read(h.reply.authorizer_len);
    }).then([this] (bufferlist bl) {
      if (h.authorizer) {
        auto reply = bl.cbegin();
        if (!h.authorizer->verify_reply(reply)) {
          logger().error("{} authorizer failed to verify reply", __func__);
          throw std::system_error(make_error_code(error::negotiation_failure));
        }
      }
      return handle_connect_reply(h.reply.tag);
    });
}

void SocketConnection::protocol_connect(entity_type_t peer_type,
                                        entity_type_t host_type)
{
  ceph_assert(!s->socket);
  ceph_assert(state != state_t::connect);
  state = state_t::connect;
  seastar::with_gate(s->dispatch_gate, [this, peer_type, host_type] {
    return seastar::connect(s->peer_addr.in4_addr())
      .then([this] (seastar::connected_socket fd) {
        s->socket.emplace(std::move(fd));
        return s->socket->read(server_header_size);
      }).then([this] (bufferlist headerbl) {
        auto p = headerbl.cbegin();
        validate_banner(p);
        entity_addr_t saddr, caddr;
        ::decode(saddr, p);
        ::decode(caddr, p);
        ceph_assert(p.end());
        validate_peer_addr(saddr, s->peer_addr);

        if (s->my_addr != caddr) {
          // take peer's address for me, but preserve my nonce
          caddr.nonce = s->my_addr.nonce;
          s->my_addr = caddr;
        }
        // encode/send client's handshake header
        bufferlist bl;
        bl.append(buffer::create_static(banner_size, banner));
        ::encode(s->my_addr, bl, 0);
        s->global_seq = get_messenger()->get_global_seq();
        return s->socket->write_flush(std::move(bl));
      }).then([this, peer_type, host_type] {
        return seastar::repeat([this, peer_type, host_type] {
          return connect(peer_type, host_type);
        });
      }).then([this] {
        return dispatcher->ms_handle_connect(this);
      }).then([this] {
        // no throw
        protocol_open();
      }).handle_exception([this, peer_type, host_type] (std::exception_ptr eptr) {
        // close the connection before returning errors
        if (is_lossy()) {
          close();
        } else {
          s->socket->close().then([this, peer_type, host_type] {
              state = state_t::none;
              s->socket.reset();
              protocol_connect(peer_type, host_type);
            });
        }
      });
  });
}

void SocketConnection::protocol_accept()
{
  ceph_assert(s->socket);
  ceph_assert(state != state_t::accept);
  state = state_t::accept;
  seastar::with_gate(s->dispatch_gate, [this] {
    // encode/send server's handshake header
    bufferlist bl;
    bl.append(buffer::create_static(banner_size, banner));
    ::encode(s->my_addr, bl, 0);
    ::encode(s->peer_addr, bl, 0);
    return s->socket->write_flush(std::move(bl))
      .then([this] {
        // read client's handshake header and connect request
        return s->socket->read(client_header_size);
      }).then([this] (bufferlist bl) {
        auto p = bl.cbegin();
        validate_banner(p);
        entity_addr_t addr;
        ::decode(addr, p);
        ceph_assert(p.end());
        if (!addr.is_blank_ip()) {
          s->peer_addr = addr;
        }
      }).then([this] {
        return seastar::repeat([this] {
          return handle_connect();
        });
      }).then([this] {
        return dispatcher->ms_handle_accept(this);
      }).then([this] {
        get_messenger()->register_conn(this);
        get_messenger()->unaccept_conn(this);
        protocol_open();
      }).handle_exception([this] (std::exception_ptr eptr) {
        return close();
      });
  });
}

void SocketConnection::protocol_open()
{
  ceph_assert(state != state_t::open);
  state = state_t::open;
  // TODO: start sending
  s->send_promise.set_value();

  seastar::with_gate(s->dispatch_gate, [this] {
    // start background processing of tags
    read_tags_until_next_message();
    return seastar::keep_doing([this] {
        return read_message()
          .then([this] (MessageRef msg) {
            // start dispatch, ignoring exceptions from the application layer
            seastar::with_gate(s->dispatch_gate, [this, msg = std::move(msg)] {
                return dispatcher->ms_dispatch(this, std::move(msg))
                  .handle_exception([] (std::exception_ptr eptr) {});
              });
            // return immediately to start on the next message
            return seastar::now();
          });
      }).handle_exception_type([this] (const std::system_error& e) {
        // TODO handle failure
        if (e.code() == error::connection_aborted ||
            e.code() == error::connection_reset) {
          return dispatcher->ms_handle_reset(this);
        } else if (e.code() == error::read_eof) {
          return dispatcher->ms_handle_remote_reset(this);
        } else {
          throw e;
        }
      }).handle_exception([] (std::exception_ptr eptr) {
        // TODO handle failure
      });
  });
}

seastar::future<> SocketConnection::fault()
{
  if (s->policy.lossy) {
    get_messenger()->unregister_conn(this);
  }
  if (s->backoff.count()) {
    s->backoff += s->backoff;
  } else {
    s->backoff = conf.ms_initial_backoff;
  }
  if (s->backoff > conf.ms_max_backoff) {
    s->backoff = conf.ms_max_backoff;
  }
  return seastar::sleep(s->backoff);
}
