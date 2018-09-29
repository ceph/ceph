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

#include <algorithm>
#include <seastar/net/packet.hh>

#include "Config.h"
#include "Dispatcher.h"
#include "Errors.h"
#include "ProtocolV1.h"
#include "Session.h"
#include "SocketConnection.h"
#include "SocketMessenger.h"

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

// handshake
/// store the banner in a non-const string for buffer::create_static()
static char banner[] = CEPH_BANNER;
constexpr size_t banner_size = sizeof(CEPH_BANNER)-1;

constexpr size_t client_header_size = banner_size + sizeof(ceph_entity_addr);
constexpr size_t server_header_size = banner_size + 2 * sizeof(ceph_entity_addr);

WRITE_RAW_ENCODER(ceph_msg_connect);
WRITE_RAW_ENCODER(ceph_msg_connect_reply);

std::ostream& operator<<(std::ostream& out, const ceph_msg_connect& c)
{
  return out << "connect{features=" << std::hex << c.features << std::dec
      << " host_type=" << c.host_type
      << " global_seq=" << c.global_seq
      << " connect_seq=" << c.connect_seq
      << " protocol_version=" << c.protocol_version
      << " authorizer_protocol=" << c.authorizer_protocol
      << " authorizer_len=" << c.authorizer_len
      << " flags=" << std::hex << static_cast<uint16_t>(c.flags) << std::dec << '}';
}

std::ostream& operator<<(std::ostream& out, const ceph_msg_connect_reply& r)
{
  return out << "connect_reply{tag=" << static_cast<uint16_t>(r.tag)
      << " features=" << std::hex << r.features << std::dec
      << " global_seq=" << r.global_seq
      << " connect_seq=" << r.connect_seq
      << " protocol_version=" << r.protocol_version
      << " authorizer_len=" << r.authorizer_len
      << " flags=" << std::hex << static_cast<uint16_t>(r.flags) << std::dec << '}';
}

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

static uint32_t get_proto_version(entity_type_t peer_type, bool connect)
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

/*
 * Constructors
 */
ProtocolV1::StateNone::StateNone(ProtocolV1 &p) : Protocol::State(p), proto(p) {}
ProtocolV1::StateAccept::StateAccept(ProtocolV1 &p) : Protocol::State(p), proto(p) {}
ProtocolV1::StateConnect::StateConnect(ProtocolV1 &p) : Protocol::State(p), proto(p) {}
ProtocolV1::StateOpen::StateOpen(ProtocolV1 &p) : Protocol::State(p), proto(p) {}
ProtocolV1::StateClose::StateClose(ProtocolV1 &p) : Protocol::State(p, false), proto(p) {}

/*
 * State-aware trigger_close
 */
void ProtocolV1::StateAccept::trigger_close() {
  messenger.unaccept_conn(&conn);
  trigger(&proto.state_close);
}

void ProtocolV1::StateConnect::trigger_close() {
  messenger.unregister_conn(&conn);
  trigger(&proto.state_close);
}

void ProtocolV1::StateOpen::trigger_close() {
  messenger.unregister_conn(&conn);
  trigger(&proto.state_close);
}

void ProtocolV1::StateClose::trigger_close() {
  /* do nothing */
}

/*
 * state-aware send
 */
seastar::future<seastar::stop_iteration>
ProtocolV1::StateAccept::send(MessageRef msg) {
  // not visible by users yet
  ceph_assert(false);
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateConnect::send(MessageRef msg) {
  return wait_execute().then([] {
      return seastar::make_ready_future<seastop>(seastop::no);
    });
}

seastar::future<>
ProtocolV1::StateOpen::write_message(MessageRef msg)
{
  msg->set_seq(++s.out_seq);
  msg->encode(s.features, messenger.get_crc_flags());
  bufferlist bl;
  bl.append(CEPH_MSGR_TAG_MSG);
  auto& header = msg->get_header();
  bl.append((const char*)&header, sizeof(header));
  bl.append(msg->get_payload());
  bl.append(msg->get_middle());
  bl.append(msg->get_data());
  auto& footer = msg->get_footer();
  if (HAVE_FEATURE(s.features, MSG_AUTH)) {
    bl.append((const char*)&footer, sizeof(footer));
  } else {
    ceph_msg_footer_old old_footer;
    if (messenger.get_crc_flags() & MSG_CRC_HEADER) {
      old_footer.front_crc = footer.front_crc;
      old_footer.middle_crc = footer.middle_crc;
    } else {
      old_footer.front_crc = old_footer.middle_crc = 0;
    }
    if (messenger.get_crc_flags() & MSG_CRC_DATA) {
      old_footer.data_crc = footer.data_crc;
    } else {
      old_footer.data_crc = 0;
    }
    old_footer.flags = footer.flags;
    bl.append((const char*)&old_footer, sizeof(old_footer));
  }
  // write as a seastar::net::packet
  return proto.socket->write_flush(std::move(bl))
    .then([this, msg = std::move(msg)] {
      if (!s.policy.lossy) {
        s.sent.push(std::move(msg));
      }
    });
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateOpen::send(MessageRef msg) {
  return write_message(std::move(msg))
    .then([] {
      return seastar::make_ready_future<seastop>(seastop::yes);
    }).handle_exception([] (std::exception_ptr eptr) {
      return seastar::make_ready_future<seastop>(seastop::no);
    });
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateClose::send(MessageRef msg) {
  return seastar::make_ready_future<seastop>(seastop::yes);
}

/*
 * state-aware keepalive
 */
seastar::future<seastar::stop_iteration>
ProtocolV1::StateAccept::keepalive() {
  // not visible by users yet
  ceph_assert(false);
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateConnect::keepalive() {
  return wait_execute().then([] {
      return seastar::make_ready_future<seastop>(seastop::no);
    });
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateOpen::keepalive() {
  k.req.stamp = ceph::coarse_real_clock::to_ceph_timespec(
    ceph::coarse_real_clock::now());
  return proto.socket->write_flush(make_static_packet(k.req))
    .then([] {
      return seastar::make_ready_future<seastop>(seastop::yes);
    }).handle_exception([] (std::exception_ptr eptr) {
      return seastar::make_ready_future<seastop>(seastop::no);
    });
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateClose::keepalive() {
  return seastar::make_ready_future<seastop>(seastop::yes);
}

/*
 * V1 Protocol Entry
 */
void
ProtocolV1::StateNone::start_accept() {
  messenger.accept_conn(&conn);
  trigger(&proto.state_accept);
}

void
ProtocolV1::StateNone::start_connect() {
  messenger.register_conn(&conn);
  trigger(&proto.state_connect);
}

/*
 * V1 Protocol Accept
 */
seastar::future<>
ProtocolV1::StateAccept::do_execute() {
  ceph_assert(proto.socket);
  // encode/send server's handshake header
  bufferlist bl;
  bl.append(buffer::create_static(banner_size, banner));
  ::encode(s.my_addr, bl, 0);
  ::encode(s.peer_addr, bl, 0);
  return proto.socket->write_flush(std::move(bl))
    .then([this] {
      // read client's handshake header and connect request
      return proto.socket->read(client_header_size);
    }).then([this] (bufferlist bl) {
      auto p = bl.cbegin();
      validate_banner(p);
      entity_addr_t addr;
      ::decode(addr, p);
      ceph_assert(p.end());
      if (!addr.is_blank_ip()) {
        s.peer_addr = addr;
      }
    }).then([this] {
      return seastar::repeat([this] {
        return repeat_accept();
      });
    }).then([this] {
      // TODO: ignore the exception
      return dispatcher.ms_handle_accept(&conn);
    }).then([this] {
      messenger.register_conn(&conn);
      messenger.unaccept_conn(&conn);
      trigger(&proto.state_open);
    }).handle_exception([this] (std::exception_ptr eptr) {
      proto.close();
    });
}

bool
ProtocolV1::StateAccept::require_auth_feature() const
{
  if (proto.h_connect.authorizer_protocol != CEPH_AUTH_CEPHX) {
    return false;
  }
  if (conf.cephx_require_signatures) {
    return true;
  }
  if (proto.h_connect.host_type == CEPH_ENTITY_TYPE_OSD ||
      proto.h_connect.host_type == CEPH_ENTITY_TYPE_MDS) {
    return conf.cephx_cluster_require_signatures;
  } else {
    return conf.cephx_service_require_signatures;
  }
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateAccept::repeat_accept() {
  return proto.socket->read(sizeof(proto.h_connect))
    .then([this](bufferlist bl) {
      auto p = bl.cbegin();
      ::decode(proto.h_connect, p);
      return proto.socket->read(proto.h_connect.authorizer_len);
    }).then([this] (bufferlist authorizer) {
      if (proto.h_connect.protocol_version != get_proto_version(proto.h_connect.host_type, false)) {
        return seastar::make_ready_future<msgr_tag_t, bufferlist>(
            CEPH_MSGR_TAG_BADPROTOVER, bufferlist{});
      }
      if (require_auth_feature()) {
        s.policy.features_required |= CEPH_FEATURE_MSG_AUTH;
      }
      if (auto feat_missing = s.policy.features_required & ~(uint64_t)proto.h_connect.features;
          feat_missing != 0) {
        return seastar::make_ready_future<msgr_tag_t, bufferlist>(
            CEPH_MSGR_TAG_FEATURES, bufferlist{});
      }
      return messenger.verify_authorizer(s.peer_type,
                                         proto.h_connect.authorizer_protocol,
                                         authorizer);
    }).then([this] (ceph::net::msgr_tag_t tag, bufferlist&& authorizer_reply) {
      memset(&proto.h_reply, 0, sizeof(proto.h_reply));
      if (tag) {
        return send_connect_reply(tag, std::move(authorizer_reply));
      }
      // TODO rework this logic to do replacing atomically
      if (auto existing = messenger.lookup_conn(s.peer_addr); existing) {
        return handle_connect_with_existing(existing, std::move(authorizer_reply));
      } else if (proto.h_connect.connect_seq > 0) {
        return send_connect_reply(CEPH_MSGR_TAG_RESETSESSION,
                                  std::move(authorizer_reply));
      }
      s.connect_seq = proto.h_connect.connect_seq + 1;
      s.peer_global_seq = proto.h_connect.global_seq;
      s.features = (uint64_t)s.policy.features_supported & (uint64_t)proto.h_connect.features;
      // TODO: cct
      return send_connect_reply_ready(CEPH_MSGR_TAG_READY, std::move(authorizer_reply));
    });
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateAccept::send_connect_reply(msgr_tag_t tag,
                                            bufferlist&& authorizer_reply)
{
  proto.h_reply.tag = tag;
  proto.h_reply.features = static_cast<uint64_t>((proto.h_connect.features &
					    s.policy.features_supported) |
					    s.policy.features_required);
  proto.h_reply.authorizer_len = authorizer_reply.length();
  return proto.socket->write(make_static_packet(proto.h_reply))
    .then([this, reply=std::move(authorizer_reply)]() mutable {
      return proto.socket->write_flush(std::move(reply));
    }).then([] {
      return seastar::make_ready_future<seastop>(seastop::no);
    });
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateAccept::send_connect_reply_ready(msgr_tag_t tag,
                                                  bufferlist&& authorizer_reply)
{
  s.global_seq = messenger.get_global_seq();
  proto.h_reply.tag = tag;
  proto.h_reply.features = s.policy.features_supported;
  proto.h_reply.global_seq = s.global_seq;
  proto.h_reply.connect_seq = s.connect_seq;
  proto.h_reply.flags = 0;
  if (s.policy.lossy) {
    proto.h_reply.flags = proto.h_reply.flags | CEPH_MSG_CONNECT_LOSSY;
  }
  proto.h_reply.authorizer_len = authorizer_reply.length();
  return proto.socket->write(make_static_packet(proto.h_reply))
    .then([this, reply=std::move(authorizer_reply)]() mutable {
      if (reply.length()) {
        return proto.socket->write(std::move(reply));
      } else {
        return seastar::now();
      }
    }).then([this] {
      if (proto.h_reply.tag == CEPH_MSGR_TAG_SEQ) {
        return proto.socket->write_flush(make_static_packet(s.in_seq))
          .then([this] {
            return proto.socket->read_exactly(sizeof(seq_num_t));
          }).then([this] (auto buf) {
            auto acked_seq = reinterpret_cast<const seq_num_t*>(buf.get());
            s.discard_requeued_up_to(*acked_seq);
          });
      } else {
        return proto.socket->flush();
      }
    }).then([this] {
      return seastar::make_ready_future<seastop>(seastop::yes);
    });
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateAccept::handle_connect_with_existing(SocketConnectionRef existing, bufferlist&& authorizer_reply)
{
  if (proto.h_connect.global_seq < existing->peer_global_seq()) {
    proto.h_reply.global_seq = existing->peer_global_seq();
    return send_connect_reply(CEPH_MSGR_TAG_RETRY_GLOBAL);
  } else if (existing->is_lossy()) {
    return replace_existing(existing, std::move(authorizer_reply));
  } else if (proto.h_connect.connect_seq == 0 && existing->connect_seq() > 0) {
    return replace_existing(existing, std::move(authorizer_reply), true);
  } else if (proto.h_connect.connect_seq < existing->connect_seq()) {
    // old attempt, or we sent READY but they didn't get it.
    proto.h_reply.connect_seq = existing->connect_seq() + 1;
    return send_connect_reply(CEPH_MSGR_TAG_RETRY_SESSION);
  } else if (proto.h_connect.connect_seq == existing->connect_seq()) {
    // if the existing connection successfully opened, and/or
    // subsequently went to standby, then the peer should bump
    // their connect_seq and retry: this is not a connection race
    // we need to resolve here.
    // TODO: refactor existing handle logic
    if (existing->is_connected()
        /*existing->get_state() == state_t::open ||
        existing->state() == state_t::standby*/) {
      if (s.policy.resetcheck && existing->connect_seq() == 0) {
        return replace_existing(existing, std::move(authorizer_reply));
      } else {
        proto.h_reply.connect_seq = existing->connect_seq() + 1;
        return send_connect_reply(CEPH_MSGR_TAG_RETRY_SESSION);
      }
    } else if (s.peer_addr < s.my_addr ||
	       existing->is_server_side()) {
      // incoming wins
      return replace_existing(existing, std::move(authorizer_reply));
    } else {
      return send_connect_reply(CEPH_MSGR_TAG_WAIT);
    }
  } else if (s.policy.resetcheck &&
	     existing->connect_seq() == 0) {
    return send_connect_reply(CEPH_MSGR_TAG_RESETSESSION);
  } else {
    return replace_existing(existing, std::move(authorizer_reply));
  }
}

// TODO: change to reuse_existing
seastar::future<seastar::stop_iteration>
ProtocolV1::StateAccept::replace_existing(SocketConnectionRef existing,
                                 bufferlist&& authorizer_reply,
                                 bool is_reset_from_peer)
{
  msgr_tag_t reply_tag;
  if (HAVE_FEATURE(proto.h_connect.features, RECONNECT_SEQ) &&
      !is_reset_from_peer) {
    reply_tag = CEPH_MSGR_TAG_SEQ;
  } else {
    reply_tag = CEPH_MSGR_TAG_READY;
  }
  messenger.unregister_conn(existing);
  if (!existing->is_lossy()) {
    // reset the in_seq if this is a hard reset from peer,
    // otherwise we respect our original connection's value
    s.in_seq = is_reset_from_peer ? 0 : existing->rx_seq_num();
    // steal outgoing queue and out_seq
    existing->requeue_sent();
    std::tie(s.out_seq, s.out_q) = existing->get_out_queue();
  }
  return send_connect_reply_ready(reply_tag, std::move(authorizer_reply));
}

/*
 * V1 Protocol Connect
 */
seastar::future<>
ProtocolV1::StateConnect::do_execute() {
  ceph_assert(!proto.socket);
  return seastar::connect(s.peer_addr.in4_addr())
    .then([this] (seastar::connected_socket fd) {
      proto.socket.emplace(std::move(fd));
      return proto.socket->read(server_header_size);
    }).then([this] (bufferlist headerbl) {
      auto p = headerbl.cbegin();
      validate_banner(p);
      entity_addr_t saddr, caddr;
      ::decode(saddr, p);
      ::decode(caddr, p);
      ceph_assert(p.end());
      validate_peer_addr(saddr, s.peer_addr);

      if (s.my_addr != caddr) {
        // take peer's address for me, but preserve my nonce
        caddr.nonce = s.my_addr.nonce;
        s.my_addr = caddr;
      }
      // encode/send client's handshake header
      bufferlist bl;
      bl.append(buffer::create_static(banner_size, banner));
      ::encode(s.my_addr, bl, 0);
      s.global_seq = messenger.get_global_seq();
      return proto.socket->write_flush(std::move(bl));
    }).then([this] {
      return seastar::repeat([this] {
        return repeat_connect();
      });
    }).then([this] {
      // TODO: ignore the exception
      return dispatcher.ms_handle_connect(&conn);
    }).then([this] {
      trigger(&proto.state_open);
    }).handle_exception([this] (std::exception_ptr eptr) {
      if (s.policy.lossy) {
        proto.close();
      } else {
        proto.socket->close().then([this] {
            proto.socket.reset();
            trigger(&proto.state_none);
            trigger(&proto.state_connect);
          });
      }
    });
}

seastar::future<seastar::stop_iteration>
ProtocolV1::StateConnect::repeat_connect() {
  memset(&proto.h_connect, 0, sizeof(proto.h_connect));
  proto.h_connect.features = s.policy.features_supported;
  proto.h_connect.host_type = messenger.get_myname().type();
  proto.h_connect.global_seq = s.global_seq;
  proto.h_connect.connect_seq = s.connect_seq;
  proto.h_connect.protocol_version = get_proto_version(s.peer_type, true);
  // this is fyi, actually, server decides!
  proto.h_connect.flags = s.policy.lossy ? CEPH_MSG_CONNECT_LOSSY : 0;

  return messenger.get_authorizer(s.peer_type, false)
    .then([this](auto&& auth) {
      authorizer = std::move(auth);
      bufferlist bl;
      if (authorizer) {
        proto.h_connect.authorizer_protocol = authorizer->protocol;
        proto.h_connect.authorizer_len = authorizer->bl.length();
        bl.append(create_static(proto.h_connect));
        bl.append(authorizer->bl);
      } else {
        proto.h_connect.authorizer_protocol = 0;
        proto.h_connect.authorizer_len = 0;
        bl.append(create_static(proto.h_connect));
      };
      return proto.socket->write_flush(std::move(bl));
    }).then([this] {
     // read the reply
      return proto.socket->read(sizeof(proto.h_reply));
    }).then([this] (bufferlist bl) {
      auto p = bl.cbegin();
      ::decode(proto.h_reply, p);
      ceph_assert(p.end());
      return proto.socket->read(proto.h_reply.authorizer_len);
    }).then([this] (bufferlist bl) {
      if (authorizer) {
        auto auth_reply = bl.cbegin();
        if (!authorizer->verify_reply(auth_reply)) {
          logger().error("{} authorizer failed to verify reply", __func__);
          throw std::system_error(make_error_code(error::negotiation_failure));
        }
      }

      //handle connect reply
      switch (proto.h_reply.tag) {
      case CEPH_MSGR_TAG_FEATURES:
        logger().error("{} connect protocol feature mispatch", __func__);
        throw std::system_error(make_error_code(error::negotiation_failure));
      case CEPH_MSGR_TAG_BADPROTOVER:
        logger().error("{} connect protocol version mispatch", __func__);
        throw std::system_error(make_error_code(error::negotiation_failure));
      case CEPH_MSGR_TAG_BADAUTHORIZER:
        if (got_bad_auth) {
          logger().error("{} got bad authorizer", __func__);
          throw std::system_error(make_error_code(error::negotiation_failure));
        }
        got_bad_auth = true;
        // try harder
        return messenger.get_authorizer(s.peer_type, true)
          .then([this](auto&& auth) {
            authorizer = std::move(auth);
            return seastar::make_ready_future<seastop>(seastop::no);
          });
      case CEPH_MSGR_TAG_RESETSESSION:
        seastar::with_gate(proto.dispatch_gate, [this] {
          return dispatcher.ms_handle_remote_reset(&conn);
        });
        s.reset();
        return seastar::make_ready_future<seastop>(seastop::no);
      case CEPH_MSGR_TAG_RETRY_GLOBAL:
        s.global_seq = messenger.get_global_seq(proto.h_reply.global_seq);
        return seastar::make_ready_future<seastop>(seastop::no);
      case CEPH_MSGR_TAG_RETRY_SESSION:
        ceph_assert(proto.h_reply.connect_seq > s.connect_seq);
        s.connect_seq = proto.h_reply.connect_seq;
        return seastar::make_ready_future<seastop>(seastop::no);
      case CEPH_MSGR_TAG_WAIT:
        logger().error("{} connect got WAIT (connection race)", __func__);
        // TODO state wait
        throw std::system_error(make_error_code(error::negotiation_failure));
        //return seastar::make_ready_future<seastop>(seastop::yes);
      case CEPH_MSGR_TAG_SEQ:
      case CEPH_MSGR_TAG_READY:
        if (auto missing = (s.policy.features_required & ~(uint64_t)proto.h_reply.features);
            missing) {
          logger().error("{} missing required features", __func__);
          throw std::system_error(make_error_code(error::negotiation_failure));
        }
        return seastar::futurize_apply([this] {
            if (proto.h_reply.tag == CEPH_MSGR_TAG_SEQ) {
              return proto.socket->read_exactly(sizeof(seq_num_t))
                .then([this] (auto buf) {
                  auto acked_seq = reinterpret_cast<const seq_num_t*>(buf.get());
                  s.discard_requeued_up_to(*acked_seq);
                  return proto.socket->write_flush(make_static_packet(s.in_seq));
                });
            } else {
              // tag CEPH_MSGR_TAG_READY
              return seastar::now();
            }
          }).then([this] {
            // hooray!
            s.peer_global_seq = proto.h_reply.global_seq;
            s.policy.lossy = proto.h_reply.flags & CEPH_MSG_CONNECT_LOSSY;
            s.connect_seq++;
            s.backoff = 0ms;
            s.features = proto.h_reply.features & proto.h_connect.features;
            if (authorizer) {
              s.session_security.reset(
                  get_auth_session_handler(nullptr,
                                           authorizer->protocol,
                                           authorizer->session_key,
                                           s.features));
            }
            authorizer.reset();
            return seastar::make_ready_future<seastop>(seastop::yes);
          });
        break;
      default:
        // unknown tag
        logger().error("{} got unknown tag", __func__, int(proto.h_reply.tag));
        throw std::system_error(make_error_code(error::negotiation_failure));
      }
    });
}

/*
 * V1 Protocol Open
 */
seastar::future<seastar::stop_iteration>
ProtocolV1::StateOpen::handle_ack()
{
  return proto.socket->read_exactly(sizeof(ceph_le64))
    .then([this] (auto buf) {
      auto seq = reinterpret_cast<const ceph_le64*>(buf.get());
      s.discard_sent_up_to(*seq);
      return seastop::no;
    });
}

seastar::future<>
ProtocolV1::StateOpen::handle_keepalive2()
{
  return proto.socket->read_exactly(sizeof(ceph_timespec))
    .then([this] (auto buf) {
      k.ack.stamp = *reinterpret_cast<const ceph_timespec*>(buf.get());
      std::cout << "keepalive2 " << k.ack.stamp.tv_sec << std::endl;
      return proto.socket->write_flush(make_static_packet(k.ack));
    });
}

seastar::future<>
ProtocolV1::StateOpen::handle_keepalive2_ack()
{
  return proto.socket->read_exactly(sizeof(ceph_timespec))
    .then([this] (auto buf) {
      auto t = reinterpret_cast<const ceph_timespec*>(buf.get());
      s.last_keepalive_ack = *t;
      std::cout << "keepalive2 ack " << t->tv_sec << std::endl;
    });
}

void
ProtocolV1::StateOpen::read_tags_until_next_message()
{
  seastar::repeat([this] {
      // read the next tag
      return proto.socket->read_exactly(1)
        .then([this] (auto buf) {
          if (buf.empty()) {
            throw std::system_error(make_error_code(error::read_eof));
          }
          switch (buf[0]) {
          case CEPH_MSGR_TAG_MSG:
            // stop looping and notify read_header()
            return seastar::make_ready_future<seastop>(seastop::yes);
          case CEPH_MSGR_TAG_ACK:
            return handle_ack();
          case CEPH_MSGR_TAG_KEEPALIVE:
            break;
          case CEPH_MSGR_TAG_KEEPALIVE2:
            return handle_keepalive2()
              .then([this] {
                return seastar::make_ready_future<seastop>(seastop::no);
              });
          case CEPH_MSGR_TAG_KEEPALIVE2_ACK:
            return handle_keepalive2_ack()
              .then([this] {
                return seastar::make_ready_future<seastop>(seastop::no);
              });
          case CEPH_MSGR_TAG_CLOSE:
            std::cout << "close" << std::endl;
            break;
          }
          return seastar::make_ready_future<seastop>(seastop::no);
        });
    }).handle_exception_type([this] (const std::system_error& e) {
      if (e.code() == error::read_eof) {
        proto.close();
      }
      throw e;
    }).then_wrapped([this] (auto fut) {
      // satisfy the message promise
      fut.forward_to(std::move(on_message));
    });
}

seastar::future<>
ProtocolV1::StateOpen::maybe_throttle()
{
  if (!s.policy.throttler_bytes) {
    return seastar::now();
  }
  const auto to_read = (m.header.front_len +
                        m.header.middle_len +
                        m.header.data_len);
  return s.policy.throttler_bytes->get(to_read);
}

seastar::future<MessageRef>
ProtocolV1::StateOpen::do_read_message()
{
  return on_message.get_future()
    .then([this] {
      on_message = seastar::promise<>{};
      // read header
      return proto.socket->read(sizeof(m.header));
    }).then([this] (bufferlist bl) {
      // throttle the traffic, maybe
      auto p = bl.cbegin();
      ::decode(m.header, p);
      return maybe_throttle();
    }).then([this] {
      // read front
      return proto.socket->read(m.header.front_len);
    }).then([this] (bufferlist bl) {
      m.front = std::move(bl);
      // read middle
      return proto.socket->read(m.header.middle_len);
    }).then([this] (bufferlist bl) {
      m.middle = std::move(bl);
      // read data
      return proto.socket->read(m.header.data_len);
    }).then([this] (bufferlist bl) {
      m.data = std::move(bl);
      // read footer
      return proto.socket->read(sizeof(m.footer));
    }).then([this] (bufferlist bl) {
      // resume background processing of tags
      read_tags_until_next_message();

      auto p = bl.cbegin();
      ::decode(m.footer, p);
      auto msg = ::decode_message(nullptr, 0, m.header, m.footer,
                                  m.front, m.middle, m.data, nullptr);
      // TODO: set time stamps
      msg->set_byte_throttler(s.policy.throttler_bytes);
      constexpr bool add_ref = false; // Message starts with 1 ref
      return MessageRef{msg, add_ref};
    });
}

bool
ProtocolV1::StateOpen::update_rx_seq(seq_num_t seq)
{
  if (seq <= s.in_seq) {
    if (HAVE_FEATURE(s.features, RECONNECT_SEQ) &&
        conf.ms_die_on_old_message) {
      ceph_abort_msg("old msgs despite reconnect_seq feature");
    }
    return false;
  } else if (seq > s.in_seq + 1) {
    if (conf.ms_die_on_skipped_message) {
      ceph_abort_msg("skipped incoming seq");
    }
    return false;
  } else {
    s.in_seq = seq;
    return true;
  }
}

seastar::future<MessageRef>
ProtocolV1::StateOpen::read_message()
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

seastar::future<>
ProtocolV1::StateOpen::do_execute() {
  //TODO: capture and handle failure at top-level
  read_tags_until_next_message();
  return seastar::keep_doing([this] {
      return read_message()
        .then([this] (MessageRef msg) {
          // start dispatch, ignoring exceptions from the application layer
          seastar::with_gate(proto.dispatch_gate, [this, msg = std::move(msg)] {
              return dispatcher.ms_dispatch(&conn, std::move(msg))
                .handle_exception([] (std::exception_ptr eptr) {});
            });
          // return immediately to start on the next message
          return seastar::now();
        });
    }).handle_exception_type([this] (const std::system_error& e) {
      // TODO handle failure
      if (e.code() == error::connection_aborted ||
          e.code() == error::connection_reset) {
        return dispatcher.ms_handle_reset(&conn);
      } else if (e.code() == error::read_eof) {
        return dispatcher.ms_handle_remote_reset(&conn);
      } else {
        throw e;
      }
    }).handle_exception([] (std::exception_ptr eptr) {
      // TODO handle failure
    });
}

/*
 * V1 Protocol Close
 */
seastar::future<>
ProtocolV1::StateClose::do_execute() {
  return proto.socket->close()
    .then([this] {
      // closing connections will unblock any dispatchers that were waiting to
      // send(). wait for any pending calls to finish
      return proto.dispatch_gate.close();
    });
}
