// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ProtocolV1.h"

#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/packet.hh>

#include "include/msgr.h"
#include "include/random.h"
#include "auth/Auth.h"
#include "auth/AuthSessionHandler.h"

#include "crimson/auth/AuthClient.h"
#include "crimson/auth/AuthServer.h"
#include "crimson/common/log.h"
#include "Dispatcher.h"
#include "Errors.h"
#include "Socket.h"
#include "SocketConnection.h"
#include "SocketMessenger.h"

WRITE_RAW_ENCODER(ceph_msg_connect);
WRITE_RAW_ENCODER(ceph_msg_connect_reply);

using crimson::common::local_conf;

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

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_ms);
}

template <typename T>
seastar::net::packet make_static_packet(const T& value) {
    return { reinterpret_cast<const char*>(&value), sizeof(value) };
}

// store the banner in a non-const string for buffer::create_static()
char banner[] = CEPH_BANNER;
constexpr size_t banner_size = sizeof(CEPH_BANNER)-1;

constexpr size_t client_header_size = banner_size + sizeof(ceph_entity_addr);
constexpr size_t server_header_size = banner_size + 2 * sizeof(ceph_entity_addr);

// check that the buffer starts with a valid banner without requiring it to
// be contiguous in memory
void validate_banner(bufferlist::const_iterator& p)
{
  auto b = std::cbegin(banner);
  auto end = b + banner_size;
  while (b != end) {
    const char *buf{nullptr};
    auto remaining = std::distance(b, end);
    auto len = p.get_ptr_and_advance(remaining, &buf);
    if (!std::equal(buf, buf + len, b)) {
      throw std::system_error(
          make_error_code(crimson::net::error::bad_connect_banner));
    }
    b += len;
  }
}

// return a static bufferptr to the given object
template <typename T>
bufferptr create_static(T& obj)
{
  return buffer::create_static(sizeof(obj), reinterpret_cast<char*>(&obj));
}

uint32_t get_proto_version(entity_type_t peer_type, bool connect)
{
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

void discard_up_to(std::deque<MessageRef>* queue,
                   crimson::net::seq_num_t seq)
{
  while (!queue->empty() &&
         queue->front()->get_seq() < seq) {
    queue->pop_front();
  }
}

} // namespace anonymous

namespace crimson::net {

ProtocolV1::ProtocolV1(ChainedDispatchersRef& dispatcher,
                       SocketConnection& conn,
                       SocketMessenger& messenger)
  : Protocol(proto_t::v1, dispatcher, conn), messenger{messenger} {}

ProtocolV1::~ProtocolV1() {}

bool ProtocolV1::is_connected() const
{
  return state == state_t::open;
}

// connecting state

void ProtocolV1::reset_session()
{
  conn.out_q = {};
  conn.sent = {};
  conn.in_seq = 0;
  h.connect_seq = 0;
  if (HAVE_FEATURE(conn.features, MSG_AUTH)) {
    // Set out_seq to a random value, so CRC won't be predictable.
    // Constant to limit starting sequence number to 2^31.  Nothing special
    // about it, just a big number.
    constexpr uint64_t SEQ_MASK = 0x7fffffff;
    conn.out_seq = ceph::util::generate_random_number<uint64_t>(0, SEQ_MASK);
  } else {
    // previously, seq #'s always started at 0.
    conn.out_seq = 0;
  }
}

seastar::future<stop_t>
ProtocolV1::handle_connect_reply(msgr_tag_t tag)
{
  if (h.auth_payload.length() && !conn.peer_is_mon()) {
    if (tag == CEPH_MSGR_TAG_CHALLENGE_AUTHORIZER) { // more
      h.auth_more = messenger.get_auth_client()->handle_auth_reply_more(
          conn.shared_from_this(), auth_meta, h.auth_payload);
      return seastar::make_ready_future<stop_t>(stop_t::no);
    } else {
      int ret = messenger.get_auth_client()->handle_auth_done(
          conn.shared_from_this(), auth_meta, 0, 0, h.auth_payload);
      if (ret < 0) {
        // fault
        logger().warn("{} AuthClient::handle_auth_done() return {}", conn, ret);
        throw std::system_error(make_error_code(error::negotiation_failure));
      }
    }
  }

  switch (tag) {
  case CEPH_MSGR_TAG_FEATURES:
    logger().error("{} connect protocol feature mispatch", __func__);
    throw std::system_error(make_error_code(error::negotiation_failure));
  case CEPH_MSGR_TAG_BADPROTOVER:
    logger().error("{} connect protocol version mispatch", __func__);
    throw std::system_error(make_error_code(error::negotiation_failure));
  case CEPH_MSGR_TAG_BADAUTHORIZER:
    logger().error("{} got bad authorizer", __func__);
    throw std::system_error(make_error_code(error::negotiation_failure));
  case CEPH_MSGR_TAG_RESETSESSION:
    reset_session();
    return seastar::make_ready_future<stop_t>(stop_t::no);
  case CEPH_MSGR_TAG_RETRY_GLOBAL:
    return messenger.get_global_seq(h.reply.global_seq).then([this] (auto gs) {
      h.global_seq = gs;
      return seastar::make_ready_future<stop_t>(stop_t::no);
    });
  case CEPH_MSGR_TAG_RETRY_SESSION:
    ceph_assert(h.reply.connect_seq > h.connect_seq);
    h.connect_seq = h.reply.connect_seq;
    return seastar::make_ready_future<stop_t>(stop_t::no);
  case CEPH_MSGR_TAG_WAIT:
    // TODO: state wait
    throw std::system_error(make_error_code(error::negotiation_failure));
  case CEPH_MSGR_TAG_SEQ:
  case CEPH_MSGR_TAG_READY:
    if (auto missing = (conn.policy.features_required & ~(uint64_t)h.reply.features);
        missing) {
      logger().error("{} missing required features", __func__);
      throw std::system_error(make_error_code(error::negotiation_failure));
    }
    return seastar::futurize_invoke([this, tag] {
        if (tag == CEPH_MSGR_TAG_SEQ) {
          return socket->read_exactly(sizeof(seq_num_t))
            .then([this] (auto buf) {
              auto acked_seq = reinterpret_cast<const seq_num_t*>(buf.get());
              discard_up_to(&conn.out_q, *acked_seq);
              return socket->write_flush(make_static_packet(conn.in_seq));
            });
        }
        // tag CEPH_MSGR_TAG_READY
        return seastar::now();
      }).then([this] {
        // hooray!
        h.peer_global_seq = h.reply.global_seq;
        conn.policy.lossy = h.reply.flags & CEPH_MSG_CONNECT_LOSSY;
        h.connect_seq++;
        h.backoff = 0ms;
        conn.set_features(h.reply.features & h.connect.features);
        if (auth_meta->authorizer) {
          session_security.reset(
              get_auth_session_handler(nullptr,
                                       auth_meta->authorizer->protocol,
                                       auth_meta->session_key,
                                       conn.features));
        } else {
          session_security.reset();
        }
        return seastar::make_ready_future<stop_t>(stop_t::yes);
      });
    break;
  default:
    // unknown tag
    logger().error("{} got unknown tag", __func__, int(tag));
    throw std::system_error(make_error_code(error::negotiation_failure));
  }
}

ceph::bufferlist ProtocolV1::get_auth_payload()
{
  // only non-mons connectings to mons use MAuth messages
  if (conn.peer_is_mon() &&
     messenger.get_mytype() != CEPH_ENTITY_TYPE_MON) {
    return {};
  } else {
    if (h.auth_more.length()) {
      logger().info("using augmented (challenge) auth payload");
      return std::move(h.auth_more);
    } else {
      auto [auth_method, preferred_modes, auth_bl] =
	messenger.get_auth_client()->get_auth_request(
            conn.shared_from_this(), auth_meta);
      auth_meta->auth_method = auth_method;
      return auth_bl;
    }
  }
}

seastar::future<stop_t>
ProtocolV1::repeat_connect()
{
  // encode ceph_msg_connect
  memset(&h.connect, 0, sizeof(h.connect));
  h.connect.features = conn.policy.features_supported;
  h.connect.host_type = messenger.get_myname().type();
  h.connect.global_seq = h.global_seq;
  h.connect.connect_seq = h.connect_seq;
  h.connect.protocol_version = get_proto_version(conn.get_peer_type(), true);
  // this is fyi, actually, server decides!
  h.connect.flags = conn.policy.lossy ? CEPH_MSG_CONNECT_LOSSY : 0;

  ceph_assert(messenger.get_auth_client());

  bufferlist bl;
  bufferlist auth_bl = get_auth_payload();
  if (auth_bl.length()) {
    h.connect.authorizer_protocol = auth_meta->auth_method;
    h.connect.authorizer_len = auth_bl.length();
    bl.append(create_static(h.connect));
    bl.claim_append(auth_bl);
  } else {
    h.connect.authorizer_protocol = 0;
    h.connect.authorizer_len = 0;
    bl.append(create_static(h.connect));
  };
  return socket->write_flush(std::move(bl))
    .then([this] {
      // read the reply
      return socket->read(sizeof(h.reply));
    }).then([this] (bufferlist bl) {
      auto p = bl.cbegin();
      ::decode(h.reply, p);
      ceph_assert(p.end());
      return socket->read(h.reply.authorizer_len);
    }).then([this] (bufferlist bl) {
      h.auth_payload = std::move(bl);
      return handle_connect_reply(h.reply.tag);
    });
}

void ProtocolV1::start_connect(const entity_addr_t& _peer_addr,
                               const entity_name_t& _peer_name)
{
  ceph_assert(state == state_t::none);
  logger().trace("{} trigger connecting, was {}", conn, static_cast<int>(state));
  state = state_t::connecting;
  set_write_state(write_state_t::delay);

  ceph_assert(!socket);
  conn.peer_addr = _peer_addr;
  conn.target_addr = _peer_addr;
  conn.set_peer_name(_peer_name);
  conn.policy = messenger.get_policy(_peer_name.type());
  messenger.register_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  gate.dispatch_in_background("start_connect", *this, [this] {
      return Socket::connect(conn.peer_addr)
        .then([this](SocketRef sock) {
          socket = std::move(sock);
          if (state == state_t::closing) {
            return socket->close().then([] {
              throw std::system_error(make_error_code(error::protocol_aborted));
            });
          }
          return seastar::now();
        }).then([this] {
          return messenger.get_global_seq();
        }).then([this] (auto gs) {
          h.global_seq = gs;
          // read server's handshake header
          return socket->read(server_header_size);
        }).then([this] (bufferlist headerbl) {
          auto p = headerbl.cbegin();
          validate_banner(p);
          entity_addr_t saddr, caddr;
          ::decode(saddr, p);
          ::decode(caddr, p);
          ceph_assert(p.end());
          if (saddr != conn.peer_addr) {
            logger().error("{} my peer_addr {} doesn't match what peer advertized {}",
                           conn, conn.peer_addr, saddr);
            throw std::system_error(
                make_error_code(crimson::net::error::bad_peer_address));
          }
          if (state != state_t::connecting) {
            throw std::system_error(make_error_code(error::protocol_aborted));
          }
          socket->learn_ephemeral_port_as_connector(caddr.get_port());
          if (unlikely(caddr.is_msgr2())) {
            logger().warn("{} peer sent a v2 address for me: {}",
                          conn, caddr);
            throw std::system_error(
                make_error_code(crimson::net::error::bad_peer_address));
          }
          caddr.set_type(entity_addr_t::TYPE_LEGACY);
          return messenger.learned_addr(caddr, conn);
        }).then([this] {
          // encode/send client's handshake header
          bufferlist bl;
          bl.append(buffer::create_static(banner_size, banner));
          ::encode(messenger.get_myaddr(), bl, 0);
          return socket->write_flush(std::move(bl));
        }).then([=] {
          return seastar::repeat([this] {
            return repeat_connect();
          });
        }).then([this] {
          execute_open(open_t::connected);
        }).handle_exception([this] (std::exception_ptr eptr) {
          // TODO: handle fault in the connecting state
          logger().warn("{} connecting fault: {}", conn, eptr);
          close(true);
        });
    });
}

// accepting state

seastar::future<stop_t> ProtocolV1::send_connect_reply(
    msgr_tag_t tag, bufferlist&& authorizer_reply)
{
  h.reply.tag = tag;
  h.reply.features = static_cast<uint64_t>((h.connect.features &
                                            conn.policy.features_supported) |
                                           conn.policy.features_required);
  h.reply.authorizer_len = authorizer_reply.length();
  return socket->write(make_static_packet(h.reply))
    .then([this, reply=std::move(authorizer_reply)]() mutable {
      return socket->write_flush(std::move(reply));
    }).then([] {
      return stop_t::no;
    });
}

seastar::future<stop_t> ProtocolV1::send_connect_reply_ready(
    msgr_tag_t tag, bufferlist&& authorizer_reply)
{
  return messenger.get_global_seq(
    ).then([this, tag, auth_len = authorizer_reply.length()] (auto gs) {
      h.global_seq = gs;
      h.reply.tag = tag;
      h.reply.features = conn.policy.features_supported;
      h.reply.global_seq = h.global_seq;
      h.reply.connect_seq = h.connect_seq;
      h.reply.flags = 0;
      if (conn.policy.lossy) {
        h.reply.flags = h.reply.flags | CEPH_MSG_CONNECT_LOSSY;
      }
      h.reply.authorizer_len = auth_len;

      session_security.reset(
          get_auth_session_handler(nullptr,
                                   auth_meta->auth_method,
                                   auth_meta->session_key,
                                   conn.features));

      return socket->write(make_static_packet(h.reply));
    }).then([this, reply=std::move(authorizer_reply)]() mutable {
      if (reply.length()) {
        return socket->write(std::move(reply));
      } else {
        return seastar::now();
      }
    }).then([this] {
      if (h.reply.tag == CEPH_MSGR_TAG_SEQ) {
        return socket->write_flush(make_static_packet(conn.in_seq))
          .then([this] {
            return socket->read_exactly(sizeof(seq_num_t));
          }).then([this] (auto buf) {
            auto acked_seq = reinterpret_cast<const seq_num_t*>(buf.get());
            discard_up_to(&conn.out_q, *acked_seq);
          });
      } else {
        return socket->flush();
      }
    }).then([] {
      return stop_t::yes;
    });
}

seastar::future<stop_t> ProtocolV1::replace_existing(
    SocketConnectionRef existing,
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
  if (!existing->is_lossy()) {
    // XXX: we decided not to support lossless connection in v1. as the
    // client's default policy is
    // Messenger::Policy::lossy_client(CEPH_FEATURE_OSDREPLYMUX) which is
    // lossy. And by the time
    // will all be performed using v2 protocol.
    ceph_abort("lossless policy not supported for v1");
  }
  existing->protocol->close(true);
  return send_connect_reply_ready(reply_tag, std::move(authorizer_reply));
}

seastar::future<stop_t> ProtocolV1::handle_connect_with_existing(
    SocketConnectionRef existing, bufferlist&& authorizer_reply)
{
  ProtocolV1 *exproto = dynamic_cast<ProtocolV1*>(existing->protocol.get());

  if (h.connect.global_seq < exproto->peer_global_seq()) {
    h.reply.global_seq = exproto->peer_global_seq();
    return send_connect_reply(CEPH_MSGR_TAG_RETRY_GLOBAL);
  } else if (existing->is_lossy()) {
    return replace_existing(existing, std::move(authorizer_reply));
  } else if (h.connect.connect_seq == 0 && exproto->connect_seq() > 0) {
    return replace_existing(existing, std::move(authorizer_reply), true);
  } else if (h.connect.connect_seq < exproto->connect_seq()) {
    // old attempt, or we sent READY but they didn't get it.
    h.reply.connect_seq = exproto->connect_seq() + 1;
    return send_connect_reply(CEPH_MSGR_TAG_RETRY_SESSION);
  } else if (h.connect.connect_seq == exproto->connect_seq()) {
    // if the existing connection successfully opened, and/or
    // subsequently went to standby, then the peer should bump
    // their connect_seq and retry: this is not a connection race
    // we need to resolve here.
    if (exproto->get_state() == state_t::open ||
        exproto->get_state() == state_t::standby) {
      if (conn.policy.resetcheck && exproto->connect_seq() == 0) {
        return replace_existing(existing, std::move(authorizer_reply));
      } else {
        h.reply.connect_seq = exproto->connect_seq() + 1;
        return send_connect_reply(CEPH_MSGR_TAG_RETRY_SESSION);
      }
    } else if (existing->peer_wins()) {
      return replace_existing(existing, std::move(authorizer_reply));
    } else {
      return send_connect_reply(CEPH_MSGR_TAG_WAIT);
    }
  } else if (conn.policy.resetcheck &&
             exproto->connect_seq() == 0) {
    return send_connect_reply(CEPH_MSGR_TAG_RESETSESSION);
  } else {
    return replace_existing(existing, std::move(authorizer_reply));
  }
}

bool ProtocolV1::require_auth_feature() const
{
  if (h.connect.authorizer_protocol != CEPH_AUTH_CEPHX) {
    return false;
  }
  if (local_conf()->cephx_require_signatures) {
    return true;
  }
  if (h.connect.host_type == CEPH_ENTITY_TYPE_OSD ||
      h.connect.host_type == CEPH_ENTITY_TYPE_MDS) {
    return local_conf()->cephx_cluster_require_signatures;
  } else {
    return local_conf()->cephx_service_require_signatures;
  }
}

seastar::future<stop_t> ProtocolV1::repeat_handle_connect()
{
  return socket->read(sizeof(h.connect))
    .then([this](bufferlist bl) {
      auto p = bl.cbegin();
      ::decode(h.connect, p);
      if (conn.get_peer_type() != 0 &&
          conn.get_peer_type() != h.connect.host_type) {
        logger().error("{} repeat_handle_connect(): my peer type does not match"
                       " what peer advertises {} != {}",
                       conn, conn.get_peer_type(), h.connect.host_type);
        throw std::system_error(make_error_code(error::protocol_aborted));
      }
      conn.set_peer_type(h.connect.host_type);
      conn.policy = messenger.get_policy(h.connect.host_type);
      if (!conn.policy.lossy && !conn.policy.server && conn.target_addr.get_port() <= 0) {
          logger().error("{} we don't know how to reconnect to peer {}",
                         conn, conn.target_addr);
        throw std::system_error(
            make_error_code(crimson::net::error::bad_peer_address));
      }
      return socket->read(h.connect.authorizer_len);
    }).then([this] (bufferlist authorizer) {
      memset(&h.reply, 0, sizeof(h.reply));
      // TODO: set reply.protocol_version
      if (h.connect.protocol_version != get_proto_version(h.connect.host_type, false)) {
        return send_connect_reply(
            CEPH_MSGR_TAG_BADPROTOVER, bufferlist{});
      }
      if (require_auth_feature()) {
        conn.policy.features_required |= CEPH_FEATURE_MSG_AUTH;
      }
      if (auto feat_missing = conn.policy.features_required & ~(uint64_t)h.connect.features;
          feat_missing != 0) {
        return send_connect_reply(
            CEPH_MSGR_TAG_FEATURES, bufferlist{});
      }

      bufferlist authorizer_reply;
      auth_meta->auth_method = h.connect.authorizer_protocol;
      auto more = static_cast<bool>(auth_meta->authorizer_challenge);
      ceph_assert(messenger.get_auth_server());
      int r = messenger.get_auth_server()->handle_auth_request(
          conn.shared_from_this(), auth_meta, more, auth_meta->auth_method, authorizer,
          &authorizer_reply);

      if (r < 0) {
        session_security.reset();
        return send_connect_reply(
            CEPH_MSGR_TAG_BADAUTHORIZER, std::move(authorizer_reply));
      } else if (r == 0) {
        ceph_assert(authorizer_reply.length());
        return send_connect_reply(
            CEPH_MSGR_TAG_CHALLENGE_AUTHORIZER, std::move(authorizer_reply));
      }

      // r > 0
      if (auto existing = messenger.lookup_conn(conn.peer_addr); existing) {
        if (existing->protocol->proto_type != proto_t::v1) {
          logger().warn("{} existing {} proto version is {} not 1, close existing",
                        conn, *existing,
                        static_cast<int>(existing->protocol->proto_type));
          // NOTE: this is following async messenger logic, but we may miss the reset event.
          existing->mark_down();
        } else {
          return handle_connect_with_existing(existing, std::move(authorizer_reply));
        }
      }
      if (h.connect.connect_seq > 0) {
        return send_connect_reply(CEPH_MSGR_TAG_RESETSESSION,
                                  std::move(authorizer_reply));
      }
      h.connect_seq = h.connect.connect_seq + 1;
      h.peer_global_seq = h.connect.global_seq;
      conn.set_features((uint64_t)conn.policy.features_supported & (uint64_t)h.connect.features);
      // TODO: cct
      return send_connect_reply_ready(CEPH_MSGR_TAG_READY, std::move(authorizer_reply));
    });
}

void ProtocolV1::start_accept(SocketRef&& sock,
                              const entity_addr_t& _peer_addr)
{
  ceph_assert(state == state_t::none);
  logger().trace("{} trigger accepting, was {}",
                 conn, static_cast<int>(state));
  state = state_t::accepting;
  set_write_state(write_state_t::delay);

  ceph_assert(!socket);
  // until we know better
  conn.target_addr = _peer_addr;
  socket = std::move(sock);
  messenger.accept_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  gate.dispatch_in_background("start_accept", *this, [this] {
      // stop learning my_addr before sending it out, so it won't change
      return messenger.learned_addr(messenger.get_myaddr(), conn).then([this] {
          // encode/send server's handshake header
          bufferlist bl;
          bl.append(buffer::create_static(banner_size, banner));
          ::encode(messenger.get_myaddr(), bl, 0);
          ::encode(conn.target_addr, bl, 0);
          return socket->write_flush(std::move(bl));
        }).then([this] {
          // read client's handshake header and connect request
          return socket->read(client_header_size);
        }).then([this] (bufferlist bl) {
          auto p = bl.cbegin();
          validate_banner(p);
          entity_addr_t addr;
          ::decode(addr, p);
          ceph_assert(p.end());
          if ((addr.is_legacy() || addr.is_any()) &&
              addr.is_same_host(conn.target_addr)) {
            // good
          } else {
            logger().error("{} peer advertized an invalid peer_addr: {},"
                           " which should be v1 and the same host with {}.",
                           conn, addr, conn.peer_addr);
            throw std::system_error(
                make_error_code(crimson::net::error::bad_peer_address));
          }
          conn.peer_addr = addr;
          conn.target_addr = conn.peer_addr;
          return seastar::repeat([this] {
            return repeat_handle_connect();
          });
        }).then([this] {
          messenger.register_conn(
            seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
          messenger.unaccept_conn(
            seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
          execute_open(open_t::accepted);
        }).handle_exception([this] (std::exception_ptr eptr) {
          // TODO: handle fault in the accepting state
          logger().warn("{} accepting fault: {}", conn, eptr);
          close(false);
        });
    });
}

// open state

ceph::bufferlist ProtocolV1::do_sweep_messages(
    const std::deque<MessageRef>& msgs,
    size_t num_msgs,
    bool require_keepalive,
    std::optional<utime_t> _keepalive_ack,
    bool require_ack)
{
  static const size_t RESERVE_MSG_SIZE = sizeof(CEPH_MSGR_TAG_MSG) +
                                         sizeof(ceph_msg_header) +
                                         sizeof(ceph_msg_footer);
  static const size_t RESERVE_MSG_SIZE_OLD = sizeof(CEPH_MSGR_TAG_MSG) +
                                             sizeof(ceph_msg_header) +
                                             sizeof(ceph_msg_footer_old);

  ceph::bufferlist bl;
  if (likely(num_msgs)) {
    if (HAVE_FEATURE(conn.features, MSG_AUTH)) {
      bl.reserve(num_msgs * RESERVE_MSG_SIZE);
    } else {
      bl.reserve(num_msgs * RESERVE_MSG_SIZE_OLD);
    }
  }

  if (unlikely(require_keepalive)) {
    k.req.stamp = ceph::coarse_real_clock::to_ceph_timespec(
      ceph::coarse_real_clock::now());
    logger().trace("{} write keepalive2 {}", conn, k.req.stamp.tv_sec);
    bl.append(create_static(k.req));
  }

  if (unlikely(_keepalive_ack.has_value())) {
    logger().trace("{} write keepalive2 ack {}", conn, *_keepalive_ack);
    k.ack.stamp = ceph_timespec(*_keepalive_ack);
    bl.append(create_static(k.ack));
  }

  if (require_ack) {
    // XXX: we decided not to support lossless connection in v1. as the
    // client's default policy is
    // Messenger::Policy::lossy_client(CEPH_FEATURE_OSDREPLYMUX) which is
    // lossy. And by the time of crimson-osd's GA, the in-cluster communication
    // will all be performed using v2 protocol.
    ceph_abort("lossless policy not supported for v1");
  }

  std::for_each(msgs.begin(), msgs.begin()+num_msgs, [this, &bl](const MessageRef& msg) {
    ceph_assert(!msg->get_seq() && "message already has seq");
    msg->set_seq(++conn.out_seq);
    auto& header = msg->get_header();
    header.src = messenger.get_myname();
    msg->encode(conn.features, messenger.get_crc_flags());
    if (session_security) {
      session_security->sign_message(msg.get());
    }
    logger().debug("{} --> #{} === {} ({})",
                   conn, msg->get_seq(), *msg, msg->get_type());
    bl.append(CEPH_MSGR_TAG_MSG);
    bl.append((const char*)&header, sizeof(header));
    bl.append(msg->get_payload());
    bl.append(msg->get_middle());
    bl.append(msg->get_data());
    auto& footer = msg->get_footer();
    if (HAVE_FEATURE(conn.features, MSG_AUTH)) {
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
  });

  return bl;
}

seastar::future<> ProtocolV1::handle_keepalive2_ack()
{
  return socket->read_exactly(sizeof(ceph_timespec))
    .then([this] (auto buf) {
      auto t = reinterpret_cast<const ceph_timespec*>(buf.get());
      k.ack_stamp = *t;
      logger().trace("{} got keepalive2 ack {}", conn, t->tv_sec);
    });
}

seastar::future<> ProtocolV1::handle_keepalive2()
{
  return socket->read_exactly(sizeof(ceph_timespec))
    .then([this] (auto buf) {
      utime_t ack{*reinterpret_cast<const ceph_timespec*>(buf.get())};
      notify_keepalive_ack(ack);
    });
}

seastar::future<> ProtocolV1::handle_ack()
{
  return socket->read_exactly(sizeof(ceph_le64))
    .then([this] (auto buf) {
      auto seq = reinterpret_cast<const ceph_le64*>(buf.get());
      discard_up_to(&conn.sent, *seq);
    });
}

seastar::future<> ProtocolV1::maybe_throttle()
{
  if (!conn.policy.throttler_bytes) {
    return seastar::now();
  }
  const auto to_read = (m.header.front_len +
                        m.header.middle_len +
                        m.header.data_len);
  return conn.policy.throttler_bytes->get(to_read);
}

seastar::future<> ProtocolV1::read_message()
{
  return socket->read(sizeof(m.header))
    .then([this] (bufferlist bl) {
      // throttle the traffic, maybe
      auto p = bl.cbegin();
      ::decode(m.header, p);
      return maybe_throttle();
    }).then([this] {
      // read front
      return socket->read(m.header.front_len);
    }).then([this] (bufferlist bl) {
      m.front = std::move(bl);
      // read middle
      return socket->read(m.header.middle_len);
    }).then([this] (bufferlist bl) {
      m.middle = std::move(bl);
      // read data
      return socket->read(m.header.data_len);
    }).then([this] (bufferlist bl) {
      m.data = std::move(bl);
      // read footer
      return socket->read(sizeof(m.footer));
    }).then([this] (bufferlist bl) {
      auto p = bl.cbegin();
      ::decode(m.footer, p);
      auto pconn = seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this());
      auto msg = ::decode_message(nullptr, 0, m.header, m.footer,
                                  m.front, m.middle, m.data, std::move(pconn));
      if (unlikely(!msg)) {
        logger().warn("{} decode message failed", conn);
        throw std::system_error{make_error_code(error::corrupted_message)};
      }
      constexpr bool add_ref = false; // Message starts with 1 ref
      // TODO: change MessageRef with foreign_ptr
      auto msg_ref = MessageRef{msg, add_ref};

      if (session_security) {
        if (unlikely(session_security->check_message_signature(msg))) {
          logger().warn("{} message signature check failed", conn);
          throw std::system_error{make_error_code(error::corrupted_message)};
        }
      }
      // TODO: set time stamps
      msg->set_byte_throttler(conn.policy.throttler_bytes);

      if (unlikely(!conn.update_rx_seq(msg->get_seq()))) {
        // skip this message
        return;
      }

      // start dispatch, ignoring exceptions from the application layer
      gate.dispatch_in_background("ms_dispatch", *this, [this, msg = std::move(msg_ref)] {
        logger().debug("{} <== #{} === {} ({})",
                       conn, msg->get_seq(), *msg, msg->get_type());
        return dispatcher->ms_dispatch(&conn, std::move(msg));
      });
    });
}

seastar::future<> ProtocolV1::handle_tags()
{
  return seastar::keep_doing([this] {
      // read the next tag
      return socket->read_exactly(1)
        .then([this] (auto buf) {
          switch (buf[0]) {
          case CEPH_MSGR_TAG_MSG:
            return read_message();
          case CEPH_MSGR_TAG_ACK:
            return handle_ack();
          case CEPH_MSGR_TAG_KEEPALIVE:
            return seastar::now();
          case CEPH_MSGR_TAG_KEEPALIVE2:
            return handle_keepalive2();
          case CEPH_MSGR_TAG_KEEPALIVE2_ACK:
            return handle_keepalive2_ack();
          case CEPH_MSGR_TAG_CLOSE:
            logger().info("{} got tag close", conn);
            throw std::system_error(make_error_code(error::protocol_aborted));
          default:
            logger().error("{} got unknown msgr tag {}",
                           conn, static_cast<int>(buf[0]));
            throw std::system_error(make_error_code(error::read_eof));
          }
        });
    });
}

void ProtocolV1::execute_open(open_t type)
{
  logger().trace("{} trigger open, was {}", conn, static_cast<int>(state));
  state = state_t::open;
  set_write_state(write_state_t::open);

  if (type == open_t::connected) {
    gate.dispatch_in_background("ms_handle_connect", *this, [this] {
      return dispatcher->ms_handle_connect(
          seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
    });
  } else { // type == open_t::accepted
    gate.dispatch_in_background("ms_handle_accept", *this, [this] {
      return dispatcher->ms_handle_accept(
          seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
    });
  }

  gate.dispatch_in_background("execute_open", *this, [this] {
      // start background processing of tags
      return handle_tags()
        .handle_exception_type([this] (const std::system_error& e) {
          logger().warn("{} open fault: {}", conn, e);
          if (e.code() == error::protocol_aborted ||
              e.code() == std::errc::connection_reset ||
              e.code() == error::read_eof) {
            close(true);
            return seastar::now();
          } else {
            throw e;
          }
        }).handle_exception([this] (std::exception_ptr eptr) {
          // TODO: handle fault in the open state
          logger().warn("{} open fault: {}", conn, eptr);
          close(true);
        });
    });
}

// closing state

void ProtocolV1::trigger_close()
{
  logger().trace("{} trigger closing, was {}",
                 conn, static_cast<int>(state));

  if (state == state_t::accepting) {
    messenger.unaccept_conn(seastar::static_pointer_cast<SocketConnection>(
      conn.shared_from_this()));
  } else if (state >= state_t::connecting && state < state_t::closing) {
    messenger.unregister_conn(seastar::static_pointer_cast<SocketConnection>(
      conn.shared_from_this()));
  } else {
    // cannot happen
    ceph_assert(false);
  }

  if (!socket) {
    ceph_assert(state == state_t::connecting);
  }

  state = state_t::closing;
}

seastar::future<> ProtocolV1::fault()
{
  if (conn.policy.lossy) {
    messenger.unregister_conn(seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
  }
  // XXX: we decided not to support lossless connection in v1. as the
  // client's default policy is
  // Messenger::Policy::lossy_client(CEPH_FEATURE_OSDREPLYMUX) which is
  // lossy. And by the time of crimson-osd's GA, the in-cluster communication
  // will all be performed using v2 protocol.
  ceph_abort("lossless policy not supported for v1");
  return seastar::now();
}

void ProtocolV1::print(std::ostream& out) const
{
  out << conn;
}

} // namespace crimson::net
