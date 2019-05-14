// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ProtocolV2.h"

#include <seastar/core/lowres_clock.hh>
#include <fmt/format.h>
#if __has_include(<fmt/chrono.h>)
#include <fmt/chrono.h>
#else
#include <fmt/time.h>
#endif
#include "include/msgr.h"
#include "include/random.h"

#include "crimson/auth/AuthClient.h"
#include "crimson/auth/AuthServer.h"

#include "Config.h"
#include "Dispatcher.h"
#include "Errors.h"
#include "Socket.h"
#include "SocketConnection.h"
#include "SocketMessenger.h"

using namespace ceph::msgr::v2;

namespace {

seastar::logger& logger() {
  return ceph::get_logger(ceph_subsys_ms);
}

seastar::future<> abort_in_fault() {
  throw std::system_error(make_error_code(ceph::net::error::negotiation_failure));
}

seastar::future<> abort_in_close() {
  throw std::system_error(make_error_code(ceph::net::error::connection_aborted));
}

inline void expect_tag(const Tag& expected,
                       const Tag& actual,
                       ceph::net::SocketConnection& conn,
                       const char *where) {
  if (actual != expected) {
    logger().error("{} {} received wrong tag: {}, expected {}",
                   conn, where,
                   static_cast<uint32_t>(actual),
                   static_cast<uint32_t>(expected));
    abort_in_fault();
  }
}

inline seastar::future<> unexpected_tag(const Tag& unexpected,
                                        ceph::net::SocketConnection& conn,
                                        const char *where) {
  logger().error("{} {} received unexpected tag: {}",
                 conn, where, static_cast<uint32_t>(unexpected));
  return abort_in_fault();
}

} // namespace anonymous

namespace fmt {
template <>
struct formatter<seastar::lowres_system_clock::time_point> {
  // ignore the format string
  template <typename ParseContext>
  constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const seastar::lowres_system_clock::time_point& t,
	      FormatContext& ctx) {
    struct tm bdt;
    time_t tt = std::chrono::duration_cast<std::chrono::seconds>(
      t.time_since_epoch()).count();
    localtime_r(&tt, &bdt);
    auto milliseconds = (t.time_since_epoch() %
			 std::chrono::seconds(1)).count();
    return format_to(ctx.out(), "{:%Y-%m-%d %H:%M:%S} {:03d}",
		     bdt, milliseconds);
  }
};
}

namespace std {
inline ostream& operator<<(
  ostream& out, const seastar::lowres_system_clock::time_point& t)
{
  return out << fmt::format("{}", t);
}
}

namespace ceph::net {

ProtocolV2::ProtocolV2(Dispatcher& dispatcher,
                       SocketConnection& conn,
                       SocketMessenger& messenger)
  : Protocol(proto_t::v2, dispatcher, conn), messenger{messenger} {}

ProtocolV2::~ProtocolV2() {}

void ProtocolV2::start_connect(const entity_addr_t& _peer_addr,
                               const entity_type_t& _peer_type)
{
  ceph_assert(state == state_t::NONE);
  ceph_assert(!socket);
  conn.peer_addr = _peer_addr;
  conn.target_addr = _peer_addr;
  conn.peer_type = _peer_type;
  conn.policy = messenger.get_policy(_peer_type);
  messenger.register_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  execute_connecting();
}

void ProtocolV2::start_accept(SocketFRef&& sock,
                              const entity_addr_t& _peer_addr)
{
  ceph_assert(state == state_t::NONE);
  ceph_assert(!socket);
  conn.target_addr = _peer_addr;
  socket = std::move(sock);
  messenger.accept_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  execute_accepting();
}

// TODO: Frame related implementations, probably to a separate class.

void ProtocolV2::enable_recording()
{
  ceph_assert(!rxbuf.length());
  ceph_assert(!txbuf.length());
  ceph_assert(!record_io);
  record_io = true;
}

seastar::future<Socket::tmp_buf> ProtocolV2::read_exactly(size_t bytes)
{
  if (unlikely(record_io)) {
    return socket->read_exactly(bytes)
    .then([this] (auto bl) {
      rxbuf.append(buffer::create(bl.share()));
      return std::move(bl);
    });
  } else {
    return socket->read_exactly(bytes);
  };
}

seastar::future<bufferlist> ProtocolV2::read(size_t bytes)
{
  if (unlikely(record_io)) {
    return socket->read(bytes)
    .then([this] (auto buf) {
      rxbuf.append(buf);
      return std::move(buf);
    });
  } else {
    return socket->read(bytes);
  }
}

seastar::future<> ProtocolV2::write(bufferlist&& buf)
{
  if (unlikely(record_io)) {
    txbuf.append(buf);
  }
  return socket->write(std::move(buf));
}

seastar::future<> ProtocolV2::write_flush(bufferlist&& buf)
{
  if (unlikely(record_io)) {
    txbuf.append(buf);
  }
  return socket->write_flush(std::move(buf));
}

size_t ProtocolV2::get_current_msg_size() const
{
  ceph_assert(!rx_segments_desc.empty());
  size_t sum = 0;
  // we don't include SegmentIndex::Msg::HEADER.
  for (__u8 idx = 1; idx < rx_segments_desc.size(); idx++) {
    sum += rx_segments_desc[idx].length;
  }
  return sum;
}

seastar::future<Tag> ProtocolV2::read_main_preamble()
{
  return read_exactly(FRAME_PREAMBLE_SIZE)
    .then([this] (auto bl) {
      if (session_stream_handlers.rx) {
        session_stream_handlers.rx->reset_rx_handler();
        /*
        bl = session_stream_handlers.rx->authenticated_decrypt_update(
            std::move(bl), segment_t::DEFAULT_ALIGNMENT);
        */
      }

      // I expect ceph_le32 will make the endian conversion for me. Passing
      // everything through ::Decode is unnecessary.
      const auto& main_preamble = \
        *reinterpret_cast<const preamble_block_t*>(bl.get());

      // verify preamble's CRC before any further processing
      const auto rx_crc = ceph_crc32c(0,
        reinterpret_cast<const unsigned char*>(&main_preamble),
        sizeof(main_preamble) - sizeof(main_preamble.crc));
      if (rx_crc != main_preamble.crc) {
        logger().error("{} crc mismatch for main preamble rx_crc={} tx_crc={}",
                       conn, rx_crc, main_preamble.crc);
        abort_in_fault();
      }
      logger().debug("{} read main preamble: tag={}, len={}", conn, (int)main_preamble.tag, bl.size());

      // currently we do support between 1 and MAX_NUM_SEGMENTS segments
      if (main_preamble.num_segments < 1 ||
          main_preamble.num_segments > MAX_NUM_SEGMENTS) {
        logger().error("{} unsupported num_segments={}",
                       conn, main_preamble.num_segments);
        abort_in_fault();
      }
      if (main_preamble.num_segments > MAX_NUM_SEGMENTS) {
        logger().error("{} num_segments too much: {}",
                       conn, main_preamble.num_segments);
        abort_in_fault();
      }

      rx_segments_desc.clear();
      rx_segments_data.clear();

      for (std::uint8_t idx = 0; idx < main_preamble.num_segments; idx++) {
        logger().debug("{} got new segment: len={} align={}",
                       conn, main_preamble.segments[idx].length,
                       main_preamble.segments[idx].alignment);
        rx_segments_desc.emplace_back(main_preamble.segments[idx]);
      }

      return static_cast<Tag>(main_preamble.tag);
    });
}

seastar::future<> ProtocolV2::read_frame_payload()
{
  ceph_assert(!rx_segments_desc.empty());
  ceph_assert(rx_segments_data.empty());

  return seastar::do_until(
    [this] { return rx_segments_desc.size() == rx_segments_data.size(); },
    [this] {
      // description of current segment to read
      const auto& cur_rx_desc = rx_segments_desc.at(rx_segments_data.size());
      // TODO: create aligned and contiguous buffer from socket
      if (cur_rx_desc.alignment != segment_t::DEFAULT_ALIGNMENT) {
        logger().debug("{} cannot allocate {} aligned buffer at segment desc index {}",
                       conn, cur_rx_desc.alignment, rx_segments_data.size());
      }
      // TODO: create aligned and contiguous buffer from socket
      return read_exactly(cur_rx_desc.length)
      .then([this] (auto tmp_bl) {
        bufferlist data;
        data.append(buffer::create(std::move(tmp_bl)));
        logger().debug("{} read frame segment[{}], length={}",
                       conn, rx_segments_data.size(), data.length());
        if (session_stream_handlers.rx) {
          // TODO
          ceph_assert(false);
        }
        rx_segments_data.emplace_back(std::move(data));
      });
    }
  ).then([this] {
    // TODO: get_epilogue_size()
    ceph_assert(!session_stream_handlers.rx);
    return read_exactly(FRAME_PLAIN_EPILOGUE_SIZE);
  }).then([this] (auto bl) {
    logger().debug("{} read frame epilogue length={}", conn, bl.size());

    __u8 late_flags;
    if (session_stream_handlers.rx) {
      // TODO
      ceph_assert(false);
    } else {
      auto& epilogue = *reinterpret_cast<const epilogue_plain_block_t*>(bl.get());
      for (std::uint8_t idx = 0; idx < rx_segments_data.size(); idx++) {
        const __u32 expected_crc = epilogue.crc_values[idx];
        const __u32 calculated_crc = rx_segments_data[idx].crc32c(-1);
        if (expected_crc != calculated_crc) {
          logger().error("{} message integrity check failed at index {}:"
                         " expected_crc={} calculated_crc={}",
                         conn, (unsigned int)idx, expected_crc, calculated_crc);
          abort_in_fault();
        } else {
          logger().debug("{} message integrity check success at index {}: crc={}",
                         conn, (unsigned int)idx, expected_crc);
        }
      }
      late_flags = epilogue.late_flags;
    }

    // we do have a mechanism that allows transmitter to start sending message
    // and abort after putting entire data field on wire. This will be used by
    // the kernel client to avoid unnecessary buffering.
    if (late_flags & FRAME_FLAGS_LATEABRT) {
      // TODO
      ceph_assert(false);
    }
  });
}

template <class F>
seastar::future<> ProtocolV2::write_frame(F &frame, bool flush)
{
  auto bl = frame.get_buffer(session_stream_handlers);
  logger().debug("{} write frame: tag={}, len={}", conn,
                 static_cast<uint32_t>(frame.tag), bl.length());
  if (flush) {
    return write_flush(std::move(bl));
  } else {
    return write(std::move(bl));
  }
}

void ProtocolV2::trigger_state(state_t _state, write_state_t _write_state, bool reentrant)
{
  if (!reentrant && _state == state) {
    logger().error("{} is not allowed to re-trigger state {}",
                   conn, get_state_name(state));
    ceph_assert(false);
  }
  logger().debug("{} trigger {}, was {}",
                 conn, get_state_name(_state), get_state_name(state));
  state = _state;
  set_write_state(_write_state);
}

seastar::future<> ProtocolV2::fault()
{
  logger().warn("{} fault during {}",
                conn, get_state_name(state));
  // TODO: <fault logic here: e.g. backoff, policies, etc.>
  // TODO: <conditions to call execute_standby()>
  // TODO: <conditions to call execute_connecting()>
  close();
  return seastar::now();
}

void ProtocolV2::dispatch_reset()
{
  seastar::with_gate(pending_dispatch, [this] {
    return dispatcher.ms_handle_reset(
        seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()))
    .handle_exception([this] (std::exception_ptr eptr) {
      logger().error("{} ms_handle_reset caust exception: {}", conn, eptr);
    });
  });
}

void ProtocolV2::reset_session(bool full)
{
  if (full) {
    server_cookie = 0;
    connect_seq = 0;
    conn.in_seq = 0;
  } else {
    conn.out_seq = 0;
    conn.in_seq = 0;
    client_cookie = 0;
    server_cookie = 0;
    connect_seq = 0;
    peer_global_seq = 0;
    // TODO:
    // discard_out_queue();
    // message_seq = 0;
    // ack_left = 0;
    seastar::with_gate(pending_dispatch, [this] {
      return dispatcher.ms_handle_remote_reset(
          seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()))
      .handle_exception([this] (std::exception_ptr eptr) {
        logger().error("{} ms_handle_remote_reset caust exception: {}", conn, eptr);
      });
    });
  }
}

seastar::future<entity_type_t, entity_addr_t> ProtocolV2::banner_exchange()
{
  // 1. prepare and send banner
  bufferlist banner_payload;
  encode((uint64_t)CEPH_MSGR2_SUPPORTED_FEATURES, banner_payload, 0);
  encode((uint64_t)CEPH_MSGR2_REQUIRED_FEATURES, banner_payload, 0);

  bufferlist bl;
  bl.append(CEPH_BANNER_V2_PREFIX, strlen(CEPH_BANNER_V2_PREFIX));
  encode((uint16_t)banner_payload.length(), bl, 0);
  bl.claim_append(banner_payload);
  return write_flush(std::move(bl)).then([this] {
      // 2. read peer banner
      unsigned banner_len = strlen(CEPH_BANNER_V2_PREFIX) + sizeof(__le16);
      return read_exactly(banner_len); // or read exactly?
    }).then([this] (auto bl) {
      // 3. process peer banner and read banner_payload
      unsigned banner_prefix_len = strlen(CEPH_BANNER_V2_PREFIX);

      if (memcmp(bl.get(), CEPH_BANNER_V2_PREFIX, banner_prefix_len) != 0) {
        if (memcmp(bl.get(), CEPH_BANNER, strlen(CEPH_BANNER)) == 0) {
          logger().error("{} peer is using V1 protocol", conn);
        } else {
          logger().error("{} peer sent bad banner", conn);
        }
        abort_in_fault();
      }
      bl.trim_front(banner_prefix_len);

      uint16_t payload_len;
      bufferlist buf;
      buf.append(buffer::create(std::move(bl)));
      auto ti = buf.cbegin();
      try {
        decode(payload_len, ti);
      } catch (const buffer::error &e) {
        logger().error("{} decode banner payload len failed", conn);
        abort_in_fault();
      }
      return read(payload_len);
    }).then([this] (bufferlist bl) {
      // 4. process peer banner_payload and send HelloFrame
      auto p = bl.cbegin();
      uint64_t peer_supported_features;
      uint64_t peer_required_features;
      try {
        decode(peer_supported_features, p);
        decode(peer_required_features, p);
      } catch (const buffer::error &e) {
        logger().error("{} decode banner payload failed", conn);
        abort_in_fault();
      }
      logger().debug("{} supported={} required={}",
                     conn, peer_supported_features, peer_required_features);

      // Check feature bit compatibility
      uint64_t supported_features = CEPH_MSGR2_SUPPORTED_FEATURES;
      uint64_t required_features = CEPH_MSGR2_REQUIRED_FEATURES;
      if ((required_features & peer_supported_features) != required_features) {
        logger().error("{} peer does not support all required features"
                       " required={} peer_supported={}",
                       conn, required_features, peer_supported_features);
        abort_in_close();
      }
      if ((supported_features & peer_required_features) != peer_required_features) {
        logger().error("{} we do not support all peer required features"
                       " peer_required={} supported={}",
                       conn, peer_required_features, supported_features);
        abort_in_close();
      }
      this->peer_required_features = peer_required_features;
      if (this->peer_required_features == 0) {
        this->connection_features = msgr2_required;
      }

      auto hello = HelloFrame::Encode(messenger.get_mytype(),
                                      conn.target_addr);
      return write_frame(hello);
    }).then([this] {
      //5. read peer HelloFrame
      return read_main_preamble();
    }).then([this] (Tag tag) {
      expect_tag(Tag::HELLO, tag, conn, __func__);
      return read_frame_payload();
    }).then([this] {
      // 6. process peer HelloFrame
      auto hello = HelloFrame::Decode(rx_segments_data.back());
      logger().debug("{} received hello: peer_type={} peer_addr_for_me={}",
                     conn, (int)hello.entity_type(), hello.peer_addr());
      return seastar::make_ready_future<entity_type_t, entity_addr_t>(
          hello.entity_type(), hello.peer_addr());
    });
}

// CONNECTING state

seastar::future<> ProtocolV2::handle_auth_reply()
{
  return read_main_preamble()
  .then([this] (Tag tag) {
    switch (tag) {
      case Tag::AUTH_BAD_METHOD:
        return read_frame_payload().then([this] {
          // handle_auth_bad_method() logic
          auto bad_method = AuthBadMethodFrame::Decode(rx_segments_data.back());
          logger().warn("{} got AuthBadMethod, method={} reslt={}, "
                        "allowed methods={}, allowed modes={}",
                        conn, bad_method.method(), cpp_strerror(bad_method.result()),
                        bad_method.allowed_methods(), bad_method.allowed_modes());
          ceph_assert(messenger.get_auth_client());
          int r = messenger.get_auth_client()->handle_auth_bad_method(
              conn.shared_from_this(), auth_meta,
              bad_method.method(), bad_method.result(),
              bad_method.allowed_methods(), bad_method.allowed_modes());
          if (r < 0) {
            logger().error("{} auth_client handle_auth_bad_method returned {}",
                           conn, r);
            abort_in_fault();
          }
          return client_auth(bad_method.allowed_methods());
        });
      case Tag::AUTH_REPLY_MORE:
        return read_frame_payload().then([this] {
          // handle_auth_reply_more() logic
          auto auth_more = AuthReplyMoreFrame::Decode(rx_segments_data.back());
          logger().debug("{} auth reply more len={}",
                         conn, auth_more.auth_payload().length());
          ceph_assert(messenger.get_auth_client());
          // let execute_connecting() take care of the thrown exception
          auto reply = messenger.get_auth_client()->handle_auth_reply_more(
            conn.shared_from_this(), auth_meta, auth_more.auth_payload());
          auto more_reply = AuthRequestMoreFrame::Encode(reply);
          return write_frame(more_reply);
        }).then([this] {
          return handle_auth_reply();
        });
      case Tag::AUTH_DONE:
        return read_frame_payload().then([this] {
          // handle_auth_done() logic
          auto auth_done = AuthDoneFrame::Decode(rx_segments_data.back());
          ceph_assert(messenger.get_auth_client());
          int r = messenger.get_auth_client()->handle_auth_done(
              conn.shared_from_this(), auth_meta,
              auth_done.global_id(),
              auth_done.con_mode(),
              auth_done.auth_payload());
          if (r < 0) {
            logger().error("{} auth_client handle_auth_done returned {}", conn, r);
            abort_in_fault();
          }
          auth_meta->con_mode = auth_done.con_mode();
          // TODO
          ceph_assert(!auth_meta->is_mode_secure());
          session_stream_handlers = { nullptr, nullptr };
          return finish_auth();
        });
      default: {
        return unexpected_tag(tag, conn, __func__);
      }
    }
  });
}

seastar::future<> ProtocolV2::client_auth(std::vector<uint32_t> &allowed_methods)
{
  // send_auth_request() logic
  ceph_assert(messenger.get_auth_client());

  try {
    auto [auth_method, preferred_modes, bl] =
      messenger.get_auth_client()->get_auth_request(conn.shared_from_this(), auth_meta);
    auth_meta->auth_method = auth_method;
    auto frame = AuthRequestFrame::Encode(auth_method, preferred_modes, bl);
    return write_frame(frame).then([this] {
      return handle_auth_reply();
    });
  } catch (const ceph::auth::error& e) {
    logger().error("{} get_initial_auth_request returned {}", conn, e);
    dispatch_reset();
    abort_in_close();
    return seastar::now();
  }
}

seastar::future<bool> ProtocolV2::process_wait()
{
  return read_frame_payload().then([this] {
    // handle_wait() logic
    logger().debug("{} received WAIT (connection race)", conn);
    WaitFrame::Decode(rx_segments_data.back());
    return false;
  });
}

seastar::future<bool> ProtocolV2::client_connect()
{
  // send_client_ident() logic
  if (!conn.policy.lossy && !client_cookie) {
    client_cookie = ceph::util::generate_random_number<uint64_t>(1, -1ll);
  }

  // TODO: get socket address and learn(not supported by seastar)
  entity_addr_t a;
  a.u.sa.sa_family = AF_INET;
  a.set_type(entity_addr_t::TYPE_MSGR2);
  logger().debug("{} learn from addr {}", conn, a);
  return messenger.learned_addr(a).then([this] {
    uint64_t flags = 0;
    if (conn.policy.lossy) {
      flags |= CEPH_MSG_CONNECT_LOSSY;
    }

    auto client_ident = ClientIdentFrame::Encode(
        messenger.get_myaddrs(),
        conn.target_addr,
        messenger.get_myname().num(),
        global_seq,
        conn.policy.features_supported,
        conn.policy.features_required | msgr2_required, flags,
        client_cookie);

    logger().debug("{} sending identification: addrs={} target={} gid={}"
                   " global_seq={} features_supported={} features_required={}"
                   " flags={} cookie={}",
                   conn, messenger.get_myaddrs(), conn.target_addr,
                   messenger.get_myname().num(), global_seq,
                   conn.policy.features_supported,
                   conn.policy.features_required | msgr2_required,
                   flags, client_cookie);
    return write_frame(client_ident);
  }).then([this] {
    return read_main_preamble();
  }).then([this] (Tag tag) {
    switch (tag) {
      case Tag::IDENT_MISSING_FEATURES:
        return read_frame_payload().then([this] {
          // handle_ident_missing_features() logic
          auto ident_missing = IdentMissingFeaturesFrame::Decode(rx_segments_data.back());
          logger().error("{} client does not support all server features: {}",
                         conn, ident_missing.features());
          abort_in_fault();
          // won't be executed
          return false;
        });
      case Tag::WAIT:
        return process_wait();
      case Tag::SERVER_IDENT:
        return read_frame_payload().then([this] {
          // handle_server_ident() logic
          auto server_ident = ServerIdentFrame::Decode(rx_segments_data.back());
          logger().debug("{} received server identification:"
                         " addrs={} gid={} global_seq={}"
                         " features_supported={} features_required={}"
                         " flags={} cookie={}",
                         conn,
                         server_ident.addrs(), server_ident.gid(),
                         server_ident.global_seq(),
                         server_ident.supported_features(),
                         server_ident.required_features(),
                         server_ident.flags(), server_ident.cookie());

          // is this who we intended to talk to?
          // be a bit forgiving here, since we may be connecting based on addresses parsed out
          // of mon_host or something.
          if (!server_ident.addrs().contains(conn.target_addr)) {
            logger().warn("{} peer identifies as {}, does not include {}",
                          conn, server_ident.addrs(), conn.target_addr);
            abort_in_fault();
          }

          server_cookie = server_ident.cookie();

          // TODO: change peer_addr to entity_addrvec_t
          ceph_assert(conn.peer_addr == server_ident.addrs().front());
          peer_name = entity_name_t(conn.get_peer_type(), server_ident.gid());
          conn.set_features(server_ident.supported_features() &
                            conn.policy.features_supported);
          peer_global_seq = server_ident.global_seq();

          // TODO: lossless policy
          ceph_assert(server_ident.flags() & CEPH_MSG_CONNECT_LOSSY);
          conn.policy.lossy = server_ident.flags() & CEPH_MSG_CONNECT_LOSSY;
          // TODO: backoff = utime_t();
          logger().debug("{} connect success {}, lossy={}, features={}",
                         conn, connect_seq, conn.policy.lossy, conn.features);

          return dispatcher.ms_handle_connect(
              seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()))
          .handle_exception([this] (std::exception_ptr eptr) {
            logger().error("{} ms_handle_connect caust exception: {}", conn, eptr);
          });
        }).then([this] {
          return true;
        });
      default: {
        unexpected_tag(tag, conn, "post_client_connect");
        // won't be executed
        return seastar::make_ready_future<bool>(false);
      }
    }
  });
}

seastar::future<bool> ProtocolV2::client_reconnect()
{
  // send_reconnect() logic
  auto reconnect = ReconnectFrame::Encode(messenger.get_myaddrs(),
                                          client_cookie,
                                          server_cookie,
                                          global_seq,
                                          connect_seq,
                                          conn.in_seq);
  logger().debug("{} reconnect to session: client_cookie={}"
                 " server_cookie={} gs={} cs={} ms={}",
                 conn, client_cookie, server_cookie,
                 global_seq, connect_seq, conn.in_seq);
  return write_frame(reconnect).then([this] {
    return read_main_preamble();
  }).then([this] (Tag tag) {
    switch (tag) {
      case Tag::SESSION_RETRY_GLOBAL:
        return read_frame_payload().then([this] {
          // handle_session_retry_global() logic
          auto retry = RetryGlobalFrame::Decode(rx_segments_data.back());
          global_seq = messenger.get_global_seq(retry.global_seq());
          logger().warn("{} received session retry global "
                        "global_seq={}, choose new gs={}",
                        conn, retry.global_seq(), global_seq);
          return client_reconnect();
        });
      case Tag::SESSION_RETRY:
        return read_frame_payload().then([this] {
          // handle_session_retry() logic
          auto retry = RetryFrame::Decode(rx_segments_data.back());
          connect_seq = retry.connect_seq() + 1;
          logger().warn("{} received session retry connect_seq={}, inc to cs={}",
                        conn, retry.connect_seq(), connect_seq);
          return client_reconnect();
        });
      case Tag::SESSION_RESET:
        return read_frame_payload().then([this] {
          // handle_session_reset() logic
          auto reset = ResetFrame::Decode(rx_segments_data.back());
          logger().warn("{} received session reset full={}", reset.full());
          reset_session(reset.full());
          return client_connect();
        });
      case Tag::WAIT:
        return process_wait();
      case Tag::SESSION_RECONNECT_OK:
        return read_frame_payload().then([this] {
          // handle_reconnect_ok() logic
          auto reconnect_ok = ReconnectOkFrame::Decode(rx_segments_data.back());
          logger().debug("{} received reconnect ok:"
                         "sms={}, lossy={}, features={}",
                         conn, reconnect_ok.msg_seq(),
                         connect_seq, conn.policy.lossy, conn.features);
          // TODO
          // discard_requeued_up_to()
          // backoff = utime_t();
          return dispatcher.ms_handle_connect(
              seastar::static_pointer_cast<SocketConnection>(
                conn.shared_from_this()))
          .handle_exception([this] (std::exception_ptr eptr) {
            logger().error("{} ms_handle_connect caust exception: {}", conn, eptr);
          });
        }).then([this] {
          return true;
        });
      default: {
        unexpected_tag(tag, conn, "post_client_reconnect");
        // won't be executed
        return seastar::make_ready_future<bool>(false);
      }
    }
  });
}

void ProtocolV2::execute_connecting()
{
  trigger_state(state_t::CONNECTING, write_state_t::delay, true);
  seastar::with_gate(pending_dispatch, [this] {
      enable_recording();
      global_seq = messenger.get_global_seq();
      return Socket::connect(conn.peer_addr)
        .then([this](SocketFRef sock) {
          socket = std::move(sock);
          if (state == state_t::CLOSING) {
            return socket->close().then([this] {
              logger().info("{} is closed during Socket::connect()", conn);
              abort_in_fault();
            });
          }
          return seastar::now();
        }).then([this] {
          return banner_exchange();
        }).then([this] (entity_type_t _peer_type,
                        entity_addr_t _peer_addr) {
          if (conn.peer_type != _peer_type) {
            logger().debug("{} connection peer type does not match what peer advertises {} != {}",
                           conn, conn.peer_type, (int)_peer_type);
            dispatch_reset();
            abort_in_close();
          }
          if (messenger.get_myaddrs().empty() ||
              messenger.get_myaddrs().front().is_blank_ip()) {
            logger().debug("peer {} says I am {}", conn.target_addr, _peer_addr);
            return messenger.learned_addr(_peer_addr);
          } else {
            return seastar::now();
          }
        }).then([this] {
          return client_auth();
        }).then([this] {
          if (!server_cookie) {
            ceph_assert(connect_seq == 0);
            return client_connect();
          } else {
            ceph_assert(connect_seq > 0);
            // TODO: lossless policy
            ceph_assert(false);
            return client_reconnect();
          }
        }).then([this] (bool proceed_or_wait) {
          if (proceed_or_wait) {
            execute_ready();
          } else {
            execute_wait();
          }
        }).handle_exception([this] (std::exception_ptr eptr) {
          // TODO: handle fault in CONNECTING state
          return fault();
        });
    });
}

// ACCEPTING state

seastar::future<> ProtocolV2::_auth_bad_method(int r)
{
  // _auth_bad_method() logic
  ceph_assert(r < 0);
  auto [allowed_methods, allowed_modes] =
      messenger.get_auth_server()->get_supported_auth_methods(conn.get_peer_type());
  logger().warn("{} send AuthBadMethod(auth_method={}, r={}, "
                "allowed_methods={}, allowed_modes={})",
                conn, auth_meta->auth_method, cpp_strerror(r),
                allowed_methods, allowed_modes);
  auto bad_method = AuthBadMethodFrame::Encode(
      auth_meta->auth_method, r, allowed_methods, allowed_modes);
  return write_frame(bad_method).then([this] {
    return server_auth();
  });
}

seastar::future<> ProtocolV2::_handle_auth_request(bufferlist& auth_payload, bool more)
{
  // _handle_auth_request() logic
  ceph_assert(messenger.get_auth_server());
  bufferlist reply;
  int r = messenger.get_auth_server()->handle_auth_request(
      conn.shared_from_this(), auth_meta,
      more, auth_meta->auth_method, auth_payload,
      &reply);
  switch (r) {
   // successful
   case 1: {
    auto auth_done = AuthDoneFrame::Encode(
        conn.peer_global_id, auth_meta->con_mode, reply);
    return write_frame(auth_done).then([this] {
      ceph_assert(auth_meta);
      // TODO
      ceph_assert(!auth_meta->is_mode_secure());
      session_stream_handlers = { nullptr, nullptr };
      return finish_auth();
    });
   }
   // auth more
   case 0: {
    auto more = AuthReplyMoreFrame::Encode(reply);
    return write_frame(more).then([this] {
      return read_main_preamble();
    }).then([this] (Tag tag) {
      expect_tag(Tag::AUTH_REQUEST_MORE, tag, conn, __func__);
      return read_frame_payload();
    }).then([this] {
      auto auth_more = AuthRequestMoreFrame::Decode(rx_segments_data.back());
      return _handle_auth_request(auth_more.auth_payload(), true);
    });
   }
   case -EBUSY: {
    logger().warn("{} auth_server handle_auth_request returned -EBUSY", conn);
    return abort_in_fault();
   }
   default: {
    logger().warn("{} auth_server handle_auth_request returned {}", conn, r);
    return _auth_bad_method(r);
   }
  }
}

seastar::future<> ProtocolV2::server_auth()
{
  return read_main_preamble()
  .then([this] (Tag tag) {
    expect_tag(Tag::AUTH_REQUEST, tag, conn, __func__);
    return read_frame_payload();
  }).then([this] {
    // handle_auth_request() logic
    auto request = AuthRequestFrame::Decode(rx_segments_data.back());
    logger().debug("{} got AuthRequest(method={}, preferred_modes={}, payload_len={})",
                   conn, request.method(), request.preferred_modes(),
                   request.auth_payload().length());
    auth_meta->auth_method = request.method();
    auth_meta->con_mode = messenger.get_auth_server()->pick_con_mode(
        conn.get_peer_type(), auth_meta->auth_method,
        request.preferred_modes());
    if (auth_meta->con_mode == CEPH_CON_MODE_UNKNOWN) {
      logger().warn("{} auth_server pick_con_mode returned mode CEPH_CON_MODE_UNKNOWN", conn);
      return _auth_bad_method(-EOPNOTSUPP);
    }
    return _handle_auth_request(request.auth_payload(), false);
  });
}

seastar::future<bool> ProtocolV2::send_wait()
{
  auto wait = WaitFrame::Encode();
  return write_frame(wait).then([this] {
    return false;
  });
}

seastar::future<bool> ProtocolV2::handle_existing_connection(SocketConnectionRef existing)
{
  // handle_existing_connection() logic
  logger().debug("{} {}: {}", conn, __func__, *existing);

  ProtocolV2 *exproto = dynamic_cast<ProtocolV2*>(existing->protocol.get());
  ceph_assert(exproto);

  if (exproto->state == state_t::CLOSING) {
    logger().warn("{} existing {} already closed.", conn, *existing);
    return send_server_ident().then([this] {
      return true;
    });
  }

  if (exproto->state == state_t::REPLACING) {
    logger().warn("{} existing racing replace happened while replacing: {}",
                  conn, *existing);
    return send_wait();
  }

  if (exproto->peer_global_seq > peer_global_seq) {
    logger().warn("{} this is a stale connection, peer_global_seq={}"
                  "existing->peer_global_seq={} close this connection",
                  conn, peer_global_seq, exproto->peer_global_seq);
    dispatch_reset();
    abort_in_close();
  }

  if (existing->policy.lossy) {
    // existing connection can be thrown out in favor of this one
    logger().warn("{} existing={} is a lossy channel. Close existing in favor of"
                  " this connection", conn, *existing);
    exproto->dispatch_reset();
    exproto->close();
    return send_server_ident().then([this] {
      return true;
    });
  }

  // TODO: lossless policy
  ceph_assert(false);
}

seastar::future<bool> ProtocolV2::server_connect()
{
  return read_frame_payload().then([this] {
    // handle_client_ident() logic
    auto client_ident = ClientIdentFrame::Decode(rx_segments_data.back());
    logger().debug("{} received client identification: addrs={} target={}"
                   " gid={} global_seq={} features_supported={}"
                   " features_required={} flags={} cookie={}",
                   conn, client_ident.addrs(), client_ident.target_addr(),
                   client_ident.gid(), client_ident.global_seq(),
                   client_ident.supported_features(),
                   client_ident.required_features(),
                   client_ident.flags(), client_ident.cookie());

    if (client_ident.addrs().empty() ||
        client_ident.addrs().front() == entity_addr_t()) {
      logger().error("{} oops, client_ident.addrs() is empty", conn);
      abort_in_fault();
    }
    if (!messenger.get_myaddrs().contains(client_ident.target_addr())) {
      logger().error("{} peer is trying to reach {} which is not us ({})",
                     conn, client_ident.target_addr(), messenger.get_myaddrs());
      abort_in_fault();
    }
    // TODO: change peer_addr to entity_addrvec_t
    entity_addr_t paddr = client_ident.addrs().front();
    conn.peer_addr = conn.target_addr;
    conn.peer_addr.set_type(paddr.get_type());
    conn.peer_addr.set_port(paddr.get_port());
    conn.peer_addr.set_nonce(paddr.get_nonce());
    logger().debug("{} got paddr={}, conn.peer_addr={}", conn, paddr, conn.peer_addr);
    conn.target_addr = conn.peer_addr;

    peer_name = entity_name_t(conn.get_peer_type(), client_ident.gid());
    conn.peer_id = client_ident.gid();
    client_cookie = client_ident.cookie();

    uint64_t feat_missing =
      (conn.policy.features_required | msgr2_required) &
      ~(uint64_t)client_ident.supported_features();
    if (feat_missing) {
      logger().warn("{} peer missing required features {}", conn, feat_missing);
      auto ident_missing_features = IdentMissingFeaturesFrame::Encode(feat_missing);
      return write_frame(ident_missing_features).then([this] {
        return false;
      });
    }
    connection_features =
        client_ident.supported_features() & conn.policy.features_supported;

    peer_global_seq = client_ident.global_seq();

    // Looks good so far, let's check if there is already an existing connection
    // to this peer.

    SocketConnectionRef existing = messenger.lookup_conn(conn.peer_addr);

    if (existing) {
      if (existing->protocol->proto_type != proto_t::v2) {
        logger().warn("{} existing {} proto version is {}, close",
                      conn, *existing,
                      static_cast<int>(existing->protocol->proto_type));
        // should unregister the existing from msgr atomically
        existing->close();
      } else {
        return handle_existing_connection(existing);
      }
    }

    // if everything is OK reply with server identification
    return send_server_ident().then([this] {
      // goto ready
      return true;
    });
  });
}

seastar::future<bool> ProtocolV2::read_reconnect()
{
  return read_main_preamble()
  .then([this] (Tag tag) {
    expect_tag(Tag::SESSION_RECONNECT, tag, conn, "read_reconnect");
    return server_reconnect();
  });
}

seastar::future<bool> ProtocolV2::send_retry(uint64_t connect_seq)
{
  auto retry = RetryFrame::Encode(connect_seq);
  return write_frame(retry).then([this] {
    return read_reconnect();
  });
}

seastar::future<bool> ProtocolV2::send_retry_global(uint64_t global_seq)
{
  auto retry = RetryGlobalFrame::Encode(global_seq);
  return write_frame(retry).then([this] {
    return read_reconnect();
  });
}

seastar::future<bool> ProtocolV2::send_reset(bool full)
{
  auto reset = ResetFrame::Encode(full);
  return write_frame(reset).then([this] {
    return read_main_preamble();
  }).then([this] (Tag tag) {
    expect_tag(Tag::CLIENT_IDENT, tag, conn, "post_send_reset");
    return server_connect();
  });
}

seastar::future<bool> ProtocolV2::server_reconnect()
{
  return read_frame_payload().then([this] {
    // handle_reconnect() logic
    auto reconnect = ReconnectFrame::Decode(rx_segments_data.back());

    logger().debug("{} received reconnect: client_cookie={} server_cookie={}"
                   " gs={} cs={} ms={}",
                   conn, reconnect.client_cookie(), reconnect.server_cookie(),
                   reconnect.global_seq(), reconnect.connect_seq(),
                   reconnect.msg_seq());

    // can peer_addrs be changed on-the-fly?
    if (conn.peer_addr != reconnect.addrs().front()) {
      logger().error("{} peer identifies as {}, while conn.peer_addr={}",
                     conn, reconnect.addrs().front(), conn.peer_addr);
      ceph_assert(false);
    }
    // TODO: change peer_addr to entity_addrvec_t
    ceph_assert(conn.peer_addr == conn.target_addr);
    peer_global_seq = reconnect.global_seq();

    SocketConnectionRef existing = messenger.lookup_conn(conn.peer_addr);

    if (!existing) {
      // there is no existing connection therefore cannot reconnect to previous
      // session
      logger().error("{} server_reconnect: no existing connection,"
                     " reseting client", conn);
      return send_reset(true);
    }

    if (existing->protocol->proto_type != proto_t::v2) {
      logger().warn("{} server_reconnect: existing {} proto version is {},"
                    "close existing and resetting client.",
                    conn, *existing,
                    static_cast<int>(existing->protocol->proto_type));
      existing->close();
      return send_reset(true);
    }

    ProtocolV2 *exproto = dynamic_cast<ProtocolV2*>(existing->protocol.get());
    ceph_assert(exproto);

    if (exproto->state == state_t::REPLACING) {
      logger().warn("{} server_reconnect: existing racing replace happened while "
                    " replacing, retry_global. existing={}", conn, *existing);
      return send_retry_global(exproto->peer_global_seq);
    }

    if (exproto->client_cookie != reconnect.client_cookie()) {
      logger().warn("{} server_reconnect: existing={} client cookie mismatch,"
                    " I must have reseted: cc={} rcc={}, reseting client.",
                    conn, *existing, exproto->client_cookie, reconnect.client_cookie());
      return send_reset(conn.policy.resetcheck);
    } else if (exproto->server_cookie == 0) {
      // this happens when:
      //   - a connects to b
      //   - a sends client_ident
      //   - b gets client_ident, sends server_ident and sets cookie X
      //   - connection fault
      //   - b reconnects to a with cookie X, connect_seq=1
      //   - a has cookie==0
      logger().warn("{} server_reconnect: I was a client and didn't received the"
                    " server_ident. Asking peer to resume session establishment",
                    conn);
      return send_reset(false);
    }

    if (exproto->peer_global_seq > reconnect.global_seq()) {
      logger().debug("{} server_reconnect: stale global_seq: sgs={} cgs={},"
                     " ask client to retry global",
                     conn, exproto->peer_global_seq, reconnect.global_seq());
      return send_retry_global(exproto->peer_global_seq);
    }

    if (exproto->connect_seq > reconnect.connect_seq()) {
      logger().debug("{} server_reconnect: stale connect_seq scs={} ccs={},"
                     " ask client to retry",
                     conn, exproto->connect_seq, reconnect.connect_seq());
      return send_retry(exproto->connect_seq);
    }

    if (exproto->connect_seq == reconnect.connect_seq()) {
      // reconnect race: both peers are sending reconnect messages
      if (existing->peer_addr > messenger.get_myaddrs().msgr2_addr() &&
          !existing->policy.server) {
        // the existing connection wins
        logger().warn("{} server_reconnect: reconnect race detected,"
                      " this connection loses to existing={},"
                      " ask client to wait", conn, *existing);
        return send_wait();
      } else {
        // this connection wins
        logger().warn("{} server_reconnect: reconnect race detected,"
                      " replacing existing={} socket by this connection's socket",
                      conn, *existing);
      }
    }

    logger().warn("{} server_reconnect: reconnect to exsiting={}", conn, *existing);

    // everything looks good
    exproto->connect_seq = reconnect.connect_seq();
    //exproto->message_seq = reconnect.msg_seq();

    // TODO: lossless policy
    // return reuse_connection(existing, exproto);
    ceph_assert(false);
  });
}

void ProtocolV2::execute_accepting()
{
  trigger_state(state_t::ACCEPTING, write_state_t::none, false);
  seastar::with_gate(pending_dispatch, [this] {
      enable_recording();
      return banner_exchange()
        .then([this] (entity_type_t _peer_type,
                      entity_addr_t _peer_addr) {
          ceph_assert(conn.get_peer_type() == -1);
          conn.peer_type = _peer_type;

          conn.policy = messenger.get_policy(_peer_type);
          logger().debug("{} accept of host type {}, lossy={} server={} standby={} resetcheck={}",
                         conn, (int)_peer_type,
                         conn.policy.lossy, conn.policy.server,
                         conn.policy.standby, conn.policy.resetcheck);
          return server_auth();
        }).then([this] {
          return read_main_preamble();
        }).then([this] (Tag tag) {
          switch (tag) {
            case Tag::CLIENT_IDENT:
              return server_connect();
            case Tag::SESSION_RECONNECT:
              // TODO: lossless policy
              ceph_assert(false);
              return server_reconnect();
            default: {
              unexpected_tag(tag, conn, "post_server_auth");
              // won't be executed
              return seastar::make_ready_future<bool>(false);
            }
          }
        }).then([this] (bool proceed_or_wait) {
          if (proceed_or_wait) {
            messenger.register_conn(
              seastar::static_pointer_cast<SocketConnection>(
                conn.shared_from_this()));
            messenger.unaccept_conn(
              seastar::static_pointer_cast<SocketConnection>(
                conn.shared_from_this()));
            execute_ready();
          } else {
            execute_server_wait();
          }
        }).handle_exception([this] (std::exception_ptr eptr) {
          // TODO: handle fault in ACCEPTING state
          return fault();
        });
    });
}

// CONNECTING or ACCEPTING state

seastar::future<> ProtocolV2::finish_auth()
{
  ceph_assert(auth_meta);

  const auto sig = auth_meta->session_key.empty() ? sha256_digest_t() :
    auth_meta->session_key.hmac_sha256(nullptr, rxbuf);
  auto sig_frame = AuthSignatureFrame::Encode(sig);
  ceph_assert(record_io);
  record_io = false;
  rxbuf.clear();
  return write_frame(sig_frame).then([this] {
    return read_main_preamble();
  }).then([this] (Tag tag) {
    expect_tag(Tag::AUTH_SIGNATURE, tag, conn, "post_finish_auth");
    return read_frame_payload();
  }).then([this] {
    // handle_auth_signature() logic
    auto sig_frame = AuthSignatureFrame::Decode(rx_segments_data.back());

    const auto actual_tx_sig = auth_meta->session_key.empty() ?
      sha256_digest_t() : auth_meta->session_key.hmac_sha256(nullptr, txbuf);
    if (sig_frame.signature() != actual_tx_sig) {
      logger().warn("{} pre-auth signature mismatch actual_tx_sig={}"
                    " sig_frame.signature()={}",
                    conn, actual_tx_sig, sig_frame.signature());
      abort_in_fault();
    }
    logger().debug("{} pre-auth signature success sig_frame.signature()={}",
                   conn, sig_frame.signature());
    txbuf.clear();
  });
}

// ACCEPTING or REPLACING state

seastar::future<> ProtocolV2::send_server_ident()
{
  // send_server_ident() logic

  // this is required for the case when this connection is being replaced
  // TODO
  // out_seq = discard_requeued_up_to(out_seq, 0);
  conn.in_seq = 0;

  if (!conn.policy.lossy) {
    server_cookie = ceph::util::generate_random_number<uint64_t>(1, -1ll);
  }

  uint64_t flags = 0;
  if (conn.policy.lossy) {
    flags = flags | CEPH_MSG_CONNECT_LOSSY;
  }

  // refered to async-conn v2: not assign gs to global_seq
  uint64_t gs = messenger.get_global_seq();
  auto server_ident = ServerIdentFrame::Encode(
          messenger.get_myaddrs(),
          messenger.get_myname().num(),
          gs,
          conn.policy.features_supported,
          conn.policy.features_required | msgr2_required,
          flags,
          server_cookie);

  logger().debug("{} sending server identification: addrs={} gid={}"
                 " global_seq={} features_supported={} features_required={}"
                 " flags={} cookie={}",
                 conn, messenger.get_myaddrs(), messenger.get_myname().num(),
                 gs, conn.policy.features_supported,
                 conn.policy.features_required | msgr2_required,
                 flags, server_cookie);

  conn.set_features(connection_features);

  // notify
  seastar::with_gate(pending_dispatch, [this] {
    return dispatcher.ms_handle_accept(
        seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()))
    .handle_exception([this] (std::exception_ptr eptr) {
      logger().error("{} ms_handle_accept caust exception: {}", conn, eptr);
    });
  });

  return write_frame(server_ident);
}

// REPLACING state

seastar::future<> ProtocolV2::send_reconnect_ok()
{
  // send_reconnect_ok() logic
  // <prepare and send ReconnectOKFrame>

  return seastar::now();
}

// READY state

ceph::bufferlist ProtocolV2::do_sweep_messages(
    const std::deque<MessageRef>& msgs,
    size_t num_msgs,
    bool require_keepalive,
    std::optional<utime_t> _keepalive_ack)
{
  ceph::bufferlist bl;

  if (unlikely(require_keepalive)) {
    auto keepalive_frame = KeepAliveFrame::Encode();
    bl.append(keepalive_frame.get_buffer(session_stream_handlers));
  }

  if (unlikely(_keepalive_ack.has_value())) {
    auto keepalive_ack_frame = KeepAliveFrameAck::Encode(*_keepalive_ack);
    bl.append(keepalive_ack_frame.get_buffer(session_stream_handlers));
  }

  std::for_each(msgs.begin(), msgs.begin()+num_msgs, [this, &bl](const MessageRef& msg) {
    // TODO: move to common code
    // set priority
    msg->get_header().src = messenger.get_myname();

    msg->encode(conn.features, 0);

    msg->set_seq(++conn.out_seq);
    uint64_t ack_seq = conn.in_seq;
    // ack_left = 0;

    ceph_msg_header &header = msg->get_header();
    ceph_msg_footer &footer = msg->get_footer();

    ceph_msg_header2 header2{header.seq,        header.tid,
                             header.type,       header.priority,
                             header.version,
                             0,                 header.data_off,
                             ack_seq,
                             footer.flags,      header.compat_version,
                             header.reserved};

    auto message = MessageFrame::Encode(header2,
        msg->get_payload(), msg->get_middle(), msg->get_data());
    logger().debug("{} write msg type={} off={} seq={}",
                   conn, header2.type, header2.data_off, header2.seq);
    bl.append(message.get_buffer(session_stream_handlers));
  });

  return bl;
}

void ProtocolV2::handle_message_ack(seq_num_t seq) {
  if (conn.policy.lossy) {  // lossy connections don't keep sent messages
    return;
  }

  // TODO: lossless policy
}

seastar::future<> ProtocolV2::read_message(utime_t throttle_stamp)
{
  return read_frame_payload()
  .then([this, throttle_stamp] {
    utime_t recv_stamp{seastar::lowres_system_clock::now()};

    // we need to get the size before std::moving segments data
    const size_t cur_msg_size = get_current_msg_size();
    auto msg_frame = MessageFrame::Decode(std::move(rx_segments_data));
    // XXX: paranoid copy just to avoid oops
    ceph_msg_header2 current_header = msg_frame.header();

    logger().debug("{} got {} + {} + {} byte message,"
                   " envelope type={} src={} off={} seq={}",
                   conn, msg_frame.front_len(), msg_frame.middle_len(),
                   msg_frame.data_len(), current_header.type, peer_name,
                   current_header.data_off, current_header.seq);

    ceph_msg_header header{current_header.seq,
                           current_header.tid,
                           current_header.type,
                           current_header.priority,
                           current_header.version,
                           msg_frame.front_len(),
                           msg_frame.middle_len(),
                           msg_frame.data_len(),
                           current_header.data_off,
                           peer_name,
                           current_header.compat_version,
                           current_header.reserved,
                           0};
    ceph_msg_footer footer{0, 0, 0, 0, current_header.flags};

    Message *message = decode_message(nullptr, 0, header, footer,
        msg_frame.front(), msg_frame.middle(), msg_frame.data(), nullptr);
    if (!message) {
      logger().warn("{} decode message failed", conn);
      abort_in_fault();
    }

    // store reservation size in message, so we don't get confused
    // by messages entering the dispatch queue through other paths.
    message->set_dispatch_throttle_size(cur_msg_size);

    message->set_throttle_stamp(throttle_stamp);
    message->set_recv_stamp(recv_stamp);
    message->set_recv_complete_stamp(utime_t{seastar::lowres_system_clock::now()});

    // check received seq#.  if it is old, drop the message.
    // note that incoming messages may skip ahead.  this is convenient for the
    // client side queueing because messages can't be renumbered, but the (kernel)
    // client will occasionally pull a message out of the sent queue to send
    // elsewhere.  in that case it doesn't matter if we "got" it or not.
    uint64_t cur_seq = conn.in_seq;
    if (message->get_seq() <= cur_seq) {
      logger().error("{} got old message {} <= {} {} {}, discarding",
                     conn, message->get_seq(), cur_seq, message, *message);
      if (HAVE_FEATURE(conn.features, RECONNECT_SEQ) &&
          conf.ms_die_on_old_message) {
        ceph_assert(0 == "old msgs despite reconnect_seq feature");
      }
      return;
    } else if (message->get_seq() > cur_seq + 1) {
      logger().error("{} missed message? skipped from seq {} to {}",
                     conn, cur_seq, message->get_seq());
      if (conf.ms_die_on_skipped_message) {
        ceph_assert(0 == "skipped incoming seq");
      }
    }

    // note last received message.
    conn.in_seq = message->get_seq();
    logger().debug("{} received message m={} seq={} from={} type={} {}",
                   conn, message, message->get_seq(), message->get_source(),
                   header.type, *message);

    if (!conn.policy.lossy) {
      // ++ack_left;
    }
    handle_message_ack(current_header.ack_seq);

    // TODO: change MessageRef with seastar::shared_ptr
    auto msg_ref = MessageRef{message, false};
    seastar::with_gate(pending_dispatch, [this, msg = std::move(msg_ref)] {
      return dispatcher.ms_dispatch(&conn, std::move(msg))
	.handle_exception([this] (std::exception_ptr eptr) {
        logger().error("{} ms_dispatch caught exception: {}", conn, eptr);
        ceph_assert(false);
      });
    });
  });
}

void ProtocolV2::execute_ready()
{
  trigger_state(state_t::READY, write_state_t::open, false);
  seastar::with_gate(pending_dispatch, [this] {
    return seastar::keep_doing([this] {
      return read_main_preamble()
      .then([this] (Tag tag) {
        switch (tag) {
          case Tag::MESSAGE: {
            return seastar::futurize_apply([this] {
              // throttle_message() logic
              if (!conn.policy.throttler_messages) {
                return seastar::now();
              }
              // TODO: message throttler
              ceph_assert(false);
              return seastar::now();
            }).then([this] {
              // throttle_bytes() logic
              if (!conn.policy.throttler_bytes) {
                return seastar::now();
              }
              size_t cur_msg_size = get_current_msg_size();
              if (!cur_msg_size) {
                return seastar::now();
              }
              logger().debug("{} wants {} bytes from policy throttler {}/{}",
                             conn, cur_msg_size,
                             conn.policy.throttler_bytes->get_current(),
                             conn.policy.throttler_bytes->get_max());
              return conn.policy.throttler_bytes->get(cur_msg_size);
            }).then([this] {
              // TODO: throttle_dispatch_queue() logic
              utime_t throttle_stamp{seastar::lowres_system_clock::now()};
              return read_message(throttle_stamp);
            });
          }
          case Tag::ACK:
            return read_frame_payload().then([this] {
              // handle_message_ack() logic
              auto ack = AckFrame::Decode(rx_segments_data.back());
              handle_message_ack(ack.seq());
            });
          case Tag::KEEPALIVE2:
            return read_frame_payload().then([this] {
              // handle_keepalive2() logic
              auto keepalive_frame = KeepAliveFrame::Decode(rx_segments_data.back());
              notify_keepalive_ack(keepalive_frame.timestamp());
              conn.set_last_keepalive(seastar::lowres_system_clock::now());
            });
          case Tag::KEEPALIVE2_ACK:
            return read_frame_payload().then([this] {
              // handle_keepalive2_ack() logic
              auto keepalive_ack_frame = KeepAliveFrameAck::Decode(rx_segments_data.back());
              conn.set_last_keepalive_ack(
                seastar::lowres_system_clock::time_point{keepalive_ack_frame.timestamp()});
              logger().debug("{} got KEEPALIVE_ACK {}",
                             conn, conn.last_keepalive_ack);
            });
          default:
            return unexpected_tag(tag, conn, "execute_ready");
        }
      });
    }).handle_exception([this] (std::exception_ptr eptr) {
      // TODO: handle fault in READY state
      return fault();
    });
  });
}

// STANDBY state

void ProtocolV2::execute_standby()
{
  // TODO not implemented
  // trigger_state(state_t::STANDBY, write_state_t::delay, false);
  ceph_assert(false);
}

// WAIT state

void ProtocolV2::execute_wait()
{
  // TODO not implemented
  // trigger_state(state_t::WAIT, write_state_t::delay, false);
  ceph_assert(false);
}

// SERVER_WAIT state

void ProtocolV2::execute_server_wait()
{
  // TODO not implemented
  // trigger_state(state_t::SERVER_WAIT, write_state_t::delay, false);
  ceph_assert(false);
}

// CLOSING state

void ProtocolV2::trigger_close()
{
  if (state == state_t::ACCEPTING) {
    messenger.unaccept_conn(
      seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
  } else if (state >= state_t::CONNECTING && state < state_t::CLOSING) {
    messenger.unregister_conn(
      seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
  } else {
    // cannot happen
    ceph_assert(false);
  }

  if (!socket) {
    ceph_assert(state == state_t::CONNECTING);
  }

  trigger_state(state_t::CLOSING, write_state_t::drop, false);
}

} // namespace ceph::net
