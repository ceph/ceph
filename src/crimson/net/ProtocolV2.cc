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

// TODO: apply the same logging policy to Protocol V1
// Log levels in V2 Protocol:
// * error level, something error that cause connection to terminate:
//   - fatal errors;
//   - bugs;
// * warn level: something unusual that identifies connection fault or replacement:
//   - unstable network;
//   - incompatible peer;
//   - auth failure;
//   - connection race;
//   - connection reset;
// * info level, something very important to show connection lifecycle,
//   which doesn't happen very frequently;
// * debug level, important logs for debugging, including:
//   - all the messages sent/received (-->/<==);
//   - all the frames exchanged (WRITE/GOT);
//   - important fields updated (UPDATE);
//   - connection state transitions (TRIGGER);
// * trace level, trivial logs showing:
//   - the exact bytes being sent/received (SEND/RECV(bytes));
//   - detailed information of sub-frames;
//   - integrity checks;
//   - etc.
seastar::logger& logger() {
  return ceph::get_logger(ceph_subsys_ms);
}

void abort_in_fault() {
  throw std::system_error(make_error_code(ceph::net::error::negotiation_failure));
}

void abort_protocol() {
  throw std::system_error(make_error_code(ceph::net::error::protocol_aborted));
}

void abort_in_close(ceph::net::ProtocolV2& proto) {
  proto.close();
  abort_protocol();
}

inline void expect_tag(const Tag& expected,
                       const Tag& actual,
                       ceph::net::SocketConnection& conn,
                       const char *where) {
  if (actual != expected) {
    logger().warn("{} {} received wrong tag: {}, expected {}",
                  conn, where,
                  static_cast<uint32_t>(actual),
                  static_cast<uint32_t>(expected));
    abort_in_fault();
  }
}

inline void unexpected_tag(const Tag& unexpected,
                           ceph::net::SocketConnection& conn,
                           const char *where) {
  logger().warn("{} {} received unexpected tag: {}",
                conn, where, static_cast<uint32_t>(unexpected));
  abort_in_fault();
}

inline uint64_t generate_client_cookie() {
  return ceph::util::generate_random_number<uint64_t>(
      1, std::numeric_limits<uint64_t>::max());
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
  conn.set_peer_type(_peer_type);
  conn.policy = messenger.get_policy(_peer_type);
  logger().info("{} ProtocolV2::start_connect(): peer_addr={}, peer_type={},"
                " policy(lossy={}, server={}, standby={}, resetcheck={})",
                conn, _peer_addr, ceph_entity_type_name(_peer_type), conn.policy.lossy,
                conn.policy.server, conn.policy.standby, conn.policy.resetcheck);
  messenger.register_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  execute_connecting();
}

void ProtocolV2::start_accept(SocketFRef&& sock,
                              const entity_addr_t& _peer_addr)
{
  ceph_assert(state == state_t::NONE);
  ceph_assert(!socket);
  // until we know better
  conn.target_addr = _peer_addr;
  conn.set_ephemeral_port(_peer_addr.get_port(),
                          SocketConnection::side_t::acceptor);
  socket = std::move(sock);
  logger().info("{} ProtocolV2::start_accept(): target_addr={}", conn, _peer_addr);
  messenger.accept_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  execute_accepting();
}

// TODO: Frame related implementations, probably to a separate class.

void ProtocolV2::enable_recording()
{
  rxbuf.clear();
  txbuf.clear();
  record_io = true;
}

seastar::future<Socket::tmp_buf> ProtocolV2::read_exactly(size_t bytes)
{
  if (unlikely(record_io)) {
    return socket->read_exactly(bytes)
    .then([this] (auto bl) {
      rxbuf.append(buffer::create(bl.share()));
      return bl;
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
      return buf;
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
      logger().trace("{} RECV({}) main preamble: tag={}, num_segments={}, crc={}",
                     conn, bl.size(), (int)main_preamble.tag,
                     (int)main_preamble.num_segments, main_preamble.crc);

      // verify preamble's CRC before any further processing
      const auto rx_crc = ceph_crc32c(0,
        reinterpret_cast<const unsigned char*>(&main_preamble),
        sizeof(main_preamble) - sizeof(main_preamble.crc));
      if (rx_crc != main_preamble.crc) {
        logger().warn("{} crc mismatch for main preamble rx_crc={} tx_crc={}",
                      conn, rx_crc, main_preamble.crc);
        abort_in_fault();
      }

      // currently we do support between 1 and MAX_NUM_SEGMENTS segments
      if (main_preamble.num_segments < 1 ||
          main_preamble.num_segments > MAX_NUM_SEGMENTS) {
        logger().warn("{} unsupported num_segments={}",
                      conn, main_preamble.num_segments);
        abort_in_fault();
      }
      if (main_preamble.num_segments > MAX_NUM_SEGMENTS) {
        logger().warn("{} num_segments too much: {}",
                      conn, main_preamble.num_segments);
        abort_in_fault();
      }

      rx_segments_desc.clear();
      rx_segments_data.clear();

      for (std::uint8_t idx = 0; idx < main_preamble.num_segments; idx++) {
        logger().trace("{} GOT frame segment: len={} align={}",
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
        logger().trace("{} cannot allocate {} aligned buffer at segment desc index {}",
                       conn, cur_rx_desc.alignment, rx_segments_data.size());
      }
      // TODO: create aligned and contiguous buffer from socket
      return read_exactly(cur_rx_desc.length)
      .then([this] (auto tmp_bl) {
        logger().trace("{} RECV({}) frame segment[{}]",
                       conn, tmp_bl.size(), rx_segments_data.size());
        bufferlist data;
        data.append(buffer::create(std::move(tmp_bl)));
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
    logger().trace("{} RECV({}) frame epilogue", conn, bl.size());

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
          logger().warn("{} message integrity check failed at index {}:"
                        " expected_crc={} calculated_crc={}",
                        conn, (unsigned int)idx, expected_crc, calculated_crc);
          abort_in_fault();
        } else {
          logger().trace("{} message integrity check success at index {}: crc={}",
                         conn, (unsigned int)idx, expected_crc);
        }
      }
      late_flags = epilogue.late_flags;
    }
    logger().trace("{} GOT frame epilogue: late_flags={}",
                   conn, (unsigned)late_flags);

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
  const auto main_preamble = reinterpret_cast<const preamble_block_t*>(bl.front().c_str());
  logger().trace("{} SEND({}) frame: tag={}, num_segments={}, crc={}",
                 conn, bl.length(), (int)main_preamble->tag,
                 (int)main_preamble->num_segments, main_preamble->crc);
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
  logger().debug("{} TRIGGER {}, was {}",
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
      logger().error("{} ms_handle_reset caught exception: {}", conn, eptr);
      ceph_abort("unexpected exception from ms_handle_reset()");
    });
  });
}

void ProtocolV2::reset_session(bool full)
{
  server_cookie = 0;
  connect_seq = 0;
  conn.in_seq = 0;
  if (full) {
    client_cookie = generate_client_cookie();
    peer_global_seq = 0;
    conn.out_seq = 0;
    // TODO:
    // discard_out_queue();
    // message_seq = 0;
    // ack_left = 0;
    seastar::with_gate(pending_dispatch, [this] {
      return dispatcher.ms_handle_remote_reset(
          seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()))
      .handle_exception([this] (std::exception_ptr eptr) {
        logger().error("{} ms_handle_remote_reset caught exception: {}", conn, eptr);
        ceph_abort("unexpected exception from ms_handle_remote_reset()");
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
  auto len_payload = static_cast<uint16_t>(banner_payload.length());
  encode(len_payload, bl, 0);
  bl.claim_append(banner_payload);
  logger().debug("{} SEND({}) banner: len_payload={}, supported={}, "
                 "required={}, banner=\"{}\"",
                 conn, bl.length(), len_payload,
                 CEPH_MSGR2_SUPPORTED_FEATURES, CEPH_MSGR2_REQUIRED_FEATURES,
                 CEPH_BANNER_V2_PREFIX);
  return write_flush(std::move(bl)).then([this] {
      // 2. read peer banner
      unsigned banner_len = strlen(CEPH_BANNER_V2_PREFIX) + sizeof(__le16);
      return read_exactly(banner_len); // or read exactly?
    }).then([this] (auto bl) {
      // 3. process peer banner and read banner_payload
      unsigned banner_prefix_len = strlen(CEPH_BANNER_V2_PREFIX);
      logger().debug("{} RECV({}) banner: \"{}\"",
                     conn, bl.size(),
                     std::string((const char*)bl.get(), banner_prefix_len));

      if (memcmp(bl.get(), CEPH_BANNER_V2_PREFIX, banner_prefix_len) != 0) {
        if (memcmp(bl.get(), CEPH_BANNER, strlen(CEPH_BANNER)) == 0) {
          logger().warn("{} peer is using V1 protocol", conn);
        } else {
          logger().warn("{} peer sent bad banner", conn);
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
        logger().warn("{} decode banner payload len failed", conn);
        abort_in_fault();
      }
      logger().debug("{} GOT banner: payload_len={}", conn, payload_len);
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
        logger().warn("{} decode banner payload failed", conn);
        abort_in_fault();
      }
      logger().debug("{} RECV({}) banner features: supported={} required={}",
                     conn, bl.length(),
                     peer_supported_features, peer_required_features);

      // Check feature bit compatibility
      uint64_t supported_features = CEPH_MSGR2_SUPPORTED_FEATURES;
      uint64_t required_features = CEPH_MSGR2_REQUIRED_FEATURES;
      if ((required_features & peer_supported_features) != required_features) {
        logger().error("{} peer does not support all required features"
                       " required={} peer_supported={}",
                       conn, required_features, peer_supported_features);
        abort_in_close(*this);
      }
      if ((supported_features & peer_required_features) != peer_required_features) {
        logger().error("{} we do not support all peer required features"
                       " peer_required={} supported={}",
                       conn, peer_required_features, supported_features);
        abort_in_close(*this);
      }
      this->peer_required_features = peer_required_features;
      if (this->peer_required_features == 0) {
        this->connection_features = msgr2_required;
      }

      auto hello = HelloFrame::Encode(messenger.get_mytype(),
                                      conn.target_addr);
      logger().debug("{} WRITE HelloFrame: my_type={}, peer_addr={}",
                     conn, ceph_entity_type_name(messenger.get_mytype()),
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
      logger().debug("{} GOT HelloFrame: my_type={} peer_addr={}",
                     conn, ceph_entity_type_name(hello.entity_type()),
                     hello.peer_addr());
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
          logger().warn("{} GOT AuthBadMethodFrame: method={} result={}, "
                        "allowed_methods={}, allowed_modes={}",
                        conn, bad_method.method(), cpp_strerror(bad_method.result()),
                        bad_method.allowed_methods(), bad_method.allowed_modes());
          ceph_assert(messenger.get_auth_client());
          int r = messenger.get_auth_client()->handle_auth_bad_method(
              conn.shared_from_this(), auth_meta,
              bad_method.method(), bad_method.result(),
              bad_method.allowed_methods(), bad_method.allowed_modes());
          if (r < 0) {
            logger().warn("{} auth_client handle_auth_bad_method returned {}",
                          conn, r);
            abort_in_fault();
          }
          return client_auth(bad_method.allowed_methods());
        });
      case Tag::AUTH_REPLY_MORE:
        return read_frame_payload().then([this] {
          // handle_auth_reply_more() logic
          auto auth_more = AuthReplyMoreFrame::Decode(rx_segments_data.back());
          logger().debug("{} GOT AuthReplyMoreFrame: payload_len={}",
                         conn, auth_more.auth_payload().length());
          ceph_assert(messenger.get_auth_client());
          // let execute_connecting() take care of the thrown exception
          auto reply = messenger.get_auth_client()->handle_auth_reply_more(
            conn.shared_from_this(), auth_meta, auth_more.auth_payload());
          auto more_reply = AuthRequestMoreFrame::Encode(reply);
          logger().debug("{} WRITE AuthRequestMoreFrame: payload_len={}",
                         conn, reply.length());
          return write_frame(more_reply);
        }).then([this] {
          return handle_auth_reply();
        });
      case Tag::AUTH_DONE:
        return read_frame_payload().then([this] {
          // handle_auth_done() logic
          auto auth_done = AuthDoneFrame::Decode(rx_segments_data.back());
          logger().debug("{} GOT AuthDoneFrame: gid={}, con_mode={}, payload_len={}",
                         conn, auth_done.global_id(),
                         ceph_con_mode_name(auth_done.con_mode()),
                         auth_done.auth_payload().length());
          ceph_assert(messenger.get_auth_client());
          int r = messenger.get_auth_client()->handle_auth_done(
              conn.shared_from_this(), auth_meta,
              auth_done.global_id(),
              auth_done.con_mode(),
              auth_done.auth_payload());
          if (r < 0) {
            logger().warn("{} auth_client handle_auth_done returned {}", conn, r);
            abort_in_fault();
          }
          auth_meta->con_mode = auth_done.con_mode();
          // TODO
          ceph_assert(!auth_meta->is_mode_secure());
          session_stream_handlers = { nullptr, nullptr };
          return finish_auth();
        });
      default: {
        unexpected_tag(tag, conn, __func__);
        return seastar::now();
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
    logger().debug("{} WRITE AuthRequestFrame: method={},"
                   " preferred_modes={}, payload_len={}",
                   conn, auth_method, preferred_modes, bl.length());
    return write_frame(frame).then([this] {
      return handle_auth_reply();
    });
  } catch (const ceph::auth::error& e) {
    logger().error("{} get_initial_auth_request returned {}", conn, e);
    dispatch_reset();
    abort_in_close(*this);
    return seastar::now();
  }
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::process_wait()
{
  return read_frame_payload().then([this] {
    // handle_wait() logic
    logger().warn("{} GOT WaitFrame", conn);
    WaitFrame::Decode(rx_segments_data.back());
    return next_step_t::wait;
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::client_connect()
{
  // send_client_ident() logic
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

  logger().debug("{} WRITE ClientIdentFrame: addrs={}, target={}, gid={},"
                 " gs={}, features_supported={}, features_required={},"
                 " flags={}, cookie={}",
                 conn, messenger.get_myaddrs(), conn.target_addr,
                 messenger.get_myname().num(), global_seq,
                 conn.policy.features_supported,
                 conn.policy.features_required | msgr2_required,
                 flags, client_cookie);
  return write_frame(client_ident).then([this] {
    return read_main_preamble();
  }).then([this] (Tag tag) {
    switch (tag) {
      case Tag::IDENT_MISSING_FEATURES:
        return read_frame_payload().then([this] {
          // handle_ident_missing_features() logic
          auto ident_missing = IdentMissingFeaturesFrame::Decode(rx_segments_data.back());
          logger().warn("{} GOT IdentMissingFeaturesFrame: features={}"
                        " (client does not support all server features)",
                        conn, ident_missing.features());
          abort_in_fault();
          return next_step_t::none;
        });
      case Tag::WAIT:
        return process_wait();
      case Tag::SERVER_IDENT:
        return read_frame_payload().then([this] {
          // handle_server_ident() logic
          auto server_ident = ServerIdentFrame::Decode(rx_segments_data.back());
          logger().debug("{} GOT ServerIdentFrame:"
                         " addrs={}, gid={}, gs={},"
                         " features_supported={}, features_required={},"
                         " flags={}, cookie={}",
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
            throw std::system_error(
                make_error_code(ceph::net::error::bad_peer_address));
          }

          server_cookie = server_ident.cookie();

          // TODO: change peer_addr to entity_addrvec_t
          if (server_ident.addrs().front() != conn.peer_addr) {
            logger().warn("{} peer advertises as {}, does not match {}",
                          conn, server_ident.addrs(), conn.peer_addr);
            throw std::system_error(
                make_error_code(ceph::net::error::bad_peer_address));
          }
          conn.set_peer_id(server_ident.gid());
          conn.set_features(server_ident.supported_features() &
                            conn.policy.features_supported);
          peer_global_seq = server_ident.global_seq();

          bool lossy = server_ident.flags() & CEPH_MSG_CONNECT_LOSSY;
          if (lossy != conn.policy.lossy) {
            logger().warn("{} UPDATE Policy(lossy={}) from server flags", conn, lossy);
            conn.policy.lossy = lossy;
          }
          if (lossy && (connect_seq != 0 || server_cookie != 0)) {
            logger().warn("{} UPDATE cs=0({}) sc=0({}) for lossy policy",
                          conn, connect_seq, server_cookie);
            connect_seq = 0;
            server_cookie = 0;
          }
          // TODO: backoff = utime_t();

          return dispatcher.ms_handle_connect(
              seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()))
          .handle_exception([this] (std::exception_ptr eptr) {
            logger().error("{} ms_handle_connect caught exception: {}", conn, eptr);
            ceph_abort("unexpected exception from ms_handle_connect()");
          });
        }).then([this] {
          return next_step_t::ready;
        });
      default: {
        unexpected_tag(tag, conn, "post_client_connect");
        return seastar::make_ready_future<next_step_t>(next_step_t::none);
      }
    }
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::client_reconnect()
{
  // send_reconnect() logic
  auto reconnect = ReconnectFrame::Encode(messenger.get_myaddrs(),
                                          client_cookie,
                                          server_cookie,
                                          global_seq,
                                          connect_seq,
                                          conn.in_seq);
  logger().debug("{} WRITE ReconnectFrame: client_cookie={},"
                 " server_cookie={}, gs={}, cs={}, msg_seq={}",
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
          logger().warn("{} GOT RetryGlobalFrame: gs={}",
                        conn, retry.global_seq());
          return messenger.get_global_seq(retry.global_seq()).then([this] (auto gs) {
            global_seq = gs;
            logger().warn("{} UPDATE: gs={}", conn, global_seq);
            return client_reconnect();
          });
        });
      case Tag::SESSION_RETRY:
        return read_frame_payload().then([this] {
          // handle_session_retry() logic
          auto retry = RetryFrame::Decode(rx_segments_data.back());
          logger().warn("{} GOT RetryFrame: cs={}",
                        conn, retry.connect_seq());
          connect_seq = retry.connect_seq() + 1;
          logger().warn("{} UPDATE: cs={}", conn, connect_seq);
          return client_reconnect();
        });
      case Tag::SESSION_RESET:
        return read_frame_payload().then([this] {
          // handle_session_reset() logic
          auto reset = ResetFrame::Decode(rx_segments_data.back());
          logger().warn("{} GOT ResetFrame: full={}", conn, reset.full());
          reset_session(reset.full());
          return client_connect();
        });
      case Tag::WAIT:
        return process_wait();
      case Tag::SESSION_RECONNECT_OK:
        return read_frame_payload().then([this] {
          // handle_reconnect_ok() logic
          auto reconnect_ok = ReconnectOkFrame::Decode(rx_segments_data.back());
          logger().debug("{} GOT ReconnectOkFrame: msg_seq={}",
                         conn, reconnect_ok.msg_seq());
          // TODO
          // discard_requeued_up_to()
          // backoff = utime_t();
          return dispatcher.ms_handle_connect(
              seastar::static_pointer_cast<SocketConnection>(
                conn.shared_from_this()))
          .handle_exception([this] (std::exception_ptr eptr) {
            logger().error("{} ms_handle_connect caught exception: {}", conn, eptr);
            ceph_abort("unexpected exception from ms_handle_connect()");
          });
        }).then([this] {
          return next_step_t::ready;
        });
      default: {
        unexpected_tag(tag, conn, "post_client_reconnect");
        return seastar::make_ready_future<next_step_t>(next_step_t::none);
      }
    }
  });
}

void ProtocolV2::execute_connecting()
{
  trigger_state(state_t::CONNECTING, write_state_t::delay, true);
  seastar::with_gate(pending_dispatch, [this] {
      // we don't know my socket_port yet
      conn.set_ephemeral_port(0, SocketConnection::side_t::none);
      return messenger.get_global_seq().then([this] (auto gs) {
          global_seq = gs;
          if (!conn.policy.lossy && server_cookie != 0) {
            assert(client_cookie != 0);
            ++connect_seq;
            logger().debug("{} UPDATE: gs={}, cs={} for reconnect",
                           conn, global_seq, connect_seq);
          } else {
            assert(connect_seq == 0);
            assert(server_cookie == 0);
            client_cookie = generate_client_cookie();
            logger().debug("{} UPDATE: gs={}, cc={} for connect",
                           conn, global_seq, client_cookie);
          }
          return Socket::connect(conn.peer_addr);
        }).then([this](SocketFRef sock) {
          logger().debug("{} socket connected", conn);
          socket = std::move(sock);
          if (state == state_t::CLOSING) {
            return socket->close().then([this] {
              logger().warn("{} is closed during Socket::connect()", conn);
              abort_protocol();
            });
          }
          return seastar::now();
        }).then([this] {
          auth_meta = seastar::make_lw_shared<AuthConnectionMeta>();
          session_stream_handlers = { nullptr, nullptr };
          enable_recording();
          return banner_exchange();
        }).then([this] (entity_type_t _peer_type,
                        entity_addr_t _my_addr_from_peer) {
          if (conn.get_peer_type() != _peer_type) {
            logger().warn("{} connection peer type does not match what peer advertises {} != {}",
                          conn, ceph_entity_type_name(conn.get_peer_type()),
                          ceph_entity_type_name(_peer_type));
            dispatch_reset();
            abort_in_close(*this);
          }
          conn.set_ephemeral_port(_my_addr_from_peer.get_port(),
                                  SocketConnection::side_t::connector);
          if (unlikely(_my_addr_from_peer.is_legacy())) {
            logger().warn("{} peer sent a legacy address for me: {}",
                          conn, _my_addr_from_peer);
            throw std::system_error(
                make_error_code(ceph::net::error::bad_peer_address));
          }
          _my_addr_from_peer.set_type(entity_addr_t::TYPE_MSGR2);
          return messenger.learned_addr(_my_addr_from_peer, conn);
        }).then([this] {
          return client_auth();
        }).then([this] {
          if (server_cookie == 0) {
            ceph_assert(connect_seq == 0);
            return client_connect();
          } else {
            ceph_assert(connect_seq > 0);
            // TODO: lossless policy
            ceph_assert(false);
            return client_reconnect();
          }
        }).then([this] (next_step_t next) {
          switch (next) {
           case next_step_t::ready: {
            logger().info("{} connected: gs={}, pgs={}, cs={},"
                          " client_cookie={}, server_cookie={}, in_seq={}, out_seq={}",
                          conn, global_seq, peer_global_seq, connect_seq,
                          client_cookie, server_cookie, conn.in_seq, conn.out_seq);
            execute_ready();
            break;
           }
           case next_step_t::wait: {
            execute_wait();
            break;
           }
           default: {
            ceph_abort("impossible next step");
           }
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
  auto bad_method = AuthBadMethodFrame::Encode(
      auth_meta->auth_method, r, allowed_methods, allowed_modes);
  logger().warn("{} WRITE AuthBadMethodFrame: method={}, result={}, "
                "allowed_methods={}, allowed_modes={})",
                conn, auth_meta->auth_method, cpp_strerror(r),
                allowed_methods, allowed_modes);
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
    logger().debug("{} WRITE AuthDoneFrame: gid={}, con_mode={}, payload_len={}",
                   conn, conn.peer_global_id,
                   ceph_con_mode_name(auth_meta->con_mode), reply.length());
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
    logger().debug("{} WRITE AuthReplyMoreFrame: payload_len={}",
                   conn, reply.length());
    return write_frame(more).then([this] {
      return read_main_preamble();
    }).then([this] (Tag tag) {
      expect_tag(Tag::AUTH_REQUEST_MORE, tag, conn, __func__);
      return read_frame_payload();
    }).then([this] {
      auto auth_more = AuthRequestMoreFrame::Decode(rx_segments_data.back());
      logger().debug("{} GOT AuthRequestMoreFrame: payload_len={}",
                     conn, auth_more.auth_payload().length());
      return _handle_auth_request(auth_more.auth_payload(), true);
    });
   }
   case -EBUSY: {
    logger().warn("{} auth_server handle_auth_request returned -EBUSY", conn);
    abort_in_fault();
    return seastar::now();
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
    logger().debug("{} GOT AuthRequestFrame: method={}, preferred_modes={},"
                   " payload_len={}",
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

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::send_wait()
{
  auto wait = WaitFrame::Encode();
  logger().warn("{} WRITE WaitFrame", conn);
  return write_frame(wait).then([this] {
    return next_step_t::wait;
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::handle_existing_connection(SocketConnectionRef existing_conn)
{
  // handle_existing_connection() logic
  logger().trace("{} {}: {}", conn, __func__, *existing_conn);

  ProtocolV2 *existing_proto = dynamic_cast<ProtocolV2*>(
      existing_conn->protocol.get());
  ceph_assert(existing_proto);

  if (existing_proto->state == state_t::CLOSING) {
    logger().warn("{} existing connection {} already closed.", conn, *existing_conn);
    return send_server_ident();
  }

  if (existing_proto->state == state_t::REPLACING) {
    logger().warn("{} racing replace happened while replacing existing connection {}",
                  conn, *existing_conn);
    return send_wait();
  }

  if (existing_proto->peer_global_seq > peer_global_seq) {
    logger().warn("{} this is a stale connection, because peer_global_seq({})"
                  "< existing->peer_global_seq({}), close this connection"
                  " in favor of existing connection {}",
                  conn, peer_global_seq,
                  existing_proto->peer_global_seq, *existing_conn);
    dispatch_reset();
    abort_in_close(*this);
  }

  if (existing_conn->policy.lossy) {
    // existing connection can be thrown out in favor of this one
    logger().warn("{} existing connection {} is a lossy channel. Close existing in favor of"
                  " this connection", conn, *existing_conn);
    existing_proto->dispatch_reset();
    existing_proto->close();
    return send_server_ident();
  }

  // TODO: lossless policy
  ceph_assert(false);
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::server_connect()
{
  return read_frame_payload().then([this] {
    // handle_client_ident() logic
    auto client_ident = ClientIdentFrame::Decode(rx_segments_data.back());
    logger().debug("{} GOT ClientIdentFrame: addrs={}, target={},"
                   " gid={}, gs={}, features_supported={},"
                   " features_required={}, flags={}, cookie={}",
                   conn, client_ident.addrs(), client_ident.target_addr(),
                   client_ident.gid(), client_ident.global_seq(),
                   client_ident.supported_features(),
                   client_ident.required_features(),
                   client_ident.flags(), client_ident.cookie());

    if (client_ident.addrs().empty() ||
        client_ident.addrs().front() == entity_addr_t()) {
      logger().warn("{} oops, client_ident.addrs() is empty", conn);
      throw std::system_error(
          make_error_code(ceph::net::error::bad_peer_address));
    }
    if (!messenger.get_myaddrs().contains(client_ident.target_addr())) {
      logger().warn("{} peer is trying to reach {} which is not us ({})",
                    conn, client_ident.target_addr(), messenger.get_myaddrs());
      throw std::system_error(
          make_error_code(ceph::net::error::bad_peer_address));
    }
    // TODO: change peer_addr to entity_addrvec_t
    entity_addr_t paddr = client_ident.addrs().front();
    if ((paddr.is_msgr2() || paddr.is_any()) &&
        paddr.is_same_host(conn.target_addr)) {
      // good
    } else {
      logger().warn("{} peer's address {} is not v2 or not the same host with {}",
                    conn, paddr, conn.target_addr);
      throw std::system_error(
          make_error_code(ceph::net::error::bad_peer_address));
    }
    conn.peer_addr = paddr;
    logger().debug("{} UPDATE: peer_addr={}", conn, conn.peer_addr);
    conn.target_addr = conn.peer_addr;
    if (!conn.policy.lossy && !conn.policy.server && conn.target_addr.get_port() <= 0) {
      logger().warn("{} we don't know how to reconnect to peer {}",
                    conn, conn.target_addr);
      throw std::system_error(
          make_error_code(ceph::net::error::bad_peer_address));
    }

    conn.set_peer_id(client_ident.gid());
    client_cookie = client_ident.cookie();

    uint64_t feat_missing =
      (conn.policy.features_required | msgr2_required) &
      ~(uint64_t)client_ident.supported_features();
    if (feat_missing) {
      auto ident_missing_features = IdentMissingFeaturesFrame::Encode(feat_missing);
      logger().warn("{} WRITE IdentMissingFeaturesFrame: features={} (peer missing)",
                    conn, feat_missing);
      return write_frame(ident_missing_features).then([this] {
        return next_step_t::wait;
      });
    }
    connection_features =
        client_ident.supported_features() & conn.policy.features_supported;
    logger().debug("{} UPDATE: connection_features={}", conn, connection_features);

    peer_global_seq = client_ident.global_seq();

    // Looks good so far, let's check if there is already an existing connection
    // to this peer.

    SocketConnectionRef existing_conn = messenger.lookup_conn(conn.peer_addr);

    if (existing_conn) {
      if (existing_conn->protocol->proto_type != proto_t::v2) {
        logger().warn("{} existing connection {} proto version is {}, close existing",
                      conn, *existing_conn,
                      static_cast<int>(existing_conn->protocol->proto_type));
        // should unregister the existing from msgr atomically
        existing_conn->close();
      } else {
        return handle_existing_connection(existing_conn);
      }
    }

    // if everything is OK reply with server identification
    return send_server_ident();
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::read_reconnect()
{
  return read_main_preamble()
  .then([this] (Tag tag) {
    expect_tag(Tag::SESSION_RECONNECT, tag, conn, "read_reconnect");
    return server_reconnect();
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::send_retry(uint64_t connect_seq)
{
  auto retry = RetryFrame::Encode(connect_seq);
  logger().warn("{} WRITE RetryFrame: cs={}", conn, connect_seq);
  return write_frame(retry).then([this] {
    return read_reconnect();
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::send_retry_global(uint64_t global_seq)
{
  auto retry = RetryGlobalFrame::Encode(global_seq);
  logger().warn("{} WRITE RetryGlobalFrame: gs={}", conn, global_seq);
  return write_frame(retry).then([this] {
    return read_reconnect();
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::send_reset(bool full)
{
  auto reset = ResetFrame::Encode(full);
  logger().warn("{} WRITE ResetFrame: full={}", conn, full);
  return write_frame(reset).then([this] {
    return read_main_preamble();
  }).then([this] (Tag tag) {
    expect_tag(Tag::CLIENT_IDENT, tag, conn, "post_send_reset");
    return server_connect();
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::server_reconnect()
{
  return read_frame_payload().then([this] {
    // handle_reconnect() logic
    auto reconnect = ReconnectFrame::Decode(rx_segments_data.back());

    logger().debug("{} GOT ReconnectFrame: client_cookie={}, server_cookie={},"
                   " gs={}, cs={}, msg_seq={}",
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

    SocketConnectionRef existing_conn = messenger.lookup_conn(conn.peer_addr);

    if (!existing_conn) {
      // there is no existing connection therefore cannot reconnect to previous
      // session
      logger().warn("{} server_reconnect: no existing connection,"
                    " reseting client", conn);
      return send_reset(true);
    }

    if (existing_conn->protocol->proto_type != proto_t::v2) {
      logger().warn("{} server_reconnect: existing connection {} proto version is {},"
                    "close existing and reset client.",
                    conn, *existing_conn,
                    static_cast<int>(existing_conn->protocol->proto_type));
      existing_conn->close();
      return send_reset(true);
    }

    ProtocolV2 *existing_proto = dynamic_cast<ProtocolV2*>(
        existing_conn->protocol.get());
    ceph_assert(existing_proto);

    if (existing_proto->state == state_t::REPLACING) {
      logger().warn("{} server_reconnect: racing replace happened while "
                    " replacing existing connection {}, retry global.",
                    conn, *existing_conn);
      return send_retry_global(existing_proto->peer_global_seq);
    }

    if (existing_proto->client_cookie != reconnect.client_cookie()) {
      logger().warn("{} server_reconnect:"
                    " client_cookie mismatch with existing connection {},"
                    " cc={} rcc={}. I must have reseted, reseting client.",
                    conn, *existing_conn,
                    existing_proto->client_cookie, reconnect.client_cookie());
      return send_reset(conn.policy.resetcheck);
    } else if (existing_proto->server_cookie == 0) {
      // this happens when:
      //   - a connects to b
      //   - a sends client_ident
      //   - b gets client_ident, sends server_ident and sets cookie X
      //   - connection fault
      //   - b reconnects to a with cookie X, connect_seq=1
      //   - a has cookie==0
      logger().warn("{} server_reconnect: I was a client and didn't received the"
                    " server_ident with existing connection {}."
                    " Asking peer to resume session establishment",
                    conn, *existing_conn);
      return send_reset(false);
    }

    if (existing_proto->peer_global_seq > reconnect.global_seq()) {
      logger().warn("{} server_reconnect: stale global_seq: exist_pgs={} peer_gs={},"
                    " with existing connection {},"
                    " ask client to retry global",
                    conn, existing_proto->peer_global_seq,
                    reconnect.global_seq(), *existing_conn);
      return send_retry_global(existing_proto->peer_global_seq);
    }

    if (existing_proto->connect_seq > reconnect.connect_seq()) {
      logger().warn("{} server_reconnect: stale connect_seq exist_cs={} peer_cs={},"
                    " with existing connection {},"
                    " ask client to retry",
                    conn, existing_proto->connect_seq, reconnect.connect_seq(),
                    *existing_conn);
      return send_retry(existing_proto->connect_seq);
    }

    if (existing_proto->connect_seq == reconnect.connect_seq()) {
      // reconnect race: both peers are sending reconnect messages
      if (existing_conn->peer_addr > messenger.get_myaddrs().msgr2_addr() &&
          !existing_conn->policy.server) {
        // the existing connection wins
        logger().warn("{} server_reconnect: reconnect race detected,"
                      " this connection loses to existing connection {},"
                      " ask client to wait", conn, *existing_conn);
        return send_wait();
      } else {
        // this connection wins
        logger().warn("{} server_reconnect: reconnect race detected,"
                      " replacing existing connection {}"
                      " socket by this connection's socket",
                      conn, *existing_conn);
      }
    }

    logger().warn("{} server_reconnect: reconnect to exsiting connection {}",
                  conn, *existing_conn);

    // everything looks good
    existing_proto->connect_seq = reconnect.connect_seq();
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
      auth_meta = seastar::make_lw_shared<AuthConnectionMeta>();
      session_stream_handlers = { nullptr, nullptr };
      enable_recording();
      return banner_exchange()
        .then([this] (entity_type_t _peer_type,
                      entity_addr_t _my_addr_from_peer) {
          ceph_assert(conn.get_peer_type() == 0);
          conn.set_peer_type(_peer_type);

          conn.policy = messenger.get_policy(_peer_type);
          logger().info("{} UPDATE: peer_type={},"
                        " policy(lossy={} server={} standby={} resetcheck={})",
                        conn, ceph_entity_type_name(_peer_type),
                        conn.policy.lossy, conn.policy.server,
                        conn.policy.standby, conn.policy.resetcheck);
          if (messenger.get_myaddr().get_port() != _my_addr_from_peer.get_port() ||
              messenger.get_myaddr().get_nonce() != _my_addr_from_peer.get_nonce()) {
            logger().warn("{} my_addr_from_peer {} port/nonce doesn't match myaddr {}",
                          conn, _my_addr_from_peer, messenger.get_myaddr());
            throw std::system_error(
                make_error_code(ceph::net::error::bad_peer_address));
          }
          return messenger.learned_addr(_my_addr_from_peer, conn);
        }).then([this] {
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
              return seastar::make_ready_future<next_step_t>(next_step_t::none);
            }
          }
        }).then([this] (next_step_t next) {
          switch (next) {
           case next_step_t::ready: {
            messenger.register_conn(
              seastar::static_pointer_cast<SocketConnection>(
                conn.shared_from_this()));
            messenger.unaccept_conn(
              seastar::static_pointer_cast<SocketConnection>(
                conn.shared_from_this()));
            logger().info("{} accepted: gs={}, pgs={}, cs={},"
                          " client_cookie={}, server_cookie={}, in_seq={}, out_seq={}",
                          conn, global_seq, peer_global_seq, connect_seq,
                          client_cookie, server_cookie, conn.in_seq, conn.out_seq);
            execute_ready();
            break;
           }
           case next_step_t::wait: {
            execute_server_wait();
            break;
           }
           default: {
            ceph_abort("impossible next step");
           }
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
  logger().debug("{} WRITE AuthSignatureFrame: signature={}", conn, sig);
  return write_frame(sig_frame).then([this] {
    return read_main_preamble();
  }).then([this] (Tag tag) {
    expect_tag(Tag::AUTH_SIGNATURE, tag, conn, "post_finish_auth");
    return read_frame_payload();
  }).then([this] {
    // handle_auth_signature() logic
    auto sig_frame = AuthSignatureFrame::Decode(rx_segments_data.back());
    logger().debug("{} GOT AuthSignatureFrame: signature={}", conn, sig_frame.signature());

    const auto actual_tx_sig = auth_meta->session_key.empty() ?
      sha256_digest_t() : auth_meta->session_key.hmac_sha256(nullptr, txbuf);
    if (sig_frame.signature() != actual_tx_sig) {
      logger().warn("{} pre-auth signature mismatch actual_tx_sig={}"
                    " sig_frame.signature()={}",
                    conn, actual_tx_sig, sig_frame.signature());
      abort_in_fault();
    }
    txbuf.clear();
  });
}

// ACCEPTING or REPLACING state

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::send_server_ident()
{
  // send_server_ident() logic

  // refered to async-conn v2: not assign gs to global_seq
  return messenger.get_global_seq().then([this] (auto gs) {
    logger().debug("{} UPDATE: gs={} for server ident", conn, global_seq);

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

    auto server_ident = ServerIdentFrame::Encode(
            messenger.get_myaddrs(),
            messenger.get_myname().num(),
            gs,
            conn.policy.features_supported,
            conn.policy.features_required | msgr2_required,
            flags,
            server_cookie);

    logger().debug("{} WRITE ServerIdentFrame: addrs={}, gid={},"
                   " gs={}, features_supported={}, features_required={},"
                   " flags={}, cookie={}",
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
        logger().error("{} ms_handle_accept caught exception: {}", conn, eptr);
        ceph_abort("unecpected exception from ms_handle_accept()");
      });
    });

    return write_frame(server_ident);
  }).then([] {
    return next_step_t::ready;
  });
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

    ceph_assert(!msg->get_seq() && "message already has seq");
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
    logger().debug("{} --> #{} === {} ({})",
		   conn, msg->get_seq(), *msg, msg->get_type());
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

    logger().trace("{} got {} + {} + {} byte message,"
                   " envelope type={} src={} off={} seq={}",
                   conn, msg_frame.front_len(), msg_frame.middle_len(),
                   msg_frame.data_len(), current_header.type, conn.get_peer_name(),
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
                           conn.get_peer_name(),
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
    logger().debug("{} <== #{} === {} ({})",
		   conn, message->get_seq(), *message, message->get_type());
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
        ceph_abort("unexpected exception from ms_dispatch()");
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
              logger().trace("{} wants {} bytes from policy throttler {}/{}",
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
              logger().debug("{} GOT AckFrame: seq={}", ack.seq());
              handle_message_ack(ack.seq());
            });
          case Tag::KEEPALIVE2:
            return read_frame_payload().then([this] {
              // handle_keepalive2() logic
              auto keepalive_frame = KeepAliveFrame::Decode(rx_segments_data.back());
              logger().debug("{} GOT KeepAliveFrame: timestamp={}",
                             conn, keepalive_frame.timestamp());
              notify_keepalive_ack(keepalive_frame.timestamp());
              conn.set_last_keepalive(seastar::lowres_system_clock::now());
            });
          case Tag::KEEPALIVE2_ACK:
            return read_frame_payload().then([this] {
              // handle_keepalive2_ack() logic
              auto keepalive_ack_frame = KeepAliveFrameAck::Decode(rx_segments_data.back());
              conn.set_last_keepalive_ack(
                seastar::lowres_system_clock::time_point{keepalive_ack_frame.timestamp()});
              logger().debug("{} GOT KeepAliveFrameAck: timestamp={}",
                             conn, conn.last_keepalive_ack);
            });
          default: {
            unexpected_tag(tag, conn, "execute_ready");
            return seastar::now();
          }
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
