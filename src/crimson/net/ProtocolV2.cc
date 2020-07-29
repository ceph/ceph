// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ProtocolV2.h"

#include <seastar/core/lowres_clock.hh>
#include <fmt/format.h>
#include "include/msgr.h"
#include "include/random.h"

#include "crimson/auth/AuthClient.h"
#include "crimson/auth/AuthServer.h"
#include "crimson/common/formatter.h"

#include "Dispatcher.h"
#include "Errors.h"
#include "Socket.h"
#include "SocketConnection.h"
#include "SocketMessenger.h"

#ifdef UNIT_TESTS_BUILT
#include "Interceptor.h"
#endif

using namespace ceph::msgr::v2;
using crimson::common::local_conf;

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
  return crimson::get_logger(ceph_subsys_ms);
}

[[noreturn]] void abort_in_fault() {
  throw std::system_error(make_error_code(crimson::net::error::negotiation_failure));
}

[[noreturn]] void abort_protocol() {
  throw std::system_error(make_error_code(crimson::net::error::protocol_aborted));
}

[[noreturn]] void abort_in_close(crimson::net::ProtocolV2& proto, bool dispatch_reset) {
  proto.close(dispatch_reset);
  abort_protocol();
}

inline void expect_tag(const Tag& expected,
                       const Tag& actual,
                       crimson::net::SocketConnection& conn,
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
                           crimson::net::SocketConnection& conn,
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

namespace crimson::net {

#ifdef UNIT_TESTS_BUILT
void intercept(Breakpoint bp, bp_type_t type,
               SocketConnection& conn, SocketRef& socket) {
  if (conn.interceptor) {
    auto action = conn.interceptor->intercept(conn, Breakpoint(bp));
    socket->set_trap(type, action, &conn.interceptor->blocker);
  }
}

#define INTERCEPT_CUSTOM(bp, type)       \
intercept({bp}, type, conn, socket)

#define INTERCEPT_FRAME(tag, type)       \
intercept({static_cast<Tag>(tag), type}, \
          type, conn, socket)

#define INTERCEPT_N_RW(bp)                               \
if (conn.interceptor) {                                  \
  auto action = conn.interceptor->intercept(conn, {bp}); \
  ceph_assert(action != bp_action_t::BLOCK);             \
  if (action == bp_action_t::FAULT) {                    \
    abort_in_fault();                                    \
  }                                                      \
}

#else
#define INTERCEPT_CUSTOM(bp, type)
#define INTERCEPT_FRAME(tag, type)
#define INTERCEPT_N_RW(bp)
#endif

seastar::future<> ProtocolV2::Timer::backoff(double seconds)
{
  logger().warn("{} waiting {} seconds ...", conn, seconds);
  cancel();
  last_dur_ = seconds;
  as = seastar::abort_source();
  auto dur = std::chrono::duration_cast<seastar::lowres_clock::duration>(
      std::chrono::duration<double>(seconds));
  return seastar::sleep_abortable(dur, *as
  ).handle_exception_type([this] (const seastar::sleep_aborted& e) {
    logger().debug("{} wait aborted", conn);
    abort_protocol();
  });
}

ProtocolV2::ProtocolV2(ChainedDispatchersRef& dispatcher,
                       SocketConnection& conn,
                       SocketMessenger& messenger)
  : Protocol(proto_t::v2, dispatcher, conn),
    messenger{messenger},
    protocol_timer{conn}
{}

ProtocolV2::~ProtocolV2() {}

bool ProtocolV2::is_connected() const {
  return state == state_t::READY ||
         state == state_t::ESTABLISHING ||
         state == state_t::REPLACING;
}

void ProtocolV2::start_connect(const entity_addr_t& _peer_addr,
                               const entity_name_t& _peer_name)
{
  ceph_assert(state == state_t::NONE);
  ceph_assert(!socket);
  conn.peer_addr = _peer_addr;
  conn.target_addr = _peer_addr;
  conn.set_peer_name(_peer_name);
  conn.policy = messenger.get_policy(_peer_name.type());
  client_cookie = generate_client_cookie();
  logger().info("{} ProtocolV2::start_connect(): peer_addr={}, peer_name={}, cc={}"
                " policy(lossy={}, server={}, standby={}, resetcheck={})",
                conn, _peer_addr, _peer_name, client_cookie,
                conn.policy.lossy, conn.policy.server,
                conn.policy.standby, conn.policy.resetcheck);
  messenger.register_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  execute_connecting();
}

void ProtocolV2::start_accept(SocketRef&& sock,
                              const entity_addr_t& _peer_addr)
{
  ceph_assert(state == state_t::NONE);
  ceph_assert(!socket);
  // until we know better
  conn.target_addr = _peer_addr;
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
  ceph_assert(rx_frame_asm.get_num_segments() > 0);
  size_t sum = 0;
  // we don't include SegmentIndex::Msg::HEADER.
  for (size_t idx = 1; idx < rx_frame_asm.get_num_segments(); idx++) {
    sum += rx_frame_asm.get_segment_logical_len(idx);
  }
  return sum;
}

seastar::future<Tag> ProtocolV2::read_main_preamble()
{
  rx_preamble.clear();
  return read_exactly(rx_frame_asm.get_preamble_onwire_len())
    .then([this] (auto bl) {
      rx_segments_data.clear();
      try {
        rx_preamble.append(buffer::create(std::move(bl)));
        const Tag tag = rx_frame_asm.disassemble_preamble(rx_preamble);
        INTERCEPT_FRAME(tag, bp_type_t::READ);
        return tag;
      } catch (FrameError& e) {
        logger().warn("{} read_main_preamble: {}", conn, e.what());
        abort_in_fault();
      }
    });
}

seastar::future<> ProtocolV2::read_frame_payload()
{
  ceph_assert(rx_segments_data.empty());

  return seastar::do_until(
    [this] { return rx_frame_asm.get_num_segments() == rx_segments_data.size(); },
    [this] {
      // TODO: create aligned and contiguous buffer from socket
      const size_t seg_idx = rx_segments_data.size();
      if (uint16_t alignment = rx_frame_asm.get_segment_align(seg_idx);
	  alignment != segment_t::DEFAULT_ALIGNMENT) {
        logger().trace("{} cannot allocate {} aligned buffer at segment desc index {}",
                       conn, alignment, rx_segments_data.size());
      }
      uint32_t onwire_len = rx_frame_asm.get_segment_onwire_len(seg_idx);
      // TODO: create aligned and contiguous buffer from socket
      return read_exactly(onwire_len).then([this] (auto tmp_bl) {
        logger().trace("{} RECV({}) frame segment[{}]",
                       conn, tmp_bl.size(), rx_segments_data.size());
        bufferlist segment;
        segment.append(buffer::create(std::move(tmp_bl)));
        rx_segments_data.emplace_back(std::move(segment));
      });
    }
  ).then([this] {
    return read_exactly(rx_frame_asm.get_epilogue_onwire_len());
  }).then([this] (auto bl) {
    logger().trace("{} RECV({}) frame epilogue", conn, bl.size());
    bool ok = false;
    try {
      rx_frame_asm.disassemble_first_segment(rx_preamble, rx_segments_data[0]);
      bufferlist rx_epilogue;
      rx_epilogue.append(buffer::create(std::move(bl)));
      ok = rx_frame_asm.disassemble_remaining_segments(rx_segments_data.data(), rx_epilogue);
    } catch (FrameError& e) {
      logger().error("read_frame_payload: {} {}", conn, e.what());
      abort_in_fault();
    } catch (ceph::crypto::onwire::MsgAuthError&) {
      logger().error("read_frame_payload: {} bad auth tag", conn);
      abort_in_fault();
    }
    // we do have a mechanism that allows transmitter to start sending message
    // and abort after putting entire data field on wire. This will be used by
    // the kernel client to avoid unnecessary buffering.
    if (!ok) {
      // TODO
      ceph_assert(false);
    }
  });
}

template <class F>
seastar::future<> ProtocolV2::write_frame(F &frame, bool flush)
{
  auto bl = frame.get_buffer(tx_frame_asm);
  const auto main_preamble = reinterpret_cast<const preamble_block_t*>(bl.front().c_str());
  logger().trace("{} SEND({}) frame: tag={}, num_segments={}, crc={}",
                 conn, bl.length(), (int)main_preamble->tag,
                 (int)main_preamble->num_segments, main_preamble->crc);
  INTERCEPT_FRAME(main_preamble->tag, bp_type_t::WRITE);
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

void ProtocolV2::fault(bool backoff, const char* func_name, std::exception_ptr eptr)
{
  if (conn.policy.lossy) {
    logger().info("{} {}: fault at {} on lossy channel, going to CLOSING -- {}",
                  conn, func_name, get_state_name(state), eptr);
    close(true);
  } else if (conn.policy.server ||
             (conn.policy.standby &&
              (!is_queued() && conn.sent.empty()))) {
    logger().info("{} {}: fault at {} with nothing to send, going to STANDBY -- {}",
                  conn, func_name, get_state_name(state), eptr);
    execute_standby();
  } else if (backoff) {
    logger().info("{} {}: fault at {}, going to WAIT -- {}",
                  conn, func_name, get_state_name(state), eptr);
    execute_wait(false);
  } else {
    logger().info("{} {}: fault at {}, going to CONNECTING -- {}",
                  conn, func_name, get_state_name(state), eptr);
    execute_connecting();
  }
}

void ProtocolV2::reset_session(bool full)
{
  server_cookie = 0;
  connect_seq = 0;
  conn.in_seq = 0;
  if (full) {
    client_cookie = generate_client_cookie();
    peer_global_seq = 0;
    reset_write();
    dispatcher->ms_handle_remote_reset(
	seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  }
}

seastar::future<std::tuple<entity_type_t, entity_addr_t>>
ProtocolV2::banner_exchange(bool is_connect)
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
  INTERCEPT_CUSTOM(custom_bp_t::BANNER_WRITE, bp_type_t::WRITE);
  return write_flush(std::move(bl)).then([this] {
      // 2. read peer banner
      unsigned banner_len = strlen(CEPH_BANNER_V2_PREFIX) + sizeof(ceph_le16);
      INTERCEPT_CUSTOM(custom_bp_t::BANNER_READ, bp_type_t::READ);
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
      INTERCEPT_CUSTOM(custom_bp_t::BANNER_PAYLOAD_READ, bp_type_t::READ);
      return read(payload_len);
    }).then([this, is_connect] (bufferlist bl) {
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
        abort_in_close(*this, is_connect);
      }
      if ((supported_features & peer_required_features) != peer_required_features) {
        logger().error("{} we do not support all peer required features"
                       " peer_required={} supported={}",
                       conn, peer_required_features, supported_features);
        abort_in_close(*this, is_connect);
      }
      this->peer_required_features = peer_required_features;
      if (this->peer_required_features == 0) {
        this->connection_features = msgr2_required;
      }
      const bool is_rev1 = HAVE_MSGR2_FEATURE(peer_supported_features, REVISION_1);
      tx_frame_asm.set_is_rev1(is_rev1);
      rx_frame_asm.set_is_rev1(is_rev1);

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
      return seastar::make_ready_future<std::tuple<entity_type_t, entity_addr_t>>(
        std::make_tuple(hello.entity_type(), hello.peer_addr()));
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
          session_stream_handlers = ceph::crypto::onwire::rxtx_t::create_handler_pair(
              nullptr, *auth_meta, tx_frame_asm.get_is_rev1(), false);
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
  } catch (const crimson::auth::error& e) {
    logger().error("{} get_initial_auth_request returned {}", conn, e);
    abort_in_close(*this, true);
    return seastar::now();
  }
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::process_wait()
{
  return read_frame_payload().then([this] {
    // handle_wait() logic
    logger().debug("{} GOT WaitFrame", conn);
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
          requeue_sent();
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
                make_error_code(crimson::net::error::bad_peer_address));
          }

          server_cookie = server_ident.cookie();

          // TODO: change peer_addr to entity_addrvec_t
          if (server_ident.addrs().front() != conn.peer_addr) {
            logger().warn("{} peer advertises as {}, does not match {}",
                          conn, server_ident.addrs(), conn.peer_addr);
            throw std::system_error(
                make_error_code(crimson::net::error::bad_peer_address));
          }
          if (conn.get_peer_id() != entity_name_t::NEW &&
              conn.get_peer_id() != server_ident.gid()) {
            logger().error("{} connection peer id ({}) does not match "
                           "what it should be ({}) during connecting, close",
                            conn, server_ident.gid(), conn.get_peer_id());
            abort_in_close(*this, true);
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

          return seastar::make_ready_future<next_step_t>(next_step_t::ready);
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
  logger().debug("{} WRITE ReconnectFrame: addrs={}, client_cookie={},"
                 " server_cookie={}, gs={}, cs={}, msg_seq={}",
                 conn, messenger.get_myaddrs(),
                 client_cookie, server_cookie,
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
            logger().warn("{} UPDATE: gs={} for retry global", conn, global_seq);
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
          requeue_up_to(reconnect_ok.msg_seq());
          return seastar::make_ready_future<next_step_t>(next_step_t::ready);
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
  if (socket) {
    socket->shutdown();
  }
  gated_execute("execute_connecting", [this] {
      return messenger.get_global_seq().then([this] (auto gs) {
          global_seq = gs;
          assert(client_cookie != 0);
          if (!conn.policy.lossy && server_cookie != 0) {
            ++connect_seq;
            logger().debug("{} UPDATE: gs={}, cs={} for reconnect",
                           conn, global_seq, connect_seq);
          } else { // conn.policy.lossy || server_cookie == 0
            assert(connect_seq == 0);
            assert(server_cookie == 0);
            logger().debug("{} UPDATE: gs={} for connect", conn, global_seq);
          }

          return wait_write_exit();
        }).then([this] {
          if (unlikely(state != state_t::CONNECTING)) {
            logger().debug("{} triggered {} before Socket::connect()",
                           conn, get_state_name(state));
            abort_protocol();
          }
          if (socket) {
            gate.dispatch_in_background("close_sockect_connecting", *this,
                           [sock = std::move(socket)] () mutable {
              return sock->close().then([sock = std::move(sock)] {});
            });
          }
          INTERCEPT_N_RW(custom_bp_t::SOCKET_CONNECTING);
          return Socket::connect(conn.peer_addr);
        }).then([this](SocketRef sock) {
          logger().debug("{} socket connected", conn);
          if (unlikely(state != state_t::CONNECTING)) {
            logger().debug("{} triggered {} during Socket::connect()",
                           conn, get_state_name(state));
            return sock->close().then([sock = std::move(sock)] {
              abort_protocol();
            });
          }
          socket = std::move(sock);
          return seastar::now();
        }).then([this] {
          auth_meta = seastar::make_lw_shared<AuthConnectionMeta>();
          session_stream_handlers = { nullptr, nullptr };
          enable_recording();
          return banner_exchange(true);
        }).then([this] (auto&& ret) {
          auto [_peer_type, _my_addr_from_peer] = std::move(ret);
          if (conn.get_peer_type() != _peer_type) {
            logger().warn("{} connection peer type does not match what peer advertises {} != {}",
                          conn, ceph_entity_type_name(conn.get_peer_type()),
                          ceph_entity_type_name(_peer_type));
            abort_in_close(*this, true);
          }
          if (unlikely(state != state_t::CONNECTING)) {
            logger().debug("{} triggered {} during banner_exchange(), abort",
                           conn, get_state_name(state));
            abort_protocol();
          }
          socket->learn_ephemeral_port_as_connector(_my_addr_from_peer.get_port());
          if (unlikely(_my_addr_from_peer.is_legacy())) {
            logger().warn("{} peer sent a legacy address for me: {}",
                          conn, _my_addr_from_peer);
            throw std::system_error(
                make_error_code(crimson::net::error::bad_peer_address));
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
            return client_reconnect();
          }
        }).then([this] (next_step_t next) {
          if (unlikely(state != state_t::CONNECTING)) {
            logger().debug("{} triggered {} at the end of execute_connecting()",
                           conn, get_state_name(state));
            abort_protocol();
          }
          switch (next) {
           case next_step_t::ready: {
            logger().info("{} connected:"
                          " gs={}, pgs={}, cs={}, client_cookie={},"
                          " server_cookie={}, in_seq={}, out_seq={}, out_q={}",
                          conn, global_seq, peer_global_seq, connect_seq,
                          client_cookie, server_cookie, conn.in_seq,
                          conn.out_seq, conn.out_q.size());
            execute_ready(true);
            break;
           }
           case next_step_t::wait: {
            logger().info("{} execute_connecting(): going to WAIT", conn);
            execute_wait(true);
            break;
           }
           default: {
            ceph_abort("impossible next step");
           }
          }
        }).handle_exception([this] (std::exception_ptr eptr) {
          if (state != state_t::CONNECTING) {
            logger().info("{} execute_connecting(): protocol aborted at {} -- {}",
                          conn, get_state_name(state), eptr);
            assert(state == state_t::CLOSING ||
                   state == state_t::REPLACING);
            return;
          }

          if (conn.policy.server ||
              (conn.policy.standby &&
               (!is_queued() && conn.sent.empty()))) {
            logger().info("{} execute_connecting(): fault at {} with nothing to send,"
                          " going to STANDBY -- {}",
                          conn, get_state_name(state), eptr);
            execute_standby();
          } else {
            logger().info("{} execute_connecting(): fault at {}, going to WAIT -- {}",
                          conn, get_state_name(state), eptr);
            execute_wait(false);
          }
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
      session_stream_handlers = ceph::crypto::onwire::rxtx_t::create_handler_pair(
          nullptr, *auth_meta, tx_frame_asm.get_is_rev1(), true);
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

bool ProtocolV2::validate_peer_name(const entity_name_t& peer_name) const
{
  auto my_peer_name = conn.get_peer_name();
  if (my_peer_name.type() != peer_name.type()) {
    return false;
  }
  if (my_peer_name.num() != entity_name_t::NEW &&
      peer_name.num() != entity_name_t::NEW &&
      my_peer_name.num() != peer_name.num()) {
    return false;
  }
  return true;
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::send_wait()
{
  auto wait = WaitFrame::Encode();
  logger().debug("{} WRITE WaitFrame", conn);
  return write_frame(wait).then([] {
    return next_step_t::wait;
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::reuse_connection(
    ProtocolV2* existing_proto, bool do_reset,
    bool reconnect, uint64_t conn_seq, uint64_t msg_seq)
{
  existing_proto->trigger_replacing(reconnect,
                                    do_reset,
                                    std::move(socket),
                                    std::move(auth_meta),
                                    std::move(session_stream_handlers),
                                    peer_global_seq,
                                    client_cookie,
                                    conn.get_peer_name(),
                                    connection_features,
                                    tx_frame_asm.get_is_rev1(),
                                    rx_frame_asm.get_is_rev1(),
                                    conn_seq,
                                    msg_seq);
#ifdef UNIT_TESTS_BUILT
  if (conn.interceptor) {
    conn.interceptor->register_conn_replaced(conn);
  }
#endif
  // close this connection because all the necessary information is delivered
  // to the exisiting connection, and jump to error handling code to abort the
  // current state.
  abort_in_close(*this, false);
  return seastar::make_ready_future<next_step_t>(next_step_t::none);
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::handle_existing_connection(SocketConnectionRef existing_conn)
{
  // handle_existing_connection() logic
  ProtocolV2 *existing_proto = dynamic_cast<ProtocolV2*>(
      existing_conn->protocol.get());
  ceph_assert(existing_proto);
  logger().debug("{}(gs={}, pgs={}, cs={}, cc={}, sc={}) connecting,"
                 " found existing {}(state={}, gs={}, pgs={}, cs={}, cc={}, sc={})",
                 conn, global_seq, peer_global_seq, connect_seq,
                 client_cookie, server_cookie,
                 existing_conn, get_state_name(existing_proto->state),
                 existing_proto->global_seq,
                 existing_proto->peer_global_seq,
                 existing_proto->connect_seq,
                 existing_proto->client_cookie,
                 existing_proto->server_cookie);

  if (!validate_peer_name(existing_conn->get_peer_name())) {
    logger().error("{} server_connect: my peer_name doesn't match"
                   " the existing connection {}, abort", conn, existing_conn);
    abort_in_fault();
  }

  if (existing_proto->state == state_t::REPLACING) {
    logger().warn("{} server_connect: racing replace happened while"
                  " replacing existing connection {}, send wait.",
                  conn, *existing_conn);
    return send_wait();
  }

  if (existing_proto->peer_global_seq > peer_global_seq) {
    logger().warn("{} server_connect:"
                  " this is a stale connection, because peer_global_seq({})"
                  " < existing->peer_global_seq({}), close this connection"
                  " in favor of existing connection {}",
                  conn, peer_global_seq,
                  existing_proto->peer_global_seq, *existing_conn);
    abort_in_fault();
  }

  if (existing_conn->policy.lossy) {
    // existing connection can be thrown out in favor of this one
    logger().warn("{} server_connect:"
                  " existing connection {} is a lossy channel. Close existing in favor of"
                  " this connection", conn, *existing_conn);
    execute_establishing(existing_conn, true);
    return seastar::make_ready_future<next_step_t>(next_step_t::ready);
  }

  if (existing_proto->server_cookie != 0) {
    if (existing_proto->client_cookie != client_cookie) {
      // Found previous session
      // peer has reset and we're going to reuse the existing connection
      // by replacing the socket
      logger().warn("{} server_connect:"
                    " found new session (cs={})"
                    " when existing {} is with stale session (cs={}, ss={}),"
                    " peer must have reset",
                    conn, client_cookie,
                    *existing_conn, existing_proto->client_cookie,
                    existing_proto->server_cookie);
      return reuse_connection(existing_proto, conn.policy.resetcheck);
    } else {
      // session establishment interrupted between client_ident and server_ident,
      // continuing...
      logger().warn("{} server_connect: found client session with existing {}"
                    " matched (cs={}, ss={}), continuing session establishment",
                    conn, *existing_conn, client_cookie, existing_proto->server_cookie);
      return reuse_connection(existing_proto);
    }
  } else {
    // Looks like a connection race: server and client are both connecting to
    // each other at the same time.
    if (existing_proto->client_cookie != client_cookie) {
      if (existing_conn->peer_wins()) {
        logger().warn("{} server_connect: connection race detected (cs={}, e_cs={}, ss=0)"
                      " and win, reusing existing {}",
                      conn, client_cookie, existing_proto->client_cookie, *existing_conn);
        return reuse_connection(existing_proto);
      } else {
        logger().warn("{} server_connect: connection race detected (cs={}, e_cs={}, ss=0)"
                      " and lose to existing {}, ask client to wait",
                      conn, client_cookie, existing_proto->client_cookie, *existing_conn);
        return existing_conn->keepalive().then([this] {
          return send_wait();
        });
      }
    } else {
      logger().warn("{} server_connect: found client session with existing {}"
                    " matched (cs={}, ss={}), continuing session establishment",
                    conn, *existing_conn, client_cookie, existing_proto->server_cookie);
      return reuse_connection(existing_proto);
    }
  }
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
          make_error_code(crimson::net::error::bad_peer_address));
    }
    if (!messenger.get_myaddrs().contains(client_ident.target_addr())) {
      logger().warn("{} peer is trying to reach {} which is not us ({})",
                    conn, client_ident.target_addr(), messenger.get_myaddrs());
      throw std::system_error(
          make_error_code(crimson::net::error::bad_peer_address));
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
          make_error_code(crimson::net::error::bad_peer_address));
    }
    conn.peer_addr = paddr;
    logger().debug("{} UPDATE: peer_addr={}", conn, conn.peer_addr);
    conn.target_addr = conn.peer_addr;
    if (!conn.policy.lossy && !conn.policy.server && conn.target_addr.get_port() <= 0) {
      logger().warn("{} we don't know how to reconnect to peer {}",
                    conn, conn.target_addr);
      throw std::system_error(
          make_error_code(crimson::net::error::bad_peer_address));
    }

    if (conn.get_peer_id() != entity_name_t::NEW &&
        conn.get_peer_id() != client_ident.gid()) {
      logger().error("{} client_ident peer_id ({}) does not match"
                     " what it should be ({}) during accepting, abort",
                      conn, client_ident.gid(), conn.get_peer_id());
      abort_in_fault();
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
      return write_frame(ident_missing_features).then([] {
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
        // NOTE: this is following async messenger logic, but we may miss the reset event.
        execute_establishing(existing_conn, false);
        return seastar::make_ready_future<next_step_t>(next_step_t::ready);
      } else {
        return handle_existing_connection(existing_conn);
      }
    } else {
      execute_establishing(nullptr, true);
      return seastar::make_ready_future<next_step_t>(next_step_t::ready);
    }
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

    logger().debug("{} GOT ReconnectFrame: addrs={}, client_cookie={},"
                   " server_cookie={}, gs={}, cs={}, msg_seq={}",
                   conn, reconnect.addrs(),
                   reconnect.client_cookie(), reconnect.server_cookie(),
                   reconnect.global_seq(), reconnect.connect_seq(),
                   reconnect.msg_seq());

    // can peer_addrs be changed on-the-fly?
    // TODO: change peer_addr to entity_addrvec_t
    entity_addr_t paddr = reconnect.addrs().front();
    if (paddr.is_msgr2() || paddr.is_any()) {
      // good
    } else {
      logger().warn("{} peer's address {} is not v2", conn, paddr);
      throw std::system_error(
          make_error_code(crimson::net::error::bad_peer_address));
    }
    if (conn.peer_addr == entity_addr_t()) {
      conn.peer_addr = paddr;
    } else if (conn.peer_addr != paddr) {
      logger().error("{} peer identifies as {}, while conn.peer_addr={},"
                     " reconnect failed",
                     conn, paddr, conn.peer_addr);
      throw std::system_error(
          make_error_code(crimson::net::error::bad_peer_address));
    }
    peer_global_seq = reconnect.global_seq();

    SocketConnectionRef existing_conn = messenger.lookup_conn(conn.peer_addr);

    if (!existing_conn) {
      // there is no existing connection therefore cannot reconnect to previous
      // session
      logger().warn("{} server_reconnect: no existing connection from address {},"
                    " reseting client", conn, conn.peer_addr);
      return send_reset(true);
    }

    if (existing_conn->protocol->proto_type != proto_t::v2) {
      logger().warn("{} server_reconnect: existing connection {} proto version is {},"
                    "close existing and reset client.",
                    conn, *existing_conn,
                    static_cast<int>(existing_conn->protocol->proto_type));
      // NOTE: this is following async messenger logic, but we may miss the reset event.
      existing_conn->mark_down();
      return send_reset(true);
    }

    ProtocolV2 *existing_proto = dynamic_cast<ProtocolV2*>(
        existing_conn->protocol.get());
    ceph_assert(existing_proto);
    logger().debug("{}(gs={}, pgs={}, cs={}, cc={}, sc={}) re-connecting,"
                   " found existing {}(state={}, gs={}, pgs={}, cs={}, cc={}, sc={})",
                   conn, global_seq, peer_global_seq, reconnect.connect_seq(),
                   reconnect.client_cookie(), reconnect.server_cookie(),
                   existing_conn,
                   get_state_name(existing_proto->state),
                   existing_proto->global_seq,
                   existing_proto->peer_global_seq,
                   existing_proto->connect_seq,
                   existing_proto->client_cookie,
                   existing_proto->server_cookie);

    if (!validate_peer_name(existing_conn->get_peer_name())) {
      logger().error("{} server_reconnect: my peer_name doesn't match"
                     " the existing connection {}, abort", conn, existing_conn);
      abort_in_fault();
    }

    if (existing_proto->state == state_t::REPLACING) {
      logger().warn("{} server_reconnect: racing replace happened while "
                    " replacing existing connection {}, retry global.",
                    conn, *existing_conn);
      return send_retry_global(existing_proto->peer_global_seq);
    }

    if (existing_proto->client_cookie != reconnect.client_cookie()) {
      logger().warn("{} server_reconnect:"
                    " client_cookie mismatch with existing connection {},"
                    " cc={} rcc={}. I must have reset, reseting client.",
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
      logger().warn("{} server_reconnect: I was a client (cc={}) and didn't received the"
                    " server_ident with existing connection {}."
                    " Asking peer to resume session establishment",
                    conn, existing_proto->client_cookie, *existing_conn);
      return send_reset(false);
    }

    if (existing_proto->peer_global_seq > reconnect.global_seq()) {
      logger().warn("{} server_reconnect: stale global_seq: exist_pgs({}) > peer_gs({}),"
                    " with existing connection {},"
                    " ask client to retry global",
                    conn, existing_proto->peer_global_seq,
                    reconnect.global_seq(), *existing_conn);
      return send_retry_global(existing_proto->peer_global_seq);
    }

    if (existing_proto->connect_seq > reconnect.connect_seq()) {
      logger().warn("{} server_reconnect: stale peer connect_seq peer_cs({}) < exist_cs({}),"
                    " with existing connection {}, ask client to retry",
                    conn, reconnect.connect_seq(),
                    existing_proto->connect_seq, *existing_conn);
      return send_retry(existing_proto->connect_seq);
    } else if (existing_proto->connect_seq == reconnect.connect_seq()) {
      // reconnect race: both peers are sending reconnect messages
      if (existing_conn->peer_wins()) {
        logger().warn("{} server_reconnect: reconnect race detected (cs={})"
                      " and win, reusing existing {}",
                      conn, reconnect.connect_seq(), *existing_conn);
        return reuse_connection(
            existing_proto, false,
            true, reconnect.connect_seq(), reconnect.msg_seq());
      } else {
        logger().warn("{} server_reconnect: reconnect race detected (cs={})"
                      " and lose to existing {}, ask client to wait",
                      conn, reconnect.connect_seq(), *existing_conn);
        return send_wait();
      }
    } else { // existing_proto->connect_seq < reconnect.connect_seq()
      logger().warn("{} server_reconnect: stale exsiting connect_seq exist_cs({}) < peer_cs({}),"
                    " reusing existing {}",
                    conn, existing_proto->connect_seq,
                    reconnect.connect_seq(), *existing_conn);
      return reuse_connection(
          existing_proto, false,
          true, reconnect.connect_seq(), reconnect.msg_seq());
    }
  });
}

void ProtocolV2::execute_accepting()
{
  trigger_state(state_t::ACCEPTING, write_state_t::none, false);
  gate.dispatch_in_background("execute_accepting", *this, [this] {
      return seastar::futurize_invoke([this] {
          INTERCEPT_N_RW(custom_bp_t::SOCKET_ACCEPTED);
          auth_meta = seastar::make_lw_shared<AuthConnectionMeta>();
          session_stream_handlers = { nullptr, nullptr };
          enable_recording();
          return banner_exchange(false);
        }).then([this] (auto&& ret) {
          auto [_peer_type, _my_addr_from_peer] = std::move(ret);
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
                make_error_code(crimson::net::error::bad_peer_address));
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
              return server_reconnect();
            default: {
              unexpected_tag(tag, conn, "post_server_auth");
              return seastar::make_ready_future<next_step_t>(next_step_t::none);
            }
          }
        }).then([this] (next_step_t next) {
          switch (next) {
           case next_step_t::ready:
            assert(state != state_t::ACCEPTING);
            break;
           case next_step_t::wait:
            if (unlikely(state != state_t::ACCEPTING)) {
              logger().debug("{} triggered {} at the end of execute_accepting()",
                             conn, get_state_name(state));
              abort_protocol();
            }
            logger().info("{} execute_accepting(): going to SERVER_WAIT", conn);
            execute_server_wait();
            break;
           default:
            ceph_abort("impossible next step");
          }
        }).handle_exception([this] (std::exception_ptr eptr) {
          logger().info("{} execute_accepting(): fault at {}, going to CLOSING -- {}",
                        conn, get_state_name(state), eptr);
          close(false);
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

// ESTABLISHING

void ProtocolV2::execute_establishing(
    SocketConnectionRef existing_conn, bool dispatch_reset) {
  if (unlikely(state != state_t::ACCEPTING)) {
    logger().debug("{} triggered {} before execute_establishing()",
                   conn, get_state_name(state));
    abort_protocol();
  }

  auto accept_me = [this] {
    messenger.register_conn(
      seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
    messenger.unaccept_conn(
      seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
  };

  trigger_state(state_t::ESTABLISHING, write_state_t::delay, false);
  if (existing_conn) {
    existing_conn->protocol->close(dispatch_reset, std::move(accept_me));
  } else {
    accept_me();
  }

  dispatcher->ms_handle_accept(
      seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));

  gated_execute("execute_establishing", [this] {
    return seastar::futurize_invoke([this] {
      return send_server_ident();
    }).then([this] {
      if (unlikely(state != state_t::ESTABLISHING)) {
        logger().debug("{} triggered {} at the end of execute_establishing()",
                       conn, get_state_name(state));
        abort_protocol();
      }
      logger().info("{} established: gs={}, pgs={}, cs={}, client_cookie={},"
                    " server_cookie={}, in_seq={}, out_seq={}, out_q={}",
                    conn, global_seq, peer_global_seq, connect_seq,
                    client_cookie, server_cookie, conn.in_seq,
                    conn.out_seq, conn.out_q.size());
      execute_ready(false);
    }).handle_exception([this] (std::exception_ptr eptr) {
      if (state != state_t::ESTABLISHING) {
        logger().info("{} execute_establishing() protocol aborted at {} -- {}",
                      conn, get_state_name(state), eptr);
        assert(state == state_t::CLOSING ||
               state == state_t::REPLACING);
        return;
      }
      fault(false, "execute_establishing()", eptr);
    });
  });
}

// ESTABLISHING or REPLACING state

seastar::future<>
ProtocolV2::send_server_ident()
{
  // send_server_ident() logic

  // refered to async-conn v2: not assign gs to global_seq
  return messenger.get_global_seq().then([this] (auto gs) {
    logger().debug("{} UPDATE: gs={} for server ident", conn, global_seq);

    // this is required for the case when this connection is being replaced
    requeue_up_to(0);
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

    return write_frame(server_ident);
  });
}

// REPLACING state

void ProtocolV2::trigger_replacing(bool reconnect,
                                   bool do_reset,
                                   SocketRef&& new_socket,
                                   AuthConnectionMetaRef&& new_auth_meta,
                                   ceph::crypto::onwire::rxtx_t new_rxtx,
                                   uint64_t new_peer_global_seq,
                                   uint64_t new_client_cookie,
                                   entity_name_t new_peer_name,
                                   uint64_t new_conn_features,
                                   bool tx_is_rev1,
                                   bool rx_is_rev1,
                                   uint64_t new_connect_seq,
                                   uint64_t new_msg_seq)
{
  trigger_state(state_t::REPLACING, write_state_t::delay, false);
  if (socket) {
    socket->shutdown();
  }
  dispatcher->ms_handle_accept(
      seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  gate.dispatch_in_background("trigger_replacing", *this,
                 [this,
                  reconnect,
                  do_reset,
                  new_socket = std::move(new_socket),
                  new_auth_meta = std::move(new_auth_meta),
                  new_rxtx = std::move(new_rxtx),
                  tx_is_rev1, rx_is_rev1,
                  new_client_cookie, new_peer_name,
                  new_conn_features, new_peer_global_seq,
                  new_connect_seq, new_msg_seq] () mutable {
    return wait_write_exit().then([this, do_reset] {
      if (do_reset) {
        reset_session(true);
      }
      protocol_timer.cancel();
      return execution_done.get_future();
    }).then([this,
             reconnect,
             new_socket = std::move(new_socket),
             new_auth_meta = std::move(new_auth_meta),
             new_rxtx = std::move(new_rxtx),
             tx_is_rev1, rx_is_rev1,
             new_client_cookie, new_peer_name,
             new_conn_features, new_peer_global_seq,
             new_connect_seq, new_msg_seq] () mutable {
      if (unlikely(state != state_t::REPLACING)) {
        return new_socket->close().then([sock = std::move(new_socket)] {
          abort_protocol();
        });
      }

      if (socket) {
        gate.dispatch_in_background("close_socket_replacing", *this,
                       [sock = std::move(socket)] () mutable {
          return sock->close().then([sock = std::move(sock)] {});
        });
      }
      socket = std::move(new_socket);
      auth_meta = std::move(new_auth_meta);
      session_stream_handlers = std::move(new_rxtx);
      record_io = false;
      peer_global_seq = new_peer_global_seq;

      if (reconnect) {
        connect_seq = new_connect_seq;
        // send_reconnect_ok() logic
        requeue_up_to(new_msg_seq);
        auto reconnect_ok = ReconnectOkFrame::Encode(conn.in_seq);
        logger().debug("{} WRITE ReconnectOkFrame: msg_seq={}", conn, conn.in_seq);
        return write_frame(reconnect_ok);
      } else {
        client_cookie = new_client_cookie;
        assert(conn.get_peer_type() == new_peer_name.type());
        if (conn.get_peer_id() == entity_name_t::NEW) {
          conn.set_peer_id(new_peer_name.num());
        }
        connection_features = new_conn_features;
        tx_frame_asm.set_is_rev1(tx_is_rev1);
        rx_frame_asm.set_is_rev1(rx_is_rev1);
        return send_server_ident();
      }
    }).then([this, reconnect] {
      if (unlikely(state != state_t::REPLACING)) {
        logger().debug("{} triggered {} at the end of trigger_replacing()",
                       conn, get_state_name(state));
        abort_protocol();
      }
      logger().info("{} replaced ({}):"
                    " gs={}, pgs={}, cs={}, client_cookie={}, server_cookie={},"
                    " in_seq={}, out_seq={}, out_q={}",
                    conn, reconnect ? "reconnected" : "connected",
                    global_seq, peer_global_seq, connect_seq, client_cookie,
                    server_cookie, conn.in_seq, conn.out_seq, conn.out_q.size());
      execute_ready(false);
    }).handle_exception([this] (std::exception_ptr eptr) {
      if (state != state_t::REPLACING) {
        logger().info("{} trigger_replacing(): protocol aborted at {} -- {}",
                      conn, get_state_name(state), eptr);
        assert(state == state_t::CLOSING);
        return;
      }
      fault(true, "trigger_replacing()", eptr);
    });
  });
}

// READY state

ceph::bufferlist ProtocolV2::do_sweep_messages(
    const std::deque<MessageRef>& msgs,
    size_t num_msgs,
    bool require_keepalive,
    std::optional<utime_t> _keepalive_ack,
    bool require_ack)
{
  ceph::bufferlist bl;

  if (unlikely(require_keepalive)) {
    auto keepalive_frame = KeepAliveFrame::Encode();
    bl.append(keepalive_frame.get_buffer(tx_frame_asm));
    INTERCEPT_FRAME(ceph::msgr::v2::Tag::KEEPALIVE2, bp_type_t::WRITE);
  }

  if (unlikely(_keepalive_ack.has_value())) {
    auto keepalive_ack_frame = KeepAliveFrameAck::Encode(*_keepalive_ack);
    bl.append(keepalive_ack_frame.get_buffer(tx_frame_asm));
    INTERCEPT_FRAME(ceph::msgr::v2::Tag::KEEPALIVE2_ACK, bp_type_t::WRITE);
  }

  if (require_ack && !num_msgs) {
    auto ack_frame = AckFrame::Encode(conn.in_seq);
    bl.append(ack_frame.get_buffer(tx_frame_asm));
    INTERCEPT_FRAME(ceph::msgr::v2::Tag::ACK, bp_type_t::WRITE);
  }

  std::for_each(msgs.begin(), msgs.begin()+num_msgs, [this, &bl](const MessageRef& msg) {
    // TODO: move to common code
    // set priority
    msg->get_header().src = messenger.get_myname();

    msg->encode(conn.features, 0);

    ceph_assert(!msg->get_seq() && "message already has seq");
    msg->set_seq(++conn.out_seq);

    ceph_msg_header &header = msg->get_header();
    ceph_msg_footer &footer = msg->get_footer();

    ceph_msg_header2 header2{header.seq,        header.tid,
                             header.type,       header.priority,
                             header.version,
                             init_le32(0),      header.data_off,
                             init_le64(conn.in_seq),
                             footer.flags,      header.compat_version,
                             header.reserved};

    auto message = MessageFrame::Encode(header2,
        msg->get_payload(), msg->get_middle(), msg->get_data());
    logger().debug("{} --> #{} === {} ({})",
		   conn, msg->get_seq(), *msg, msg->get_type());
    bl.append(message.get_buffer(tx_frame_asm));
    INTERCEPT_FRAME(ceph::msgr::v2::Tag::MESSAGE, bp_type_t::WRITE);
  });

  return bl;
}

seastar::future<> ProtocolV2::read_message(utime_t throttle_stamp)
{
  return read_frame_payload()
  .then([this, throttle_stamp] {
    utime_t recv_stamp{seastar::lowres_system_clock::now()};

    // we need to get the size before std::moving segments data
    const size_t cur_msg_size = get_current_msg_size();
    auto msg_frame = MessageFrame::Decode(rx_segments_data);
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
                           init_le32(msg_frame.front_len()),
                           init_le32(msg_frame.middle_len()),
                           init_le32(msg_frame.data_len()),
                           current_header.data_off,
                           conn.get_peer_name(),
                           current_header.compat_version,
                           current_header.reserved,
                           init_le32(0)};
    ceph_msg_footer footer{init_le32(0), init_le32(0),
                           init_le32(0), init_le64(0), current_header.flags};

    auto pconn = seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this());
    Message *message = decode_message(nullptr, 0, header, footer,
        msg_frame.front(), msg_frame.middle(), msg_frame.data(),
        std::move(pconn));
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
          local_conf()->ms_die_on_old_message) {
        ceph_assert(0 == "old msgs despite reconnect_seq feature");
      }
      return;
    } else if (message->get_seq() > cur_seq + 1) {
      logger().error("{} missed message? skipped from seq {} to {}",
                     conn, cur_seq, message->get_seq());
      if (local_conf()->ms_die_on_skipped_message) {
        ceph_assert(0 == "skipped incoming seq");
      }
    }

    // note last received message.
    conn.in_seq = message->get_seq();
    logger().debug("{} <== #{} === {} ({})",
		   conn, message->get_seq(), *message, message->get_type());
    notify_ack();
    ack_writes(current_header.ack_seq);

    // TODO: change MessageRef with seastar::shared_ptr
    auto msg_ref = MessageRef{message, false};
    std::ignore = dispatcher->ms_dispatch(&conn, std::move(msg_ref));
  });
}

void ProtocolV2::execute_ready(bool dispatch_connect)
{
  assert(conn.policy.lossy || (client_cookie != 0 && server_cookie != 0));
  trigger_state(state_t::READY, write_state_t::open, false);
  if (dispatch_connect) {
    dispatcher->ms_handle_connect(
	seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  }
#ifdef UNIT_TESTS_BUILT
  if (conn.interceptor) {
    conn.interceptor->register_conn_ready(conn);
  }
#endif
  gated_execute("execute_ready", [this] {
    protocol_timer.cancel();
    return seastar::keep_doing([this] {
      return read_main_preamble()
      .then([this] (Tag tag) {
        switch (tag) {
          case Tag::MESSAGE: {
            return seastar::futurize_invoke([this] {
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
              logger().debug("{} GOT AckFrame: seq={}", conn, ack.seq());
              ack_writes(ack.seq());
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
      if (state != state_t::READY) {
        logger().info("{} execute_ready(): protocol aborted at {} -- {}",
                      conn, get_state_name(state), eptr);
        assert(state == state_t::REPLACING ||
               state == state_t::CLOSING);
        return;
      }
      fault(false, "execute_ready()", eptr);
    });
  });
}

// STANDBY state

void ProtocolV2::execute_standby()
{
  trigger_state(state_t::STANDBY, write_state_t::delay, true);
  if (socket) {
    socket->shutdown();
  }
}

void ProtocolV2::notify_write()
{
  if (unlikely(state == state_t::STANDBY && !conn.policy.server)) {
    logger().info("{} notify_write(): at {}, going to CONNECTING",
                  conn, get_state_name(state));
    execute_connecting();
  }
}

// WAIT state

void ProtocolV2::execute_wait(bool max_backoff)
{
  trigger_state(state_t::WAIT, write_state_t::delay, true);
  if (socket) {
    socket->shutdown();
  }
  gated_execute("execute_wait", [this, max_backoff] {
    double backoff = protocol_timer.last_dur();
    if (max_backoff) {
      backoff = local_conf().get_val<double>("ms_max_backoff");
    } else if (backoff > 0) {
      backoff = std::min(local_conf().get_val<double>("ms_max_backoff"), 2 * backoff);
    } else {
      backoff = local_conf().get_val<double>("ms_initial_backoff");
    }
    return protocol_timer.backoff(backoff).then([this] {
      if (unlikely(state != state_t::WAIT)) {
        logger().debug("{} triggered {} at the end of execute_wait()",
                       conn, get_state_name(state));
        abort_protocol();
      }
      logger().info("{} execute_wait(): going to CONNECTING", conn);
      execute_connecting();
    }).handle_exception([this] (std::exception_ptr eptr) {
      logger().info("{} execute_wait(): protocol aborted at {} -- {}",
                    conn, get_state_name(state), eptr);
      assert(state == state_t::REPLACING ||
             state == state_t::CLOSING);
    });
  });
}

// SERVER_WAIT state

void ProtocolV2::execute_server_wait()
{
  trigger_state(state_t::SERVER_WAIT, write_state_t::delay, false);
  gated_execute("execute_server_wait", [this] {
    return read_exactly(1).then([this] (auto bl) {
      logger().warn("{} SERVER_WAIT got read, abort", conn);
      abort_in_fault();
    }).handle_exception([this] (std::exception_ptr eptr) {
      logger().info("{} execute_server_wait(): fault at {}, going to CLOSING -- {}",
                    conn, get_state_name(state), eptr);
      close(false);
    });
  });
}

// CLOSING state

void ProtocolV2::trigger_close()
{
  if (state == state_t::ACCEPTING || state == state_t::SERVER_WAIT) {
    messenger.unaccept_conn(
      seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
  } else if (state >= state_t::ESTABLISHING && state < state_t::CLOSING) {
    messenger.unregister_conn(
      seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
  } else {
    // cannot happen
    ceph_assert(false);
  }

  protocol_timer.cancel();
  messenger.closing_conn(
      seastar::static_pointer_cast<SocketConnection>(
	conn.shared_from_this()));
  trigger_state(state_t::CLOSING, write_state_t::drop, false);
}

void ProtocolV2::on_closed()
{
  messenger.closed_conn(
      seastar::static_pointer_cast<SocketConnection>(
	conn.shared_from_this()));
}

void ProtocolV2::print(std::ostream& out) const
{
  out << conn;
}

} // namespace crimson::net
