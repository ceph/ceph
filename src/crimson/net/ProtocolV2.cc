// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ProtocolV2.h"

#include <fmt/format.h>
#include <fmt/ranges.h>
#include "include/msgr.h"
#include "include/random.h"
#include "msg/msg_fmt.h"

#include "crimson/auth/AuthClient.h"
#include "crimson/auth/AuthServer.h"
#include "crimson/common/formatter.h"
#include "crimson/common/log.h"

#include "Errors.h"
#include "SocketMessenger.h"

using namespace ceph::msgr::v2;
using crimson::common::local_conf;

namespace {

// TODO: CEPH_MSGR2_FEATURE_COMPRESSION
const uint64_t CRIMSON_MSGR2_SUPPORTED_FEATURES =
  (CEPH_MSGR2_FEATURE_REVISION_1 |
   // CEPH_MSGR2_FEATURE_COMPRESSION |
   UINT64_C(0));

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

#define ABORT_IN_CLOSE(is_dispatch_reset) { \
  do_close(is_dispatch_reset);              \
  abort_protocol();                         \
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

ProtocolV2::ProtocolV2(SocketConnection& conn,
                       IOHandler &io_handler)
  : conn{conn},
    messenger{conn.messenger},
    io_handler{io_handler},
    frame_assembler{FrameAssemblerV2::create(conn)},
    auth_meta{seastar::make_lw_shared<AuthConnectionMeta>()},
    protocol_timer{conn}
{
  io_states = io_handler.get_states();
}

ProtocolV2::~ProtocolV2() {}

void ProtocolV2::start_connect(const entity_addr_t& _peer_addr,
                               const entity_name_t& _peer_name)
{
  assert(seastar::this_shard_id() == conn.get_messenger_shard_id());
  ceph_assert(state == state_t::NONE);
  ceph_assert(!gate.is_closed());
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

void ProtocolV2::start_accept(SocketFRef&& new_socket,
                              const entity_addr_t& _peer_addr)
{
  assert(seastar::this_shard_id() == conn.get_messenger_shard_id());
  ceph_assert(state == state_t::NONE);
  // until we know better
  conn.target_addr = _peer_addr;
  frame_assembler->set_socket(std::move(new_socket));
  has_socket = true;
  is_socket_valid = true;
  logger().info("{} ProtocolV2::start_accept(): target_addr={}", conn, _peer_addr);
  messenger.accept_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));

  auto cc_seq = crosscore.prepare_submit();
  gate.dispatch_in_background("set_accepted_sid", conn, [this, cc_seq] {
    return io_handler.set_accepted_sid(
        cc_seq,
        frame_assembler->get_socket_shard_id(),
        seastar::make_foreign(conn.shared_from_this()));
  });

  execute_accepting();
}

void ProtocolV2::trigger_state_phase1(state_t new_state)
{
  ceph_assert_always(!gate.is_closed());
  if (new_state == state) {
    logger().error("{} is not allowed to re-trigger state {}",
                   conn, get_state_name(state));
    ceph_abort();
  }
  if (state == state_t::CLOSING) {
    logger().error("{} CLOSING is not allowed to trigger state {}",
                   conn, get_state_name(new_state));
    ceph_abort();
  }
  logger().debug("{} TRIGGER {}, was {}",
                 conn, get_state_name(new_state), get_state_name(state));

  if (state == state_t::READY) {
    // from READY
    ceph_assert_always(!need_exit_io);
    ceph_assert_always(!pr_exit_io.has_value());
    need_exit_io = true;
    pr_exit_io = seastar::shared_promise<>();
  }

  if (new_state == state_t::STANDBY && !conn.policy.server) {
    need_notify_out = true;
  } else {
    need_notify_out = false;
  }

  state = new_state;
}

void ProtocolV2::trigger_state_phase2(
    state_t new_state, io_state_t new_io_state)
{
  ceph_assert_always(new_state == state);
  ceph_assert_always(!gate.is_closed());
  ceph_assert_always(!pr_switch_io_shard.has_value());

  FrameAssemblerV2Ref fa;
  if (new_state == state_t::READY) {
    assert(new_io_state == io_state_t::open);
    assert(io_handler.get_shard_id() ==
           frame_assembler->get_socket_shard_id());
    frame_assembler->set_shard_id(io_handler.get_shard_id());
    fa = std::move(frame_assembler);
  } else {
    assert(new_io_state != io_state_t::open);
  }

  auto cc_seq = crosscore.prepare_submit();
  logger().debug("{} send {} IOHandler::set_io_state(): new_state={}, new_io_state={}, "
                 "fa={}, set_notify_out={}",
                 conn, cc_seq, get_state_name(new_state), new_io_state,
                 fa ? fmt::format("(sid={})", fa->get_shard_id()) : "N/A",
                 need_notify_out);
  gate.dispatch_in_background(
      "set_io_state", conn,
      [this, cc_seq, new_io_state, fa=std::move(fa)]() mutable {
    return seastar::smp::submit_to(
        io_handler.get_shard_id(),
        [this, cc_seq, new_io_state,
         fa=std::move(fa), set_notify_out=need_notify_out]() mutable {
      return io_handler.set_io_state(
          cc_seq, new_io_state, std::move(fa), set_notify_out);
    });
  });

  if (need_exit_io) {
    // from READY
    auto cc_seq = crosscore.prepare_submit();
    logger().debug("{} send {} IOHandler::wait_io_exit_dispatching() ...",
                   conn, cc_seq);
    assert(pr_exit_io.has_value());
    assert(new_io_state != io_state_t::open);
    need_exit_io = false;
    gate.dispatch_in_background("exit_io", conn, [this, cc_seq] {
      return seastar::smp::submit_to(
          io_handler.get_shard_id(), [this, cc_seq] {
        return io_handler.wait_io_exit_dispatching(cc_seq);
      }).then([this, cc_seq](auto ret) {
        logger().debug("{} finish {} IOHandler::wait_io_exit_dispatching(), {}",
                       conn, cc_seq, ret.io_states);
        frame_assembler = std::move(ret.frame_assembler);
        assert(seastar::this_shard_id() == conn.get_messenger_shard_id());
        ceph_assert_always(
            seastar::this_shard_id() == frame_assembler->get_shard_id());
        ceph_assert_always(!frame_assembler->is_socket_valid());
        assert(!need_exit_io);
        io_states = ret.io_states;
        pr_exit_io->set_value();
        pr_exit_io = std::nullopt;
      });
    });
  }
}

void ProtocolV2::fault(
    state_t expected_state,
    const char *where,
    std::exception_ptr eptr)
{
  assert(expected_state == state_t::CONNECTING ||
         expected_state == state_t::ESTABLISHING ||
         expected_state == state_t::REPLACING ||
         expected_state == state_t::READY);
  const char *e_what;
  try {
    std::rethrow_exception(eptr);
  } catch (std::exception &e) {
    e_what = e.what();
  }

  if (state != expected_state) {
    logger().info("{} protocol {} {} is aborted at inconsistent {} -- {}",
                  conn,
                  get_state_name(expected_state),
                  where,
                  get_state_name(state),
                  e_what);
#ifndef NDEBUG
    if (expected_state == state_t::REPLACING) {
      assert(state == state_t::CLOSING);
    } else if (expected_state == state_t::READY) {
      assert(state == state_t::CLOSING ||
             state == state_t::REPLACING ||
             state == state_t::CONNECTING ||
             state == state_t::STANDBY);
    } else {
      assert(state == state_t::CLOSING ||
             state == state_t::REPLACING);
    }
#endif
    return;
  }
  assert(state == expected_state);

  if (state != state_t::CONNECTING && conn.policy.lossy) {
    // socket will be shutdown in do_close()
    logger().info("{} protocol {} {} fault on lossy channel, going to CLOSING -- {}",
                  conn, get_state_name(state), where, e_what);
    do_close(true);
    return;
  }

  if (likely(has_socket)) {
    if (likely(is_socket_valid)) {
      ceph_assert_always(state != state_t::READY);
      frame_assembler->shutdown_socket<true>(&gate);
      is_socket_valid = false;
    } else {
      ceph_assert_always(state != state_t::ESTABLISHING);
    }
  } else { // !has_socket
    ceph_assert_always(state == state_t::CONNECTING);
    assert(!is_socket_valid);
  }

  if (conn.policy.server ||
      (conn.policy.standby && !io_states.is_out_queued_or_sent())) {
    if (conn.policy.server) {
      logger().info("{} protocol {} {} fault as server, going to STANDBY {} -- {}",
                    conn,
                    get_state_name(state),
                    where,
                    io_states,
                    e_what);
    } else {
      logger().info("{} protocol {} {} fault with nothing to send, going to STANDBY {} -- {}",
                    conn,
                    get_state_name(state),
                    where,
                    io_states,
                    e_what);
    }
    execute_standby();
  } else if (state == state_t::CONNECTING ||
             state == state_t::REPLACING) {
    logger().info("{} protocol {} {} fault, going to WAIT {} -- {}",
                  conn,
                  get_state_name(state),
                  where,
                  io_states,
                  e_what);
    execute_wait(false);
  } else {
    assert(state == state_t::READY ||
           state == state_t::ESTABLISHING);
    logger().info("{} protocol {} {} fault, going to CONNECTING {} -- {}",
                  conn,
                  get_state_name(state),
                  where,
                  io_states,
                  e_what);
    execute_connecting();
  }
}

void ProtocolV2::reset_session(bool full)
{
  server_cookie = 0;
  connect_seq = 0;
  if (full) {
    client_cookie = generate_client_cookie();
    peer_global_seq = 0;
  }

  auto cc_seq = crosscore.prepare_submit();
  logger().debug("{} send {} IOHandler::reset_session({})",
                 conn, cc_seq, full);
  io_states.reset_session(full);
  gate.dispatch_in_background(
      "reset_session", conn, [this, cc_seq, full] {
    return seastar::smp::submit_to(
        io_handler.get_shard_id(), [this, cc_seq, full] {
      return io_handler.reset_session(cc_seq, full);
    });
  });
  // user can make changes
}

seastar::future<std::tuple<entity_type_t, entity_addr_t>>
ProtocolV2::banner_exchange(bool is_connect)
{
  // 1. prepare and send banner
  bufferlist banner_payload;
  encode((uint64_t)CRIMSON_MSGR2_SUPPORTED_FEATURES, banner_payload, 0);
  encode((uint64_t)CEPH_MSGR2_REQUIRED_FEATURES, banner_payload, 0);

  bufferlist bl;
  bl.append(CEPH_BANNER_V2_PREFIX, strlen(CEPH_BANNER_V2_PREFIX));
  auto len_payload = static_cast<uint16_t>(banner_payload.length());
  encode(len_payload, bl, 0);
  bl.claim_append(banner_payload);
  logger().debug("{} SEND({}) banner: len_payload={}, supported={}, "
                 "required={}, banner=\"{}\"",
                 conn, bl.length(), len_payload,
                 CRIMSON_MSGR2_SUPPORTED_FEATURES,
                 CEPH_MSGR2_REQUIRED_FEATURES,
                 CEPH_BANNER_V2_PREFIX);
#ifdef UNIT_TESTS_BUILT
  return frame_assembler->intercept_frame(custom_bp_t::BANNER_WRITE, true
  ).then([this, bl=std::move(bl)]() mutable {
    return frame_assembler->write_flush(std::move(bl));
  }
#else
  return frame_assembler->write_flush(std::move(bl)
#endif
  ).then([this] {
    // 2. read peer banner
    unsigned banner_len = strlen(CEPH_BANNER_V2_PREFIX) + sizeof(ceph_le16);
#ifdef UNIT_TESTS_BUILT
    return frame_assembler->intercept_frame(custom_bp_t::BANNER_READ, false
    ).then([this, banner_len] {
      return frame_assembler->read_exactly(banner_len);
    });
#else
    return frame_assembler->read_exactly(banner_len);
#endif
  }).then([this](auto bptr) {
    // 3. process peer banner and read banner_payload
    unsigned banner_prefix_len = strlen(CEPH_BANNER_V2_PREFIX);
    logger().debug("{} RECV({}) banner: \"{}\"",
                   conn, bptr.length(),
                   std::string(bptr.c_str(), banner_prefix_len));

    if (memcmp(bptr.c_str(), CEPH_BANNER_V2_PREFIX, banner_prefix_len) != 0) {
      if (memcmp(bptr.c_str(), CEPH_BANNER, strlen(CEPH_BANNER)) == 0) {
        logger().warn("{} peer is using V1 protocol", conn);
      } else {
        logger().warn("{} peer sent bad banner", conn);
      }
      abort_in_fault();
    }

    bptr.set_offset(bptr.offset() + banner_prefix_len);
    bptr.set_length(bptr.length() - banner_prefix_len);
    assert(bptr.length() == sizeof(ceph_le16));

    uint16_t payload_len;
    bufferlist buf;
    buf.append(std::move(bptr));
    auto ti = buf.cbegin();
    try {
      decode(payload_len, ti);
    } catch (const buffer::error &e) {
      logger().warn("{} decode banner payload len failed", conn);
      abort_in_fault();
    }
    logger().debug("{} GOT banner: payload_len={}", conn, payload_len);
#ifdef UNIT_TESTS_BUILT
    return frame_assembler->intercept_frame(
      custom_bp_t::BANNER_PAYLOAD_READ, false
    ).then([this, payload_len] {
      return frame_assembler->read(payload_len);
    });
#else
    return frame_assembler->read(payload_len);
#endif
  }).then([this, is_connect] (bufferlist bl) {
    // 4. process peer banner_payload and send HelloFrame
    auto p = bl.cbegin();
    uint64_t _peer_supported_features;
    uint64_t _peer_required_features;
    try {
      decode(_peer_supported_features, p);
      decode(_peer_required_features, p);
    } catch (const buffer::error &e) {
      logger().warn("{} decode banner payload failed", conn);
      abort_in_fault();
    }
    logger().debug("{} RECV({}) banner features: supported={} required={}",
                   conn, bl.length(),
                   _peer_supported_features, _peer_required_features);

    // Check feature bit compatibility
    uint64_t supported_features = CRIMSON_MSGR2_SUPPORTED_FEATURES;
    uint64_t required_features = CEPH_MSGR2_REQUIRED_FEATURES;
    if ((required_features & _peer_supported_features) != required_features) {
      logger().error("{} peer does not support all required features"
                     " required={} peer_supported={}",
                     conn, required_features, _peer_supported_features);
      ABORT_IN_CLOSE(is_connect);
    }
    if ((supported_features & _peer_required_features) != _peer_required_features) {
      logger().error("{} we do not support all peer required features"
                     " peer_required={} supported={}",
                     conn, _peer_required_features, supported_features);
      ABORT_IN_CLOSE(is_connect);
    }
    peer_supported_features = _peer_supported_features;
    bool is_rev1 = HAVE_MSGR2_FEATURE(peer_supported_features, REVISION_1);
    frame_assembler->set_is_rev1(is_rev1);

    auto hello = HelloFrame::Encode(messenger.get_mytype(),
                                    conn.target_addr);
    logger().debug("{} WRITE HelloFrame: my_type={}, peer_addr={}",
                   conn, ceph_entity_type_name(messenger.get_mytype()),
                   conn.target_addr);
    return frame_assembler->write_flush_frame(hello);
  }).then([this] {
    //5. read peer HelloFrame
    return frame_assembler->read_main_preamble();
  }).then([this](auto ret) {
    expect_tag(Tag::HELLO, ret.tag, conn, "read_hello_frame");
    return frame_assembler->read_frame_payload();
  }).then([this](auto payload) {
    // 6. process peer HelloFrame
    auto hello = HelloFrame::Decode(payload->back());
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
  return frame_assembler->read_main_preamble(
  ).then([this](auto ret) {
    switch (ret.tag) {
      case Tag::AUTH_BAD_METHOD:
        return frame_assembler->read_frame_payload(
        ).then([this](auto payload) {
          // handle_auth_bad_method() logic
          auto bad_method = AuthBadMethodFrame::Decode(payload->back());
          logger().warn("{} GOT AuthBadMethodFrame: method={} result={}, "
                        "allowed_methods={}, allowed_modes={}",
                        conn, bad_method.method(), cpp_strerror(bad_method.result()),
                        bad_method.allowed_methods(), bad_method.allowed_modes());
          ceph_assert(messenger.get_auth_client());
          int r = messenger.get_auth_client()->handle_auth_bad_method(
              conn, *auth_meta,
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
        return frame_assembler->read_frame_payload(
        ).then([this](auto payload) {
          // handle_auth_reply_more() logic
          auto auth_more = AuthReplyMoreFrame::Decode(payload->back());
          logger().debug("{} GOT AuthReplyMoreFrame: payload_len={}",
                         conn, auth_more.auth_payload().length());
          ceph_assert(messenger.get_auth_client());
          // let execute_connecting() take care of the thrown exception
          auto reply = messenger.get_auth_client()->handle_auth_reply_more(
            conn, *auth_meta, auth_more.auth_payload());
          auto more_reply = AuthRequestMoreFrame::Encode(reply);
          logger().debug("{} WRITE AuthRequestMoreFrame: payload_len={}",
                         conn, reply.length());
          return frame_assembler->write_flush_frame(more_reply);
        }).then([this] {
          return handle_auth_reply();
        });
      case Tag::AUTH_DONE:
        return frame_assembler->read_frame_payload(
        ).then([this](auto payload) {
          // handle_auth_done() logic
          auto auth_done = AuthDoneFrame::Decode(payload->back());
          logger().debug("{} GOT AuthDoneFrame: gid={}, con_mode={}, payload_len={}",
                         conn, auth_done.global_id(),
                         ceph_con_mode_name(auth_done.con_mode()),
                         auth_done.auth_payload().length());
          ceph_assert(messenger.get_auth_client());
          int r = messenger.get_auth_client()->handle_auth_done(
              conn,
              *auth_meta,
              auth_done.global_id(),
              auth_done.con_mode(),
              auth_done.auth_payload());
          if (r < 0) {
            logger().warn("{} auth_client handle_auth_done returned {}", conn, r);
            abort_in_fault();
          }
          auth_meta->con_mode = auth_done.con_mode();
          frame_assembler->create_session_stream_handlers(*auth_meta, false);
          return finish_auth();
        });
      default: {
        unexpected_tag(ret.tag, conn, "handle_auth_reply");
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
      messenger.get_auth_client()->get_auth_request(conn, *auth_meta);
    auth_meta->auth_method = auth_method;
    auto frame = AuthRequestFrame::Encode(auth_method, preferred_modes, bl);
    logger().debug("{} WRITE AuthRequestFrame: method={},"
                   " preferred_modes={}, payload_len={}",
                   conn, auth_method, preferred_modes, bl.length());
    return frame_assembler->write_flush_frame(frame
    ).then([this] {
      return handle_auth_reply();
    });
  } catch (const crimson::auth::error& e) {
    logger().error("{} get_initial_auth_request returned {}", conn, e.what());
    ABORT_IN_CLOSE(true);
    return seastar::now();
  }
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::process_wait()
{
  return frame_assembler->read_frame_payload(
  ).then([this](auto payload) {
    // handle_wait() logic
    logger().debug("{} GOT WaitFrame", conn);
    WaitFrame::Decode(payload->back());
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
  return frame_assembler->write_flush_frame(client_ident
  ).then([this] {
    return frame_assembler->read_main_preamble();
  }).then([this](auto ret) {
    switch (ret.tag) {
      case Tag::IDENT_MISSING_FEATURES:
        return frame_assembler->read_frame_payload(
        ).then([this](auto payload) {
          // handle_ident_missing_features() logic
          auto ident_missing = IdentMissingFeaturesFrame::Decode(payload->back());
          logger().warn("{} GOT IdentMissingFeaturesFrame: features={}"
                        " (client does not support all server features)",
                        conn, ident_missing.features());
          abort_in_fault();
          return next_step_t::none;
        });
      case Tag::WAIT:
        return process_wait();
      case Tag::SERVER_IDENT:
        return frame_assembler->read_frame_payload(
        ).then([this](auto payload) {
          if (unlikely(state != state_t::CONNECTING)) {
            logger().debug("{} triggered {} at receiving SERVER_IDENT",
                           conn, get_state_name(state));
            abort_protocol();
          }

          // handle_server_ident() logic
          auto cc_seq = crosscore.prepare_submit();
          logger().debug("{} send {} IOHandler::requeue_out_sent()",
                         conn, cc_seq);
          io_states.requeue_out_sent();
          gate.dispatch_in_background(
              "requeue_out_sent", conn, [this, cc_seq] {
            return seastar::smp::submit_to(
                io_handler.get_shard_id(), [this, cc_seq] {
              return io_handler.requeue_out_sent(cc_seq);
            });
          });

          auto server_ident = ServerIdentFrame::Decode(payload->back());
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
            ABORT_IN_CLOSE(true);
          }
          conn.set_peer_id(server_ident.gid());
          conn.set_features(server_ident.supported_features() &
                            conn.policy.features_supported);
          logger().debug("{} UPDATE: features={}", conn, conn.get_features());
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
        unexpected_tag(ret.tag, conn, "post_client_connect");
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
                                          io_states.in_seq);
  logger().debug("{} WRITE ReconnectFrame: addrs={}, client_cookie={},"
                 " server_cookie={}, gs={}, cs={}, in_seq={}",
                 conn, messenger.get_myaddrs(),
                 client_cookie, server_cookie,
                 global_seq, connect_seq, io_states.in_seq);
  return frame_assembler->write_flush_frame(reconnect).then([this] {
    return frame_assembler->read_main_preamble();
  }).then([this](auto ret) {
    switch (ret.tag) {
      case Tag::SESSION_RETRY_GLOBAL:
        return frame_assembler->read_frame_payload(
        ).then([this](auto payload) {
          // handle_session_retry_global() logic
          auto retry = RetryGlobalFrame::Decode(payload->back());
          logger().warn("{} GOT RetryGlobalFrame: gs={}",
                        conn, retry.global_seq());
          global_seq = messenger.get_global_seq(retry.global_seq());
          logger().warn("{} UPDATE: gs={} for retry global", conn, global_seq);
          return client_reconnect();
        });
      case Tag::SESSION_RETRY:
        return frame_assembler->read_frame_payload(
        ).then([this](auto payload) {
          // handle_session_retry() logic
          auto retry = RetryFrame::Decode(payload->back());
          logger().warn("{} GOT RetryFrame: cs={}",
                        conn, retry.connect_seq());
          connect_seq = retry.connect_seq() + 1;
          logger().warn("{} UPDATE: cs={}", conn, connect_seq);
          return client_reconnect();
        });
      case Tag::SESSION_RESET:
        return frame_assembler->read_frame_payload(
        ).then([this](auto payload) {
          if (unlikely(state != state_t::CONNECTING)) {
            logger().debug("{} triggered {} before reset_session()",
                           conn, get_state_name(state));
            abort_protocol();
          }
          // handle_session_reset() logic
          auto reset = ResetFrame::Decode(payload->back());
          logger().warn("{} GOT ResetFrame: full={}", conn, reset.full());

          reset_session(reset.full());
          // user can make changes

          return client_connect();
        });
      case Tag::WAIT:
        return process_wait();
      case Tag::SESSION_RECONNECT_OK:
        return frame_assembler->read_frame_payload(
        ).then([this](auto payload) {
          if (unlikely(state != state_t::CONNECTING)) {
            logger().debug("{} triggered {} at receiving RECONNECT_OK",
                           conn, get_state_name(state));
            abort_protocol();
          }

          // handle_reconnect_ok() logic
          auto reconnect_ok = ReconnectOkFrame::Decode(payload->back());
          auto cc_seq = crosscore.prepare_submit();
          logger().debug("{} GOT ReconnectOkFrame: msg_seq={}, "
                         "send {} IOHandler::requeue_out_sent_up_to()",
                         conn, reconnect_ok.msg_seq(), cc_seq);

          io_states.requeue_out_sent_up_to();
          auto msg_seq = reconnect_ok.msg_seq();
          gate.dispatch_in_background(
              "requeue_out_reconnecting", conn, [this, cc_seq, msg_seq] {
            return seastar::smp::submit_to(
                io_handler.get_shard_id(), [this, cc_seq, msg_seq] {
              return io_handler.requeue_out_sent_up_to(cc_seq, msg_seq);
            });
          });

          return seastar::make_ready_future<next_step_t>(next_step_t::ready);
        });
      default: {
        unexpected_tag(ret.tag, conn, "post_client_reconnect");
        return seastar::make_ready_future<next_step_t>(next_step_t::none);
      }
    }
  });
}

void ProtocolV2::execute_connecting()
{
  ceph_assert_always(!is_socket_valid);
  trigger_state(state_t::CONNECTING, io_state_t::delay);
  gated_execute("execute_connecting", conn, [this] {
    global_seq = messenger.get_global_seq();
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
    return wait_exit_io().then([this] {
#ifdef UNIT_TESTS_BUILT
      // process custom_bp_t::SOCKET_CONNECTING
      // supports CONTINUE/FAULT/BLOCK
      if (!conn.interceptor) {
        return seastar::now();
      }
      return conn.interceptor->intercept(
        conn, {Breakpoint{custom_bp_t::SOCKET_CONNECTING}}
      ).then([this](bp_action_t action) {
        switch (action) {
        case bp_action_t::CONTINUE:
          return seastar::now();
        case bp_action_t::FAULT:
          logger().info("[Test] got FAULT");
          abort_in_fault();
        case bp_action_t::BLOCK:
          logger().info("[Test] got BLOCK");
          return conn.interceptor->blocker.block();
        default:
          ceph_abort("unexpected action from trap");
          return seastar::now();
        }
      });;
    }).then([this] {
#endif
      ceph_assert_always(frame_assembler);
      if (unlikely(state != state_t::CONNECTING)) {
        logger().debug("{} triggered {} before Socket::connect()",
                       conn, get_state_name(state));
        abort_protocol();
      }
      return Socket::connect(conn.peer_addr);
    }).then([this](SocketRef _new_socket) {
      logger().debug("{} socket connected", conn);
      if (unlikely(state != state_t::CONNECTING)) {
        logger().debug("{} triggered {} during Socket::connect()",
                       conn, get_state_name(state));
        return _new_socket->close().then([sock=std::move(_new_socket)] {
          abort_protocol();
        });
      }
      SocketFRef new_socket = seastar::make_foreign(std::move(_new_socket));
      if (!has_socket) {
        frame_assembler->set_socket(std::move(new_socket));
        has_socket = true;
      } else {
        gate.dispatch_in_background(
          "replace_socket_connecting",
          conn,
          [this, new_socket=std::move(new_socket)]() mutable {
            return frame_assembler->replace_shutdown_socket(std::move(new_socket));
          }
        );
      }
      is_socket_valid = true;
      return seastar::now();
    }).then([this] {
      auth_meta = seastar::make_lw_shared<AuthConnectionMeta>();
      frame_assembler->reset_handlers();
      frame_assembler->start_recording();
      return banner_exchange(true);
    }).then([this] (auto&& ret) {
      auto [_peer_type, _my_addr_from_peer] = std::move(ret);
      if (conn.get_peer_type() != _peer_type) {
        logger().warn("{} connection peer type does not match what peer advertises {} != {}",
                      conn, ceph_entity_type_name(conn.get_peer_type()),
                      ceph_entity_type_name(_peer_type));
        ABORT_IN_CLOSE(true);
      }
      if (unlikely(state != state_t::CONNECTING)) {
        logger().debug("{} triggered {} during banner_exchange(), abort",
                       conn, get_state_name(state));
        abort_protocol();
      }
      frame_assembler->learn_socket_ephemeral_port_as_connector(
          _my_addr_from_peer.get_port());
      if (unlikely(_my_addr_from_peer.is_legacy())) {
        logger().warn("{} peer sent a legacy address for me: {}",
                      conn, _my_addr_from_peer);
        throw std::system_error(
            make_error_code(crimson::net::error::bad_peer_address));
      }
      _my_addr_from_peer.set_type(entity_addr_t::TYPE_MSGR2);
      messenger.learned_addr(_my_addr_from_peer, conn);
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
        if (unlikely(state != state_t::CONNECTING)) {
          logger().debug("{} triggered {} before dispatch_connect(), abort",
                         conn, get_state_name(state));
          abort_protocol();
        }

        auto cc_seq = crosscore.prepare_submit();
        // there are 2 hops with dispatch_connect()
        crosscore.prepare_submit();
        logger().info("{} connected: gs={}, pgs={}, cs={}, "
                      "client_cookie={}, server_cookie={}, {}, new_sid={}, "
                      "send {} IOHandler::dispatch_connect()",
                      conn, global_seq, peer_global_seq, connect_seq,
                      client_cookie, server_cookie, io_states,
                      frame_assembler->get_socket_shard_id(), cc_seq);

        // set io_handler to a new shard
        auto new_io_shard = frame_assembler->get_socket_shard_id();
        ConnectionFRef conn_fref = seastar::make_foreign(
            conn.shared_from_this());
        ceph_assert_always(!pr_switch_io_shard.has_value());
        pr_switch_io_shard = seastar::shared_promise<>();
        return seastar::smp::submit_to(
            io_handler.get_shard_id(),
            [this, cc_seq, new_io_shard,
             conn_fref=std::move(conn_fref)]() mutable {
          return io_handler.dispatch_connect(
              cc_seq, new_io_shard, std::move(conn_fref));
        }).then([this, new_io_shard] {
          ceph_assert_always(io_handler.get_shard_id() == new_io_shard);
          pr_switch_io_shard->set_value();
          pr_switch_io_shard = std::nullopt;
          // user can make changes

          if (unlikely(state != state_t::CONNECTING)) {
            logger().debug("{} triggered {} after dispatch_connect(), abort",
                           conn, get_state_name(state));
            abort_protocol();
          }
          execute_ready();
        });
       }
       case next_step_t::wait: {
        logger().info("{} execute_connecting(): going to WAIT(max-backoff)", conn);
        ceph_assert_always(is_socket_valid);
        frame_assembler->shutdown_socket<true>(&gate);
        is_socket_valid = false;
        execute_wait(true);
        return seastar::now();
       }
       default: {
        ceph_abort("impossible next step");
       }
      }
    }).handle_exception([this](std::exception_ptr eptr) {
      fault(state_t::CONNECTING, "execute_connecting", eptr);
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
  return frame_assembler->write_flush_frame(bad_method
  ).then([this] {
    return server_auth();
  });
}

seastar::future<> ProtocolV2::_handle_auth_request(bufferlist& auth_payload, bool more)
{
  // _handle_auth_request() logic
  ceph_assert(messenger.get_auth_server());
  bufferlist reply;
  int r = messenger.get_auth_server()->handle_auth_request(
      conn,
      *auth_meta,
      more,
      auth_meta->auth_method,
      auth_payload,
      &conn.peer_global_id,
      &reply);
  switch (r) {
   // successful
   case 1: {
    auto auth_done = AuthDoneFrame::Encode(
        conn.peer_global_id, auth_meta->con_mode, reply);
    logger().debug("{} WRITE AuthDoneFrame: gid={}, con_mode={}, payload_len={}",
                   conn, conn.peer_global_id,
                   ceph_con_mode_name(auth_meta->con_mode), reply.length());
    return frame_assembler->write_flush_frame(auth_done
    ).then([this] {
      ceph_assert(auth_meta);
      frame_assembler->create_session_stream_handlers(*auth_meta, true);
      return finish_auth();
    });
   }
   // auth more
   case 0: {
    auto more = AuthReplyMoreFrame::Encode(reply);
    logger().debug("{} WRITE AuthReplyMoreFrame: payload_len={}",
                   conn, reply.length());
    return frame_assembler->write_flush_frame(more
    ).then([this] {
      return frame_assembler->read_main_preamble();
    }).then([this](auto ret) {
      expect_tag(Tag::AUTH_REQUEST_MORE, ret.tag, conn, "read_auth_request_more");
      return frame_assembler->read_frame_payload();
    }).then([this](auto payload) {
      auto auth_more = AuthRequestMoreFrame::Decode(payload->back());
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
  return frame_assembler->read_main_preamble(
  ).then([this](auto ret) {
    expect_tag(Tag::AUTH_REQUEST, ret.tag, conn, "read_auth_request");
    return frame_assembler->read_frame_payload();
  }).then([this](auto payload) {
    // handle_auth_request() logic
    auto request = AuthRequestFrame::Decode(payload->back());
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
  return frame_assembler->write_flush_frame(wait
  ).then([] {
    return next_step_t::wait;
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::reuse_connection(
    ProtocolV2* existing_proto, bool do_reset,
    bool reconnect, uint64_t conn_seq, uint64_t msg_seq)
{
  if (unlikely(state != state_t::ACCEPTING)) {
    logger().debug("{} triggered {} before trigger_replacing()",
                   conn, get_state_name(state));
    abort_protocol();
  }

  existing_proto->trigger_replacing(reconnect,
                                    do_reset,
                                    frame_assembler->to_replace(),
                                    std::move(auth_meta),
                                    peer_global_seq,
                                    client_cookie,
                                    conn.get_peer_name(),
                                    conn.get_features(),
                                    peer_supported_features,
                                    conn_seq,
                                    msg_seq);
  ceph_assert_always(has_socket && is_socket_valid);
  is_socket_valid = false;
  has_socket = false;
#ifdef UNIT_TESTS_BUILT
  if (conn.interceptor) {
    conn.interceptor->register_conn_replaced(
        conn.get_local_shared_foreign_from_this());
  }
#endif
  // close this connection because all the necessary information is delivered
  // to the exisiting connection, and jump to error handling code to abort the
  // current state.
  ABORT_IN_CLOSE(false);
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
                 fmt::ptr(existing_conn.get()), get_state_name(existing_proto->state),
                 existing_proto->global_seq,
                 existing_proto->peer_global_seq,
                 existing_proto->connect_seq,
                 existing_proto->client_cookie,
                 existing_proto->server_cookie);

  if (!validate_peer_name(existing_conn->get_peer_name())) {
    logger().error("{} server_connect: my peer_name doesn't match"
                   " the existing connection {}, abort", conn, fmt::ptr(existing_conn.get()));
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
    if (unlikely(state != state_t::ACCEPTING)) {
      logger().debug("{} triggered {} before execute_establishing()",
                     conn, get_state_name(state));
      abort_protocol();
    }
    execute_establishing(existing_conn);
    return seastar::make_ready_future<next_step_t>(next_step_t::ready);
  }

  if (existing_proto->server_cookie != 0) {
    if (existing_proto->client_cookie != client_cookie) {
      // Found previous session
      // peer has reset and we're going to reuse the existing connection
      // by replacing the socket
      logger().warn("{} server_connect:"
                    " found new session (cs={})"
                    " when existing {} {} is with stale session (cs={}, ss={}),"
                    " peer must have reset",
                    conn,
                    client_cookie,
                    get_state_name(existing_proto->state),
                    *existing_conn,
                    existing_proto->client_cookie,
                    existing_proto->server_cookie);
      return reuse_connection(existing_proto, conn.policy.resetcheck);
    } else {
      // session establishment interrupted between client_ident and server_ident,
      // continuing...
      logger().warn("{} server_connect: found client session with existing {} {}"
                    " matched (cs={}, ss={}), continuing session establishment",
                    conn,
                    get_state_name(existing_proto->state),
                    *existing_conn,
                    client_cookie,
                    existing_proto->server_cookie);
      return reuse_connection(existing_proto);
    }
  } else {
    // Looks like a connection race: server and client are both connecting to
    // each other at the same time.
    if (existing_proto->client_cookie != client_cookie) {
      if (existing_conn->peer_wins()) {
        // acceptor (this connection, the peer) wins
        logger().warn("{} server_connect: connection race detected (cs={}, e_cs={}, ss=0)"
                      " and win, reusing existing {} {}",
                      conn,
                      client_cookie,
                      existing_proto->client_cookie,
                      get_state_name(existing_proto->state),
                      *existing_conn);
        return reuse_connection(existing_proto);
      } else {
        // acceptor (this connection, the peer) loses
        logger().warn("{} server_connect: connection race detected (cs={}, e_cs={}, ss=0)"
                      " and lose to existing {}, ask client to wait",
                      conn, client_cookie, existing_proto->client_cookie, *existing_conn);
        return existing_conn->send_keepalive().then([this] {
          return send_wait();
        });
      }
    } else {
      logger().warn("{} server_connect: found client session with existing {} {}"
                    " matched (cs={}, ss={}), continuing session establishment",
                    conn,
                    get_state_name(existing_proto->state),
                    *existing_conn,
                    client_cookie,
                    existing_proto->server_cookie);
      return reuse_connection(existing_proto);
    }
  }
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::server_connect()
{
  return frame_assembler->read_frame_payload(
  ).then([this](auto payload) {
    // handle_client_ident() logic
    auto client_ident = ClientIdentFrame::Decode(payload->back());
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
    conn.peer_addr = client_ident.addrs().front();
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
      return frame_assembler->write_flush_frame(ident_missing_features
      ).then([] {
        return next_step_t::wait;
      });
    }
    conn.set_features(client_ident.supported_features() &
                      conn.policy.features_supported);
    logger().debug("{} UPDATE: features={}", conn, conn.get_features());

    peer_global_seq = client_ident.global_seq();

    bool lossy = client_ident.flags() & CEPH_MSG_CONNECT_LOSSY;
    if (lossy != conn.policy.lossy) {
      logger().warn("{} my lossy policy {} doesn't match client {}, ignore",
                    conn, conn.policy.lossy, lossy);
    }

    // Looks good so far, let's check if there is already an existing connection
    // to this peer.

    SocketConnectionRef existing_conn = messenger.lookup_conn(conn.peer_addr);

    if (existing_conn) {
      return handle_existing_connection(existing_conn);
    } else {
      if (unlikely(state != state_t::ACCEPTING)) {
        logger().debug("{} triggered {} before execute_establishing()",
                       conn, get_state_name(state));
        abort_protocol();
      }
      execute_establishing(nullptr);
      return seastar::make_ready_future<next_step_t>(next_step_t::ready);
    }
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::read_reconnect()
{
  return frame_assembler->read_main_preamble(
  ).then([this](auto ret) {
    expect_tag(Tag::SESSION_RECONNECT, ret.tag, conn, "read_session_reconnect");
    return server_reconnect();
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::send_retry(uint64_t connect_seq)
{
  auto retry = RetryFrame::Encode(connect_seq);
  logger().warn("{} WRITE RetryFrame: cs={}", conn, connect_seq);
  return frame_assembler->write_flush_frame(retry
  ).then([this] {
    return read_reconnect();
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::send_retry_global(uint64_t global_seq)
{
  auto retry = RetryGlobalFrame::Encode(global_seq);
  logger().warn("{} WRITE RetryGlobalFrame: gs={}", conn, global_seq);
  return frame_assembler->write_flush_frame(retry
  ).then([this] {
    return read_reconnect();
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::send_reset(bool full)
{
  auto reset = ResetFrame::Encode(full);
  logger().warn("{} WRITE ResetFrame: full={}", conn, full);
  return frame_assembler->write_flush_frame(reset
  ).then([this] {
    return frame_assembler->read_main_preamble();
  }).then([this](auto ret) {
    expect_tag(Tag::CLIENT_IDENT, ret.tag, conn, "post_send_reset");
    return server_connect();
  });
}

seastar::future<ProtocolV2::next_step_t>
ProtocolV2::server_reconnect()
{
  return frame_assembler->read_frame_payload(
  ).then([this](auto payload) {
    // handle_reconnect() logic
    auto reconnect = ReconnectFrame::Decode(payload->back());

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

    ProtocolV2 *existing_proto = dynamic_cast<ProtocolV2*>(
        existing_conn->protocol.get());
    ceph_assert(existing_proto);
    logger().debug("{}(gs={}, pgs={}, cs={}, cc={}, sc={}) re-connecting,"
                   " found existing {}(state={}, gs={}, pgs={}, cs={}, cc={}, sc={})",
                   conn, global_seq, peer_global_seq, reconnect.connect_seq(),
                   reconnect.client_cookie(), reconnect.server_cookie(),
                   fmt::ptr(existing_conn.get()),
                   get_state_name(existing_proto->state),
                   existing_proto->global_seq,
                   existing_proto->peer_global_seq,
                   existing_proto->connect_seq,
                   existing_proto->client_cookie,
                   existing_proto->server_cookie);

    if (!validate_peer_name(existing_conn->get_peer_name())) {
      logger().error("{} server_reconnect: my peer_name doesn't match"
                     " the existing connection {}, abort", conn, fmt::ptr(existing_conn.get()));
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
        // acceptor (this connection, the peer) wins
        logger().warn("{} server_reconnect: reconnect race detected (cs={})"
                      " and win, reusing existing {} {}",
                      conn,
                      reconnect.connect_seq(),
                      get_state_name(existing_proto->state),
                      *existing_conn);
        return reuse_connection(
            existing_proto, false,
            true, reconnect.connect_seq(), reconnect.msg_seq());
      } else {
        // acceptor (this connection, the peer) loses
        logger().warn("{} server_reconnect: reconnect race detected (cs={})"
                      " and lose to existing {}, ask client to wait",
                      conn, reconnect.connect_seq(), *existing_conn);
        return send_wait();
      }
    } else { // existing_proto->connect_seq < reconnect.connect_seq()
      logger().warn("{} server_reconnect: stale exsiting connect_seq exist_cs({}) < peer_cs({}),"
                    " reusing existing {} {}",
                    conn,
                    existing_proto->connect_seq,
                    reconnect.connect_seq(),
                    get_state_name(existing_proto->state),
                    *existing_conn);
      return reuse_connection(
          existing_proto, false,
          true, reconnect.connect_seq(), reconnect.msg_seq());
    }
  });
}

void ProtocolV2::execute_accepting()
{
  assert(is_socket_valid);
  trigger_state(state_t::ACCEPTING, io_state_t::none);
  gate.dispatch_in_background("execute_accepting", conn, [this] {
    return seastar::futurize_invoke([this] {
#ifdef UNIT_TESTS_BUILT
      if (conn.interceptor) {
        // only notify socket accepted
        gate.dispatch_in_background(
            "test_intercept_socket_accepted", conn, [this] {
          return conn.interceptor->intercept(
            conn, {Breakpoint{custom_bp_t::SOCKET_ACCEPTED}}
          ).then([](bp_action_t action) {
            ceph_assert(action == bp_action_t::CONTINUE);
          });
        });
      }
#endif
      auth_meta = seastar::make_lw_shared<AuthConnectionMeta>();
      frame_assembler->reset_handlers();
      frame_assembler->start_recording();
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
      if (!messenger.get_myaddr().is_blank_ip() &&
          (messenger.get_myaddr().get_port() != _my_addr_from_peer.get_port() ||
          messenger.get_myaddr().get_nonce() != _my_addr_from_peer.get_nonce())) {
        logger().warn("{} my_addr_from_peer {} port/nonce doesn't match myaddr {}",
                      conn, _my_addr_from_peer, messenger.get_myaddr());
        throw std::system_error(
            make_error_code(crimson::net::error::bad_peer_address));
      }
      messenger.learned_addr(_my_addr_from_peer, conn);
      return server_auth();
    }).then([this] {
      return frame_assembler->read_main_preamble();
    }).then([this](auto ret) {
      switch (ret.tag) {
        case Tag::CLIENT_IDENT:
          return server_connect();
        case Tag::SESSION_RECONNECT:
          return server_reconnect();
        default: {
          unexpected_tag(ret.tag, conn, "post_server_auth");
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
    }).handle_exception([this](std::exception_ptr eptr) {
      const char *e_what;
      try {
        std::rethrow_exception(eptr);
      } catch (std::exception &e) {
        e_what = e.what();
      }
      logger().info("{} execute_accepting(): fault at {}, going to CLOSING -- {}",
                    conn, get_state_name(state), e_what);
      do_close(false);
    });
  });
}

// CONNECTING or ACCEPTING state

seastar::future<> ProtocolV2::finish_auth()
{
  ceph_assert(auth_meta);

  auto records = frame_assembler->stop_recording();
  const auto sig = auth_meta->session_key.empty() ? sha256_digest_t() :
    auth_meta->session_key.hmac_sha256(nullptr, records.rxbuf);
  auto sig_frame = AuthSignatureFrame::Encode(sig);
  logger().debug("{} WRITE AuthSignatureFrame: signature={}", conn, sig);
  return frame_assembler->write_flush_frame(sig_frame
  ).then([this] {
    return frame_assembler->read_main_preamble();
  }).then([this](auto ret) {
    expect_tag(Tag::AUTH_SIGNATURE, ret.tag, conn, "post_finish_auth");
    return frame_assembler->read_frame_payload();
  }).then([this, txbuf=std::move(records.txbuf)](auto payload) {
    // handle_auth_signature() logic
    auto sig_frame = AuthSignatureFrame::Decode(payload->back());
    logger().debug("{} GOT AuthSignatureFrame: signature={}", conn, sig_frame.signature());

    const auto actual_tx_sig = auth_meta->session_key.empty() ?
      sha256_digest_t() : auth_meta->session_key.hmac_sha256(nullptr, txbuf);
    if (sig_frame.signature() != actual_tx_sig) {
      logger().warn("{} pre-auth signature mismatch actual_tx_sig={}"
                    " sig_frame.signature()={}",
                    conn, actual_tx_sig, sig_frame.signature());
      abort_in_fault();
    }
  });
}

// ESTABLISHING

void ProtocolV2::execute_establishing(SocketConnectionRef existing_conn) {
  auto accept_me = [this] {
    messenger.register_conn(
      seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
    messenger.unaccept_conn(
      seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
  };

  ceph_assert_always(is_socket_valid);
  trigger_state(state_t::ESTABLISHING, io_state_t::delay);
  bool is_replace;
  if (existing_conn) {
    logger().info("{} start establishing: gs={}, pgs={}, cs={}, "
                  "client_cookie={}, server_cookie={}, {}, new_sid={}, "
                  "close existing {}",
                  conn, global_seq, peer_global_seq, connect_seq,
                  client_cookie, server_cookie,
                  io_states, frame_assembler->get_socket_shard_id(),
                  *existing_conn);
    is_replace = true;
    ProtocolV2 *existing_proto = dynamic_cast<ProtocolV2*>(
        existing_conn->protocol.get());
    existing_proto->do_close(
        true, // is_dispatch_reset
        std::move(accept_me));
    if (unlikely(state != state_t::ESTABLISHING)) {
      logger().warn("{} triggered {} during execute_establishing(), "
                    "the accept event will not be delivered!",
                    conn, get_state_name(state));
      abort_protocol();
    }
  } else {
    logger().info("{} start establishing: gs={}, pgs={}, cs={}, "
                  "client_cookie={}, server_cookie={}, {}, new_sid={}, "
                  "no existing",
                  conn, global_seq, peer_global_seq, connect_seq,
                  client_cookie, server_cookie, io_states,
                  frame_assembler->get_socket_shard_id());
    is_replace = false;
    accept_me();
  }

  gated_execute("execute_establishing", conn, [this, is_replace] {
    ceph_assert_always(state == state_t::ESTABLISHING);

    // set io_handler to a new shard
    auto cc_seq = crosscore.prepare_submit();
    // there are 2 hops with dispatch_accept()
    crosscore.prepare_submit();
    auto new_io_shard = frame_assembler->get_socket_shard_id();
    logger().debug("{} send {} IOHandler::dispatch_accept({})",
                   conn, cc_seq, new_io_shard);
    ConnectionFRef conn_fref = seastar::make_foreign(
        conn.shared_from_this());
    ceph_assert_always(!pr_switch_io_shard.has_value());
    pr_switch_io_shard = seastar::shared_promise<>();
    return seastar::smp::submit_to(
        io_handler.get_shard_id(),
        [this, cc_seq, new_io_shard, is_replace,
         conn_fref=std::move(conn_fref)]() mutable {
      return io_handler.dispatch_accept(
          cc_seq, new_io_shard, std::move(conn_fref), is_replace);
    }).then([this, new_io_shard] {
      ceph_assert_always(io_handler.get_shard_id() == new_io_shard);
      pr_switch_io_shard->set_value();
      pr_switch_io_shard = std::nullopt;
      // user can make changes

      if (unlikely(state != state_t::ESTABLISHING)) {
        logger().debug("{} triggered {} after dispatch_accept() during execute_establishing()",
                       conn, get_state_name(state));
        abort_protocol();
      }

      return send_server_ident();
    }).then([this] {
      if (unlikely(state != state_t::ESTABLISHING)) {
        logger().debug("{} triggered {} at the end of execute_establishing()",
                       conn, get_state_name(state));
        abort_protocol();
      }
      logger().info("{} established, going to ready", conn);
      execute_ready();
    }).handle_exception([this](std::exception_ptr eptr) {
      fault(state_t::ESTABLISHING, "execute_establishing", eptr);
    });
  });
}

// ESTABLISHING or REPLACING state

seastar::future<>
ProtocolV2::send_server_ident()
{
  ceph_assert_always(state == state_t::ESTABLISHING ||
                     state == state_t::REPLACING);
  // send_server_ident() logic

  // refered to async-conn v2: not assign gs to global_seq
  global_seq = messenger.get_global_seq();
  auto cc_seq = crosscore.prepare_submit();
  logger().debug("{} UPDATE: gs={} for server ident, "
                 "send {} IOHandler::reset_peer_state()",
                 conn, global_seq, cc_seq);

  // this is required for the case when this connection is being replaced
  io_states.reset_peer_state();
  gate.dispatch_in_background(
      "reset_peer_state", conn, [this, cc_seq] {
    return seastar::smp::submit_to(
        io_handler.get_shard_id(), [this, cc_seq] {
      return io_handler.reset_peer_state(cc_seq);
    });
  });

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
          global_seq,
          conn.policy.features_supported,
          conn.policy.features_required | msgr2_required,
          flags,
          server_cookie);

  logger().debug("{} WRITE ServerIdentFrame: addrs={}, gid={},"
                 " gs={}, features_supported={}, features_required={},"
                 " flags={}, cookie={}",
                 conn, messenger.get_myaddrs(), messenger.get_myname().num(),
                 global_seq, conn.policy.features_supported,
                 conn.policy.features_required | msgr2_required,
                 flags, server_cookie);

  return frame_assembler->write_flush_frame(server_ident);
}

// REPLACING state

void ProtocolV2::trigger_replacing(bool reconnect,
                                   bool do_reset,
                                   FrameAssemblerV2::mover_t &&mover,
                                   AuthConnectionMetaRef&& new_auth_meta,
                                   uint64_t new_peer_global_seq,
                                   uint64_t new_client_cookie,
                                   entity_name_t new_peer_name,
                                   uint64_t new_conn_features,
                                   uint64_t new_peer_supported_features,
                                   uint64_t new_connect_seq,
                                   uint64_t new_msg_seq)
{
  ceph_assert_always(state >= state_t::ESTABLISHING);
  ceph_assert_always(state <= state_t::WAIT);
  ceph_assert_always(has_socket || state == state_t::CONNECTING);
  // mover.socket shouldn't be shutdown

  logger().info("{} start replacing ({}): pgs was {}, cs was {}, "
                "client_cookie was {}, {}, new_sid={}",
                conn, reconnect ? "reconnected" : "connected",
                peer_global_seq, connect_seq, client_cookie,
                io_states, mover.socket->get_shard_id());
  if (is_socket_valid) {
    frame_assembler->shutdown_socket<true>(&gate);
    is_socket_valid = false;
  }
  trigger_state_phase1(state_t::REPLACING);
  gate.dispatch_in_background(
      "trigger_replacing",
      conn,
      [this,
       reconnect,
       do_reset,
       mover = std::move(mover),
       new_auth_meta = std::move(new_auth_meta),
       new_client_cookie, new_peer_name,
       new_conn_features, new_peer_supported_features,
       new_peer_global_seq,
       new_connect_seq, new_msg_seq] () mutable {
    ceph_assert_always(state == state_t::REPLACING);
    auto new_io_shard = mover.socket->get_shard_id();
    // state may become CLOSING below, but we cannot abort the chain until
    // mover.socket is correctly handled (closed or replaced).

    // this is preemptive
    return wait_switch_io_shard(
    ).then([this] {
      if (unlikely(state != state_t::REPLACING)) {
        ceph_assert_always(state == state_t::CLOSING);
        return seastar::now();
      }

      trigger_state_phase2(state_t::REPLACING, io_state_t::delay);
      return wait_exit_io();
    }).then([this] {
      if (unlikely(state != state_t::REPLACING)) {
        ceph_assert_always(state == state_t::CLOSING);
        return seastar::now();
      }

      ceph_assert_always(frame_assembler);
      protocol_timer.cancel();
      auto done = std::move(execution_done);
      execution_done = seastar::now();
      return done;
    }).then([this, new_io_shard] {
      if (unlikely(state != state_t::REPLACING)) {
        ceph_assert_always(state == state_t::CLOSING);
        return seastar::now();
      }

      // set io_handler to a new shard
      // we should prevent parallel switching core attemps
      auto cc_seq = crosscore.prepare_submit();
      // there are 2 hops with dispatch_accept()
      crosscore.prepare_submit();
      logger().debug("{} send {} IOHandler::dispatch_accept({})",
                     conn, cc_seq, new_io_shard);
      ConnectionFRef conn_fref = seastar::make_foreign(
          conn.shared_from_this());
      ceph_assert_always(!pr_switch_io_shard.has_value());
      pr_switch_io_shard = seastar::shared_promise<>();
      return seastar::smp::submit_to(
          io_handler.get_shard_id(),
          [this, cc_seq, new_io_shard,
           conn_fref=std::move(conn_fref)]() mutable {
        return io_handler.dispatch_accept(
            cc_seq, new_io_shard, std::move(conn_fref), false);
      }).then([this, new_io_shard] {
        ceph_assert_always(io_handler.get_shard_id() == new_io_shard);
        pr_switch_io_shard->set_value();
        pr_switch_io_shard = std::nullopt;
        // user can make changes
      });
    }).then([this,
             reconnect,
             do_reset,
             mover = std::move(mover),
             new_auth_meta = std::move(new_auth_meta),
             new_client_cookie, new_peer_name,
             new_conn_features, new_peer_supported_features,
             new_peer_global_seq,
             new_connect_seq, new_msg_seq] () mutable {
      if (state == state_t::REPLACING && do_reset) {
        reset_session(true);
        // user can make changes
      }

      if (unlikely(state != state_t::REPLACING)) {
        logger().debug("{} triggered {} in the middle of trigger_replacing(), abort",
                       conn, get_state_name(state));
        ceph_assert_always(state == state_t::CLOSING);
        return mover.socket->close(
        ).then([sock = std::move(mover.socket)] {
          abort_protocol();
        });
      }

      auth_meta = std::move(new_auth_meta);
      peer_global_seq = new_peer_global_seq;
      gate.dispatch_in_background(
        "replace_frame_assembler",
        conn,
        [this, mover=std::move(mover)]() mutable {
          return frame_assembler->replace_by(std::move(mover));
        }
      );
      is_socket_valid = true;
      has_socket = true;

      if (reconnect) {
        connect_seq = new_connect_seq;
        // send_reconnect_ok() logic

        auto cc_seq = crosscore.prepare_submit();
        logger().debug("{} send {} IOHandler::requeue_out_sent_up_to({})",
                       conn, cc_seq, new_msg_seq);
        io_states.requeue_out_sent_up_to();
        gate.dispatch_in_background(
            "requeue_out_replacing", conn, [this, cc_seq, new_msg_seq] {
          return seastar::smp::submit_to(
              io_handler.get_shard_id(), [this, cc_seq, new_msg_seq] {
            return io_handler.requeue_out_sent_up_to(cc_seq, new_msg_seq);
          });
        });

        auto reconnect_ok = ReconnectOkFrame::Encode(io_states.in_seq);
        logger().debug("{} WRITE ReconnectOkFrame: msg_seq={}", conn, io_states.in_seq);
        return frame_assembler->write_flush_frame(reconnect_ok);
      } else {
        client_cookie = new_client_cookie;
        assert(conn.get_peer_type() == new_peer_name.type());
        if (conn.get_peer_id() == entity_name_t::NEW) {
          conn.set_peer_id(new_peer_name.num());
        }
        conn.set_features(new_conn_features);
        peer_supported_features = new_peer_supported_features;
        bool is_rev1 = HAVE_MSGR2_FEATURE(peer_supported_features, REVISION_1);
        frame_assembler->set_is_rev1(is_rev1);
        return send_server_ident();
      }
    }).then([this, reconnect] {
      if (unlikely(state != state_t::REPLACING)) {
        logger().debug("{} triggered {} at the end of trigger_replacing(), abort",
                       conn, get_state_name(state));
        ceph_assert_always(state == state_t::CLOSING);
        abort_protocol();
      }
      logger().info("{} replaced ({}), going to ready: "
                    "gs={}, pgs={}, cs={}, "
                    "client_cookie={}, server_cookie={}, {}",
                    conn, reconnect ? "reconnected" : "connected",
                    global_seq, peer_global_seq, connect_seq,
                    client_cookie, server_cookie, io_states);
      execute_ready();
    }).handle_exception([this](std::exception_ptr eptr) {
      fault(state_t::REPLACING, "trigger_replacing", eptr);
    });
  });
}

// READY state

seastar::future<> ProtocolV2::notify_out_fault(
    cc_seq_t cc_seq,
    const char *where,
    std::exception_ptr eptr,
    io_handler_state _io_states)
{
  assert(seastar::this_shard_id() == conn.get_messenger_shard_id());
  if (!crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} notify_out_fault(), wait at {}",
                   conn, cc_seq, crosscore.get_in_seq());
    return crosscore.wait(cc_seq
    ).then([this, cc_seq, where, eptr, _io_states] {
      return notify_out_fault(cc_seq, where, eptr, _io_states);
    });
  }

  io_states = _io_states;
  logger().debug("{} got {} notify_out_fault(): io_states={}",
                 conn, cc_seq, io_states);
  fault(state_t::READY, where, eptr);
  return seastar::now();
}

void ProtocolV2::execute_ready()
{
  assert(conn.policy.lossy || (client_cookie != 0 && server_cookie != 0));
  protocol_timer.cancel();
  ceph_assert_always(is_socket_valid);
  // I'm not responsible to shutdown the socket at READY
  is_socket_valid = false;
  trigger_state(state_t::READY, io_state_t::open);
#ifdef UNIT_TESTS_BUILT
  if (conn.interceptor) {
    // FIXME: doesn't support cross-core
    conn.interceptor->register_conn_ready(
        conn.get_local_shared_foreign_from_this());
  }
#endif
}

// STANDBY state

void ProtocolV2::execute_standby()
{
  ceph_assert_always(!is_socket_valid);
  trigger_state(state_t::STANDBY, io_state_t::delay);
}

seastar::future<> ProtocolV2::notify_out(
    cc_seq_t cc_seq)
{
  assert(seastar::this_shard_id() == conn.get_messenger_shard_id());
  if (!crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} notify_out(), wait at {}",
                   conn, cc_seq, crosscore.get_in_seq());
    return crosscore.wait(cc_seq
    ).then([this, cc_seq] {
      return notify_out(cc_seq);
    });
  }

  logger().debug("{} got {} notify_out(): at {}",
                 conn, cc_seq, get_state_name(state));
  io_states.is_out_queued = true;
  if (unlikely(state == state_t::STANDBY && !conn.policy.server)) {
    logger().info("{} notify_out(): at {}, going to CONNECTING",
                  conn, get_state_name(state));
    execute_connecting();
  }
  return seastar::now();
}

// WAIT state

void ProtocolV2::execute_wait(bool max_backoff)
{
  ceph_assert_always(!is_socket_valid);
  trigger_state(state_t::WAIT, io_state_t::delay);
  gated_execute("execute_wait", conn, [this, max_backoff] {
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
    }).handle_exception([this](std::exception_ptr eptr) {
      const char *e_what;
      try {
        std::rethrow_exception(eptr);
      } catch (std::exception &e) {
        e_what = e.what();
      }
      logger().info("{} execute_wait(): protocol aborted at {} -- {}",
                    conn, get_state_name(state), e_what);
      assert(state == state_t::REPLACING ||
             state == state_t::CLOSING);
    });
  });
}

// SERVER_WAIT state

void ProtocolV2::execute_server_wait()
{
  ceph_assert_always(is_socket_valid);
  trigger_state(state_t::SERVER_WAIT, io_state_t::none);
  gated_execute("execute_server_wait", conn, [this] {
    return frame_assembler->read_exactly(1
    ).then([this](auto bptr) {
      logger().warn("{} SERVER_WAIT got read, abort", conn);
      abort_in_fault();
    }).handle_exception([this](std::exception_ptr eptr) {
      const char *e_what;
      try {
        std::rethrow_exception(eptr);
      } catch (std::exception &e) {
        e_what = e.what();
      }
      logger().info("{} execute_server_wait(): fault at {}, going to CLOSING -- {}",
                    conn, get_state_name(state), e_what);
      do_close(false);
    });
  });
}

// CLOSING state

seastar::future<> ProtocolV2::notify_mark_down(
    cc_seq_t cc_seq)
{
  assert(seastar::this_shard_id() == conn.get_messenger_shard_id());
  if (!crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} notify_mark_down(), wait at {}",
                   conn, cc_seq, crosscore.get_in_seq());
    return crosscore.wait(cc_seq
    ).then([this, cc_seq] {
      return notify_mark_down(cc_seq);
    });
  }

  logger().debug("{} got {} notify_mark_down()",
                 conn, cc_seq);
  do_close(false);
  return seastar::now();
}

seastar::future<> ProtocolV2::close_clean_yielded()
{
  // yield() so that do_close() can be called *after* close_clean_yielded() is
  // applied to all connections in a container using
  // seastar::parallel_for_each(). otherwise, we could erase a connection in
  // the container when seastar::parallel_for_each() is still iterating in it.
  // that'd lead to a segfault.
  return seastar::yield(
  ).then([this] {
    do_close(false);
    return pr_closed_clean.get_shared_future();

  // connection may be unreferenced from the messenger,
  // so need to hold the additional reference.
  }).finally([conn_ref = conn.shared_from_this()] {});;
}

void ProtocolV2::do_close(
    bool is_dispatch_reset,
    std::optional<std::function<void()>> f_accept_new)
{
  if (state == state_t::CLOSING) {
    // already closing
    return;
  }

  bool is_replace = f_accept_new ? true : false;
  logger().info("{} closing: reset {}, replace {}", conn,
                is_dispatch_reset ? "yes" : "no",
                is_replace ? "yes" : "no");

  /*
   * atomic operations
   */

  ceph_assert_always(!gate.is_closed());

  // messenger registrations, must before user events
  messenger.closing_conn(
      seastar::static_pointer_cast<SocketConnection>(
        conn.shared_from_this()));
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
  if (f_accept_new) {
    // the replacing connection must be registerred after the replaced
    // connection is unreigsterred.
    (*f_accept_new)();
  }

  protocol_timer.cancel();
  if (is_socket_valid) {
    frame_assembler->shutdown_socket<true>(&gate);
    is_socket_valid = false;
  }

  trigger_state_phase1(state_t::CLOSING);
  gate.dispatch_in_background(
      "close_io", conn, [this, is_dispatch_reset, is_replace] {
    // this is preemptive
    return wait_switch_io_shard(
    ).then([this, is_dispatch_reset, is_replace] {
      trigger_state_phase2(state_t::CLOSING, io_state_t::drop);
      auto cc_seq = crosscore.prepare_submit();
      logger().debug("{} send {} IOHandler::close_io(reset={}, replace={})",
                     conn, cc_seq, is_dispatch_reset, is_replace);

      std::ignore = gate.close(
      ).then([this] {
        ceph_assert_always(!need_exit_io);
        ceph_assert_always(!pr_exit_io.has_value());
        if (has_socket) {
          ceph_assert_always(frame_assembler);
          return frame_assembler->close_shutdown_socket();
        } else {
          return seastar::now();
        }
      }).then([this] {
        logger().debug("{} closed!", conn);
        messenger.closed_conn(
            seastar::static_pointer_cast<SocketConnection>(
              conn.shared_from_this()));
        pr_closed_clean.set_value();
#ifdef UNIT_TESTS_BUILT
        closed_clean = true;
        if (conn.interceptor) {
          conn.interceptor->register_conn_closed(
              conn.get_local_shared_foreign_from_this());
        }
#endif
      // connection is unreferenced from the messenger,
      // so need to hold the additional reference.
      }).handle_exception([conn_ref = conn.shared_from_this(), this] (auto eptr) {
        logger().error("{} closing got unexpected exception {}",
                       conn, eptr);
        ceph_abort();
      });

      return seastar::smp::submit_to(
          io_handler.get_shard_id(),
          [this, cc_seq, is_dispatch_reset, is_replace] {
        return io_handler.close_io(cc_seq, is_dispatch_reset, is_replace);
      });
      // user can make changes
    });
  });
}

} // namespace crimson::net
