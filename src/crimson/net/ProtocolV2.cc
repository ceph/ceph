// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ProtocolV2.h"

#include "include/msgr.h"

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

namespace ceph::net {

ProtocolV2::ProtocolV2(Dispatcher& dispatcher,
                       SocketConnection& conn,
                       SocketMessenger& messenger)
  : Protocol(2, dispatcher, conn, messenger) {}

ProtocolV2::~ProtocolV2() {}

void ProtocolV2::start_connect(const entity_addr_t& _peer_addr,
                               const entity_type_t& _peer_type)
{
  ceph_assert(state == state_t::NONE);
  ceph_assert(!socket);
  conn.peer_addr = _peer_addr;
  conn.peer_type = _peer_type;
  // TODO: lossless policy
  conn.policy = SocketPolicy::lossy_client(0);
  messenger.register_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  execute_connecting();
}

void ProtocolV2::start_accept(SocketFRef&& sock,
                              const entity_addr_t& _peer_addr)
{
  ceph_assert(state == state_t::NONE);
  ceph_assert(!socket);
  socket = std::move(sock);
  messenger.accept_conn(
    seastar::static_pointer_cast<SocketConnection>(conn.shared_from_this()));
  execute_accepting();
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

seastar::future<> ProtocolV2::banner_exchange()
{
  // 1. <prepare and send banner>
  // 2. then: <read banner>
  // 3. then: <process banner and read banner_payload>
  // 4. then: <process banner_payload and send HelloFrame>
  // 5. then: <read peer HelloFrame>
  // 6. then: <process peer HelloFrame>
  return seastar::now();
}

// CONNECTING state

seastar::future<> ProtocolV2::handle_auth_reply()
{
//return read_main_preamble()
//.then([this] (Tag tag) {
//  switch (tag) {
//    case Tag::AUTH_BAD_METHOD:
//      handle_auth_bad_method() logic
//      return client_auth(bad_method.allowed_methods());
//    case Tag::AUTH_REPLY_MORE:
//      <prepare AuthReplyMoreFrame and send it>
//      <then:>
//        return handle_auth_reply()
//    case Tag::AUTH_DONE:
//      <handle AuthDoneFrame>
//      <client auth is successful!>
        return seastar::now();
//    default: {
//      return unexpected_tag(tag, conn, __func__);
//    }
//});
}

seastar::future<> ProtocolV2::client_auth(std::vector<uint32_t> &allowed_methods)
{
  // send_auth_request() logic
  // <prepare AuthRequestFrame and send>
  // <then:>
        return handle_auth_reply();
}

seastar::future<bool> ProtocolV2::process_wait()
{
//return read_frame_payload()
//.then([this] (bufferlist payload) {
//  handle_wait() logic
//  return false;
//});
  return seastar::make_ready_future<bool>(false);
}

seastar::future<bool> ProtocolV2::client_connect()
{
  // send_client_ident() logic
  // <prepare and send ClientIdentFrame>
  // return read_main_preamble()
  // .then([this] (Tag tag) {
  //   switch (tag) {
  //     case Tag::IDENT_MISSING_FEATURES:
  //       abort_in_fault();
  //     case Tag::WAIT:
  //       return process_wait();
  //     case Tag::SERVER_IDENT:
  //       <handle ServerIdentFrame>
           return seastar::make_ready_future<bool>(true);
  //     default: {
  //       unexpected_tag(tag, conn, "post_client_connect");
  //     }
  //   }
  // });
}

seastar::future<bool> ProtocolV2::client_reconnect()
{
  // send_reconnect() logic
  // <prepare ReconnectFrame and send>
  // <then:>
  //   return read_main_preamble()
  //   .then([this] (Tag tag) {
  //     switch (tag) {
  //       case Tag::SESSION_RETRY_GLOBAL:
  //         <handle RetryGlobalFrame>
  //         return client_reconnect();
  //       case Tag::SESSION_RETRY:
  //         <handle RetryFrame>
  //         return client_reconnect();
  //       case Tag::SESSION_RESET:
  //         <handle ResetFrame>
  //         return client_connect();
  //       case Tag::WAIT:
  //         return process_wait();
  //       case Tag::SESSION_RECONNECT_OK:
  //         <handle ReconnectOkFrame>
             return seastar::make_ready_future<bool>(true);
  //       default: {
  //         unexpected_tag(tag, conn, "post_client_reconnect");
  //       }
  //     }
  //   });
}

void ProtocolV2::execute_connecting()
{
  trigger_state(state_t::CONNECTING, write_state_t::delay, true);
  seastar::with_gate(pending_dispatch, [this] {
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
        }).then([this] {
          return client_auth();
        }).then([this] {
          if (1) { // TODO check connect or reconnect
            return client_connect();
          } else {
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
  // <prepare and send AuthBadMethodFrame>
  // <then:>
       return server_auth();
}

seastar::future<> ProtocolV2::_handle_auth_request(bufferlist& auth_payload, bool more)
{
  // _handle_auth_request() logic
  // <case done:>
  //   <prepare and send AuthDoneFrame>
  //   <then: server auth successful!>
         return seastar::now();
  // <case more:>
  //   <prepare and send AuthReplyMoreFrame>
  //   <then:>
  //     return read_main_preamble()
  //     .then([this] (Tag tag) {
  //       expect_tag(Tag::AUTH_REQUEST_MORE, tag, conn, __func__);
  //       <handle_auth_request_more() logic>
  //       <process AuthRequestMoreFrame>
  //       return _handle_auth_request(auth_more.auth_payload(), true);
  //     });
  // <case bad:>
  //   return _auth_bad_method();
}

seastar::future<> ProtocolV2::server_auth()
{
  // return read_main_preamble()
  // .then([this] (Tag tag) {
  //   expect_tag(Tag::AUTH_REQUEST, tag, conn, __func__);
  //   <handle_auth_request() logic>
  //   <case bad request:>
  //     return _auth_bad_method();
  //   <...>
       auto dummy_auth = bufferlist{};
       return _handle_auth_request(/*request.auth_payload()*/dummy_auth, false);
  // });
}

seastar::future<bool> ProtocolV2::send_wait()
{
  // <prepare and send WaitFrame>
  // <then:>
       return seastar::make_ready_future<bool>(false);
}

seastar::future<bool> ProtocolV2::handle_existing_connection(SocketConnectionRef existing)
{
  // TODO: lossless policy
  ceph_assert(false);
}

seastar::future<bool> ProtocolV2::server_connect()
{
  // handle_client_ident() logic
  // <process ClientIdentFrame>
  // <case feature missing:>
  //  <prepare and send IdentMissingFeaturesFrame>
  //  <then: trigger SERVER_WAIT>
  //    return seastar::make_ready_future<bool>(false);
  // <case existing:>
  //  return handle_existing_connection(existing);
  // <case everything OK:>
      return send_server_ident()
      .then([this] {
        // goto ready
        return true;
      });
}

seastar::future<bool> ProtocolV2::read_reconnect()
{
  // return read_main_preamble()
  // .then([this] (Tag tag) {
  //   expect_tag(Tag::SESSION_RECONNECT, tag, conn, "read_reconnect");
       return server_reconnect();
  // });
}

seastar::future<bool> ProtocolV2::send_retry(uint64_t connect_seq)
{
  // <prepare and send RetryFrame>
  // <then:>
       return read_reconnect();
}

seastar::future<bool> ProtocolV2::send_retry_global(uint64_t global_seq)
{
  // <prepare and send RetryGlobalFrame>
  // <then:>
       return read_reconnect();
}

seastar::future<bool> ProtocolV2::send_reset(bool full)
{
  // <prepare and send ResetFrame>
  // <then:>
  //   return read_main_preamble()
  //   .then([this] (Tag tag) {
  //     expect_tag(Tag::CLIENT_IDENT, tag, conn, "post_send_reset");
         return server_connect();
  //   });
}

seastar::future<bool> ProtocolV2::server_reconnect()
{
  // handle_reconnect() logic
  // <process ReconnectFrame>
  // <case no existing:>
       return send_reset(0);
  // <retry global cases:>
  //   return send_retry_global();
  // <case wait:>
  //   return send_wait();
  // <case retry:>
  //   return send_retry();
  // <other reset cases:>
  //   return send_reset();
  // TODO: lossless policy
  //   return reuse_connection(existing, exproto);
}

void ProtocolV2::execute_accepting()
{
  trigger_state(state_t::ACCEPTING, write_state_t::none, false);
  seastar::with_gate(pending_dispatch, [this] {
      return banner_exchange()
        .then([this] {
          return server_auth();
        }).then([this] {
      //  return read_main_preamble()
      //}).then([this] (Tag tag) {
      //  switch (tag) {
      //    case Tag::CLIENT_IDENT:
              return server_connect();
      //    case Tag::SESSION_RECONNECT:
      //      return server_reconnect();
      //    default: {
      //      unexpected_tag(tag, conn, "post_server_auth");
      //    }
      //  }
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

// ACCEPTING or REPLACING state

seastar::future<> ProtocolV2::send_server_ident()
{
  // send_server_ident() logic
  // <prepare and send ServerIdentFrame>

  return seastar::now();
}

// REPLACING state

seastar::future<> ProtocolV2::send_reconnect_ok()
{
  // send_reconnect_ok() logic
  // <prepare and send ReconnectOKFrame>

  return seastar::now();
}

// READY state

seastar::future<> ProtocolV2::write_message(MessageRef msg)
{
  // TODO not implemented
  // <scheduled by parent, to send out the message on the wire>
  ceph_assert(false);
}

seastar::future<> ProtocolV2::do_keepalive()
{
  // TODO not implemented
  // <scheduled by parent, to send out KeepAliveFrame on the wire>
  ceph_assert(false);
}

seastar::future<> ProtocolV2::do_keepalive_ack()
{
  // TODO not implemented
  // <scheduled by parent, to send out KeepAliveAckFrame on the wire>
  ceph_assert(false);
}

void ProtocolV2::execute_ready()
{
  // <schedule sending messages, AckFrame, KeepAliveFrame, KeepAliveAckFrame,
  //  i.e. trigger READY state with write_state_t::open>
  //   trigger_state(state_t::READY, write_state_t::open, false);
  // TODO: schedule reading messages, AckFrame, KeepAliveFrame, KeepAliveAckFrame
  state = state_t::READY;
  logger().info("{} reaches READY state successfully.", conn);
  close();
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
