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
  conn.target_addr = _peer_addr;
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
  return write_flush(std::move(bl))
    .then([this] {
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
        }).then([this] (entity_type_t _peer_type,
                        entity_addr_t _peer_addr) {
          if (conn.peer_type != _peer_type) {
            logger().debug("{} connection peer type does not match what peer advertises {} != {}",
                           conn, conn.peer_type, (int)_peer_type);
            dispatch_reset();
            abort_in_close();
          }
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
        .then([this] (entity_type_t _peer_type,
                      entity_addr_t _peer_addr) {
          ceph_assert(conn.get_peer_type() == -1);
          conn.peer_type = _peer_type;

          // TODO: lossless policy
          conn.policy = SocketPolicy::stateless_server(0);
          logger().debug("{} accept of host type {}, lossy={} server={} standby={} resetcheck={}",
                         conn, (int)_peer_type,
                         conn.policy.lossy, conn.policy.server,
                         conn.policy.standby, conn.policy.resetcheck);
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
