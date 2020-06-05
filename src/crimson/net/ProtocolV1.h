// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "Protocol.h"

class AuthAuthorizer;
class AuthSessionHandler;

namespace crimson::net {

class ProtocolV1 final : public Protocol {
 public:
  ProtocolV1(ChainedDispatchersRef& dispatcher,
             SocketConnection& conn,
             SocketMessenger& messenger);
  ~ProtocolV1() override;
  void print(std::ostream&) const final;
 private:
  bool is_connected() const override;

  void start_connect(const entity_addr_t& peer_addr,
                     const entity_name_t& peer_name) override;

  void start_accept(SocketRef&& socket,
                    const entity_addr_t& peer_addr) override;

  void trigger_close() override;

  ceph::bufferlist do_sweep_messages(
      const std::deque<MessageRef>& msgs,
      size_t num_msgs,
      bool require_keepalive,
      std::optional<utime_t> keepalive_ack,
      bool require_ack) override;

 private:
  SocketMessenger &messenger;

  enum class state_t {
    none,
    accepting,
    connecting,
    open,
    standby,
    wait,
    closing
  };
  state_t state = state_t::none;

  // state for handshake
  struct Handshake {
    ceph_msg_connect connect;
    ceph_msg_connect_reply reply;
    ceph::bufferlist auth_payload;  // auth(orizer) payload read off the wire
    ceph::bufferlist auth_more;     // connect-side auth retry (we added challenge)
    std::chrono::milliseconds backoff;
    uint32_t connect_seq = 0;
    uint32_t peer_global_seq = 0;
    uint32_t global_seq;
  } h;

  std::unique_ptr<AuthSessionHandler> session_security;

  // state for an incoming message
  struct MessageReader {
    ceph_msg_header header;
    ceph_msg_footer footer;
    bufferlist front;
    bufferlist middle;
    bufferlist data;
  } m;

  struct Keepalive {
    struct {
      const char tag = CEPH_MSGR_TAG_KEEPALIVE2;
      ceph_timespec stamp;
    } __attribute__((packed)) req;
    struct {
      const char tag = CEPH_MSGR_TAG_KEEPALIVE2_ACK;
      ceph_timespec stamp;
    } __attribute__((packed)) ack;
    ceph_timespec ack_stamp;
  } k;

 private:
  // connecting
  void reset_session();
  seastar::future<stop_t> handle_connect_reply(crimson::net::msgr_tag_t tag);
  seastar::future<stop_t> repeat_connect();
  ceph::bufferlist get_auth_payload();

  // accepting
  seastar::future<stop_t> send_connect_reply(
      msgr_tag_t tag, bufferlist&& authorizer_reply = {});
  seastar::future<stop_t> send_connect_reply_ready(
      msgr_tag_t tag, bufferlist&& authorizer_reply);
  seastar::future<stop_t> replace_existing(
      SocketConnectionRef existing,
      bufferlist&& authorizer_reply,
      bool is_reset_from_peer = false);
  seastar::future<stop_t> handle_connect_with_existing(
      SocketConnectionRef existing, bufferlist&& authorizer_reply);
  bool require_auth_feature() const;
  seastar::future<stop_t> repeat_handle_connect();

  // open
  seastar::future<> handle_keepalive2_ack();
  seastar::future<> handle_keepalive2();
  seastar::future<> handle_ack();
  seastar::future<> maybe_throttle();
  seastar::future<> read_message();
  seastar::future<> handle_tags();

  enum class open_t {
    connected,
    accepted
  };
  void execute_open(open_t type);

  // replacing
  // the number of connections initiated in this session, increment when a
  // new connection is established
  uint32_t connect_seq() const { return h.connect_seq; }
  // the client side should connect us with a gseq. it will be reset with
  // the one of exsting connection if it's greater.
  uint32_t peer_global_seq() const { return h.peer_global_seq; }
  // current state of ProtocolV1
  state_t get_state() const { return state; }

  seastar::future<> fault();
};

} // namespace crimson::net
