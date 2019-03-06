// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "Protocol.h"
#include "msg/async/frames_v2.h"

namespace ceph::net {

class ProtocolV2 : public Protocol {
 public:
  ProtocolV2(Dispatcher& dispatcher,
             SocketConnection& conn,
             SocketMessenger& messenger);
  ~ProtocolV2() override;

 private:
  void start_connect(const entity_addr_t& peer_addr,
                     const entity_type_t& peer_type) override;

  void start_accept(SocketFRef&& socket,
                    const entity_addr_t& peer_addr) override;

  void trigger_close() override;

  seastar::future<> write_message(MessageRef msg) override;

  seastar::future<> do_keepalive() override;

  seastar::future<> do_keepalive_ack() override;

 private:
  enum state_t {
    NONE,
    ACCEPTING,
    CONNECTING,
    READY,
    STANDBY,
    WAIT,           // ? CLIENT_WAIT
    SERVER_WAIT,    // ?
    REPLACING,      // ?
    CLOSING
  };
  state_t state = state_t::NONE;

  static const char *get_state_name(int state) {
    const char *const statenames[] = {"NONE",
                                      "ACCEPTING",
                                      "CONNECTING",
                                      "READY",
                                      "STANDBY",
                                      "WAIT",           // ? CLIENT_WAIT
                                      "SERVER_WAIT",    // ?
                                      "REPLACING",      // ?
                                      "CLOSING"};
    return statenames[state];
  }

  void trigger_state(state_t state, write_state_t write_state, bool reentrant);

  uint64_t global_seq = 0;

 private:
  seastar::future<> fault();
  seastar::future<> banner_exchange();

  // CONNECTING (client)
  seastar::future<> handle_auth_reply();
  inline seastar::future<> client_auth() {
    std::vector<uint32_t> empty;
    return client_auth(empty);
  }
  seastar::future<> client_auth(std::vector<uint32_t> &allowed_methods);

  seastar::future<bool> process_wait();
  seastar::future<bool> client_connect();
  seastar::future<bool> client_reconnect();
  void execute_connecting();

  // ACCEPTING (server)
  seastar::future<> _auth_bad_method(int r);
  seastar::future<> _handle_auth_request(bufferlist& auth_payload, bool more);
  seastar::future<> server_auth();

  seastar::future<bool> send_wait();

  seastar::future<bool> handle_existing_connection(SocketConnectionRef existing);
  seastar::future<bool> server_connect();

  seastar::future<bool> read_reconnect();
  seastar::future<bool> send_retry(uint64_t connect_seq);
  seastar::future<bool> send_retry_global(uint64_t global_seq);
  seastar::future<bool> send_reset(bool full);
  seastar::future<bool> server_reconnect();

  void execute_accepting();

  // ACCEPTING/REPLACING (server)
  seastar::future<> send_server_ident();

  // REPLACING (server)
  seastar::future<> send_reconnect_ok();

  // READY
  void execute_ready();

  // STANDBY
  void execute_standby();

  // WAIT
  void execute_wait();

  // SERVER_WAIT
  void execute_server_wait();
};

} // namespace ceph::net
