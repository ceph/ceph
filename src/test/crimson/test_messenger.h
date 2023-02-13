// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "msg/msg_types.h"

namespace ceph::net::test {

constexpr uint64_t CMD_CLI_NONCE = 1;
constexpr int64_t CMD_CLI_OSD = 1;
constexpr uint64_t TEST_NONCE = 2;
constexpr int64_t TEST_OSD = 2;
constexpr uint64_t CMD_SRV_NONCE = 3;
constexpr int64_t CMD_SRV_OSD = 3;
constexpr uint64_t TEST_PEER_NONCE = 2;
constexpr int64_t TEST_PEER_OSD = 4;

inline entity_addr_t get_test_peer_addr(
    const entity_addr_t &cmd_peer_addr) {
  entity_addr_t test_peer_addr = cmd_peer_addr;
  test_peer_addr.set_port(cmd_peer_addr.get_port() + 1);
  test_peer_addr.set_nonce(TEST_PEER_NONCE);
  return test_peer_addr;
}

enum class cmd_t : char {
  none = '\0',
  shutdown,
  suite_start,
  suite_stop,
  suite_connect_me,
  suite_send_me,
  suite_keepalive_me,
  suite_markdown,
  suite_recv_op
};

enum class policy_t : char {
  none = '\0',
  stateful_server,
  stateless_server,
  lossless_peer,
  lossless_peer_reuse,
  lossy_client,
  lossless_client
};

inline std::ostream& operator<<(std::ostream& out, const cmd_t& cmd) {
  switch(cmd) {
   case cmd_t::none:
    return out << "none";
   case cmd_t::shutdown:
    return out << "shutdown";
   case cmd_t::suite_start:
    return out << "suite_start";
   case cmd_t::suite_stop:
    return out << "suite_stop";
   case cmd_t::suite_connect_me:
    return out << "suite_connect_me";
   case cmd_t::suite_send_me:
    return out << "suite_send_me";
   case cmd_t::suite_keepalive_me:
    return out << "suite_keepalive_me";
   case cmd_t::suite_markdown:
    return out << "suite_markdown";
   case cmd_t::suite_recv_op:
    return out << "suite_recv_op";
   default:
    ceph_abort();
  }
}

inline std::ostream& operator<<(std::ostream& out, const policy_t& policy) {
  switch(policy) {
   case policy_t::none:
    return out << "none";
   case policy_t::stateful_server:
    return out << "stateful_server";
   case policy_t::stateless_server:
    return out << "stateless_server";
   case policy_t::lossless_peer:
    return out << "lossless_peer";
   case policy_t::lossless_peer_reuse:
    return out << "lossless_peer_reuse";
   case policy_t::lossy_client:
    return out << "lossy_client";
   case policy_t::lossless_client:
    return out << "lossless_client";
   default:
    ceph_abort();
  }
}

} // namespace ceph::net::test
