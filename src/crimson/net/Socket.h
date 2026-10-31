// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <optional>
#include <utility>
#include <vector>

#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>

#include "include/buffer.h"

#include "crimson/common/log.h"
#include "Errors.h"
#include "Fwd.h"

#ifdef UNIT_TESTS_BUILT
#include "Interceptor.h"
#endif

namespace crimson::net {

class Socket;
using SocketRef = std::unique_ptr<Socket>;
using SocketFRef = seastar::foreign_ptr<SocketRef>;

class Socket {
  struct construct_tag {};

public:
  // if acceptor side, peer is using a different port (ephemeral_port)
  // if connector side, I'm using a different port (ephemeral_port)
  enum class side_t {
    acceptor,
    connector
  };
  Socket(seastar::connected_socket &&, side_t, uint16_t e_port, construct_tag);

  ~Socket();

  Socket(Socket&& o) = delete;

  seastar::shard_id get_shard_id() const {
    return sid;
  }

  side_t get_side() const {
    return side;
  }

  uint16_t get_ephemeral_port() const {
    return ephemeral_port;
  }

  seastar::socket_address get_local_address() const {
    return socket.local_address();
  }

  bool is_shutdown() const {
    assert(seastar::this_shard_id() == sid);
    return socket_is_shutdown;
  }

  // learn my ephemeral_port as connector.
  // unfortunately, there's no way to identify which port I'm using as
  // connector with current seastar interface.
  void learn_ephemeral_port_as_connector(uint16_t port) {
    assert(side == side_t::connector &&
           (ephemeral_port == 0 || ephemeral_port == port));
    ephemeral_port = port;
  }

  /// read the requested number of bytes into a bufferlist
  seastar::future<bufferlist> read(size_t bytes);

  seastar::future<bufferptr> read_exactly(size_t bytes);

  seastar::future<> write(bufferlist);

  seastar::future<> flush();

  seastar::future<> write_flush(bufferlist);

  // preemptively disable further reads or writes, can only be shutdown once.
  void shutdown();

  /// Socket can only be closed once.
  seastar::future<> close();

  static seastar::future<SocketRef>
  connect(const entity_addr_t& peer_addr);

  /*
   * test interfaces
   */

  // shutdown for tests
  void force_shutdown() {
    assert(seastar::this_shard_id() == sid);
    socket.shutdown_input();
    socket.shutdown_output();
  }

  // shutdown input_stream only, for tests
  void force_shutdown_in() {
    assert(seastar::this_shard_id() == sid);
    socket.shutdown_input();
  }

  // shutdown output_stream only, for tests
  void force_shutdown_out() {
    assert(seastar::this_shard_id() == sid);
    socket.shutdown_output();
  }

private:
  const seastar::shard_id sid;
  seastar::connected_socket socket;
  seastar::input_stream<char> in;
  seastar::output_stream<char> out;
  bool socket_is_shutdown;
  side_t side;
  uint16_t ephemeral_port;

#ifndef NDEBUG
  bool closed = false;
#endif

  /// buffer state for read()
  struct {
    bufferlist buffer;
    size_t remaining;
  } r;

#ifdef UNIT_TESTS_BUILT
public:
  void set_trap(bp_type_t type, bp_action_t action, socket_blocker* blocker_);

private:
  seastar::future<> try_trap_pre(bp_action_t& trap);

  seastar::future<> try_trap_post(bp_action_t& trap);

  bp_action_t next_trap_read = bp_action_t::CONTINUE;
  bp_action_t next_trap_write = bp_action_t::CONTINUE;
  socket_blocker* blocker = nullptr;

#endif
  friend class ShardedServerSocket;
};

using listen_ertr = crimson::errorator<
  crimson::ct_error::address_in_use, // The address is already bound
  crimson::ct_error::address_not_available // https://techoverflow.net/2021/08/06/how-i-fixed-python-oserror-errno-99-cannot-assign-requested-address/
  >;

class ShardedServerSocket
    : public seastar::peering_sharded_service<ShardedServerSocket> {
  struct construct_tag {};

public:
  ShardedServerSocket(
      seastar::shard_id sid,
      bool dispatch_only_on_primary_sid,
      bool listen_on_primary_only,
      construct_tag);

  ~ShardedServerSocket();

  ShardedServerSocket(ShardedServerSocket&&) = delete;
  ShardedServerSocket(const ShardedServerSocket&) = delete;
  ShardedServerSocket& operator=(ShardedServerSocket&&) = delete;
  ShardedServerSocket& operator=(const ShardedServerSocket&) = delete;

  bool is_fixed_shard_dispatching() const {
    return dispatch_only_on_primary_sid;
  }

  using port_range_t = std::pair<uint32_t, uint32_t>;  // [min, max]

  /// shard_port_range: also listen on one port per reactor, taken from this
  /// range (skipping ports in use), each accepted on its reactor. A reactor
  /// that gets no port is not an error; see get_shard_ports().
  listen_ertr::future<> listen(
    entity_addr_t addr,
    std::optional<port_range_t> shard_port_range = std::nullopt);

  /// Port bound for each reactor by listen() (0 = none); empty without
  /// per-reactor ports. Valid on every shard once listen() resolves.
  const std::vector<uint16_t>& get_shard_ports() const {
    return shard_ports;
  }

  using accept_func_t =
    std::function<seastar::future<>(SocketRef, entity_addr_t)>;
  seastar::future<> accept(accept_func_t &&_fn_accept);

  seastar::future<> shutdown_destroy();

  static seastar::future<ShardedServerSocket*> create(
      bool dispatch_only_on_this_shard,
      bool listen_on_primary_only = false);

private:
  const seastar::shard_id primary_sid;
  /// XXX: Remove once all infrastructure uses multi-core messenger
  const bool dispatch_only_on_primary_sid;
  // Bind and accept on primary_sid only. Used by per-reactor messengers so
  // each one takes a single port instead of listening on every reactor.
  const bool listen_on_primary_only;
  entity_addr_t listen_addr;
  std::optional<seastar::server_socket> listener;
  /// Per-reactor ports. Seastar's posix stack creates the kernel socket only
  /// on shard 0; there it is bound with fixed_cpu(k) and accept() hands each
  /// connection to reactor k, where an accept endpoint for the same address
  /// must exist. So shard 0 holds every reactor's kernel socket and shard k
  /// holds the endpoint for its own port.
  std::vector<seastar::server_socket> shard_listeners;
  /// the port of each entry of shard_listeners
  std::vector<uint16_t> shard_listener_ports;
  std::vector<uint16_t> shard_ports;

  /// shard 0: bind a kernel socket per reactor from [min_port, max_port]
  std::vector<uint16_t> bind_shard_ports(const entity_addr_t& addr,
                                         uint32_t min_port, uint32_t max_port);
  /// all shards: bind_shard_ports(), then each reactor's accept endpoint;
  /// a reactor whose endpoint fails gets no port, on every shard
  seastar::future<> listen_shard_ports(entity_addr_t addr,
                                       std::optional<port_range_t> range);
  /// reactor k's accept endpoint for ports[k]; false if it failed
  bool listen_shard_endpoint(const entity_addr_t& addr,
                             const std::vector<uint16_t>& ports);
  /// close our listener on `port`, if any (before accept() armed it)
  void close_shard_listener(uint16_t port);
  void arm_accept_loop(seastar::server_socket& sock);
  seastar::gate shutdown_gate;
  accept_func_t fn_accept;

  using sharded_service_t = seastar::sharded<ShardedServerSocket>;
  std::unique_ptr<sharded_service_t> service;
};

} // namespace crimson::net
