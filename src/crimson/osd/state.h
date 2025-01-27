// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string_view>
#include <ostream>

#include <seastar/core/shared_future.hh>

class OSDMap;

namespace crimson::osd {

// seastar::sharded puts start_single on core 0
constexpr core_id_t PRIMARY_CORE = 0;

/**
 * OSDState
 *
 * Maintains state representing the OSD's progress from booting through
 * shutdown.
 *
 * Shards other than PRIMARY_CORE may use their local instance to check
 * on ACTIVE and STOPPING.  All other methods are restricted to
 * PRIMARY_CORE (such methods start with an assert to this effect).
 */
class OSDState : public seastar::peering_sharded_service<OSDState> {

  enum class State {
    INITIALIZING,
    PREBOOT,
    BOOTING,
    ACTIVE,
    PRESTOP,
    STOPPING,
    WAITING_FOR_HEALTHY,
  };

  State state = State::INITIALIZING;
  mutable seastar::shared_promise<> wait_for_active;

  /// Sets local instance state to active, called from set_active
  void _set_active() {
    state = State::ACTIVE;
    wait_for_active.set_value();
    wait_for_active = {};
  }
  /// Sets local instance state to stopping, called from set_stopping
  void _set_stopping() {
    state = State::STOPPING;
    wait_for_active.set_exception(crimson::common::system_shutdown_exception{});
    wait_for_active = {};
  }
public:
  bool is_initializing() const {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    return state == State::INITIALIZING;
  }
  bool is_preboot() const {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    return state == State::PREBOOT;
  }
  bool is_booting() const {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    return state == State::BOOTING;
  }
  bool is_active() const {
    return state == State::ACTIVE;
  }
  seastar::future<> when_active() const {
    return is_active() ? seastar::now()
                       : wait_for_active.get_shared_future();
  };
  bool is_prestop() const {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    return state == State::PRESTOP;
  }
  bool is_stopping() const {
    return state == State::STOPPING;
  }
  bool is_waiting_for_healthy() const {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    return state == State::WAITING_FOR_HEALTHY;
  }
  void set_preboot() {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    state = State::PREBOOT;
  }
  void set_booting() {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    state = State::BOOTING;
  }
  /// Sets all shards to active
  seastar::future<> set_active() {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    return container().invoke_on_all([](auto& osd_state) {
      osd_state._set_active();
    });
  }
  void set_prestop() {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    state = State::PRESTOP;
  }
  /// Sets all shards to stopping
  seastar::future<> set_stopping() {
    ceph_assert(seastar::this_shard_id() == PRIMARY_CORE);
    return container().invoke_on_all([](auto& osd_state) {
      osd_state._set_stopping();
    });
  }
  std::string_view to_string() const {
    switch (state) {
    case State::INITIALIZING: return "initializing";
    case State::PREBOOT: return "preboot";
    case State::BOOTING: return "booting";
    case State::ACTIVE: return "active";
    case State::PRESTOP: return "prestop";
    case State::STOPPING: return "stopping";
    case State::WAITING_FOR_HEALTHY: return "waiting_for_healthy";
    default: return "???";
    }
  }
};

inline std::ostream&
operator<<(std::ostream& os, const OSDState& s) {
  return os << s.to_string();
}
}
