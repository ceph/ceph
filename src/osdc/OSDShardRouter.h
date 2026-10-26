// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <vector>

/**
 * OSDShardRouter
 *
 * Per-OSD-session routing of ops to the reactor (core) of a crimson OSD that
 * runs their PG. A crimson OSD accepts client connections on one port per
 * reactor and names, in each MOSDOpReply, the reactor that runs the op's PG
 * and that reactor's port. The router learns those from replies, opens a
 * connection to a reactor's port the first time an op for one of its PGs is
 * sent, and hands that connection out for the PG's ops. Anything it does not
 * know (classic OSD, PG not seen yet, reactor without a port, routing paused)
 * goes on the session's main connection, where the OSD forwards it, so
 * routing is only ever a hint.
 *
 * Ordering: while a PG has ops in flight they all stay on one connection;
 * a PG moves to another connection only when it is idle.
 *
 * Fallback: a reactor connection that resets before delivering any reply
 * (port blocked by a firewall, not exposed by a container, ...) pauses
 * routing for the session, starting at 10s and doubling up to 10 minutes;
 * the first reply received on a reactor connection resets the backoff.
 *
 * Not thread safe except owns() and index_of(), which may be called without
 * the session lock (as the dispatch path does); everything else runs under
 * the session lock. Templated on the PG id and connection types so it can be
 * unit tested without a messenger; Objecter uses <spg_t, ConnectionRef>.
 */
template <typename PGId, typename ConnRef,
          typename Clock = std::chrono::steady_clock>
class OSDShardRouter {
public:
  using time_point = typename Clock::time_point;
  using duration = typename Clock::duration;
  /// open a connection to the OSD on `port`; may return null
  using connect_fn_t = std::function<ConnRef(uint16_t port)>;

  static constexpr duration initial_backoff = std::chrono::seconds(10);
  static constexpr duration max_backoff = std::chrono::minutes(10);

  /// The connection for an op of `pgid`, pinned until unroute().
  /// enabled == false: a PG with no ops in flight takes `main`; one with ops
  /// in flight stays where they are, so its ops never overtake each other.
  ConnRef route(const PGId& pgid, const ConnRef& main,
                const connect_fn_t& connect, time_point now,
                bool enabled = true) {
    auto& r = routes[pgid];
    if (r.inflight == 0) {
      r.con = enabled ? pick(pgid, main, connect, now) : main;
    }
    ++r.inflight;
    return r.con;
  }

  /// An op of `pgid` that route() sent on `con` completed or is resent.
  void unroute(const PGId& pgid, const ConnRef& con) {
    auto it = routes.find(pgid);
    if (it != routes.end() && it->second.con == con &&
        --it->second.inflight == 0) {
      routes.erase(it);
    }
  }

  /// A reply for `pgid`, received on `via`, said the PG runs on reactor
  /// `core`, which accepts client connections on `port` (port 0: none).
  void learn(const PGId& pgid, uint16_t core, uint16_t port, const ConnRef& via) {
    if (port == 0) {
      return;
    }
    pg_cores[pgid] = core;
    if (core >= ports.size()) {
      ports.resize(core + 1, 0);
      std::lock_guard l{conns_lock};
      conns.resize(core + 1);
      proven.resize(core + 1, false);
    }
    ports[core] = port;
    std::lock_guard l{conns_lock};
    for (size_t i = 0; i < conns.size(); ++i) {
      if (conns[i] && conns[i] == via) {
        proven[i] = true;   // this reactor port is reachable
        backoff = initial_backoff;
      }
    }
  }

  /// The reactor connection index of `c`, or -1.
  template <typename Ptr>
  int index_of(const Ptr* c) const {
    std::lock_guard l{conns_lock};
    for (size_t i = 0; i < conns.size(); ++i) {
      if (conns[i] && &*conns[i] == c) {
        return static_cast<int>(i);
      }
    }
    return -1;
  }

  /// `c` is one of our reactor connections
  bool owns(const ConnRef& c) const {
    if (!c) {
      return false;
    }
    std::lock_guard l{conns_lock};
    return std::find(conns.begin(), conns.end(), c) != conns.end();
  }

  /// Reactor connection `index` reset: forget it and return the PGs that
  /// were pinned to it (their ops must be resent, and any backoffs the OSD
  /// sent on it are gone). If it never delivered a reply, pause routing.
  std::vector<PGId> retire(int index, time_point now) {
    ConnRef dead;
    bool was_proven = false;
    {
      std::lock_guard l{conns_lock};
      if (index < 0 || static_cast<size_t>(index) >= conns.size()) {
        return {};
      }
      std::swap(dead, conns[index]);
      was_proven = proven[index];
      proven[index] = false;
    }
    if (!dead) {
      return {};
    }
    if (!was_proven) {
      paused_until = now + backoff;
      backoff = std::min<duration>(backoff * 2, max_backoff);
    }
    std::vector<PGId> pgs;
    for (auto it = routes.begin(); it != routes.end();) {
      if (it->second.con == dead) {
        pgs.push_back(it->first);
        it = routes.erase(it);
      } else {
        ++it;
      }
    }
    return pgs;
  }

  /// The session's main connection was reopened or closed (e.g. the OSD
  /// restarted; its reactor ports may differ): forget everything learned and
  /// return the reactor connections for the caller to mark down. The
  /// backoff state is kept.
  std::vector<ConnRef> reset() {
    std::vector<ConnRef> old;
    {
      std::lock_guard l{conns_lock};
      old.swap(conns);
      proven.clear();
    }
    std::erase_if(old, [](const auto& c) { return !c; });
    routes.clear();
    pg_cores.clear();
    ports.clear();
    return old;
  }

  /// the reactor last named for `pgid`, if any
  std::optional<uint16_t> core_of(const PGId& pgid) const {
    auto it = pg_cores.find(pgid);
    if (it == pg_cores.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  bool is_paused(time_point now) const { return now < paused_until; }

  size_t num_conns() const {
    std::lock_guard l{conns_lock};
    return std::count_if(conns.begin(), conns.end(),
                         [](const auto& c) { return bool(c); });
  }

private:
  struct route_t {
    ConnRef con;
    unsigned inflight = 0;
  };

  ConnRef pick(const PGId& pgid, const ConnRef& main,
               const connect_fn_t& connect, time_point now) {
    if (is_paused(now)) {
      return main;
    }
    auto it = pg_cores.find(pgid);
    if (it == pg_cores.end()) {
      return main;   // not named yet: the first reply will
    }
    const uint16_t core = it->second;
    if (core >= ports.size() || ports[core] == 0) {
      return main;
    }
    {
      std::lock_guard l{conns_lock};
      if (conns[core]) {
        return conns[core];
      }
    }
    // non-blocking: messages queue until it is established; if it fails,
    // its reset retires it and resends what was queued on it
    ConnRef c = connect(ports[core]);
    if (!c) {
      return main;
    }
    std::lock_guard l{conns_lock};
    conns[core] = c;
    proven[core] = false;
    return c;
  }

  std::map<PGId, route_t> routes;      ///< PGs with ops in flight
  std::map<PGId, uint16_t> pg_cores;   ///< PG -> reactor, from replies
  std::vector<uint16_t> ports;         ///< reactor -> port, from replies
  mutable std::mutex conns_lock;       ///< guards conns and proven
  std::vector<ConnRef> conns;          ///< reactor -> connection, lazily
  std::vector<bool> proven;            ///< a reply arrived on conns[i]
  time_point paused_until{};
  duration backoff = initial_backoff;
};
