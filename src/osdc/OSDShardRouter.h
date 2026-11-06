// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
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
 * routing for the session, starting at initial_backoff and doubling up to
 * max_backoff; the first reply received on a reactor connection resets the
 * backoff. retire() reports whether the connection had been proven, so the
 * caller can also remember unreachable ports beyond this session.
 *
 * Locking: everything runs under the session lock except owns() and
 * index_of(), which the dispatch path calls before taking it; they read a
 * lock-free mirror of the connections. Templated on the PG id, connection
 * and clock types so it can be unit tested without a messenger; Objecter
 * uses <spg_t, ConnectionRef, ceph::coarse_mono_clock>.
 */
template <typename PGId, typename ConnRef,
          typename Clock = std::chrono::steady_clock>
class OSDShardRouter {
public:
  using time_point = typename Clock::time_point;
  using duration = typename Clock::duration;
  /// open a connection to the OSD on `port`; may return null
  using connect_fn_t = std::function<ConnRef(uint16_t port)>;

  static constexpr duration initial_backoff = std::chrono::minutes(1);
  static constexpr duration max_backoff = std::chrono::hours(1);
  /// reactors beyond this are reached through the main connection
  static constexpr size_t max_cores = 256;

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

  /// A current reply for `pgid`, received on `via` at `now`, said the PG runs
  /// on reactor `core`, which accepts client connections on `port` (port 0:
  /// none). Cheap when nothing changed: no lock, no map insertion.
  /// Returns true if this reply is the first one on a reactor connection
  /// (its port is reachable after all).
  bool learn(const PGId& pgid, uint16_t core, uint16_t port,
             const ConnRef& via, time_point now) {
    if (port == 0 || core >= max_cores) {
      return false;
    }
    if (auto it = pg_cores.find(pgid); it == pg_cores.end()) {
      pg_cores.emplace(pgid, core);
    } else if (it->second != core) {
      it->second = core;
    }
    if (core >= ports.size()) {
      ports.resize(core + 1, 0);
      conns.resize(core + 1);
      proven.resize(core + 1, false);
      last_reply.resize(core + 1);
    }
    ports[core] = port;
    // a reply on one of our reactor connections: it is reachable and alive
    for (size_t i = 0; i < conns.size(); ++i) {
      if (conns[i] && conns[i] == via) {
        last_reply[i] = now;
        if (!proven[i]) {
          proven[i] = true;
          backoff = initial_backoff;
          return true;
        }
        break;
      }
    }
    return false;
  }

  /// The reactor connection index of `c`, or -1. No lock.
  template <typename Ptr>
  int index_of(const Ptr* c) const {
    if (!c) {
      return -1;
    }
    for (size_t i = 0; i < max_cores; ++i) {
      if (mirror[i].load(std::memory_order_acquire) == c) {
        return static_cast<int>(i);
      }
    }
    return -1;
  }

  /// `c` is one of our reactor connections. No lock.
  bool owns(const ConnRef& c) const {
    return c && index_of(&*c) >= 0;
  }

  struct retired_t {
    std::vector<PGId> pgs;   ///< PGs that were pinned to it
    bool was_proven = false; ///< it had delivered a reply
  };

  /// Reactor connection `index` reset or was given up on: forget it and
  /// return the PGs that were pinned to it (their ops must be resent, and any
  /// backoffs the OSD sent on it are gone). If it never delivered a reply,
  /// pause routing.
  retired_t retire(int index, time_point now) {
    retired_t ret;
    if (index < 0 || static_cast<size_t>(index) >= conns.size() ||
        !conns[index]) {
      return ret;
    }
    ConnRef dead;
    std::swap(dead, conns[index]);
    mirror[index].store(nullptr, std::memory_order_release);
    ret.was_proven = proven[index];
    proven[index] = false;
    if (!ret.was_proven) {
      pause(now);
    }
    for (auto it = routes.begin(); it != routes.end();) {
      if (it->second.con == dead) {
        ret.pgs.push_back(it->first);
        it = routes.erase(it);
      } else {
        ++it;
      }
    }
    return ret;
  }

  /// Reactor connections with ops pinned to them that have delivered no
  /// reply since `cutoff`: likely dead without a reset (e.g. a silently
  /// dropped TCP flow). The caller retires them and resends their ops.
  std::vector<int> stale(time_point cutoff) const {
    std::vector<int> ret;
    for (size_t i = 0; i < conns.size(); ++i) {
      if (!conns[i] || last_reply[i] >= cutoff) {
        continue;
      }
      if (std::any_of(routes.begin(), routes.end(), [&](const auto& r) {
            return r.second.con == conns[i];
          })) {
        ret.push_back(static_cast<int>(i));
      }
    }
    return ret;
  }

  /// The session's main connection was reopened or closed (e.g. the OSD
  /// restarted; its reactor ports may differ): forget everything learned and
  /// return the reactor connections for the caller to mark down. The
  /// backoff state is kept.
  std::vector<ConnRef> reset() {
    std::vector<ConnRef> old;
    old.swap(conns);
    for (auto& m : mirror) {
      m.store(nullptr, std::memory_order_release);
    }
    std::erase_if(old, [](const auto& c) { return !c; });
    proven.clear();
    last_reply.clear();
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

  /// pause routing until at least `until` (e.g. ports known unreachable
  /// from another session), without touching the backoff
  void pause_until(time_point until) {
    paused_until = std::max(paused_until, until);
  }

  /// pause routing as if a reactor port had proven unreachable
  void pause(time_point now) {
    paused_until = std::max(paused_until, now + backoff);
    backoff = std::min<duration>(backoff * 2, max_backoff);
  }

  /// the reactor connection at `index`, if any
  ConnRef conn_at(int index) const {
    if (index < 0 || static_cast<size_t>(index) >= conns.size()) {
      return ConnRef{};
    }
    return conns[index];
  }

  /// any reactor connection open
  bool has_conns() const {
    return std::any_of(conns.begin(), conns.end(),
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
    if (conns[core]) {
      return conns[core];
    }
    // non-blocking: messages queue until it is established; if it fails,
    // its reset retires it and resends what was queued on it
    ConnRef c = connect(ports[core]);
    if (!c) {
      return main;
    }
    conns[core] = c;
    mirror[core].store(&*c, std::memory_order_release);
    proven[core] = false;
    last_reply[core] = now;   // the staleness clock starts at the connect
    return c;
  }

  std::map<PGId, route_t> routes;      ///< PGs with ops in flight
  std::map<PGId, uint16_t> pg_cores;   ///< PG -> reactor, from replies
  std::vector<uint16_t> ports;         ///< reactor -> port, from replies
  std::vector<ConnRef> conns;          ///< reactor -> connection, lazily
  std::vector<bool> proven;            ///< a reply arrived on conns[i]
  std::vector<time_point> last_reply;  ///< last reply on conns[i]
  /// conns[i]'s raw pointer, for owns()/index_of() without the session lock
  std::array<std::atomic<const void*>, max_cores> mirror{};
  time_point paused_until{};
  duration backoff = initial_backoff;
};
