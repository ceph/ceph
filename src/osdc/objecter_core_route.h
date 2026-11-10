// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "common/ceph_time.h"
#include "include/types.h"
#include "include/utime.h"
#include "msg/Connection.h"
#include "osd/osd_types.h"

/**
 * Routing the ops of an Objecter's OSD session to the reactor cores of a
 * Crimson OSD (objecter_use_osd_core_hints).
 *
 * A Crimson OSD with per-core listeners (crimson_osd_core_listeners)
 * attaches to its op replies an osd_core_hint_t: the core owning the
 * op's PG, and the port of that core's listener. The session opens a
 * connection to that port, on the OSD's address its main connection uses,
 * and, once it is connected, sends the PG's ops on it, saving the OSD a
 * cross-core hop per op.
 *
 * Reachability: a lossy client connection that cannot connect (e.g. a
 * firewall letting only the OSD's main ports through) is not reset by
 * the messenger: it keeps retrying, holding the messages queued on it.
 * So a PG is routed to a core connection only once its handshake
 * completed (SessionRoutes::note_connected()). One not connected in time
 * is given up on, and its host is blocked for a while (HostBlocks).
 *
 * Liveness: a connected core connection that silently stops working (e.g.
 * a firewall dropping its flow, without a reset) would hold its ops until
 * the messenger's idle timeout. Those carrying laggy ops are probed with
 * a keepalive (SessionRoutes::probe_core()), and given up on if it is not
 * answered.
 *
 * Ordering: the OSD orders a client's ops per connection. A PG's route
 * changes only while none of its ops may still be on the way to the OSD
 * on the current connection: none in flight, and none canceled but not
 * answered yet (see SessionRoutes::unanswered). A PG kept busy would
 * never switch; after objecter_osd_core_switch_grace, its new ops are
 * held until it drains (see pg_route_t::holding).
 *
 * SessionRoutes holds the routing state of one OSD session, and makes the
 * routing decisions. The Objecter does the rest: sending, holding and
 * resending ops, the hold timers, connecting, perf counters and logging.
 * All of it is protected by its OSD session's lock (OSDSession::lock),
 * except has_drained_holds.
 */
namespace osdc::core_route {

using time_point = ceph::coarse_mono_time;

/// the route of an op (Objecter::Op::core_route)
struct op_route_t {
  /// set while the op is accounted for as in flight in its session's
  /// pg_routes: the PG it was sent for, and the core connection it was
  /// sent on (nullopt: the session's main connection)
  std::optional<spg_t> pgid;
  std::optional<uint32_t> core;
  /// held, not sent, until its PG switches to another connection (see
  /// pg_route_t::holding); still in its session's ops. Set only while
  /// that hold lasts: when it ends, the op is sent, or, if paused, waits
  /// to be resent when unpaused (as any paused op)
  bool switch_held = false;

  bool is_routed() const { return pgid.has_value(); }
};

/// how a PG's ops are routed to the OSD core owning it
struct pg_route_t {
  /// the core the OSD last hinted for the PG
  std::optional<uint32_t> hinted_core;
  /// the core connection the PG's ops are sent on (nullopt: the main
  /// connection). Changed only while the PG is not busy(), so that its
  /// ops arrive in order. Always one of SessionRoutes::core_cons
  std::optional<uint32_t> core;
  /// the PG's ops in flight (sent, and still assigned to the session)
  unsigned in_flight = 0;
  /// the PG's ops canceled after being sent, and not answered yet
  /// (see SessionRoutes::unanswered)
  unsigned unanswered = 0;
  /// since when the PG has been due to switch to another connection
  /// (see SessionRoutes::desired_core()), but could not, being busy().
  /// Unset: not due to switch
  std::optional<time_point> switch_pending_since;
  /// whether, while due to switch, the PG was kept from switching
  /// only by canceled ops not answered yet (for the perf counters)
  bool delayed_by_unanswered = false;
  /// since when the PG has been due to switch, and the holds started
  /// since; unlike switch_pending_since, not restarted by an expired
  /// hold (for the perf counters)
  std::optional<time_point> switch_due_since;
  unsigned switch_holds = 0;
  /// After a switch has been pending for objecter_osd_core_switch_grace,
  /// the PG's new ops are held (op_route_t::switch_held), so that its ops
  /// in flight drain, and the PG switches. The hold ends once they drain,
  /// or after objecter_osd_core_switch_max_hold at most (the held ops
  /// are then sent on the current connection, and the cycle restarts).
  bool holding = false;
  time_point hold_since;
  /// the timer event ending the hold (0: none); and the hold's id, to
  /// tell a stale event
  uint64_t hold_event = 0;
  uint64_t hold_seq = 0;

  /// whether ops of the PG may still be on their way to (or queued at)
  /// the OSD on the route's connection: the route may not change
  bool busy() const { return in_flight + unanswered > 0; }

  /// the PG is no longer due to switch: it switched, the hint went
  /// away, or the route was reset
  void end_switch_wait() {
    switch_pending_since.reset();
    delayed_by_unanswered = false;
    switch_due_since.reset();
    switch_holds = 0;
  }
};

/// An op canceled after it was sent (e.g. timed out): the OSD may still
/// execute it, on the connection it was sent on, and it still answers
/// it. Until the answer arrives (or the connection resets), a new op of
/// the PG on another connection could overtake it.
struct unanswered_op_t {
  spg_t pgid;
  /// the core connection it was sent on (nullopt: the main connection)
  std::optional<uint32_t> core;
  time_point since;
};

/// what applying a hint did (SessionRoutes::apply_hint())
struct hint_outcome_t {
  enum class what_t {
    unchanged,  ///< the PG was hinted to this core already
    invalid,    ///< ignored: no port, or no OSD address to use it on
    blocked,    ///< ignored: connecting to the core is not allowed now
    changed,    ///< the PG is hinted to a new core
  };
  what_t what = what_t::unchanged;
  /// changed: the core hinted before
  std::optional<uint32_t> prev_core;
  /// changed: no connection to the hinted core yet. The caller opens one
  /// to 'addr', and adds it with add_core_con()
  bool connect = false;
  /// connect: the address of the core's listener (the main connection's
  /// msgr2 address, on the hinted port)
  entity_addr_t addr;
};

/// canceled ops no longer awaited: (tid, PG)
using forgotten_t = std::vector<std::pair<ceph_tid_t, spg_t>>;

/// what routing an op did (SessionRoutes::route_op())
struct route_outcome_t {
  enum class what_t {
    stayed,    ///< the PG stays on its connection
    switched,  ///< the PG switched to another connection
    pending,   ///< the PG is due to switch, but is busy()
  };
  what_t what = what_t::stayed;
  /// the connection the PG should be on
  std::optional<uint32_t> desired;
  /// switched after having been due to (for a while): how long, and the
  /// holds of the PG's ops it took
  std::optional<ceph::timespan> switch_delay;
  unsigned switch_holds = 0;
  /// pending: the PG has just become due to switch
  bool pending_started = false;
  /// pending: the PG has no op in flight, only canceled ops not answered
  /// yet; and whether that is first noticed for this switch
  bool pending_unanswered = false;
  bool first_delayed_by_unanswered = false;
  /// canceled ops given up on (see SessionRoutes::prune_unanswered())
  forgotten_t forgotten;
};

/// whether a PG's new ops are held (SessionRoutes::hold_state())
enum class hold_t {
  none,     ///< not held
  holding,  ///< held: the PG is holding them already
  due,      ///< the PG is to start holding them (start_hold())
};

/// how a hold ended (SessionRoutes::end_hold())
struct hold_end_t {
  /// how long it lasted
  ceph::timespan held_for;
  /// the timer event that would have ended it (0: none), to cancel
  uint64_t timer_event = 0;
};

/// what probing a core connection found (SessionRoutes::probe_core())
enum class probe_t {
  send,   ///< a probe starts: send a keepalive
  wait,   ///< not answered yet, and not overdue
  alive,  ///< answered: the connection works (its ops are just slow)
  dead,   ///< not answered in time: the connection likely stopped working
};

/// what resetting one core connection did (SessionRoutes::reset_core())
struct core_reset_t {
  /// the connection, removed from core_cons: to be marked down
  ConnectionRef con;
  /// the PGs whose ops were sent on it: back on the main connection
  std::set<spg_t> rerouted;
  /// the PGs whose hold ended: their held ops are to be sent
  std::set<spg_t> released_holds;
  /// the timer events of the ended holds, to cancel
  std::vector<uint64_t> timer_events;
};

/// what resetting all core connections did (SessionRoutes::reset_all())
struct all_reset_t {
  /// the connections (by core), removed from core_cons: to be marked down
  std::map<uint32_t, ConnectionRef> cons;
  /// the timer events of the ended holds, to cancel
  std::vector<uint64_t> timer_events;
};

/// the routing state of an OSD session (see above)
class SessionRoutes {
public:
  /// connections to the per-core listeners of the OSD, by core; in
  /// addition to the session's main connection
  std::map<uint32_t, ConnectionRef> core_cons;
  /// the core connections not connected yet, since when. Their cores are
  /// not routed to (see note_connected())
  std::map<uint32_t, time_point> connecting;
  /// the keepalive probes of core connections (see probe_core()): when
  /// each was sent, by core
  std::map<uint32_t, utime_t> probes;
  /// by PG
  std::map<spg_t, pg_route_t> pg_routes;
  /// by tid
  std::map<ceph_tid_t, unanswered_op_t> unanswered;
  /// the PGs whose hold may end, their ops having drained. Their held
  /// ops are sent by the Objecter (take_drained_holds()), not where the
  /// drain is noticed: the callers there may be iterating the session's
  /// ops, or no longer hold the Objecter's lock
  std::set<spg_t> drained_holds;
  /// whether drained_holds is not empty: readable without the lock
  std::atomic<bool> has_drained_holds = false;

  // -- connections

  /// the core of c, if c is one of core_cons
  std::optional<uint32_t> core_of(const ConnectionRef& c) const;

  /// the core connection a PG's ops should be sent on (nullopt: the main
  /// connection): the hinted core's, once connected to it
  std::optional<uint32_t> desired_core(const pg_route_t& route) const {
    if (route.hinted_core && core_cons.contains(*route.hinted_core) &&
        !connecting.contains(*route.hinted_core)) {
      return route.hinted_core;
    }
    return std::nullopt;
  }

  /// whether a new core connection may be opened now; called only when
  /// one would be
  using may_connect_fn_t = std::function<bool()>;

  /// note the core hinted for pgid in an op reply. main_con: the
  /// session's main connection, whose address (and OSD instance) the
  /// core's listener is reached at, on the hinted port. A hint asking for
  /// a new connection that may_connect() refuses is not recorded: a later
  /// one will ask again
  hint_outcome_t apply_hint(const spg_t& pgid,
                            const osd_core_hint_t& hint,
                            const ConnectionRef& main_con,
                            const may_connect_fn_t& may_connect);

  /// a connection to the core was opened at 'now', as apply_hint() asked.
  /// It is not routed to until connected
  void add_core_con(uint32_t core, ConnectionRef con, time_point now) {
    core_cons[core] = std::move(con);
    connecting[core] = now;
  }

  /// c completed its handshake: if it is a core connection not connected
  /// yet, its core may now be routed to. Returns that core
  std::optional<uint32_t> note_connected(const ConnectionRef& c);

  /// the core connections still not connected, opened before 'cutoff'
  std::vector<std::pair<uint32_t, ConnectionRef>>
  overdue_connects(time_point cutoff) const;

  /// The core connection of 'core' carries laggy ops: probe it with a
  /// keepalive, which the OSD's messenger answers however slow the ops
  /// are. now: the current time; last_ack: the time of the last answered
  /// keepalive of the connection (Connection::get_last_keepalive_ack());
  /// deadline: how long to wait for the answer
  probe_t probe_core(uint32_t core, utime_t now, utime_t last_ack,
                     ceph::timespan deadline);

  /// stop probing the cores not in 'laggy': their ops are no longer laggy
  void end_probes_except(const std::set<uint32_t>& laggy);

  // -- routing

  /// the time before which canceled ops are no longer awaited; called
  /// only when there are such ops to check
  using cutoff_fn_t = std::function<time_point()>;

  /// choose the connection to send an op of pgid on, and account for the
  /// op as in flight on it (in op)
  route_outcome_t route_op(const spg_t& pgid,
                           op_route_t& op,
                           time_point now,
                           const cutoff_fn_t& unanswered_cutoff);

  /// stop accounting for op as in flight (if it is)
  void unroute(op_route_t& op);

  // -- canceled ops

  /// op (tid) is being canceled: if it was sent, keep its PG's route
  /// busy until the OSD answers it. Returns whether it was noted
  bool note_unanswered(ceph_tid_t tid, const op_route_t& op, time_point now);

  /// a reply to a tid no longer in the session arrived: if it answers a
  /// canceled op, forget that op, and return its PG
  std::optional<spg_t> answered_late(ceph_tid_t tid);

  /// forget the canceled ops sent on the core connection 'core' (nullopt:
  /// on the main connection): their answers will not arrive. Returns
  /// how many
  unsigned forget_unanswered(std::optional<uint32_t> core);
  /// as forget_unanswered(), for all of them
  unsigned forget_all_unanswered();

  /// forget the canceled ops sent before 'cutoff': the OSD may have
  /// dropped them (e.g. at an interval change), and their answer would
  /// never arrive
  forgotten_t prune_unanswered(time_point cutoff);

  // -- holds

  /// how long a PG may be due to switch before its new ops are held
  /// (0: never hold); called only when that is to be checked
  using grace_fn_t = std::function<ceph::timespan()>;

  /// whether pgid's new ops are to be held: the PG is holding, or has
  /// been due to switch for the grace period, but is still busy
  hold_t hold_state(const spg_t& pgid, time_point now, const grace_fn_t& grace);

  /// start holding pgid's new ops; seq: the hold's id. The caller sets
  /// the route's hold_event
  pg_route_t& start_hold(const spg_t& pgid, time_point now, uint64_t seq);

  /// end the route's hold, without sending its held ops. drained: the
  /// route is no longer busy (else: the hold expired, and the PG waits
  /// for the grace period again)
  hold_end_t end_hold(pg_route_t& route, bool drained, time_point now);

  /// the route of pgid, if it is in the hold seq
  pg_route_t* find_hold(const spg_t& pgid, uint64_t seq);

  /// the PGs whose hold is to end, their ops having drained; empties
  /// drained_holds
  std::vector<spg_t> take_drained_holds();

  // -- resets

  /// the core connection 'core' reset: forget it, and the hints to the
  /// core (so that the next one reconnects). Its PGs fall back to the
  /// main connection, and their holds end
  core_reset_t reset_core(uint32_t core);

  /// all of the session's connections reset: forget the core connections
  /// and all hints, and end all holds. The caller then unroutes the ops
  /// sent on the core connections, and forgets the canceled ones
  all_reset_t reset_all();

  // -- stats

  /// the PGs holding their ops, and those due to switch but busy
  std::pair<uint64_t, uint64_t> count_switching() const;

private:
  /// note that the PG's hold may end, if the route is no longer busy
  void note_if_drained(const spg_t& pgid, const pg_route_t& route);

  /// forget the canceled ops matching pred; returns them
  template <typename Pred>
  forgotten_t forget_unanswered_if(Pred&& pred);
};

/**
 * The hosts whose per-core listeners could not be connected to (e.g. a
 * firewall letting only the OSD's main ports through), shared by all the
 * sessions of an Objecter: no core connection is opened to them for a
 * while, starting at initial_backoff and doubling, up to max_backoff, as
 * long as they keep failing. A connection that does connect lifts the
 * block. Thread safe.
 */
class HostBlocks {
public:
  static constexpr ceph::timespan initial_backoff = std::chrono::minutes(1);
  static constexpr ceph::timespan max_backoff = std::chrono::hours(1);

  /// the host of addr (port and nonce cleared), as blocked
  static entity_addr_t host_of(const entity_addr_t& addr);

  /// whether opening core connections to host is blocked at 'now'
  bool is_blocked(const entity_addr_t& host, time_point now) const;

  /// a core connection to host did not connect: block it. Returns for
  /// how long
  ceph::timespan block(const entity_addr_t& host, time_point now);

  /// a core connection to host connected: lift its block, and reset its
  /// backoff. Returns whether there was any
  bool unblock(const entity_addr_t& host);

private:
  struct block_t {
    time_point until;
    ceph::timespan backoff;
  };
  mutable std::mutex lock;
  /// expired blocks are kept, for their backoff
  std::map<entity_addr_t, block_t> blocks;
  /// blocks is not empty: lets the common case skip the lock
  std::atomic<bool> any = false;
};

} // namespace osdc::core_route
