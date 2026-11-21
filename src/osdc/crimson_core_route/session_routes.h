// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
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
 * Crimson OSD.
 *
 * A Crimson OSD listening for clients on a port per reactor core attaches
 * to its op replies an osd_core_hint_t: the core owning the op's PG, and
 * the port of that core's listener. The session opens a connection to
 * that port, on the OSD's address its main connection uses, and, once it
 * is connected, sends the PG's ops on it, saving the OSD a cross-core hop
 * per op.
 *
 * Reachability: a lossy client connection that cannot connect (e.g. a
 * firewall letting only the OSD's main ports through) is not reset by
 * the messenger: it keeps retrying, holding the messages queued on it.
 * So a PG is routed to a core connection only once its handshake
 * completed (SessionRoutes::admit_core_con()). One not connected in time
 * is given up on, and its host is blocked for a while (HostBlocks).
 *
 * Turning the routing off at runtime: every PG's target is then the main
 * connection (config_t::enabled). A PG switches back as it would to a
 * core, once it drains; the core connections no PG uses any more are then
 * closed.
 *
 * Liveness: a connected core connection that silently stops working (e.g.
 * a firewall dropping its flow, without a reset) would hold its ops until
 * the messenger's idle timeout. Those carrying laggy ops are probed with
 * a keepalive, and given up on if it is not answered.
 *
 * Ordering: the OSD orders a client's ops per connection. A PG's route
 * changes only while none of its ops may still be on the way to the OSD
 * on the current connection: none in flight, and none canceled but not
 * answered yet (see unanswered_op_t). A PG kept busy would never switch;
 * after a grace period, its new ops are held until it drains (see
 * pg_route_t::holding).
 *
 * Activation: a session tracks its ops only once it followed a hint of its
 * OSD (with the routing on): before that, every op goes on the main
 * connection, and no route is kept (e.g. for Classic OSDs, that send no
 * hints). Only the canceled ops are: their answers are awaited all the
 * same (see unanswered_op_t). The first hint followed counts the session's
 * ops as in flight on the main connection, and the canceled ones as
 * unanswered (so that their PGs switch only once they drain). A session
 * reset makes the session inactive again.
 *
 * Routes left idle (see config_t::route_idle_ttl) are forgotten by the
 * periodic check: e.g. those of PGs that moved to other OSDs.
 *
 * SessionRoutes holds the routing state of one OSD session, and makes the
 * routing decisions. The Objecter does the rest: sending, holding and
 * resending ops, the hold timers, connecting, perf counters and logging.
 * It drives SessionRoutes by events, each applying a complete transition
 * of the state, and returning what the Objecter is to do:
 *  - follow_hint(): an op reply carried a hint;
 *  - admit_core_con(): a connection completed its handshake;
 *  - route_or_hold(): an op is to be sent;
 *  - retire_op(): an op left the session (answered, or moved away);
 *  - retire_canceled_op(): an op is canceled;
 *  - canceled_op_answered(): a reply to an op no longer in the session;
 *  - release_drained_holds(): the Objecter may send ops (see there);
 *  - expire_hold(): the timer of a hold fired;
 *  - drop_core_con(): a core connection reset, or is to be closed;
 *  - reset_session(): all of the session's connections reset;
 *  - periodic_timeouts_check(): the Objecter's tick.
 * Its state is private: the accessors are for logging and tests. All of
 * it is protected by its OSD session's lock (OSDSession::lock), except
 * has_drained_holds().
 */
namespace osdc::core_route {

using time_point = ceph::coarse_mono_time;

/// a reactor core of a Crimson OSD (as in osd_core_hint_t::core)
using core_id_t = uint32_t;

/// The routing configuration. The Objecter keeps it, refreshed as its
/// options change, and passes it to the events that use it
struct config_t {
  /// objecter_use_osd_core_hints. Off: every PG's target is the main
  /// connection
  bool enabled = false;
  /// objecter_osd_core_switch_grace: how long a busy PG may be due to
  /// switch connections before its new ops are held (0: never hold)
  ceph::timespan grace = ceph::timespan::zero();
  /// objecter_osd_core_switch_max_hold: the longest hold. Also how long a
  /// canceled op is awaited
  ceph::timespan max_hold = ceph::timespan::zero();
  /// ms_connection_ready_timeout: a core connection not connected by then
  /// is given up on
  ceph::timespan connect_deadline = ceph::timespan::zero();
  /// objecter_timeout: a keepalive probe not answered by then: the
  /// connection is given up on
  ceph::timespan probe_deadline = ceph::timespan::zero();
  /// (not an option) a route not used for that long, with no op in flight
  /// or awaited and no hold, is forgotten by the periodic check
  ceph::timespan route_idle_ttl = std::chrono::minutes(10);
};

/// The route of an op (kept in each of the Objecter's ops). Set and
/// cleared by the events only
struct op_route_t {
  /// the PG the op was sent (routed) or held for
  std::optional<spg_t> pgid;
  /// sent: the core connection it was sent on (nullopt: the session's
  /// main connection)
  std::optional<core_id_t> core;
  /// sent: the generation of its PG's route at the time. A reset of the
  /// connection starts a new one, as does a route forgotten and created
  /// again: the op is then no longer accounted for as in flight
  /// (SessionRoutes::is_in_flight()). 0 (with pgid set): sent while the
  /// session was inactive, untracked
  uint64_t gen = 0;
  /// held, not sent: the id of the hold (pg_route_t::hold_seq; 0: not
  /// held). The op is held while that hold lasts (SessionRoutes::is_held())
  uint64_t hold_seq = 0;
};

/// how a PG's ops are routed to the OSD core owning it
struct pg_route_t {
  /// the core the OSD last hinted for the PG
  std::optional<core_id_t> hinted_core;
  /// the core connection the PG's ops are sent on (nullopt: the main
  /// connection). Changed only while the PG is not busy(), so that its
  /// ops arrive in order: all of its ops in flight were sent on it
  std::optional<core_id_t> core;
  /// the PG's ops in flight (sent, and still assigned to the session)
  unsigned in_flight = 0;
  /// the PG's ops canceled after being sent, and not answered yet
  unsigned unanswered = 0;
  /// the generation of the route: starts anew when the connection its ops
  /// were sent on resets (see op_route_t::gen). Unique in the process (as
  /// routes are forgotten and created again): 0 is none
  uint64_t gen = 0;
  /// when an op of the PG was last routed, a hint for it followed, or the
  /// route last found busy (or holding) by the periodic check (see
  /// config_t::route_idle_ttl)
  time_point last_used;
  /// since when the PG has been due to switch to another connection,
  /// but could not, being busy(). Unset: not due to switch
  std::optional<time_point> switch_pending_since;
  /// whether, while due to switch, the PG was kept from switching
  /// only by canceled ops not answered yet (for the perf counters)
  bool delayed_by_unanswered = false;
  /// since when the PG has been due to switch, and the holds started
  /// since; unlike switch_pending_since, not restarted by an expired
  /// hold (for the perf counters)
  std::optional<time_point> switch_due_since;
  unsigned switch_holds = 0;
  /// After a switch has been pending for config_t::grace, the PG's new
  /// ops are held (op_route_t::hold_seq), so that its ops in flight
  /// drain, and the PG switches. The hold ends once they drain, or after
  /// config_t::max_hold at most (the held ops are then sent on the current
  /// connection, and the cycle restarts).
  ///
  /// The Objecter arms a timer for each hold, to call expire_hold(). A
  /// hold that ends earlier does not cancel it: the timer fires later, and
  /// finds no hold with its id. That costs the timer thread a wakeup, and
  /// short holds of the Objecter's rwlock (shared) and of the session's
  /// lock, up to max_hold later; cancelling it would cost the timer's
  /// mutex, and two set erases, on the op-reply path. Holds are rare
  /// (none in the fio comparisons: busy PGs mostly drain within the
  /// grace), at worst about one per busy PG when the routing is turned on
  /// or off. Hold ids are unique in the process, so a late timer never
  /// ends a newer hold.
  bool holding = false;
  time_point hold_since;
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
  std::optional<core_id_t> core;
  time_point since;
};

/// canceled ops no longer awaited: (tid, PG)
using forgotten_t = std::vector<std::pair<ceph_tid_t, spg_t>>;

/// opens a connection to the core listener at the address; null if it
/// could not
using connect_fn_t = std::function<ConnectionRef(const entity_addr_t&)>;

/// visits an op of the session: its PG, and its route
using op_visitor_t = std::function<void(const spg_t&, op_route_t&)>;
/// calls the visitor for each op of the session (sent or not)
using for_each_op_fn_t = std::function<void(const op_visitor_t&)>;

/// what following a hint did (SessionRoutes::follow_hint())
struct hint_followed_t {
  enum class what_t {
    unchanged,       ///< the PG was hinted to this core already
    invalid,         ///< ignored: the routing is off, or no port (or the
                     ///< main connection's), or no OSD address to use it on
    blocked,         ///< ignored: connecting to the core's host is blocked
    connect_failed,  ///< ignored: the connection could not be opened
    changed,         ///< the PG is hinted to a new core
  };
  what_t what = what_t::unchanged;
  /// changed: the core hinted before
  std::optional<core_id_t> prev_core;
  /// changed: the connection opened to the core (null: it was open
  /// already). Not routed to until connected (admit_core_con())
  ConnectionRef opened;
  /// all but unchanged and invalid: the address of the core's listener
  /// (the main connection's msgr2 address, on the hinted port)
  entity_addr_t addr;
};

/// a core connection admitted for routing (SessionRoutes::admit_core_con())
struct core_con_admitted_t {
  core_id_t core;
  /// the block of its host was lifted
  bool unblocked = false;
};

/// what routing (or holding) an op did (SessionRoutes::route_or_hold())
struct routing_t {
  enum class what_t {
    stayed,    ///< sent: the PG stays on its connection
    switched,  ///< sent: the PG switched to another connection
    pending,   ///< sent: the PG is due to switch, but is busy()
    held,      ///< not sent: held until its PG switches
  };
  what_t what = what_t::stayed;
  /// sent: the core connection the op is to be sent on (nullopt: the main
  /// connection), and the connection itself (null: the main one)
  std::optional<core_id_t> core;
  ConnectionRef con;
  /// sent: the connection the PG should be on
  std::optional<core_id_t> desired;
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
  /// held: the hold's id; and whether the hold started with this op. If
  /// so, the Objecter arms a timer of config_t::max_hold, to call
  /// expire_hold()
  uint64_t hold_seq = 0;
  bool hold_started = false;
  /// canceled ops given up on: awaited for config_t::max_hold
  forgotten_t forgotten;
};

/// A hold that ended (SessionRoutes::release_drained_holds(),
/// expire_hold()). Its ops (op_route_t::hold_seq) are no longer held: the
/// Objecter sends them
struct hold_end_t {
  spg_t pgid;
  uint64_t hold_seq = 0;
  ceph::timespan held_for;
  /// whether the PG drained: its held ops then switch it. Else the hold
  /// expired: they go on its current connection, and the PG waits for the
  /// grace period again
  bool drained = false;
  /// expire_hold(): canceled ops given up on
  forgotten_t forgotten;
};

/// why a core connection is dropped (SessionRoutes::drop_core_con())
enum class drop_why_t {
  reset,            ///< it reset (the messenger's ms_handle_reset())
  unresponsive,     ///< it did not answer a keepalive probe in time
  connect_timeout,  ///< it did not connect in time
  unused,           ///< the routing is off, and nothing uses it
};

/// what dropping a core connection did (SessionRoutes::drop_core_con())
struct core_con_dropped_t {
  core_id_t core;
  /// the connection, no longer the session's: to be marked down
  ConnectionRef con;
  /// the PGs whose ops were sent on it: back on the main connection. Their
  /// ops in flight are no longer accounted for: the Objecter resends them
  std::set<spg_t> rerouted;
  /// the holds that ended: their held ops are to be sent
  std::vector<std::pair<spg_t, uint64_t>> released_holds;
  /// the canceled ops sent on it: their answers will not arrive
  forgotten_t forgotten;
  /// connect_timeout: how long its host is blocked
  ceph::timespan blocked_for = ceph::timespan::zero();
};

/// what resetting the session did (SessionRoutes::reset_session())
struct session_reset_t {
  /// the core connections (by core), no longer the session's: to be
  /// marked down
  std::map<core_id_t, ConnectionRef> cons;
  /// the canceled ops: their answers will not arrive
  forgotten_t forgotten;
};

/// what the periodic check found (SessionRoutes::periodic_timeouts_check())
struct timeouts_checked_t {
  /// canceled ops given up on: awaited for config_t::max_hold
  forgotten_t forgotten;
  /// core connections carrying laggy ops: the Objecter sends each a
  /// keepalive
  std::vector<ConnectionRef> to_probe;
  /// core connections to drop (drop_core_con(), with the matching
  /// drop_why_t), by core: not answering their probes; not connected in
  /// time; and, with the routing off, used by nothing (the Objecter
  /// first moves the watches registered on them)
  std::vector<std::pair<core_id_t, ConnectionRef>> unresponsive;
  std::vector<std::pair<core_id_t, ConnectionRef>> connect_timed_out;
  std::vector<std::pair<core_id_t, ConnectionRef>> unused;
  /// the PGs holding their ops, and those due to switch but busy
  uint64_t pgs_holding = 0;
  uint64_t pgs_switch_pending = 0;
  /// the idle routes forgotten (config_t::route_idle_ttl)
  uint64_t routes_pruned = 0;
};

/**
 * The hosts whose per-core listeners could not be connected to (e.g. a
 * firewall letting only the OSD's main ports through), shared by all the
 * sessions of an Objecter: no core connection is opened to them for a
 * while, starting at initial_backoff and doubling, up to max_backoff, each
 * time they fail again after a block ended. A connection that does connect
 * lifts the block. Thread safe.
 */
class HostBlocks {
public:
  static constexpr ceph::timespan initial_backoff = std::chrono::minutes(1);
  static constexpr ceph::timespan max_backoff = std::chrono::hours(1);

  /// the host of addr (port and nonce cleared), as blocked
  static entity_addr_t host_of(const entity_addr_t& addr);

  /// whether opening core connections to host is blocked at 'now'
  bool is_blocked(const entity_addr_t& host, time_point now) const;

  /// a core connection to host did not connect: block it, unless it is
  /// blocked already (e.g. other connections to it, opened before the
  /// block, timing out too: that is still the same failure). Returns for
  /// how long it is blocked from now
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

/// the routing state of an OSD session (see above)
class SessionRoutes {
public:
  // -- events

  /// An op reply hinted the core owning pgid. main_con: the session's main
  /// connection, whose address (and OSD instance) the core's listener is
  /// reached at, on the hinted port. A new connection to the core is
  /// opened with connect(), unless its host is blocked; a hint it is not
  /// opened for is not recorded: a later one will try again. With the
  /// routing off (config_t::enabled), hints are ignored: no connection
  /// would be used. The first hint followed activates the session: the
  /// ops for_each_op() visits are then accounted for as in flight on the
  /// main connection
  hint_followed_t follow_hint(const spg_t& pgid,
                              const osd_core_hint_t& hint,
                              const ConnectionRef& main_con,
                              time_point now,
                              HostBlocks& blocks,
                              const connect_fn_t& connect,
                              const for_each_op_fn_t& for_each_op,
                              const config_t& cfg);

  /// con completed its handshake. If it is a core connection not
  /// connected yet, its core may now be routed to, and its host is no
  /// longer blocked
  std::optional<core_con_admitted_t> admit_core_con(const ConnectionRef& con,
                                                    HostBlocks& blocks);

  /// An op of pgid is to be sent: choose its connection, and account for
  /// it as in flight on it; or hold it, if its PG holds its new ops to
  /// switch. A previous route (or hold) of the op is void. An inactive
  /// session (see above) sends it on the main connection, and does not
  /// account for it: it only notes it as sent (see op_route_t::gen)
  routing_t route_or_hold(const spg_t& pgid,
                          op_route_t& op,
                          time_point now,
                          const config_t& cfg);

  /// The op is not sent (any more): no longer accounted for as in flight
  /// or held. A hold whose PG drains with it ends on the next
  /// release_drained_holds()
  void retire_op(op_route_t& op);

  /// The op (tid) is canceled. If it is in flight (or was sent while the
  /// session was inactive), its PG stays busy until the OSD answers it
  /// (canceled_op_answered()), or the connection it was sent on resets, or
  /// after config_t::max_hold. Returns it then
  std::optional<unanswered_op_t> retire_canceled_op(ceph_tid_t tid,
                                                    op_route_t& op,
                                                    time_point now);

  /// A reply to a tid no longer in the session arrived: if it answers a
  /// canceled op, that op is no longer awaited. Returns its PG
  std::optional<spg_t> canceled_op_answered(ceph_tid_t tid);

  /// End the holds whose PGs drained (see has_drained_holds()). Called by
  /// the Objecter where it may send ops: where a drain is noticed, it may
  /// be iterating the session's ops, or no longer hold its rwlock
  std::vector<hold_end_t> release_drained_holds(time_point now);

  /// The timer of the hold hold_seq of pgid fired: end the hold, unless it
  /// ended already (see pg_route_t::holding)
  std::optional<hold_end_t> expire_hold(const spg_t& pgid,
                                        uint64_t hold_seq,
                                        time_point now,
                                        const config_t& cfg);

  /// Drop the core connection con, for 'why': forget it, the hints to its
  /// core (so that the next one reconnects), and the canceled ops sent on
  /// it. Its PGs fall back to the main connection, and their holds end.
  /// nullopt if con is not (or no longer) one of the session's core
  /// connections, or no longer qualifies: connect_timeout, but connected
  /// since; unused, but used since. connect_timeout blocks its host
  std::optional<core_con_dropped_t> drop_core_con(const ConnectionRef& con,
                                                  drop_why_t why,
                                                  time_point now,
                                                  HostBlocks& blocks);

  /// All of the session's connections reset: forget the core connections,
  /// all routes and canceled ops (ending all holds), and the session is
  /// inactive again. Nothing is in flight any more: the Objecter resends
  /// (or moves away) all of the session's ops
  session_reset_t reset_session();

  /// The Objecter's periodic check: forget canceled ops awaited for
  /// max_hold, and idle routes (route_idle_ttl); probe the core
  /// connections carrying laggy ops (the cores of the laggy ops sent on
  /// core connections), and find those not answering, those not connected
  /// in time, and (the routing off) those unused. now: as for the other
  /// events; probe_now: the clock of the keepalives
  /// (Connection::get_last_keepalive_ack())
  timeouts_checked_t periodic_timeouts_check(time_point now,
                                             utime_t probe_now,
                                             const std::set<core_id_t>& laggy,
                                             const config_t& cfg);

  // -- accessors (for logging and tests)

  /// whether the session tracks its ops (see above)
  bool is_active() const { return active; }

  /// the route of pgid (nullptr: none)
  const pg_route_t* route_of(const spg_t& pgid) const;

  /// the connection to core (null: none)
  ConnectionRef core_con(core_id_t core) const;

  /// the core of con, if it is one of the session's core connections
  std::optional<core_id_t> core_of(const ConnectionRef& con) const;

  /// whether the connection to core is opened but not connected yet
  bool is_connecting(core_id_t core) const { return connecting.contains(core); }

  size_t num_core_cons() const { return core_cons.size(); }

  /// whether canceled ops are awaited
  bool has_unanswered() const { return !unanswered.empty(); }

  /// whether op is accounted for as in flight
  bool is_in_flight(const op_route_t& op) const;

  /// whether op was sent while the session is (still) inactive
  bool is_sent_untracked(const op_route_t& op) const;

  /// whether op is held by its PG's hold
  bool is_held(const op_route_t& op) const;

  /// whether holds may end, their PGs having drained
  /// (release_drained_holds()); readable without the session's lock
  bool has_drained_holds() const { return drained_flag; }

private:
  /// what probing a core connection found (probe_core())
  enum class probe_t {
    send,   ///< a probe starts: send a keepalive
    wait,   ///< not answered yet, and not overdue
    alive,  ///< answered: the connection works (its ops are just slow)
    dead,   ///< not answered in time: the connection likely stopped working
  };

  /// whether the session tracks its ops: its OSD sent a hint (see above)
  bool active = false;
  /// connections to the per-core listeners of the OSD, by core; in
  /// addition to the session's main connection
  std::map<core_id_t, ConnectionRef> core_cons;
  /// the core connections not connected yet, since when. Their cores are
  /// not routed to
  std::map<core_id_t, time_point> connecting;
  /// the keepalive probes of core connections: when each was sent, by core
  std::map<core_id_t, utime_t> probes;
  /// by PG
  std::map<spg_t, pg_route_t> pg_routes;
  /// by tid
  std::map<ceph_tid_t, unanswered_op_t> unanswered;
  /// the PGs whose hold may end, their ops having drained
  std::set<spg_t> drained_holds;
  /// whether drained_holds is not empty: readable without the lock
  std::atomic<bool> drained_flag = false;

  /// the core connection a PG's ops should be sent on (nullopt: the main
  /// connection): the hinted core's, once connected to it, if the routing
  /// is enabled
  std::optional<core_id_t> desired_core(const pg_route_t& route,
                                        bool enabled) const;

  /// the route of pgid, created (with a new generation) if none
  pg_route_t& route_for(const spg_t& pgid, time_point now);

  /// start tracking the session's ops: account for those visited as in
  /// flight on the main connection, and for the canceled ones awaited as
  /// unanswered
  void activate(time_point now, const for_each_op_fn_t& for_each_op);

  /// account for op as in flight on its PG's connection, switching it if
  /// due and possible
  void route_op(const spg_t& pgid,
                pg_route_t& route,
                op_route_t& op,
                time_point now,
                const config_t& cfg,
                routing_t& out);

  /// whether the core connection of core is used: a PG is routed via it,
  /// or holds its ops to switch to it, or a canceled op sent on it
  /// awaits its answer
  bool is_used(core_id_t core) const;

  /// probe the core connection of core (see periodic_timeouts_check()).
  /// last_ack: the time of its last answered keepalive
  probe_t probe_core(core_id_t core, utime_t now, utime_t last_ack,
                     ceph::timespan deadline);

  void start_hold(pg_route_t& route, time_point now);
  hold_end_t end_hold(const spg_t& pgid, pg_route_t& route, bool drained,
                      time_point now);

  /// forget the core connection of core, and its hints; its PGs fall back
  /// to the main connection, with a new generation
  void reset_core(core_id_t core, core_con_dropped_t& out);

  /// note that the PG's hold may end, if the route is no longer busy
  void note_if_drained(const spg_t& pgid, const pg_route_t& route);

  /// forget the canceled ops matching pred; returns them
  template <typename Pred>
  forgotten_t forget_unanswered_if(Pred&& pred);

  /// forget the canceled ops awaited since before now - max_hold
  forgotten_t prune_unanswered(time_point now, const config_t& cfg);
};

} // namespace osdc::core_route
