// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
#include "osdc/crimson_core_route/session_routes.h"

#include <algorithm>

#include "include/ceph_assert.h"

namespace osdc::core_route {

namespace {
/// the last hold id: unique in the process, so that the timer of an
/// ended hold never ends a newer one (whatever its session or Objecter)
std::atomic<uint64_t> last_hold_seq{0};
/// the last route generation: unique in the process, so that an op sent
/// before its route was reset, or forgotten and created again, is never
/// taken as in flight on the new one
std::atomic<uint64_t> last_route_gen{0};
} // namespace

// -- events: connections

hint_followed_t SessionRoutes::follow_hint(const spg_t& pgid,
                                           const osd_core_hint_t& hint,
                                           const ConnectionRef& main_con,
                                           time_point now,
                                           HostBlocks& blocks,
                                           const connect_fn_t& connect,
                                           const for_each_op_fn_t& for_each_op,
                                           const config_t& cfg)
{
  if (!cfg.enabled) {
    // not recorded: once the routing is turned on, the next hint connects
    return hint_followed_t{.what = hint_followed_t::what_t::invalid};
  }
  const auto existing = pg_routes.find(pgid);
  if (existing != pg_routes.end() &&
      existing->second.hinted_core == hint.core) {
    // the common case
    return hint_followed_t{.what = hint_followed_t::what_t::unchanged};
  }

  hint_followed_t out;
  // the core's listener: the OSD instance's address we already use (as
  // the reply came from it), on the hinted port
  if (hint.port == 0 || !main_con) {
    out.what = hint_followed_t::what_t::invalid;
    return out;
  }
  out.addr = main_con->get_peer_addrs().msgr2_addr();
  if (out.addr.is_blank_ip()) {
    out.what = hint_followed_t::what_t::invalid;
    return out;
  }
  out.addr.set_port(hint.port);

  if (!core_cons.contains(hint.core)) {
    if (blocks.is_blocked(HostBlocks::host_of(out.addr), now)) {
      out.what = hint_followed_t::what_t::blocked;
      return out;
    }
    auto con = connect(out.addr);
    if (!con) {
      out.what = hint_followed_t::what_t::connect_failed;
      return out;
    }
    // not routed to until connected (admit_core_con())
    core_cons[hint.core] = con;
    connecting[hint.core] = now;
    out.opened = std::move(con);
  }
  if (!active) {
    // the first hint followed: track the session's ops from now on. (Not
    // for hints ignored: e.g. a host blocked by a firewall would cost the
    // tracking, with nothing to route to)
    activate(now, for_each_op);
  }
  // (a route is created only for a hint followed)
  auto& route =
    existing != pg_routes.end() ? existing->second : route_for(pgid, now);
  route.last_used = now;
  out.what = hint_followed_t::what_t::changed;
  out.prev_core = route.hinted_core;
  route.hinted_core = hint.core;
  return out;
}

std::optional<core_con_admitted_t>
SessionRoutes::admit_core_con(const ConnectionRef& con, HostBlocks& blocks)
{
  const auto core = core_of(con);
  if (!core || !connecting.erase(*core)) {
    // the main connection, not ours (any more), or connected already
    return std::nullopt;
  }
  core_con_admitted_t out{*core};
  if (const auto& addrs = con->get_peer_addrs(); !addrs.empty()) {
    out.unblocked = blocks.unblock(HostBlocks::host_of(addrs.front()));
  }
  return out;
}

std::optional<core_con_dropped_t>
SessionRoutes::drop_core_con(const ConnectionRef& con,
                             drop_why_t why,
                             time_point now,
                             HostBlocks& blocks)
{
  const auto core = core_of(con);
  if (!core) {
    // the main connection, or dropped (or replaced) already
    return std::nullopt;
  }
  switch (why) {
  case drop_why_t::connect_timeout:
    if (!connecting.contains(*core)) {
      // connected since
      return std::nullopt;
    }
    break;
  case drop_why_t::unused:
    if (is_used(*core)) {
      return std::nullopt;
    }
    break;
  case drop_why_t::reset:
  case drop_why_t::unresponsive:
    break;
  }

  core_con_dropped_t out{*core};
  reset_core(*core, out);
  // their answers will not arrive
  out.forgotten = forget_unanswered_if([&core](const unanswered_op_t& u) {
    return u.core == core;
  });
  if (why == drop_why_t::connect_timeout) {
    if (const auto& addrs = out.con->get_peer_addrs(); !addrs.empty()) {
      out.blocked_for = blocks.block(HostBlocks::host_of(addrs.front()), now);
    }
  }
  return out;
}

session_reset_t SessionRoutes::reset_session()
{
  session_reset_t out;
  out.cons.swap(core_cons);
  connecting.clear();
  probes.clear();
  out.forgotten = forget_unanswered_if([](const unanswered_op_t&) {
    return true;
  });
  // what was in flight went with the connections, and the holds end. (The
  // ops keep their routes: their generations are no longer used)
  pg_routes.clear();
  drained_holds.clear();
  drained_flag = false;
  // until the OSD (maybe another instance) hints again
  active = false;
  return out;
}

timeouts_checked_t
SessionRoutes::periodic_timeouts_check(time_point now,
                                       utime_t probe_now,
                                       const std::set<core_id_t>& laggy,
                                       const config_t& cfg)
{
  timeouts_checked_t out;
  if (!unanswered.empty()) {
    out.forgotten = prune_unanswered(now, cfg);
  }
  if (!active) {
    // nothing else is tracked
    return out;
  }

  for (const auto& [core, since] : connecting) {
    if (since + cfg.connect_deadline < now) {
      out.connect_timed_out.emplace_back(core, core_cons.at(core));
    }
  }

  // the cores no longer laggy are no longer probed
  std::erase_if(probes, [&laggy](const auto& probe) {
    return !laggy.contains(probe.first);
  });
  for (auto core : laggy) {
    auto c = core_cons.find(core);
    if (c == core_cons.end() || connecting.contains(core)) {
      continue;
    }
    switch (probe_core(core, probe_now, c->second->get_last_keepalive_ack(),
                       cfg.probe_deadline)) {
    case probe_t::send:
      out.to_probe.push_back(c->second);
      break;
    case probe_t::dead:
      out.unresponsive.emplace_back(core, c->second);
      break;
    case probe_t::wait:
    case probe_t::alive:
      break;
    }
  }

  if (!cfg.enabled) {
    for (const auto& [core, con] : core_cons) {
      if (!is_used(core)) {
        out.unused.emplace_back(core, con);
      }
    }
  }

  for (auto i = pg_routes.begin(); i != pg_routes.end();) {
    auto& route = i->second;
    if (route.holding) {
      ++out.pgs_holding;
      route.last_used = now;
    } else if (route.busy()) {
      if (route.switch_due_since) {
        ++out.pgs_switch_pending;
      }
      // (an op may stay in flight for long: the route is idle only from
      // when it is no longer busy)
      route.last_used = now;
    } else if (route.last_used + cfg.route_idle_ttl < now) {
      // e.g. the PG moved to another OSD. (Were it to come back, its first
      // op goes on the main connection, and the hint in its reply
      // switches it again)
      i = pg_routes.erase(i);
      ++out.routes_pruned;
      continue;
    }
    ++i;
  }
  return out;
}

// -- events: ops

routing_t SessionRoutes::route_or_hold(const spg_t& pgid,
                                       op_route_t& op,
                                       time_point now,
                                       const config_t& cfg)
{
  // a previous send (or hold) of op is void
  retire_op(op);
  routing_t out;
  if (!active) {
    // on the main connection, untracked (see SessionRoutes). Only noted as
    // sent, in case it is canceled (see retire_canceled_op())
    op.pgid = pgid;
    return out;
  }
  auto& route = route_for(pgid, now);
  if (!route.holding && route.switch_pending_since && route.busy() &&
      desired_core(route, cfg.enabled) != route.core &&
      cfg.grace > ceph::timespan::zero() &&
      *route.switch_pending_since + cfg.grace <= now) {
    // due to switch for the grace period, but still busy: hold its new
    // ops, so that it drains. Unless only canceled ops kept it busy, whose
    // answers might never arrive
    if (route.unanswered > 0) {
      out.forgotten = prune_unanswered(now, cfg);
    }
    if (route.busy()) {
      start_hold(route, now);
      out.hold_started = true;
    }
  }
  if (route.holding) {
    op.pgid = pgid;
    op.hold_seq = route.hold_seq;
    out.what = routing_t::what_t::held;
    out.hold_seq = route.hold_seq;
    return out;
  }
  route_op(pgid, route, op, now, cfg, out);
  return out;
}

void SessionRoutes::route_op(const spg_t& pgid,
                             pg_route_t& route,
                             op_route_t& op,
                             time_point now,
                             const config_t& cfg,
                             routing_t& out)
{
  route.last_used = now;
  out.desired = desired_core(route, cfg.enabled);
  if (out.desired == route.core) {
    route.end_switch_wait();
  } else {
    if (route.unanswered > 0) {
      // the answers of canceled ops might never arrive
      auto forgotten = prune_unanswered(now, cfg);
      out.forgotten.insert(out.forgotten.end(),
                           forgotten.begin(), forgotten.end());
    }
    if (!route.busy()) {
      route.core = out.desired;
      if (route.switch_due_since) {
        // the switch was delayed
        out.switch_delay = now - *route.switch_due_since;
        out.switch_holds = route.switch_holds;
      }
      route.end_switch_wait();
      out.what = routing_t::what_t::switched;
    } else {
      // to switch once the PG's ops drain
      out.what = routing_t::what_t::pending;
      if (!route.switch_pending_since) {
        route.switch_pending_since = now;
        out.pending_started = true;
      }
      if (!route.switch_due_since) {
        route.switch_due_since = route.switch_pending_since;
      }
      if (route.in_flight == 0) {
        // but for canceled ops not answered yet, the PG would switch
        out.pending_unanswered = true;
        if (!route.delayed_by_unanswered) {
          route.delayed_by_unanswered = true;
          out.first_delayed_by_unanswered = true;
        }
      }
    }
  }

  ++route.in_flight;
  op.pgid = pgid;
  op.core = route.core;
  op.gen = route.gen;
  op.hold_seq = 0;
  out.core = route.core;
  if (route.core) {
    out.con = core_cons.at(*route.core);
  }
}

void SessionRoutes::retire_op(op_route_t& op)
{
  if (is_in_flight(op)) {
    const auto pgid = *op.pgid;
    auto& route = pg_routes.at(pgid);
    ceph_assert(route.in_flight > 0);
    --route.in_flight;
    note_if_drained(pgid, route);
  }
  op = op_route_t{};
}

std::optional<unanswered_op_t>
SessionRoutes::retire_canceled_op(ceph_tid_t tid,
                                  op_route_t& op,
                                  time_point now)
{
  std::optional<unanswered_op_t> awaited;
  if (is_in_flight(op)) {
    // (noted before the op is retired: its PG stays busy)
    const unanswered_op_t u{*op.pgid, op.core, now};
    if (unanswered.emplace(tid, u).second) {
      ++pg_routes.at(u.pgid).unanswered;
      awaited = u;
    }
  } else if (is_sent_untracked(op)) {
    // sent while the session was inactive: no route accounts for it yet
    // (see activate()). Its answer is awaited all the same: were the
    // session to activate, a new op of its PG on a core connection could
    // overtake it
    const unanswered_op_t u{*op.pgid, std::nullopt, now};
    if (unanswered.emplace(tid, u).second) {
      awaited = u;
    }
  }
  retire_op(op);
  return awaited;
}

std::optional<spg_t> SessionRoutes::canceled_op_answered(ceph_tid_t tid)
{
  auto op = unanswered.find(tid);
  if (op == unanswered.end()) {
    return std::nullopt;
  }
  const auto pgid = op->second.pgid;
  unanswered.erase(op);
  if (active) {
    auto& route = pg_routes.at(pgid);
    ceph_assert(route.unanswered > 0);
    --route.unanswered;
    note_if_drained(pgid, route);
  }
  return pgid;
}

// -- events: holds

std::vector<hold_end_t> SessionRoutes::release_drained_holds(time_point now)
{
  std::vector<hold_end_t> ended;
  if (!drained_flag) {
    return ended;
  }
  auto candidates = std::move(drained_holds);
  drained_holds.clear();
  drained_flag = false;
  for (const auto& pgid : candidates) {
    auto route = pg_routes.find(pgid);
    // (a hold may have ended since, or another one started)
    if (route != pg_routes.end() && route->second.holding &&
        !route->second.busy()) {
      ended.push_back(end_hold(pgid, route->second, true, now));
    }
  }
  return ended;
}

std::optional<hold_end_t> SessionRoutes::expire_hold(const spg_t& pgid,
                                                     uint64_t hold_seq,
                                                     time_point now,
                                                     const config_t& cfg)
{
  auto i = pg_routes.find(pgid);
  if (i == pg_routes.end() || !i->second.holding ||
      i->second.hold_seq != hold_seq) {
    // the hold ended already
    return std::nullopt;
  }
  auto& route = i->second;
  forgotten_t forgotten;
  if (route.unanswered > 0) {
    forgotten = prune_unanswered(now, cfg);
  }
  auto ended = end_hold(pgid, route, !route.busy(), now);
  ended.forgotten = std::move(forgotten);
  return ended;
}

// -- accessors

const pg_route_t* SessionRoutes::route_of(const spg_t& pgid) const
{
  auto route = pg_routes.find(pgid);
  return route == pg_routes.end() ? nullptr : &route->second;
}

ConnectionRef SessionRoutes::core_con(core_id_t core) const
{
  auto con = core_cons.find(core);
  return con == core_cons.end() ? nullptr : con->second;
}

std::optional<core_id_t> SessionRoutes::core_of(const ConnectionRef& con) const
{
  for (const auto& [core, core_con] : core_cons) {
    if (core_con == con) {
      return core;
    }
  }
  return std::nullopt;
}

bool SessionRoutes::is_in_flight(const op_route_t& op) const
{
  if (!op.pgid || op.hold_seq) {
    return false;
  }
  const auto route = route_of(*op.pgid);
  return route && route->gen == op.gen;
}

bool SessionRoutes::is_sent_untracked(const op_route_t& op) const
{
  return !active && op.pgid && !op.hold_seq && op.gen == 0;
}

bool SessionRoutes::is_held(const op_route_t& op) const
{
  if (!op.pgid || !op.hold_seq) {
    return false;
  }
  const auto route = route_of(*op.pgid);
  return route && route->holding && route->hold_seq == op.hold_seq;
}

// -- the rest

pg_route_t& SessionRoutes::route_for(const spg_t& pgid, time_point now)
{
  auto [i, added] = pg_routes.try_emplace(pgid);
  if (added) {
    i->second.gen = ++last_route_gen;
    i->second.last_used = now;
  }
  return i->second;
}

void SessionRoutes::activate(time_point now,
                             const for_each_op_fn_t& for_each_op)
{
  ceph_assert(!active);
  active = true;
  // the canceled ops sent while inactive, still awaited
  for (const auto& [tid, u] : unanswered) {
    ++route_for(u.pgid, now).unanswered;
  }
  if (!for_each_op) {
    return;
  }
  // Some may not have been sent yet: their next route_or_hold() retires
  // them first. Until then, they keep their PGs busy: an op left unsent
  // for long (e.g. paused, or its PG backed off) delays its PG's switch,
  // possibly through holds expiring, until it is sent
  for_each_op([this, now](const spg_t& pgid, op_route_t& op) {
    if (is_in_flight(op)) {
      // visited already
      return;
    }
    auto& route = route_for(pgid, now);
    ++route.in_flight;
    op = op_route_t{.pgid = pgid, .core = std::nullopt, .gen = route.gen};
  });
}

std::optional<core_id_t> SessionRoutes::desired_core(const pg_route_t& route,
                                                     bool enabled) const
{
  if (enabled && route.hinted_core &&
      core_cons.contains(*route.hinted_core) &&
      !connecting.contains(*route.hinted_core)) {
    return route.hinted_core;
  }
  return std::nullopt;
}

bool SessionRoutes::is_used(core_id_t core) const
{
  for (const auto& [pgid, route] : pg_routes) {
    if (route.core == core || (route.holding && route.hinted_core == core)) {
      return true;
    }
  }
  for (const auto& [tid, u] : unanswered) {
    if (u.core == core) {
      return true;
    }
  }
  return false;
}

SessionRoutes::probe_t SessionRoutes::probe_core(core_id_t core,
                                                 utime_t now,
                                                 utime_t last_ack,
                                                 ceph::timespan deadline)
{
  auto [probe, added] = probes.try_emplace(core, now);
  if (added) {
    return probe_t::send;
  }
  const auto sent = probe->second;
  if (last_ack >= sent) {
    // (the answer echoes the time the keepalive was sent)
    probes.erase(probe);
    return probe_t::alive;
  }
  if (static_cast<double>(now - sent) >=
      std::chrono::duration<double>(deadline).count()) {
    probes.erase(probe);
    return probe_t::dead;
  }
  return probe_t::wait;
}

void SessionRoutes::start_hold(pg_route_t& route, time_point now)
{
  ceph_assert(!route.holding);
  route.holding = true;
  route.hold_since = now;
  route.hold_seq = ++last_hold_seq;
  ++route.switch_holds;
}

hold_end_t SessionRoutes::end_hold(const spg_t& pgid,
                                   pg_route_t& route,
                                   bool drained,
                                   time_point now)
{
  ceph_assert(route.holding);
  route.holding = false;
  hold_end_t ended{pgid, route.hold_seq, now - route.hold_since, drained};
  if (!drained) {
    // the held ops go on the current connection. Wait for the grace
    // period again before holding again.
    route.switch_pending_since = now;
  }
  return ended;
}

void SessionRoutes::reset_core(core_id_t core, core_con_dropped_t& out)
{
  auto con = core_cons.find(core);
  ceph_assert(con != core_cons.end());
  out.con = std::move(con->second);
  core_cons.erase(con);
  connecting.erase(core);
  probes.erase(core);

  for (auto& [pgid, route] : pg_routes) {
    if (route.hinted_core != core && route.core != core) {
      continue;
    }
    // a new start for the route
    route.end_switch_wait();
    if (route.holding) {
      route.holding = false;
      out.released_holds.emplace_back(pgid, route.hold_seq);
    }
    if (route.hinted_core == core) {
      route.hinted_core.reset();
    }
    if (route.core == core) {
      // all of its ops in flight were sent on that connection
      route.core.reset();
      route.in_flight = 0;
      route.gen = ++last_route_gen;
      out.rerouted.insert(pgid);
    }
  }
}

void SessionRoutes::note_if_drained(const spg_t& pgid,
                                    const pg_route_t& route)
{
  if (route.holding && !route.busy()) {
    drained_holds.insert(pgid);
    drained_flag = true;
  }
}

template <typename Pred>
forgotten_t SessionRoutes::forget_unanswered_if(Pred&& pred)
{
  forgotten_t forgotten;
  for (auto op = unanswered.begin(); op != unanswered.end();) {
    if (!pred(op->second)) {
      ++op;
      continue;
    }
    const auto pgid = op->second.pgid;
    forgotten.emplace_back(op->first, pgid);
    op = unanswered.erase(op);
    if (active) {
      auto& route = pg_routes.at(pgid);
      ceph_assert(route.unanswered > 0);
      --route.unanswered;
      note_if_drained(pgid, route);
    }
  }
  return forgotten;
}

forgotten_t SessionRoutes::prune_unanswered(time_point now,
                                            const config_t& cfg)
{
  // the OSD may have dropped them (e.g. at an interval change): their
  // answers would never arrive
  return forget_unanswered_if([now, &cfg](const unanswered_op_t& u) {
    return u.since + cfg.max_hold < now;
  });
}

// -- HostBlocks

entity_addr_t HostBlocks::host_of(const entity_addr_t& addr)
{
  auto host = addr;
  host.set_port(0);
  host.set_nonce(0);
  return host;
}

bool HostBlocks::is_blocked(const entity_addr_t& host, time_point now) const
{
  if (!any) {
    // the common case
    return false;
  }
  std::lock_guard l{lock};
  auto i = blocks.find(host);
  return i != blocks.end() && now < i->second.until;
}

ceph::timespan HostBlocks::block(const entity_addr_t& host, time_point now)
{
  std::lock_guard l{lock};
  auto [i, added] = blocks.try_emplace(host, block_t{now, initial_backoff});
  auto& b = i->second;
  if (!added) {
    b.backoff = std::min(b.backoff * 2, max_backoff);
  }
  b.until = std::max<time_point>(b.until, now + b.backoff);
  any = true;
  return b.backoff;
}

bool HostBlocks::unblock(const entity_addr_t& host)
{
  if (!any) {
    return false;
  }
  std::lock_guard l{lock};
  const bool erased = blocks.erase(host) > 0;
  any = !blocks.empty();
  return erased;
}

} // namespace osdc::core_route
