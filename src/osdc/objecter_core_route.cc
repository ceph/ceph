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

#include "osdc/objecter_core_route.h"

#include <algorithm>

#include "include/ceph_assert.h"

namespace osdc::core_route {

// -- connections

std::optional<uint32_t> SessionRoutes::core_of(const ConnectionRef& c) const
{
  for (const auto& [core, core_con] : core_cons) {
    if (core_con == c) {
      return core;
    }
  }
  return std::nullopt;
}

std::optional<uint32_t> SessionRoutes::note_connected(const ConnectionRef& c)
{
  const auto core = core_of(c);
  if (!core || !connecting.erase(*core)) {
    // the main connection, or connected already
    return std::nullopt;
  }
  return core;
}

std::vector<std::pair<uint32_t, ConnectionRef>>
SessionRoutes::overdue_connects(time_point cutoff) const
{
  std::vector<std::pair<uint32_t, ConnectionRef>> overdue;
  for (const auto& [core, since] : connecting) {
    if (since < cutoff) {
      overdue.emplace_back(core, core_cons.at(core));
    }
  }
  return overdue;
}

probe_t SessionRoutes::probe_core(uint32_t core,
                                  utime_t now,
                                  utime_t last_ack,
                                  ceph::timespan deadline)
{
  auto [i, added] = probes.try_emplace(core, now);
  if (added) {
    return probe_t::send;
  }
  const auto sent = i->second;
  if (last_ack >= sent) {
    // (the answer echoes the time the keepalive was sent)
    probes.erase(i);
    return probe_t::alive;
  }
  if (static_cast<double>(now - sent) >=
      std::chrono::duration<double>(deadline).count()) {
    probes.erase(i);
    return probe_t::dead;
  }
  return probe_t::wait;
}

void SessionRoutes::end_probes_except(const std::set<uint32_t>& laggy)
{
  std::erase_if(probes, [&laggy](const auto& probe) {
    return !laggy.contains(probe.first);
  });
}

hint_outcome_t SessionRoutes::apply_hint(const spg_t& pgid,
                                         const osd_core_hint_t& hint,
                                         const ConnectionRef& main_con,
                                         const may_connect_fn_t& may_connect)
{
  hint_outcome_t out;
  auto& route = pg_routes[pgid];
  if (route.hinted_core == hint.core) {
    // the common case
    return out;
  }

  // the core's listener: the OSD instance's address we already use (as
  // the reply came from it), on the hinted port
  if (hint.port == 0 || !main_con) {
    out.what = hint_outcome_t::what_t::invalid;
    return out;
  }
  out.addr = main_con->get_peer_addrs().msgr2_addr();
  if (out.addr.is_blank_ip()) {
    out.what = hint_outcome_t::what_t::invalid;
    return out;
  }
  out.addr.set_port(hint.port);

  out.connect = !core_cons.contains(hint.core);
  if (out.connect && !may_connect()) {
    out.what = hint_outcome_t::what_t::blocked;
    out.connect = false;
    return out;
  }
  out.what = hint_outcome_t::what_t::changed;
  out.prev_core = route.hinted_core;
  route.hinted_core = hint.core;
  return out;
}

// -- routing

route_outcome_t SessionRoutes::route_op(const spg_t& pgid,
                                        op_route_t& op,
                                        time_point now,
                                        const cutoff_fn_t& unanswered_cutoff,
                                        bool enabled)
{
  ceph_assert(!op.is_routed());
  route_outcome_t out;
  auto& route = pg_routes[pgid];
  out.desired = desired_core(route, enabled);
  if (out.desired == route.core) {
    route.end_switch_wait();
  } else {
    if (route.unanswered > 0) {
      // the answers of canceled ops might never arrive
      out.forgotten = prune_unanswered(unanswered_cutoff());
    }
    if (!route.busy()) {
      route.core = out.desired;
      if (route.switch_due_since) {
        // the switch was delayed
        out.switch_delay = now - *route.switch_due_since;
        out.switch_holds = route.switch_holds;
      }
      route.end_switch_wait();
      out.what = route_outcome_t::what_t::switched;
    } else {
      // to switch once the PG's ops drain
      out.what = route_outcome_t::what_t::pending;
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
  return out;
}

void SessionRoutes::unroute(op_route_t& op)
{
  if (!op.is_routed()) {
    return;
  }
  const auto pgid = *op.pgid;
  auto& route = pg_routes.at(pgid);
  ceph_assert(route.in_flight > 0);
  --route.in_flight;
  op.pgid.reset();
  op.core.reset();
  note_if_drained(pgid, route);
}

// -- canceled ops

bool SessionRoutes::note_unanswered(ceph_tid_t tid,
                                    const op_route_t& op,
                                    time_point now)
{
  if (!op.is_routed()) {
    // never sent, or the connection it was sent on is gone
    return false;
  }
  auto [i, added] = unanswered.emplace(
    tid, unanswered_op_t{*op.pgid, op.core, now});
  if (added) {
    ++pg_routes.at(*op.pgid).unanswered;
  }
  return added;
}

std::optional<spg_t> SessionRoutes::answered_late(ceph_tid_t tid)
{
  auto i = unanswered.find(tid);
  if (i == unanswered.end()) {
    return std::nullopt;
  }
  const auto pgid = i->second.pgid;
  auto& route = pg_routes.at(pgid);
  ceph_assert(route.unanswered > 0);
  --route.unanswered;
  unanswered.erase(i);
  note_if_drained(pgid, route);
  return pgid;
}

template <typename Pred>
forgotten_t SessionRoutes::forget_unanswered_if(Pred&& pred)
{
  forgotten_t forgotten;
  for (auto i = unanswered.begin(); i != unanswered.end();) {
    if (!pred(i->second)) {
      ++i;
      continue;
    }
    const auto pgid = i->second.pgid;
    auto& route = pg_routes.at(pgid);
    ceph_assert(route.unanswered > 0);
    --route.unanswered;
    forgotten.emplace_back(i->first, pgid);
    i = unanswered.erase(i);
    note_if_drained(pgid, route);
  }
  return forgotten;
}

unsigned SessionRoutes::forget_unanswered(std::optional<uint32_t> core)
{
  return forget_unanswered_if([&core](const unanswered_op_t& u) {
    return u.core == core;
  }).size();
}

unsigned SessionRoutes::forget_all_unanswered()
{
  return forget_unanswered_if([](const unanswered_op_t&) {
    return true;
  }).size();
}

forgotten_t SessionRoutes::prune_unanswered(time_point cutoff)
{
  return forget_unanswered_if([cutoff](const unanswered_op_t& u) {
    return u.since < cutoff;
  });
}

// -- holds

hold_t SessionRoutes::hold_state(const spg_t& pgid,
                                 time_point now,
                                 const grace_fn_t& grace_fn,
                                 bool enabled)
{
  auto& route = pg_routes[pgid];
  if (route.holding) {
    return hold_t::holding;
  }
  if (!route.switch_pending_since || !route.busy() ||
      desired_core(route, enabled) == route.core) {
    // not due to switch, or route_op() switches now
    return hold_t::none;
  }
  const auto grace = grace_fn();
  if (grace <= ceph::timespan::zero() ||
      now - *route.switch_pending_since < grace) {
    return hold_t::none;
  }
  return hold_t::due;
}

pg_route_t& SessionRoutes::start_hold(const spg_t& pgid,
                                      time_point now,
                                      uint64_t seq)
{
  auto& route = pg_routes.at(pgid);
  ceph_assert(!route.holding);
  route.holding = true;
  route.hold_since = now;
  route.hold_seq = seq;
  ++route.switch_holds;
  return route;
}

hold_end_t SessionRoutes::end_hold(pg_route_t& route,
                                   bool drained,
                                   time_point now)
{
  ceph_assert(route.holding);
  route.holding = false;
  hold_end_t ended{now - route.hold_since, route.hold_event};
  route.hold_event = 0;
  if (!drained) {
    // the held ops go on the current connection. Wait for the grace
    // period again before holding again.
    route.switch_pending_since = now;
  }
  return ended;
}

pg_route_t* SessionRoutes::find_hold(const spg_t& pgid, uint64_t seq)
{
  auto i = pg_routes.find(pgid);
  if (i == pg_routes.end() || !i->second.holding ||
      i->second.hold_seq != seq) {
    return nullptr;
  }
  return &i->second;
}

std::vector<spg_t> SessionRoutes::take_drained_holds()
{
  std::vector<spg_t> drained;
  if (!has_drained_holds) {
    return drained;
  }
  auto candidates = std::move(drained_holds);
  drained_holds.clear();
  has_drained_holds = false;
  for (const auto& pgid : candidates) {
    auto i = pg_routes.find(pgid);
    // (a hold may have ended since, or another one started)
    if (i != pg_routes.end() && i->second.holding && !i->second.busy()) {
      drained.push_back(pgid);
    }
  }
  return drained;
}

void SessionRoutes::note_if_drained(const spg_t& pgid,
                                    const pg_route_t& route)
{
  if (route.holding && !route.busy()) {
    drained_holds.insert(pgid);
    has_drained_holds = true;
  }
}

// -- resets

core_reset_t SessionRoutes::reset_core(uint32_t core)
{
  auto i = core_cons.find(core);
  ceph_assert(i != core_cons.end());
  core_reset_t reset;
  reset.con = std::move(i->second);
  core_cons.erase(i);
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
      if (route.hold_event) {
        reset.timer_events.push_back(route.hold_event);
        route.hold_event = 0;
      }
      reset.released_holds.insert(pgid);
    }
    if (route.hinted_core == core) {
      route.hinted_core.reset();
    }
    if (route.core == core) {
      route.core.reset();
      reset.rerouted.insert(pgid);
    }
  }
  return reset;
}

all_reset_t SessionRoutes::reset_all()
{
  all_reset_t reset;
  reset.cons.swap(core_cons);
  connecting.clear();
  probes.clear();
  for (auto& [pgid, route] : pg_routes) {
    route.hinted_core.reset();
    route.core.reset();
    route.end_switch_wait();
    if (route.holding) {
      route.holding = false;
      if (route.hold_event) {
        reset.timer_events.push_back(route.hold_event);
        route.hold_event = 0;
      }
    }
  }
  drained_holds.clear();
  has_drained_holds = false;
  return reset;
}

std::vector<uint32_t> SessionRoutes::idle_cores() const
{
  std::set<uint32_t> used;
  for (const auto& [pgid, route] : pg_routes) {
    if (route.core) {
      used.insert(*route.core);
    }
    if (route.holding && route.hinted_core) {
      used.insert(*route.hinted_core);
    }
  }
  for (const auto& [tid, u] : unanswered) {
    if (u.core) {
      used.insert(*u.core);
    }
  }
  std::vector<uint32_t> idle;
  for (const auto& [core, con] : core_cons) {
    if (!used.contains(core)) {
      idle.push_back(core);
    }
  }
  return idle;
}

// -- stats

std::pair<uint64_t, uint64_t> SessionRoutes::count_switching() const
{
  uint64_t holding = 0;
  uint64_t pending = 0;
  for (const auto& [pgid, route] : pg_routes) {
    if (route.holding) {
      ++holding;
    } else if (route.switch_due_since && route.busy()) {
      ++pending;
    }
  }
  return {holding, pending};
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
