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

/**
 * osdc::core_route::SessionRoutes on its own: the routing state of an OSD
 * session (objecter_use_osd_core_hints), and the decisions taken on it.
 * The time is passed in, so nothing here waits. The Objecter's use of it
 * is tested by unittest_objecter_core_routes.
 */

#include <chrono>

#include <gtest/gtest.h>

#include "osdc/objecter_core_route.h"
#include "test/osd/MockConnection.h"

using namespace osdc::core_route;
using namespace std::chrono_literals;

namespace {

time_point at(int seconds)
{
  return time_point{std::chrono::seconds(seconds)};
}

entity_addr_t addr(const char* s)
{
  entity_addr_t a;
  a.parse(s);
  return a;
}

class SessionRoutesTest : public ::testing::Test {
protected:
  SessionRoutes routes;
  const spg_t pg{pg_t(0, 1)};
  const spg_t pg2{pg_t(1, 1)};
  static constexpr uint32_t core = 3;
  static constexpr uint32_t other_core = 5;

  ConnectionRef main_con = make_con("v2:127.0.0.1:6800/1234");
  ConnectionRef core_con = make_con("v2:127.0.0.1:6810/1234");
  ConnectionRef other_con = make_con("v2:127.0.0.1:6811/1234");

  /// prunes nothing (no canceled op is older), and counts its calls
  unsigned cutoff_calls = 0;
  SessionRoutes::cutoff_fn_t no_cutoff = [this] {
    ++cutoff_calls;
    return at(0);
  };

  static ConnectionRef make_con(const char* a)
  {
    auto c = ceph::make_ref<MockConnection>(0);
    c->set_peer_addrs(entity_addrvec_t(addr(a)));
    return c;
  }

  /// a hint of pgid to core c, whose listener is on 'port'
  osd_core_hint_t hint(uint32_t c, uint16_t port = 6810) const
  {
    return osd_core_hint_t(c, port);
  }

  /// pgid is hinted to 'core', and connected to it
  void hinted_and_connected(const spg_t& pgid = spg_t(pg_t(0, 1)))
  {
    auto out = routes.apply_hint(pgid, hint(core), main_con);
    if (out.connect) {
      routes.add_core_con(core, core_con);
    }
  }

  route_outcome_t route(op_route_t& op, int now, const spg_t* pgid = nullptr)
  {
    return routes.route_op(pgid ? *pgid : pg, op, at(now), no_cutoff);
  }

  const pg_route_t& route_of(const spg_t& pgid) const
  {
    return routes.pg_routes.at(pgid);
  }

  /// a PG due to switch to 'core', busy with op a (on the main connection)
  void make_pending(op_route_t& a)
  {
    route(a, 100);
    hinted_and_connected();
    op_route_t b;
    ASSERT_EQ(route_outcome_t::what_t::pending, route(b, 101).what);
    routes.unroute(b);
  }
};

} // namespace

// -- connections and hints

TEST_F(SessionRoutesTest, CoreOfFindsOnlyCoreConnections)
{
  routes.add_core_con(core, core_con);
  EXPECT_EQ(std::optional<uint32_t>(core), routes.core_of(core_con));
  EXPECT_FALSE(routes.core_of(main_con));
  EXPECT_FALSE(routes.core_of(other_con));
}

TEST_F(SessionRoutesTest, FirstHintAsksToConnect)
{
  auto out = routes.apply_hint(pg, hint(core), main_con);
  EXPECT_EQ(hint_outcome_t::what_t::changed, out.what);
  EXPECT_FALSE(out.prev_core);
  EXPECT_TRUE(out.connect);
  // the main connection's address (IP, nonce), on the hinted port
  EXPECT_EQ(addr("v2:127.0.0.1:6810/1234"), out.addr);
  EXPECT_EQ(std::optional<uint32_t>(core), route_of(pg).hinted_core);
}

TEST_F(SessionRoutesTest, SameHintIsUnchanged)
{
  hinted_and_connected();
  auto out = routes.apply_hint(pg, hint(core), main_con);
  EXPECT_EQ(hint_outcome_t::what_t::unchanged, out.what);
  EXPECT_FALSE(out.connect);
}

TEST_F(SessionRoutesTest, HintToAConnectedCoreDoesNotConnect)
{
  hinted_and_connected();
  // another PG of the same core
  auto out = routes.apply_hint(pg2, hint(core), main_con);
  EXPECT_EQ(hint_outcome_t::what_t::changed, out.what);
  EXPECT_FALSE(out.connect);
}

TEST_F(SessionRoutesTest, NewHintReportsThePreviousCore)
{
  hinted_and_connected();
  auto out = routes.apply_hint(
    pg, hint(other_core, 6811), main_con);
  EXPECT_EQ(hint_outcome_t::what_t::changed, out.what);
  EXPECT_EQ(std::optional<uint32_t>(core), out.prev_core);
  EXPECT_TRUE(out.connect);
}

TEST_F(SessionRoutesTest, UnusableHintIsIgnored)
{
  const auto invalid = hint_outcome_t::what_t::invalid;
  // no port
  EXPECT_EQ(invalid, routes.apply_hint(pg, hint(core, 0), main_con).what);
  // no main connection, whose address to use
  EXPECT_EQ(invalid, routes.apply_hint(pg, hint(core), nullptr).what);
  // a main connection without a msgr2 address
  EXPECT_EQ(invalid, routes.apply_hint(
    pg, hint(core), make_con("v1:127.0.0.1:6789/1234")).what);
  EXPECT_FALSE(route_of(pg).hinted_core);
}

TEST_F(SessionRoutesTest, DesiredCoreOnlyOnceConnected)
{
  routes.apply_hint(pg, hint(core), main_con);
  EXPECT_FALSE(routes.desired_core(route_of(pg)));
  routes.add_core_con(core, core_con);
  EXPECT_EQ(std::optional<uint32_t>(core), routes.desired_core(route_of(pg)));
}

// -- routing

TEST_F(SessionRoutesTest, UnhintedPgStaysOnMain)
{
  op_route_t op;
  auto out = route(op, 100);
  EXPECT_EQ(route_outcome_t::what_t::stayed, out.what);
  EXPECT_TRUE(op.is_routed());
  EXPECT_EQ(std::optional<spg_t>(pg), op.pgid);
  EXPECT_FALSE(op.core);
  EXPECT_EQ(1u, route_of(pg).in_flight);
}

TEST_F(SessionRoutesTest, IdlePgSwitchesAtOnce)
{
  hinted_and_connected();
  op_route_t op;
  auto out = route(op, 100);
  EXPECT_EQ(route_outcome_t::what_t::switched, out.what);
  EXPECT_EQ(std::optional<uint32_t>(core), op.core);
  // it was not kept from switching
  EXPECT_FALSE(out.switch_delay);

  op_route_t next;
  EXPECT_EQ(route_outcome_t::what_t::stayed, route(next, 101).what);
  EXPECT_EQ(std::optional<uint32_t>(core), next.core);
  EXPECT_EQ(2u, route_of(pg).in_flight);
}

TEST_F(SessionRoutesTest, BusyPgSwitchesOnceDrained)
{
  op_route_t a, b, c, d;
  route(a, 100);
  hinted_and_connected();

  auto out = route(b, 110);
  EXPECT_EQ(route_outcome_t::what_t::pending, out.what);
  EXPECT_TRUE(out.pending_started);
  EXPECT_FALSE(out.pending_unanswered);
  EXPECT_FALSE(b.core);  // with a, on the main connection

  out = route(c, 111);
  EXPECT_EQ(route_outcome_t::what_t::pending, out.what);
  EXPECT_FALSE(out.pending_started);

  routes.unroute(a);
  routes.unroute(b);
  routes.unroute(c);
  EXPECT_FALSE(a.is_routed());
  EXPECT_FALSE(route_of(pg).busy());

  out = route(d, 115);
  EXPECT_EQ(route_outcome_t::what_t::switched, out.what);
  EXPECT_EQ(std::optional<uint32_t>(core), d.core);
  // due to switch since b was routed
  ASSERT_TRUE(out.switch_delay);
  EXPECT_EQ(ceph::timespan(5s), *out.switch_delay);
  EXPECT_EQ(0u, out.switch_holds);
  EXPECT_FALSE(route_of(pg).switch_pending_since);
}

TEST_F(SessionRoutesTest, UnrouteOfAnUnroutedOpIsHarmless)
{
  op_route_t op;
  routes.unroute(op);
  route(op, 100);
  routes.unroute(op);
  routes.unroute(op);
  EXPECT_EQ(0u, route_of(pg).in_flight);
}

TEST_F(SessionRoutesTest, CutoffIsReadOnlyForCanceledOps)
{
  op_route_t a, b;
  route(a, 100);
  hinted_and_connected();
  route(b, 101);  // pending, but nothing canceled
  EXPECT_EQ(0u, cutoff_calls);
}

// -- canceled ops

TEST_F(SessionRoutesTest, CanceledOpKeepsPgBusyUntilAnswered)
{
  op_route_t a, b, c, d;
  route(a, 100);
  hinted_and_connected();
  // a times out: canceled, then removed from the session
  EXPECT_TRUE(routes.note_unanswered(1, a, at(101)));
  routes.unroute(a);
  EXPECT_TRUE(route_of(pg).busy());

  auto out = route(b, 102);
  EXPECT_EQ(route_outcome_t::what_t::pending, out.what);
  EXPECT_TRUE(out.pending_unanswered);
  EXPECT_TRUE(out.first_delayed_by_unanswered);
  EXPECT_FALSE(b.core);
  EXPECT_EQ(1u, cutoff_calls);

  // with b in flight, not only the canceled op keeps the PG
  out = route(c, 103);
  EXPECT_FALSE(out.pending_unanswered);
  routes.unroute(b);
  routes.unroute(c);
  out = route(c, 104);
  EXPECT_TRUE(out.pending_unanswered);
  EXPECT_FALSE(out.first_delayed_by_unanswered);
  routes.unroute(c);

  // a's late answer
  EXPECT_EQ(std::optional<spg_t>(pg), routes.answered_late(1));
  EXPECT_FALSE(route_of(pg).busy());
  EXPECT_EQ(route_outcome_t::what_t::switched, route(d, 105).what);
}

TEST_F(SessionRoutesTest, UnsentOpIsNotAwaited)
{
  op_route_t op;
  EXPECT_FALSE(routes.note_unanswered(1, op, at(100)));
  EXPECT_TRUE(routes.unanswered.empty());
}

TEST_F(SessionRoutesTest, CanceledOpIsNotedOnce)
{
  op_route_t op;
  route(op, 100);
  EXPECT_TRUE(routes.note_unanswered(1, op, at(101)));
  EXPECT_FALSE(routes.note_unanswered(1, op, at(102)));
  EXPECT_EQ(1u, route_of(pg).unanswered);
}

TEST_F(SessionRoutesTest, AnswerToAnUnknownTidIsIgnored)
{
  EXPECT_FALSE(routes.answered_late(7));
}

TEST_F(SessionRoutesTest, PruneForgetsOnlyOlderCanceledOps)
{
  op_route_t a, b;
  route(a, 100);
  route(b, 100);
  routes.note_unanswered(1, a, at(100));
  routes.note_unanswered(2, b, at(200));
  routes.unroute(a);
  routes.unroute(b);

  auto forgotten = routes.prune_unanswered(at(150));
  ASSERT_EQ(1u, forgotten.size());
  EXPECT_EQ(1u, forgotten.front().first);
  EXPECT_EQ(pg, forgotten.front().second);
  EXPECT_EQ(1u, routes.unanswered.count(2));
  EXPECT_EQ(1u, route_of(pg).unanswered);
}

TEST_F(SessionRoutesTest, RouteOpPrunesOldCanceledOpsAndSwitches)
{
  op_route_t a, b;
  route(a, 100);
  routes.note_unanswered(1, a, at(100));
  routes.unroute(a);
  hinted_and_connected();

  auto out = routes.route_op(pg, b, at(300), [] { return at(200); });
  ASSERT_EQ(1u, out.forgotten.size());
  EXPECT_EQ(1u, out.forgotten.front().first);
  EXPECT_EQ(route_outcome_t::what_t::switched, out.what);
  EXPECT_EQ(std::optional<uint32_t>(core), b.core);
}

TEST_F(SessionRoutesTest, ForgetCanceledOpsByConnection)
{
  // pg on the main connection, pg2 on the core connection
  op_route_t on_main, on_core;
  route(on_main, 100);
  hinted_and_connected(pg2);
  route(on_core, 100, &pg2);
  ASSERT_EQ(std::optional<uint32_t>(core), on_core.core);
  routes.note_unanswered(1, on_main, at(101));
  routes.note_unanswered(2, on_core, at(101));
  routes.unroute(on_main);
  routes.unroute(on_core);

  EXPECT_EQ(1u, routes.forget_unanswered(core));
  EXPECT_EQ(1u, routes.unanswered.count(1));
  EXPECT_FALSE(route_of(pg2).busy());
  EXPECT_TRUE(route_of(pg).busy());
  EXPECT_EQ(1u, routes.forget_all_unanswered());
  EXPECT_TRUE(routes.unanswered.empty());
  EXPECT_FALSE(route_of(pg).busy());
}

// -- holds

TEST_F(SessionRoutesTest, HoldIsDueAfterTheGrace)
{
  unsigned grace_calls = 0;
  SessionRoutes::grace_fn_t grace = [&grace_calls] {
    ++grace_calls;
    return ceph::timespan(10s);
  };
  op_route_t a;
  // not due to switch: the grace is not even read
  route(a, 100);
  EXPECT_EQ(hold_t::none, routes.hold_state(pg, at(200), grace));
  EXPECT_EQ(0u, grace_calls);

  hinted_and_connected();
  op_route_t b;
  route(b, 101);  // pending since 101
  routes.unroute(b);
  EXPECT_EQ(hold_t::none, routes.hold_state(pg, at(105), grace));
  EXPECT_EQ(hold_t::due, routes.hold_state(pg, at(111), grace));
  EXPECT_EQ(2u, grace_calls);
  // a zero grace: never hold
  EXPECT_EQ(hold_t::none, routes.hold_state(
    pg, at(500), [] { return ceph::timespan::zero(); }));
}

TEST_F(SessionRoutesTest, NoHoldOnceNotBusy)
{
  op_route_t a;
  make_pending(a);
  routes.unroute(a);
  // route_op() would switch now
  EXPECT_EQ(hold_t::none, routes.hold_state(
    pg, at(500), [] { return ceph::timespan(1s); }));
}

TEST_F(SessionRoutesTest, HoldDrainsWhenItsOpsAreAnswered)
{
  op_route_t a;
  make_pending(a);
  auto& held = routes.start_hold(pg, at(120), 42);
  held.hold_event = 7;
  EXPECT_TRUE(held.holding);
  EXPECT_EQ(1u, held.switch_holds);
  EXPECT_EQ(hold_t::holding, routes.hold_state(
    pg, at(121), [] { return ceph::timespan(1s); }));
  EXPECT_FALSE(routes.has_drained_holds);

  routes.unroute(a);
  EXPECT_TRUE(routes.has_drained_holds);
  EXPECT_EQ(std::vector<spg_t>{pg}, routes.take_drained_holds());
  EXPECT_FALSE(routes.has_drained_holds);
  EXPECT_TRUE(routes.take_drained_holds().empty());

  auto ended = routes.end_hold(routes.pg_routes.at(pg), true, at(125));
  EXPECT_EQ(ceph::timespan(5s), ended.held_for);
  EXPECT_EQ(7u, ended.timer_event);
  EXPECT_FALSE(route_of(pg).holding);
  EXPECT_EQ(0u, route_of(pg).hold_event);

  // the first op sent switches, after one hold
  op_route_t b;
  auto out = route(b, 126);
  EXPECT_EQ(route_outcome_t::what_t::switched, out.what);
  EXPECT_EQ(1u, out.switch_holds);
}

TEST_F(SessionRoutesTest, ExpiredHoldRestartsTheGrace)
{
  op_route_t a;
  make_pending(a);
  routes.start_hold(pg, at(120), 42);
  routes.end_hold(routes.pg_routes.at(pg), false, at(122));
  EXPECT_FALSE(route_of(pg).holding);
  EXPECT_EQ(std::optional<time_point>(at(122)),
            route_of(pg).switch_pending_since);
  // the delay is still counted from the first time it was due
  EXPECT_EQ(std::optional<time_point>(at(101)), route_of(pg).switch_due_since);
}

TEST_F(SessionRoutesTest, DrainOfAnEndedHoldIsNotTaken)
{
  op_route_t a;
  make_pending(a);
  routes.start_hold(pg, at(120), 42);
  routes.unroute(a);
  routes.end_hold(routes.pg_routes.at(pg), true, at(121));
  EXPECT_TRUE(routes.take_drained_holds().empty());
}

TEST_F(SessionRoutesTest, FindHoldMatchesItsId)
{
  op_route_t a;
  make_pending(a);
  EXPECT_EQ(nullptr, routes.find_hold(pg, 42));
  routes.start_hold(pg, at(120), 42);
  EXPECT_EQ(&routes.pg_routes.at(pg), routes.find_hold(pg, 42));
  EXPECT_EQ(nullptr, routes.find_hold(pg, 41));
  EXPECT_EQ(nullptr, routes.find_hold(pg2, 42));
  routes.end_hold(routes.pg_routes.at(pg), false, at(121));
  EXPECT_EQ(nullptr, routes.find_hold(pg, 42));
}

// -- resets

TEST_F(SessionRoutesTest, CoreResetFallsBackToMain)
{
  // pg on the core connection, pg2 holding to switch to it
  hinted_and_connected();
  op_route_t on_core;
  route(on_core, 100);
  op_route_t busy;
  route(busy, 100, &pg2);
  hinted_and_connected(pg2);
  op_route_t pending;
  route(pending, 101, &pg2);
  auto& held = routes.start_hold(pg2, at(110), 42);
  held.hold_event = 7;

  auto reset = routes.reset_core(core);
  EXPECT_EQ(core_con, reset.con);
  EXPECT_TRUE(routes.core_cons.empty());
  EXPECT_EQ(std::set<spg_t>{pg}, reset.rerouted);
  EXPECT_EQ(std::set<spg_t>{pg2}, reset.released_holds);
  EXPECT_EQ(std::vector<uint64_t>{7}, reset.timer_events);
  for (const auto& pgid : {pg, pg2}) {
    EXPECT_FALSE(route_of(pgid).hinted_core);
    EXPECT_FALSE(route_of(pgid).core);
    EXPECT_FALSE(route_of(pgid).holding);
    EXPECT_FALSE(route_of(pgid).switch_pending_since);
  }
  // the ops in flight are the caller's to resend
  EXPECT_EQ(1u, route_of(pg).in_flight);
}

TEST_F(SessionRoutesTest, CoreResetLeavesOtherCoresAlone)
{
  hinted_and_connected();
  routes.apply_hint(pg2, hint(other_core, 6811), main_con);
  routes.add_core_con(other_core, other_con);
  op_route_t op;
  route(op, 100, &pg2);

  auto reset = routes.reset_core(core);
  EXPECT_TRUE(reset.rerouted.empty());
  EXPECT_EQ(std::optional<uint32_t>(other_core), route_of(pg2).core);
  EXPECT_EQ(std::optional<uint32_t>(other_core), routes.core_of(other_con));
}

TEST_F(SessionRoutesTest, ResetAllForgetsEverything)
{
  op_route_t a;
  make_pending(a);
  routes.add_core_con(other_core, other_con);
  routes.start_hold(pg, at(120), 42).hold_event = 7;
  routes.unroute(a);  // drained
  ASSERT_TRUE(routes.has_drained_holds);

  auto reset = routes.reset_all();
  EXPECT_EQ(2u, reset.cons.size());
  EXPECT_EQ(std::vector<uint64_t>{7}, reset.timer_events);
  EXPECT_TRUE(routes.core_cons.empty());
  EXPECT_FALSE(routes.has_drained_holds);
  EXPECT_TRUE(routes.drained_holds.empty());
  EXPECT_FALSE(route_of(pg).hinted_core);
  EXPECT_FALSE(route_of(pg).holding);

  op_route_t b;
  route(b, 130);
  EXPECT_FALSE(b.core);
}

// -- stats

TEST_F(SessionRoutesTest, CountSwitching)
{
  EXPECT_EQ(std::make_pair(uint64_t(0), uint64_t(0)),
            routes.count_switching());
  op_route_t a;
  make_pending(a);
  EXPECT_EQ(std::make_pair(uint64_t(0), uint64_t(1)),
            routes.count_switching());
  routes.start_hold(pg, at(120), 42);
  EXPECT_EQ(std::make_pair(uint64_t(1), uint64_t(0)),
            routes.count_switching());
}
