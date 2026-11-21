// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/**
 * osdc::core_route::SessionRoutes: the routing state of an Objecter's OSD
 * session, driven by its events. The time is passed in, so nothing here
 * waits.
 */

#include <chrono>

#include <gtest/gtest.h>

#include "osdc/crimson_core_route/session_routes.h"
#include "test/osd/MockConnection.h"

using namespace osdc::core_route;
using namespace std::chrono_literals;

namespace {

time_point at(int seconds)
{
  return time_point{std::chrono::seconds(seconds)};
}

/// the clock of the keepalives
utime_t real_at(int seconds)
{
  return utime_t(seconds, 0);
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
  HostBlocks blocks;
  const spg_t pg{pg_t(0, 1)};
  const spg_t pg2{pg_t(1, 1)};
  static constexpr core_id_t core = 3;
  static constexpr core_id_t other_core = 5;
  static constexpr uint16_t core_port = 6810;
  static constexpr uint16_t other_port = 6811;

  ConnectionRef main_con = make_con("v2:127.0.0.1:6800/1234");
  ConnectionRef core_con = make_con("v2:127.0.0.1:6810/1234");
  ConnectionRef other_con = make_con("v2:127.0.0.1:6811/1234");
  /// the OSD's host (as its core listeners are blocked)
  const entity_addr_t host =
    HostBlocks::host_of(addr("v2:127.0.0.1:6800/1234"));

  /// routing on; no holds (grace 0); canceled ops awaited for long
  config_t cfg{
    .enabled = true,
    .grace = ceph::timespan::zero(),
    .max_hold = ceph::timespan(1000s),
    .connect_deadline = ceph::timespan(10s),
    .probe_deadline = ceph::timespan(10s),
  };

  /// opens core_con or other_con, by port (null: if 'connect_fails'), and
  /// counts its calls
  bool connect_fails = false;
  unsigned connect_calls = 0;
  entity_addr_t connected_to;
  connect_fn_t connect = [this](const entity_addr_t& a) -> ConnectionRef {
    ++connect_calls;
    connected_to = a;
    if (connect_fails) {
      return nullptr;
    }
    return a.get_port() == core_port ? core_con : other_con;
  };

  /// the session's ops, as visited when the session activates (none,
  /// unless a test sets them)
  for_each_op_fn_t session_ops;

  /// most tests start with an active session: it followed a hint (of a PG
  /// of its own), whose connection and route are then forgotten
  void SetUp() override
  {
    const spg_t spare{pg_t(99, 1)};
    ASSERT_EQ(hint_followed_t::what_t::changed,
              follow(spare, hint(core)).what);
    ASSERT_TRUE(routes.is_active());
    ASSERT_TRUE(routes.drop_core_con(core_con, drop_why_t::reset, at(90),
                                     blocks));
    const auto ttl = cfg.route_idle_ttl;
    cfg.route_idle_ttl = ceph::timespan::zero();
    ASSERT_EQ(1u, check(91).routes_pruned);
    cfg.route_idle_ttl = ttl;
    ASSERT_EQ(nullptr, routes.route_of(spare));
    ASSERT_EQ(0u, routes.num_core_cons());
    connect_calls = 0;
  }

  static ConnectionRef make_con(const char* a)
  {
    auto c = ceph::make_ref<MockConnection>(0);
    c->set_peer_addrs(entity_addrvec_t(addr(a)));
    return c;
  }

  /// a hint of core c, whose listener is on 'port'
  static osd_core_hint_t hint(core_id_t c, uint16_t port = core_port)
  {
    return osd_core_hint_t(c, port);
  }

  hint_followed_t follow(const spg_t& pgid, const osd_core_hint_t& h,
                         int now = 90)
  {
    return routes.follow_hint(pgid, h, main_con, at(now), blocks, connect,
                              session_ops, cfg);
  }

  /// pgid is hinted to 'core', and connected to it
  void hinted_and_connected(const spg_t& pgid = spg_t(pg_t(0, 1)))
  {
    auto out = follow(pgid, hint(core));
    if (out.opened) {
      ASSERT_TRUE(routes.admit_core_con(out.opened, blocks));
    }
  }

  static osd_core_hint_t hint_of(core_id_t c)
  {
    return hint(c, c == core ? core_port : other_port);
  }

  /// a connection to core c is opened (for a PG of its own), not connected
  /// yet
  void open_core(core_id_t c)
  {
    ASSERT_TRUE(follow(spg_t(pg_t(100 + c, 1)), hint_of(c)).opened);
  }

  /// a connection to core c is opened, and connected
  void connected(core_id_t c)
  {
    open_core(c);
    ASSERT_TRUE(routes.admit_core_con(routes.core_con(c), blocks));
  }

  routing_t route(op_route_t& op, int now, const spg_t* pgid = nullptr)
  {
    return routes.route_or_hold(pgid ? *pgid : pg, op, at(now), cfg);
  }

  timeouts_checked_t check(int now, int probe_now = 0,
                           const std::set<core_id_t>& laggy = {})
  {
    return routes.periodic_timeouts_check(at(now), real_at(probe_now),
                                          laggy, cfg);
  }

  const pg_route_t& route_of(const spg_t& pgid) const
  {
    auto route = routes.route_of(pgid);
    ceph_assert(route);
    return *route;
  }

  /// a PG due to switch to 'core', busy with op a (on the main connection),
  /// since 101
  void make_pending(op_route_t& a)
  {
    route(a, 100);
    hinted_and_connected();
    op_route_t b;
    ASSERT_EQ(routing_t::what_t::pending, route(b, 101).what);
    routes.retire_op(b);
  }

  std::vector<core_id_t> unused_cores(int now = 100)
  {
    std::vector<core_id_t> unused;
    for (const auto& [c, con] : check(now).unused) {
      unused.push_back(c);
    }
    return unused;
  }
};

} // namespace

// -- connections and hints

TEST_F(SessionRoutesTest, CoreOfFindsOnlyCoreConnections)
{
  open_core(core);
  EXPECT_EQ(std::optional<core_id_t>(core), routes.core_of(core_con));
  EXPECT_FALSE(routes.core_of(main_con));
  EXPECT_FALSE(routes.core_of(other_con));
}

TEST_F(SessionRoutesTest, FirstHintConnects)
{
  auto out = follow(pg, hint(core));
  EXPECT_EQ(hint_followed_t::what_t::changed, out.what);
  EXPECT_FALSE(out.prev_core);
  EXPECT_EQ(core_con, out.opened);
  // the main connection's address (IP, nonce), on the hinted port
  EXPECT_EQ(addr("v2:127.0.0.1:6810/1234"), out.addr);
  EXPECT_EQ(out.addr, connected_to);
  EXPECT_EQ(std::optional<core_id_t>(core), route_of(pg).hinted_core);
  EXPECT_EQ(core_con, routes.core_con(core));
  EXPECT_TRUE(routes.is_connecting(core));
}

TEST_F(SessionRoutesTest, SameHintIsUnchanged)
{
  hinted_and_connected();
  auto out = follow(pg, hint(core));
  EXPECT_EQ(hint_followed_t::what_t::unchanged, out.what);
  EXPECT_FALSE(out.opened);
  EXPECT_EQ(1u, connect_calls);
}

TEST_F(SessionRoutesTest, HintToAConnectedCoreDoesNotConnect)
{
  hinted_and_connected();
  // another PG of the same core
  auto out = follow(pg2, hint(core));
  EXPECT_EQ(hint_followed_t::what_t::changed, out.what);
  EXPECT_FALSE(out.opened);
  EXPECT_EQ(1u, connect_calls);
}

TEST_F(SessionRoutesTest, NewHintReportsThePreviousCore)
{
  hinted_and_connected();
  auto out = follow(pg, hint(other_core, other_port));
  EXPECT_EQ(hint_followed_t::what_t::changed, out.what);
  EXPECT_EQ(std::optional<core_id_t>(core), out.prev_core);
  EXPECT_EQ(other_con, out.opened);
}

TEST_F(SessionRoutesTest, UnusableHintIsIgnored)
{
  const auto invalid = hint_followed_t::what_t::invalid;
  // no port
  EXPECT_EQ(invalid, follow(pg, hint(core, 0)).what);
  // no main connection, whose address to use
  EXPECT_EQ(invalid, routes.follow_hint(
    pg, hint(core), nullptr, at(90), blocks, connect, session_ops,
    cfg).what);
  // a main connection without a msgr2 address
  EXPECT_EQ(invalid, routes.follow_hint(
    pg, hint(core), make_con("v1:127.0.0.1:6789/1234"), at(90), blocks,
    connect, session_ops, cfg).what);
  EXPECT_EQ(nullptr, routes.route_of(pg));
  EXPECT_EQ(0u, connect_calls);
}

TEST_F(SessionRoutesTest, HintWhileRoutingOffIsIgnored)
{
  cfg.enabled = false;
  EXPECT_EQ(hint_followed_t::what_t::invalid, follow(pg, hint(core)).what);
  EXPECT_EQ(0u, connect_calls);
  EXPECT_EQ(0u, routes.num_core_cons());
  EXPECT_EQ(nullptr, routes.route_of(pg));

  // not recorded: turned on, the same hint connects
  cfg.enabled = true;
  auto out = follow(pg, hint(core));
  EXPECT_EQ(hint_followed_t::what_t::changed, out.what);
  EXPECT_EQ(core_con, out.opened);
  EXPECT_EQ(1u, connect_calls);
}

TEST_F(SessionRoutesTest, CoreIsRoutedToOnlyOnceAdmitted)
{
  follow(pg, hint(core));
  op_route_t a;
  // opened, but not connected yet. Not even due to switch: the core may
  // never connect
  auto out = route(a, 101);
  EXPECT_EQ(routing_t::what_t::stayed, out.what);
  EXPECT_FALSE(a.core);
  EXPECT_FALSE(out.con);
  EXPECT_FALSE(route_of(pg).switch_pending_since);
  routes.retire_op(a);

  auto admitted = routes.admit_core_con(core_con, blocks);
  ASSERT_TRUE(admitted);
  EXPECT_EQ(core, admitted->core);
  EXPECT_FALSE(admitted->unblocked);
  op_route_t b;
  out = route(b, 102);
  EXPECT_EQ(routing_t::what_t::switched, out.what);
  EXPECT_EQ(std::optional<core_id_t>(core), b.core);
  EXPECT_EQ(core_con, out.con);
}

TEST_F(SessionRoutesTest, OnlyConnectingCoreConsAreAdmitted)
{
  EXPECT_FALSE(routes.admit_core_con(main_con, blocks));
  open_core(core);
  EXPECT_TRUE(routes.admit_core_con(core_con, blocks));
  // connected already
  EXPECT_FALSE(routes.admit_core_con(core_con, blocks));
}

TEST_F(SessionRoutesTest, ConnectTimeoutsAreFound)
{
  open_core(core);  // at 90
  connected(other_core);
  EXPECT_TRUE(check(100).connect_timed_out.empty());
  auto timed_out = check(101).connect_timed_out;
  ASSERT_EQ(1u, timed_out.size());
  EXPECT_EQ(core, timed_out.front().first);
  EXPECT_EQ(core_con, timed_out.front().second);

  routes.admit_core_con(core_con, blocks);
  EXPECT_TRUE(check(111).connect_timed_out.empty());
}

TEST_F(SessionRoutesTest, DropsForgetConnectingCons)
{
  open_core(core);
  ASSERT_TRUE(routes.drop_core_con(core_con, drop_why_t::reset, at(95),
                                   blocks));
  EXPECT_FALSE(routes.is_connecting(core));
  EXPECT_FALSE(routes.core_con(core));
  open_core(core);
  routes.reset_session();
  EXPECT_FALSE(routes.is_connecting(core));
  EXPECT_EQ(0u, routes.num_core_cons());
}

TEST_F(SessionRoutesTest, BlockedHintIsNotRecorded)
{
  blocks.block(host, at(90));
  auto out = follow(pg, hint(core));
  EXPECT_EQ(hint_followed_t::what_t::blocked, out.what);
  EXPECT_FALSE(out.opened);
  EXPECT_EQ(nullptr, routes.route_of(pg));
  EXPECT_EQ(0u, connect_calls);

  // tried again on the next hint
  blocks.unblock(host);
  out = follow(pg, hint(core));
  EXPECT_EQ(hint_followed_t::what_t::changed, out.what);
  EXPECT_TRUE(out.opened);
}

TEST_F(SessionRoutesTest, BlocksMatterOnlyForNewConnections)
{
  hinted_and_connected();
  blocks.block(host, at(90));
  // another PG of a connected core
  EXPECT_EQ(hint_followed_t::what_t::changed, follow(pg2, hint(core)).what);
  EXPECT_EQ(1u, connect_calls);
}

TEST_F(SessionRoutesTest, FailedConnectIsNotRecorded)
{
  connect_fails = true;
  auto out = follow(pg, hint(core));
  EXPECT_EQ(hint_followed_t::what_t::connect_failed, out.what);
  EXPECT_EQ(nullptr, routes.route_of(pg));
  EXPECT_EQ(0u, routes.num_core_cons());

  // tried again on the next hint
  connect_fails = false;
  out = follow(pg, hint(core));
  EXPECT_EQ(hint_followed_t::what_t::changed, out.what);
  EXPECT_EQ(core_con, out.opened);
  EXPECT_EQ(2u, connect_calls);
}

TEST_F(SessionRoutesTest, AdmissionLiftsTheHostBlock)
{
  open_core(core);
  blocks.block(host, at(91));
  auto admitted = routes.admit_core_con(core_con, blocks);
  ASSERT_TRUE(admitted);
  EXPECT_TRUE(admitted->unblocked);
  EXPECT_FALSE(blocks.is_blocked(host, at(92)));
}

// a hint naming the main connection's port: connecting to it would hand
// back (or duplicate) the main connection
TEST_F(SessionRoutesTest, MainPortHintIsIgnored)
{
  EXPECT_EQ(hint_followed_t::what_t::invalid,
            follow(pg, hint(core, 6800)).what);
  EXPECT_EQ(0u, connect_calls);
  // nor is the main connection ever taken as a core connection
  connect = [this](const entity_addr_t&) -> ConnectionRef {
    ++connect_calls;
    return main_con;
  };
  EXPECT_EQ(hint_followed_t::what_t::invalid, follow(pg, hint(core)).what);
  EXPECT_EQ(1u, connect_calls);
  EXPECT_EQ(0u, routes.num_core_cons());
  EXPECT_FALSE(routes.core_of(main_con));
}

// -- routing

TEST_F(SessionRoutesTest, UnhintedPgStaysOnMain)
{
  op_route_t op;
  auto out = route(op, 100);
  EXPECT_EQ(routing_t::what_t::stayed, out.what);
  EXPECT_TRUE(routes.is_in_flight(op));
  EXPECT_EQ(std::optional<spg_t>(pg), op.pgid);
  EXPECT_FALSE(op.core);
  EXPECT_FALSE(out.core);
  EXPECT_FALSE(out.con);
  EXPECT_EQ(1u, route_of(pg).in_flight);
}

TEST_F(SessionRoutesTest, IdlePgSwitchesAtOnce)
{
  hinted_and_connected();
  op_route_t op;
  auto out = route(op, 100);
  EXPECT_EQ(routing_t::what_t::switched, out.what);
  EXPECT_EQ(std::optional<core_id_t>(core), op.core);
  EXPECT_EQ(std::optional<core_id_t>(core), out.core);
  EXPECT_EQ(core_con, out.con);
  // it was not kept from switching
  EXPECT_FALSE(out.switch_delay);

  op_route_t next;
  EXPECT_EQ(routing_t::what_t::stayed, route(next, 101).what);
  EXPECT_EQ(std::optional<core_id_t>(core), next.core);
  EXPECT_EQ(2u, route_of(pg).in_flight);
}

TEST_F(SessionRoutesTest, BusyPgSwitchesOnceDrained)
{
  op_route_t a, b, c, d;
  route(a, 100);
  hinted_and_connected();

  auto out = route(b, 110);
  EXPECT_EQ(routing_t::what_t::pending, out.what);
  EXPECT_TRUE(out.pending_started);
  EXPECT_FALSE(out.pending_unanswered);
  EXPECT_FALSE(b.core);  // with a, on the main connection

  out = route(c, 111);
  EXPECT_EQ(routing_t::what_t::pending, out.what);
  EXPECT_FALSE(out.pending_started);

  routes.retire_op(a);
  routes.retire_op(b);
  routes.retire_op(c);
  EXPECT_FALSE(routes.is_in_flight(a));
  EXPECT_FALSE(route_of(pg).busy());

  out = route(d, 115);
  EXPECT_EQ(routing_t::what_t::switched, out.what);
  EXPECT_EQ(std::optional<core_id_t>(core), d.core);
  // due to switch since b was routed
  ASSERT_TRUE(out.switch_delay);
  EXPECT_EQ(ceph::timespan(5s), *out.switch_delay);
  EXPECT_EQ(0u, out.switch_holds);
  EXPECT_FALSE(route_of(pg).switch_pending_since);
}

TEST_F(SessionRoutesTest, RetireOfAnUnroutedOpIsHarmless)
{
  op_route_t op;
  routes.retire_op(op);
  route(op, 100);
  routes.retire_op(op);
  routes.retire_op(op);
  EXPECT_EQ(0u, route_of(pg).in_flight);
}

TEST_F(SessionRoutesTest, RoutingAgainVoidsThePreviousRoute)
{
  // e.g. resent after a map change
  op_route_t op;
  route(op, 100);
  route(op, 101);
  EXPECT_EQ(1u, route_of(pg).in_flight);
  routes.retire_op(op);
  EXPECT_EQ(0u, route_of(pg).in_flight);
}

// -- canceled ops

TEST_F(SessionRoutesTest, CanceledOpKeepsPgBusyUntilAnswered)
{
  op_route_t a, b, c, d;
  route(a, 100);
  hinted_and_connected();
  // a times out
  auto awaited = routes.retire_canceled_op(1, a, at(101));
  ASSERT_TRUE(awaited);
  EXPECT_EQ(pg, awaited->pgid);
  EXPECT_FALSE(awaited->core);
  EXPECT_FALSE(routes.is_in_flight(a));
  EXPECT_TRUE(route_of(pg).busy());

  auto out = route(b, 102);
  EXPECT_EQ(routing_t::what_t::pending, out.what);
  EXPECT_TRUE(out.pending_unanswered);
  EXPECT_TRUE(out.first_delayed_by_unanswered);
  EXPECT_FALSE(b.core);

  // with b in flight, not only the canceled op keeps the PG
  out = route(c, 103);
  EXPECT_FALSE(out.pending_unanswered);
  routes.retire_op(b);
  routes.retire_op(c);
  out = route(c, 104);
  EXPECT_TRUE(out.pending_unanswered);
  EXPECT_FALSE(out.first_delayed_by_unanswered);
  routes.retire_op(c);

  // a's late answer
  EXPECT_EQ(std::optional<spg_t>(pg), routes.canceled_op_answered(1));
  EXPECT_FALSE(route_of(pg).busy());
  EXPECT_EQ(routing_t::what_t::switched, route(d, 105).what);
}

TEST_F(SessionRoutesTest, UnsentOpIsNotAwaited)
{
  op_route_t op;
  EXPECT_FALSE(routes.retire_canceled_op(1, op, at(100)));
  // nor a held one
  cfg.grace = ceph::timespan(1s);
  op_route_t a;
  make_pending(a);
  ASSERT_EQ(routing_t::what_t::held, route(op, 102).what);
  EXPECT_FALSE(routes.retire_canceled_op(2, op, at(103)));
  EXPECT_FALSE(routes.is_held(op));
  EXPECT_FALSE(routes.has_unanswered());
}

TEST_F(SessionRoutesTest, CanceledOpIsAwaitedOnce)
{
  op_route_t op;
  route(op, 100);
  EXPECT_TRUE(routes.retire_canceled_op(1, op, at(101)));
  EXPECT_FALSE(routes.retire_canceled_op(1, op, at(102)));
  EXPECT_EQ(1u, route_of(pg).unanswered);
}

TEST_F(SessionRoutesTest, AnswerToAnUnknownTidIsIgnored)
{
  EXPECT_FALSE(routes.canceled_op_answered(7));
}

TEST_F(SessionRoutesTest, PeriodicCheckForgetsOnlyOlderCanceledOps)
{
  cfg.max_hold = ceph::timespan(50s);
  op_route_t a, b;
  route(a, 100);
  route(b, 100);
  routes.retire_canceled_op(1, a, at(100));
  routes.retire_canceled_op(2, b, at(200));

  auto forgotten = check(151).forgotten;
  ASSERT_EQ(1u, forgotten.size());
  EXPECT_EQ(1u, forgotten.front().first);
  EXPECT_EQ(pg, forgotten.front().second);
  EXPECT_EQ(1u, route_of(pg).unanswered);
  EXPECT_FALSE(routes.canceled_op_answered(1));
  EXPECT_TRUE(routes.canceled_op_answered(2));
}

TEST_F(SessionRoutesTest, RoutingForgetsOldCanceledOpsAndSwitches)
{
  cfg.max_hold = ceph::timespan(100s);
  op_route_t a, b;
  route(a, 100);
  routes.retire_canceled_op(1, a, at(100));
  hinted_and_connected();

  auto out = route(b, 300);
  ASSERT_EQ(1u, out.forgotten.size());
  EXPECT_EQ(1u, out.forgotten.front().first);
  EXPECT_EQ(routing_t::what_t::switched, out.what);
  EXPECT_EQ(std::optional<core_id_t>(core), b.core);
}

TEST_F(SessionRoutesTest, CoreDropForgetsTheCanceledOpsSentOnIt)
{
  // pg on the main connection, pg2 on the core connection
  op_route_t on_main, on_core;
  route(on_main, 100);
  hinted_and_connected(pg2);
  route(on_core, 100, &pg2);
  ASSERT_EQ(std::optional<core_id_t>(core), on_core.core);
  routes.retire_canceled_op(1, on_main, at(101));
  routes.retire_canceled_op(2, on_core, at(101));

  auto dropped = routes.drop_core_con(core_con, drop_why_t::reset, at(102),
                                      blocks);
  ASSERT_TRUE(dropped);
  EXPECT_EQ((forgotten_t{{2, pg2}}), dropped->forgotten);
  EXPECT_FALSE(route_of(pg2).busy());
  EXPECT_TRUE(route_of(pg).busy());
  EXPECT_EQ((forgotten_t{{1, pg}}), routes.reset_session().forgotten);
  EXPECT_FALSE(routes.has_unanswered());
  EXPECT_EQ(nullptr, routes.route_of(pg));
}

TEST_F(SessionRoutesTest, CanceledOpOfADroppedConIsNotAwaited)
{
  hinted_and_connected();
  op_route_t op;
  route(op, 100);
  ASSERT_EQ(std::optional<core_id_t>(core), op.core);
  routes.drop_core_con(core_con, drop_why_t::reset, at(101), blocks);
  // its connection is gone: no answer will come
  EXPECT_FALSE(routes.retire_canceled_op(1, op, at(102)));
  EXPECT_FALSE(routes.has_unanswered());
  EXPECT_FALSE(route_of(pg).busy());
}

// -- holds

TEST_F(SessionRoutesTest, HoldStartsAfterTheGrace)
{
  cfg.grace = ceph::timespan(10s);
  op_route_t a, b, c, d, e;
  route(a, 100);
  hinted_and_connected();
  route(b, 101);  // pending since 101
  EXPECT_EQ(routing_t::what_t::pending, route(c, 110).what);

  auto out = route(d, 111);
  EXPECT_EQ(routing_t::what_t::held, out.what);
  EXPECT_TRUE(out.hold_started);
  EXPECT_NE(0u, out.hold_seq);
  EXPECT_EQ(out.hold_seq, d.hold_seq);
  EXPECT_TRUE(routes.is_held(d));
  EXPECT_FALSE(routes.is_in_flight(d));
  EXPECT_TRUE(route_of(pg).holding);
  EXPECT_EQ(1u, route_of(pg).switch_holds);
  EXPECT_EQ(3u, route_of(pg).in_flight);

  // the next op joins the hold
  auto next = route(e, 112);
  EXPECT_EQ(routing_t::what_t::held, next.what);
  EXPECT_FALSE(next.hold_started);
  EXPECT_EQ(out.hold_seq, next.hold_seq);
}

TEST_F(SessionRoutesTest, ZeroGraceNeverHolds)
{
  op_route_t a, b;
  make_pending(a);
  EXPECT_EQ(routing_t::what_t::pending, route(b, 500).what);
}

TEST_F(SessionRoutesTest, NoHoldOnceNotBusy)
{
  cfg.grace = ceph::timespan(1s);
  op_route_t a, b;
  make_pending(a);
  routes.retire_op(a);
  EXPECT_EQ(routing_t::what_t::switched, route(b, 500).what);
}

TEST_F(SessionRoutesTest, NoHoldWhenOnlyOldCanceledOpsKeepThePg)
{
  cfg.grace = ceph::timespan(10s);
  cfg.max_hold = ceph::timespan(2s);
  op_route_t a, b, c;
  route(a, 100);
  hinted_and_connected();
  routes.retire_canceled_op(1, a, at(101));
  route(b, 102);  // pending since 102
  routes.retire_op(b);
  // due to hold, but only a's answer, no longer awaited, kept the PG
  auto out = route(c, 112);
  EXPECT_EQ(routing_t::what_t::switched, out.what);
  EXPECT_FALSE(out.hold_started);
  EXPECT_EQ((forgotten_t{{1, pg}}), out.forgotten);
}

TEST_F(SessionRoutesTest, HoldEndsWhenItsPgDrains)
{
  cfg.grace = ceph::timespan(10s);
  op_route_t a, held;
  make_pending(a);
  const auto seq = route(held, 120).hold_seq;
  EXPECT_FALSE(routes.has_drained_holds());

  routes.retire_op(a);
  EXPECT_TRUE(routes.has_drained_holds());
  auto ended = routes.release_drained_holds(at(125));
  ASSERT_EQ(1u, ended.size());
  EXPECT_EQ(pg, ended.front().pgid);
  EXPECT_EQ(seq, ended.front().hold_seq);
  EXPECT_EQ(ceph::timespan(5s), ended.front().held_for);
  EXPECT_TRUE(ended.front().drained);
  EXPECT_FALSE(routes.has_drained_holds());
  EXPECT_TRUE(routes.release_drained_holds(at(125)).empty());
  EXPECT_FALSE(route_of(pg).holding);
  EXPECT_FALSE(routes.is_held(held));

  // the held op, sent first, switches, after one hold
  auto out = route(held, 126);
  EXPECT_EQ(routing_t::what_t::switched, out.what);
  EXPECT_EQ(1u, out.switch_holds);
}

TEST_F(SessionRoutesTest, ExpiredHoldRestartsTheGrace)
{
  cfg.grace = ceph::timespan(10s);
  op_route_t a, held;
  make_pending(a);
  const auto seq = route(held, 120).hold_seq;
  auto ended = routes.expire_hold(pg, seq, at(122), cfg);
  ASSERT_TRUE(ended);
  EXPECT_FALSE(ended->drained);
  EXPECT_EQ(ceph::timespan(2s), ended->held_for);
  EXPECT_FALSE(route_of(pg).holding);
  EXPECT_FALSE(routes.is_held(held));
  EXPECT_EQ(std::optional<time_point>(at(122)),
            route_of(pg).switch_pending_since);
  // the delay is still counted from the first time it was due
  EXPECT_EQ(std::optional<time_point>(at(101)), route_of(pg).switch_due_since);
  // the held op goes on the current connection
  auto out = route(held, 123);
  EXPECT_EQ(routing_t::what_t::pending, out.what);
  EXPECT_FALSE(held.core);
}

TEST_F(SessionRoutesTest, DrainOfAnEndedHoldIsNotReleased)
{
  cfg.grace = ceph::timespan(10s);
  op_route_t a, held;
  make_pending(a);
  const auto seq = route(held, 120).hold_seq;
  routes.retire_op(a);
  // the timer fires before the drain is released
  auto ended = routes.expire_hold(pg, seq, at(121), cfg);
  ASSERT_TRUE(ended);
  EXPECT_TRUE(ended->drained);
  EXPECT_TRUE(routes.release_drained_holds(at(121)).empty());
}

TEST_F(SessionRoutesTest, StaleHoldTimersAreIgnored)
{
  cfg.grace = ceph::timespan(10s);
  op_route_t a, held;
  make_pending(a);
  EXPECT_FALSE(routes.expire_hold(pg, 42, at(119), cfg));
  const auto seq = route(held, 120).hold_seq;
  EXPECT_FALSE(routes.expire_hold(pg, seq + 1, at(121), cfg));
  EXPECT_FALSE(routes.expire_hold(pg2, seq, at(121), cfg));
  routes.retire_op(a);
  ASSERT_EQ(1u, routes.release_drained_holds(at(121)).size());
  // the timer of the ended hold
  EXPECT_FALSE(routes.expire_hold(pg, seq, at(122), cfg));
}

TEST_F(SessionRoutesTest, ExpiredHoldForgetsOldCanceledOps)
{
  cfg.grace = ceph::timespan(10s);
  cfg.max_hold = ceph::timespan(2s);
  op_route_t a, b, held;
  make_pending(a);
  route(b, 110);
  const auto seq = route(held, 120).hold_seq;
  ASSERT_NE(0u, seq);
  routes.retire_op(a);
  routes.retire_canceled_op(1, b, at(120));
  // only b's answer kept the PG: no longer awaited
  auto ended = routes.expire_hold(pg, seq, at(123), cfg);
  ASSERT_TRUE(ended);
  EXPECT_TRUE(ended->drained);
  EXPECT_EQ((forgotten_t{{1, pg}}), ended->forgotten);
}

// -- drops and resets

TEST_F(SessionRoutesTest, CoreDropFallsBackToMain)
{
  cfg.grace = ceph::timespan(5s);
  // pg on the core connection, pg2 holding to switch to it
  hinted_and_connected();
  op_route_t on_core;
  route(on_core, 100);
  op_route_t busy;
  route(busy, 100, &pg2);
  hinted_and_connected(pg2);
  op_route_t pending;
  route(pending, 101, &pg2);
  op_route_t held;
  const auto seq = route(held, 110, &pg2).hold_seq;
  ASSERT_NE(0u, seq);

  auto dropped = routes.drop_core_con(core_con, drop_why_t::reset, at(111),
                                      blocks);
  ASSERT_TRUE(dropped);
  EXPECT_EQ(core, dropped->core);
  EXPECT_EQ(core_con, dropped->con);
  EXPECT_EQ(0u, routes.num_core_cons());
  EXPECT_EQ(std::set<spg_t>{pg}, dropped->rerouted);
  EXPECT_EQ((std::vector<std::pair<spg_t, uint64_t>>{{pg2, seq}}),
            dropped->released_holds);
  EXPECT_EQ(ceph::timespan::zero(), dropped->blocked_for);
  for (const auto& pgid : {pg, pg2}) {
    EXPECT_FALSE(route_of(pgid).hinted_core);
    EXPECT_FALSE(route_of(pgid).core);
    EXPECT_FALSE(route_of(pgid).holding);
    EXPECT_FALSE(route_of(pgid).switch_pending_since);
  }
  EXPECT_FALSE(routes.is_held(held));
  // what was in flight on it is no longer accounted for (the Objecter
  // resends it); pg2's ops, on the main connection, still are
  EXPECT_EQ(0u, route_of(pg).in_flight);
  EXPECT_FALSE(routes.is_in_flight(on_core));
  routes.retire_op(on_core);
  EXPECT_EQ(0u, route_of(pg).in_flight);
  EXPECT_EQ(2u, route_of(pg2).in_flight);
}

TEST_F(SessionRoutesTest, CoreDropLeavesOtherCoresAlone)
{
  hinted_and_connected();
  follow(pg2, hint(other_core, other_port));
  routes.admit_core_con(other_con, blocks);
  op_route_t op;
  route(op, 100, &pg2);

  auto dropped = routes.drop_core_con(core_con, drop_why_t::reset, at(101),
                                      blocks);
  ASSERT_TRUE(dropped);
  EXPECT_TRUE(dropped->rerouted.empty());
  EXPECT_EQ(std::optional<core_id_t>(other_core), route_of(pg2).core);
  EXPECT_EQ(std::optional<core_id_t>(other_core), routes.core_of(other_con));
  EXPECT_TRUE(routes.is_in_flight(op));
}

TEST_F(SessionRoutesTest, DropIgnoresConsNoLongerQualifying)
{
  // not core connections of the session
  EXPECT_FALSE(routes.drop_core_con(main_con, drop_why_t::reset, at(100),
                                    blocks));
  EXPECT_FALSE(routes.drop_core_con(other_con, drop_why_t::unresponsive,
                                    at(100), blocks));
  // connected since it timed out
  hinted_and_connected();
  EXPECT_FALSE(routes.drop_core_con(core_con, drop_why_t::connect_timeout,
                                    at(100), blocks));
  // used since it was found unused
  op_route_t op;
  route(op, 100);
  EXPECT_FALSE(routes.drop_core_con(core_con, drop_why_t::unused, at(101),
                                    blocks));
  EXPECT_EQ(core_con, routes.core_con(core));
  // dropped already
  EXPECT_TRUE(routes.drop_core_con(core_con, drop_why_t::reset, at(102),
                                   blocks));
  EXPECT_FALSE(routes.drop_core_con(core_con, drop_why_t::reset, at(103),
                                    blocks));
}

TEST_F(SessionRoutesTest, ConnectTimeoutBlocksTheHost)
{
  open_core(core);
  auto dropped = routes.drop_core_con(core_con, drop_why_t::connect_timeout,
                                      at(101), blocks);
  ASSERT_TRUE(dropped);
  EXPECT_EQ(HostBlocks::initial_backoff, dropped->blocked_for);
  EXPECT_TRUE(blocks.is_blocked(host, at(101)));
  EXPECT_EQ(hint_followed_t::what_t::blocked, follow(pg, hint(core), 102).what);
}

TEST_F(SessionRoutesTest, ResetSessionForgetsEverything)
{
  cfg.grace = ceph::timespan(10s);
  op_route_t a, held, other;
  make_pending(a);
  connected(other_core);
  route(other, 100, &pg2);
  route(held, 120);
  routes.retire_op(a);  // drained
  ASSERT_TRUE(routes.has_drained_holds());

  auto reset = routes.reset_session();
  EXPECT_EQ(2u, reset.cons.size());
  EXPECT_EQ(0u, routes.num_core_cons());
  EXPECT_FALSE(routes.has_drained_holds());
  EXPECT_TRUE(routes.release_drained_holds(at(121)).empty());
  EXPECT_FALSE(routes.is_active());
  EXPECT_EQ(nullptr, routes.route_of(pg));
  EXPECT_EQ(nullptr, routes.route_of(pg2));
  EXPECT_FALSE(routes.is_held(held));
  // nothing is in flight any more: the Objecter resends it all
  EXPECT_FALSE(routes.is_in_flight(other));
  routes.retire_op(other);

  // until the OSD hints again, ops are not tracked
  op_route_t b;
  EXPECT_EQ(routing_t::what_t::stayed, route(b, 130).what);
  EXPECT_FALSE(routes.is_in_flight(b));
  EXPECT_EQ(nullptr, routes.route_of(pg));

  // the next hint tracks the session's ops again. One sent before the
  // reset is not taken as in flight on the new route
  session_ops = [&](const op_visitor_t& visit) { visit(pg, b); };
  follow(pg, hint(core), 131);
  EXPECT_TRUE(routes.is_active());
  EXPECT_TRUE(routes.is_in_flight(b));
  EXPECT_EQ(1u, route_of(pg).in_flight);
  routes.retire_op(a);
  routes.retire_op(held);
  EXPECT_EQ(1u, route_of(pg).in_flight);
}

// -- activation (the session starts inactive)

namespace {
class InactiveSessionRoutesTest : public SessionRoutesTest {
protected:
  void SetUp() override {}
};
} // namespace

TEST_F(InactiveSessionRoutesTest, OpsAreNotTracked)
{
  EXPECT_FALSE(routes.is_active());
  op_route_t op;
  auto out = route(op, 100);
  EXPECT_EQ(routing_t::what_t::stayed, out.what);
  EXPECT_FALSE(out.core);
  EXPECT_FALSE(out.con);
  EXPECT_FALSE(routes.is_in_flight(op));
  EXPECT_EQ(nullptr, routes.route_of(pg));
  routes.retire_op(op);
  EXPECT_EQ(nullptr, routes.route_of(pg));
  // an op never sent is not awaited when canceled
  op_route_t unsent;
  EXPECT_FALSE(routes.retire_canceled_op(1, unsent, at(101)));
  EXPECT_FALSE(routes.has_unanswered());
  EXPECT_EQ(0u, check(102).routes_pruned);
}

TEST_F(InactiveSessionRoutesTest, CanceledOpIsAwaitedAcrossActivation)
{
  op_route_t a0, a, b, c;
  route(a0, 100);
  route(a, 100);
  // a times out before the OSD's first reply
  auto awaited = routes.retire_canceled_op(1, a, at(101));
  ASSERT_TRUE(awaited);
  EXPECT_EQ(pg, awaited->pgid);
  EXPECT_FALSE(awaited->core);
  EXPECT_TRUE(routes.has_unanswered());
  EXPECT_EQ(nullptr, routes.route_of(pg));

  // a0's reply brings the first hint
  session_ops = [&](const op_visitor_t& visit) { visit(pg, a0); };
  hinted_and_connected();
  EXPECT_EQ(1u, route_of(pg).in_flight);
  EXPECT_EQ(1u, route_of(pg).unanswered);
  routes.retire_op(a0);

  // a may still be on its way, on the main connection
  auto out = route(b, 102);
  EXPECT_EQ(routing_t::what_t::pending, out.what);
  EXPECT_TRUE(out.pending_unanswered);
  EXPECT_FALSE(b.core);
  routes.retire_op(b);
  EXPECT_EQ(std::optional<spg_t>(pg), routes.canceled_op_answered(1));
  EXPECT_EQ(routing_t::what_t::switched, route(c, 103).what);
}

TEST_F(InactiveSessionRoutesTest, CanceledOpIsAwaitedWhileInactive)
{
  cfg.max_hold = ceph::timespan(50s);
  op_route_t a, b;
  route(a, 100);
  routes.retire_canceled_op(1, a, at(100));
  // forgotten after max_hold
  EXPECT_TRUE(check(150).forgotten.empty());
  EXPECT_EQ((forgotten_t{{1, pg}}), check(151).forgotten);
  EXPECT_FALSE(routes.has_unanswered());

  // or once answered
  route(b, 152);
  ASSERT_TRUE(routes.retire_canceled_op(2, b, at(152)));
  EXPECT_EQ(std::optional<spg_t>(pg), routes.canceled_op_answered(2));
  EXPECT_FALSE(routes.has_unanswered());
  EXPECT_FALSE(routes.is_active());
  EXPECT_EQ(nullptr, routes.route_of(pg));
}

TEST_F(InactiveSessionRoutesTest, CanceledOpIsForgottenOnSessionReset)
{
  op_route_t a;
  route(a, 100);
  routes.retire_canceled_op(1, a, at(101));
  EXPECT_EQ((forgotten_t{{1, pg}}), routes.reset_session().forgotten);
  EXPECT_FALSE(routes.has_unanswered());
}

TEST_F(InactiveSessionRoutesTest, HintWhileRoutingOffDoesNotActivate)
{
  cfg.enabled = false;
  follow(pg, hint(core));
  EXPECT_FALSE(routes.is_active());
}

TEST_F(InactiveSessionRoutesTest, OnlyAFollowedHintActivates)
{
  // unusable
  EXPECT_EQ(hint_followed_t::what_t::invalid, follow(pg, hint(core, 0)).what);
  EXPECT_FALSE(routes.is_active());
  // its host blocked
  blocks.block(host, at(90));
  EXPECT_EQ(hint_followed_t::what_t::blocked, follow(pg, hint(core)).what);
  EXPECT_FALSE(routes.is_active());
  blocks.unblock(host);
  // not connected to
  connect_fails = true;
  EXPECT_EQ(hint_followed_t::what_t::connect_failed,
            follow(pg, hint(core)).what);
  EXPECT_FALSE(routes.is_active());
  EXPECT_EQ(nullptr, routes.route_of(pg));

  connect_fails = false;
  EXPECT_EQ(hint_followed_t::what_t::changed, follow(pg, hint(core)).what);
  EXPECT_TRUE(routes.is_active());
}

TEST_F(InactiveSessionRoutesTest, FirstHintCountsTheSessionOps)
{
  op_route_t a, b, c, d;
  route(a, 100);  // not tracked
  route(b, 100, &pg2);
  // (a visited twice is counted once)
  session_ops = [&](const op_visitor_t& visit) {
    visit(pg, a);
    visit(pg2, b);
    visit(pg, a);
  };
  hinted_and_connected();
  EXPECT_TRUE(routes.is_active());
  EXPECT_TRUE(routes.is_in_flight(a));
  EXPECT_FALSE(a.core);
  EXPECT_EQ(1u, route_of(pg).in_flight);
  EXPECT_EQ(1u, route_of(pg2).in_flight);

  // a is on the main connection: pg switches once it drains
  EXPECT_EQ(routing_t::what_t::pending, route(c, 101).what);
  routes.retire_op(a);
  routes.retire_op(c);
  EXPECT_EQ(routing_t::what_t::switched, route(d, 102).what);
}

// -- idle routes

TEST_F(SessionRoutesTest, IdleRoutesArePruned)
{
  cfg.route_idle_ttl = ceph::timespan(60s);
  op_route_t a, b, c;
  route(a, 100);
  routes.retire_op(a);  // pg is idle since 100
  route(b, 100, &pg2);  // pg2 is busy
  EXPECT_EQ(0u, check(160).routes_pruned);
  EXPECT_EQ(1u, check(161).routes_pruned);
  EXPECT_EQ(nullptr, routes.route_of(pg));
  EXPECT_EQ(1u, route_of(pg2).in_flight);

  // still tracked
  route(c, 162);
  EXPECT_EQ(1u, route_of(pg).in_flight);
}

TEST_F(SessionRoutesTest, IdleTimeStartsWhenTheRouteDrains)
{
  cfg.route_idle_ttl = ceph::timespan(60s);
  op_route_t a;
  route(a, 100);
  // in flight for longer than the ttl
  EXPECT_EQ(0u, check(200).routes_pruned);
  routes.retire_op(a);
  EXPECT_EQ(0u, check(260).routes_pruned);
  EXPECT_EQ(1u, check(261).routes_pruned);
}

TEST_F(SessionRoutesTest, HoldingRoutesAreNotPruned)
{
  cfg.grace = ceph::timespan(10s);
  cfg.route_idle_ttl = ceph::timespan(60s);
  op_route_t a, held;
  make_pending(a);
  route(held, 120);
  routes.retire_op(a);  // drained, but the hold is not released yet
  EXPECT_EQ(0u, check(500).routes_pruned);
  EXPECT_EQ(1u, check(500).pgs_holding);
  EXPECT_EQ(1u, routes.release_drained_holds(at(501)).size());
}

TEST_F(SessionRoutesTest, OpOfAForgottenRouteIsNotInFlight)
{
  cfg.route_idle_ttl = ceph::timespan(60s);
  hinted_and_connected();
  op_route_t a, b;
  route(a, 100);
  ASSERT_EQ(std::optional<core_id_t>(core), a.core);
  // a's connection resets: the Objecter is to resend a
  routes.drop_core_con(core_con, drop_why_t::reset, at(101), blocks);
  EXPECT_EQ(1u, check(200).routes_pruned);

  // a new route for pg, before a is resent
  route(b, 201);
  EXPECT_FALSE(routes.is_in_flight(a));
  routes.retire_op(a);
  EXPECT_EQ(1u, route_of(pg).in_flight);
}

// -- stats

TEST_F(SessionRoutesTest, CountSwitching)
{
  cfg.grace = ceph::timespan(10s);
  auto counts = check(100);
  EXPECT_EQ(0u, counts.pgs_holding);
  EXPECT_EQ(0u, counts.pgs_switch_pending);
  op_route_t a, held;
  make_pending(a);
  counts = check(110);
  EXPECT_EQ(0u, counts.pgs_holding);
  EXPECT_EQ(1u, counts.pgs_switch_pending);
  route(held, 120);
  counts = check(120);
  EXPECT_EQ(1u, counts.pgs_holding);
  EXPECT_EQ(0u, counts.pgs_switch_pending);
}

// -- routing turned off

TEST_F(SessionRoutesTest, RoutingOffReturnsPgToMainOnceDrained)
{
  hinted_and_connected();
  op_route_t a, b, c;
  ASSERT_EQ(routing_t::what_t::switched, route(a, 100).what);

  // turned off while a is in flight on the core connection: b follows a
  cfg.enabled = false;
  auto out = route(b, 101);
  EXPECT_EQ(routing_t::what_t::pending, out.what);
  EXPECT_FALSE(out.desired);
  EXPECT_EQ(std::optional<core_id_t>(core), b.core);

  routes.retire_op(a);
  routes.retire_op(b);
  out = route(c, 102);
  EXPECT_EQ(routing_t::what_t::switched, out.what);
  EXPECT_FALSE(c.core);
}

TEST_F(SessionRoutesTest, RoutingOffKeepsIdlePgOnMain)
{
  hinted_and_connected();
  op_route_t a;
  cfg.enabled = false;
  EXPECT_EQ(routing_t::what_t::stayed, route(a, 100).what);
  EXPECT_FALSE(a.core);
  // still hinted: turned on again, the PG switches
  cfg.enabled = true;
  EXPECT_EQ(routing_t::what_t::switched, route(a, 101).what);
}

TEST_F(SessionRoutesTest, RoutingOffHoldsBusyPgAfterTheGrace)
{
  cfg.grace = ceph::timespan(10s);
  hinted_and_connected();
  op_route_t a, b, c, d, e;
  route(a, 100);
  cfg.enabled = false;
  route(b, 101);  // pending since 101
  routes.retire_op(b);
  // a, on the core connection, must drain before the PG returns to main
  EXPECT_EQ(routing_t::what_t::pending, route(c, 105).what);
  routes.retire_op(c);
  // turned on again, the PG is where it should be
  cfg.enabled = true;
  EXPECT_EQ(routing_t::what_t::stayed, route(d, 111).what);
  routes.retire_op(d);
  cfg.enabled = false;
  route(b, 112);  // pending since 112
  EXPECT_EQ(routing_t::what_t::held, route(e, 122).what);
}

TEST_F(SessionRoutesTest, UnusedCoresAreThoseNothingUses)
{
  connected(core);
  connected(other_core);
  // routing on: nothing is unused
  EXPECT_TRUE(check(100).unused.empty());
  cfg.enabled = false;
  EXPECT_EQ((std::vector<core_id_t>{core, other_core}), unused_cores());

  // a PG routed via core
  cfg.enabled = true;
  follow(pg, hint(core));
  op_route_t a;
  route(a, 100);
  ASSERT_EQ(std::optional<core_id_t>(core), a.core);
  cfg.enabled = false;
  EXPECT_EQ((std::vector<core_id_t>{other_core}), unused_cores());

  // ... still awaiting the answer to a canceled op sent there
  routes.retire_canceled_op(1, a, at(101));
  route(a, 102);  // pending: it stays on core
  routes.retire_op(a);
  EXPECT_EQ((std::vector<core_id_t>{other_core}), unused_cores());
  routes.canceled_op_answered(1);
  // the PG's route still names core, until its next op switches it
  EXPECT_EQ((std::vector<core_id_t>{other_core}), unused_cores());
  route(a, 103);
  routes.retire_op(a);
  EXPECT_EQ((std::vector<core_id_t>{core, other_core}), unused_cores());

  // a PG holding its ops to switch to other_core
  cfg.enabled = true;
  cfg.grace = ceph::timespan(1s);
  op_route_t x, y, z;
  route(x, 200, &pg2);
  follow(pg2, hint(other_core, other_port));
  route(y, 201, &pg2);  // pending since 201
  ASSERT_EQ(routing_t::what_t::held, route(z, 203, &pg2).what);
  cfg.enabled = false;
  EXPECT_EQ((std::vector<core_id_t>{core}), unused_cores());
}

// -- keepalive probes

TEST_F(SessionRoutesTest, ProbeAnsweredMeansAlive)
{
  connected(core);
  // nothing answered yet
  EXPECT_EQ(std::vector<ConnectionRef>{core_con},
            check(100, 100, {core}).to_probe);
  auto out = check(105, 105, {core});
  EXPECT_TRUE(out.to_probe.empty());
  EXPECT_TRUE(out.unresponsive.empty());
  // the answer echoes the time of the keepalive
  core_con->set_last_keepalive_ack(real_at(100));
  out = check(106, 106, {core});
  EXPECT_TRUE(out.to_probe.empty());
  EXPECT_TRUE(out.unresponsive.empty());
  // still laggy: probed again
  EXPECT_EQ(std::vector<ConnectionRef>{core_con},
            check(111, 111, {core}).to_probe);
}

TEST_F(SessionRoutesTest, ProbeNotAnsweredInTimeMeansUnresponsive)
{
  connected(core);
  core_con->set_last_keepalive_ack(real_at(50));
  check(100, 100, {core});
  // an older answer does not count
  core_con->set_last_keepalive_ack(real_at(99));
  EXPECT_TRUE(check(109, 109, {core}).unresponsive.empty());
  auto out = check(110, 110, {core});
  ASSERT_EQ(1u, out.unresponsive.size());
  EXPECT_EQ(core, out.unresponsive.front().first);
  EXPECT_EQ(core_con, out.unresponsive.front().second);
  // the verdict is given once
  EXPECT_FALSE(check(111, 111, {core}).to_probe.empty());
}

TEST_F(SessionRoutesTest, ProbesEndWhenNoLongerLaggy)
{
  connected(core);
  connected(other_core);
  EXPECT_EQ(2u, check(100, 100, {core, other_core}).to_probe.size());
  check(101, 101, {other_core});
  // a new probe for core, not a late verdict on the old one
  auto out = check(200, 200, {core, other_core});
  EXPECT_EQ(std::vector<ConnectionRef>{core_con}, out.to_probe);
  ASSERT_EQ(1u, out.unresponsive.size());
  EXPECT_EQ(other_core, out.unresponsive.front().first);
}

TEST_F(SessionRoutesTest, DropsEndProbes)
{
  connected(core);
  connected(other_core);
  check(100, 100, {core, other_core});
  routes.drop_core_con(core_con, drop_why_t::reset, at(101), blocks);
  // other_core's probe goes on
  EXPECT_TRUE(check(105, 105, {other_core}).to_probe.empty());
  routes.reset_session();
  connected(other_core);
  EXPECT_EQ(std::vector<ConnectionRef>{other_con},
            check(106, 106, {other_core}).to_probe);
}

TEST_F(SessionRoutesTest, OnlyConnectedCoresAreProbed)
{
  open_core(core);
  EXPECT_TRUE(check(100, 100, {core}).to_probe.empty());
  // nor cores with no connection
  EXPECT_TRUE(check(100, 100, {other_core}).to_probe.empty());
}

// -- HostBlocks

namespace {
const entity_addr_t host1 = HostBlocks::host_of(addr("v2:10.0.0.1:6810/1234"));
const entity_addr_t host2 = HostBlocks::host_of(addr("v2:10.0.0.2:6810/1234"));
} // namespace

TEST(HostBlocks, HostOfIgnoresPortAndNonce)
{
  EXPECT_EQ(host1, HostBlocks::host_of(addr("v2:10.0.0.1:6820/99")));
  EXPECT_NE(host1, host2);
}

TEST(HostBlocks, NothingBlockedAtFirst)
{
  HostBlocks blocks;
  EXPECT_FALSE(blocks.is_blocked(host1, at(100)));
  EXPECT_FALSE(blocks.unblock(host1));
}

TEST(HostBlocks, BlockLastsItsBackoff)
{
  HostBlocks blocks;
  EXPECT_EQ(HostBlocks::initial_backoff, blocks.block(host1, at(100)));
  EXPECT_TRUE(blocks.is_blocked(host1, at(159)));
  EXPECT_FALSE(blocks.is_blocked(host1, at(160)));
  EXPECT_FALSE(blocks.is_blocked(host2, at(100)));
}

TEST(HostBlocks, RepeatedBlocksBackOff)
{
  HostBlocks blocks;
  auto expected = HostBlocks::initial_backoff;
  int now = 0;
  for (int i = 0; i < 10; ++i) {
    // each time after the previous block expired
    EXPECT_EQ(expected, blocks.block(host1, at(now)));
    now += std::chrono::duration_cast<std::chrono::seconds>(expected).count();
    expected = std::min(expected * 2, HostBlocks::max_backoff);
  }
  EXPECT_EQ(HostBlocks::max_backoff, blocks.block(host1, at(now)));
}

// other connections to the host, opened before it was blocked, timing out
// too: the same failure, not a reason to back off further
TEST(HostBlocks, BlocksWithinABlockDoNotBackOff)
{
  HostBlocks blocks;
  EXPECT_EQ(HostBlocks::initial_backoff, blocks.block(host1, at(100)));
  for (int now = 101; now < 110; ++now) {
    EXPECT_EQ(HostBlocks::initial_backoff - std::chrono::seconds(now - 100),
              blocks.block(host1, at(now)));
  }
  EXPECT_FALSE(blocks.is_blocked(host1, at(160)));
  // failing again once the block ended: backs off further
  EXPECT_EQ(2 * HostBlocks::initial_backoff, blocks.block(host1, at(170)));
}

TEST(HostBlocks, UnblockResetsTheBackoff)
{
  HostBlocks blocks;
  blocks.block(host1, at(100));
  blocks.block(host1, at(200));
  blocks.block(host2, at(100));
  EXPECT_TRUE(blocks.unblock(host1));
  EXPECT_FALSE(blocks.is_blocked(host1, at(201)));
  EXPECT_TRUE(blocks.is_blocked(host2, at(101)));
  EXPECT_EQ(HostBlocks::initial_backoff, blocks.block(host1, at(300)));
}
