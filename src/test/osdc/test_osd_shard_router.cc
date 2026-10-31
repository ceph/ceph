// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "osdc/OSDShardRouter.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"

namespace {

struct FakeClock {
  using duration = std::chrono::nanoseconds;
  using time_point = std::chrono::time_point<FakeClock, duration>;
};

using Conn = std::shared_ptr<int>;   // the int is the port it dialled
using Router = OSDShardRouter<int, Conn, FakeClock>;
using namespace std::chrono_literals;

struct RouterTest : ::testing::Test {
  Router r;
  Conn main = std::make_shared<int>(6800);
  std::vector<uint16_t> dialled;
  FakeClock::time_point now{};
  Router::connect_fn_t connect = [this](uint16_t port) {
    dialled.push_back(port);
    return std::make_shared<int>(port);
  };
  /// a current reply on `via` naming `core` and its port 6800 + core
  void reply(int pg, uint16_t core, const Conn& via) {
    r.learn(pg, core, 6800 + core, via, now);
  }
};

TEST_F(RouterTest, UnknownPGUsesMainConnection)
{
  EXPECT_EQ(r.route(1, main, connect, now), main);
  EXPECT_TRUE(dialled.empty());
  EXPECT_FALSE(r.has_conns());
}

TEST_F(RouterTest, LearnedPGUsesItsReactorConnection)
{
  reply(1, 3, main);
  auto c = r.route(1, main, connect, now);
  ASSERT_NE(c, main);
  EXPECT_EQ(*c, 6803);
  EXPECT_EQ(dialled, std::vector<uint16_t>{6803});
  EXPECT_TRUE(r.owns(c));
  EXPECT_EQ(r.index_of(c.get()), 3);
  EXPECT_FALSE(r.owns(main));
  EXPECT_EQ(r.index_of(main.get()), -1);
  EXPECT_TRUE(r.has_conns());
}

TEST_F(RouterTest, ConnectionIsSharedByPGsOfOneReactor)
{
  reply(1, 2, main);
  reply(7, 2, main);
  auto a = r.route(1, main, connect, now);
  auto b = r.route(7, main, connect, now);
  EXPECT_EQ(a, b);
  EXPECT_EQ(dialled.size(), 1u);
}

TEST_F(RouterTest, PortZeroNamesNothing)
{
  r.learn(1, 2, 0, main, now);
  EXPECT_EQ(r.route(1, main, connect, now), main);
  EXPECT_FALSE(r.core_of(1));
}

TEST_F(RouterTest, CoreBeyondTheCapIsNotRouted)
{
  r.learn(1, Router::max_cores, 7000, main, now);
  EXPECT_FALSE(r.core_of(1));
  EXPECT_EQ(r.route(1, main, connect, now), main);
}

TEST_F(RouterTest, BusyPGStaysOnItsConnectionUntilIdle)
{
  // first op goes out before the PG's reactor is known
  auto first = r.route(1, main, connect, now);
  EXPECT_EQ(first, main);
  reply(1, 4, main);
  // still in flight on main: the next op must follow it (ordering)
  auto second = r.route(1, main, connect, now);
  EXPECT_EQ(second, main);
  r.unroute(1, main);
  r.unroute(1, main);
  // idle now: moves to its reactor
  auto third = r.route(1, main, connect, now);
  EXPECT_NE(third, main);
  EXPECT_EQ(*third, 6804);
}

TEST_F(RouterTest, DisabledKeepsBusyPGButSendsIdleOnesToMain)
{
  reply(1, 1, main);
  auto c = r.route(1, main, connect, now);
  ASSERT_NE(c, main);
  // in flight: stays on its connection even when routing is turned off
  EXPECT_EQ(r.route(1, main, connect, now, false), c);
  r.unroute(1, c);
  r.unroute(1, c);
  EXPECT_EQ(r.route(1, main, connect, now, false), main);
}

TEST_F(RouterTest, RetireReturnsPinnedPGsAndUnprovenPauses)
{
  reply(1, 1, main);
  reply(2, 1, main);
  reply(3, 5, main);
  auto c1 = r.route(1, main, connect, now);
  r.route(2, main, connect, now);
  auto c5 = r.route(3, main, connect, now);
  auto ret = r.retire(r.index_of(c1.get()), now);
  std::sort(ret.pgs.begin(), ret.pgs.end());
  EXPECT_EQ(ret.pgs, (std::vector<int>{1, 2}));
  EXPECT_FALSE(ret.was_proven);
  EXPECT_FALSE(r.owns(c1));
  EXPECT_EQ(r.index_of(c1.get()), -1);
  EXPECT_TRUE(r.owns(c5));
  // c1 never delivered a reply: routing is paused, new picks use main
  EXPECT_TRUE(r.is_paused(now));
  EXPECT_EQ(r.route(1, main, connect, now), main);
  // PG 3 is still pinned to its reactor connection while in flight
  EXPECT_EQ(r.route(3, main, connect, now), c5);
}

TEST_F(RouterTest, BackoffDoublesAndAReplyResetsIt)
{
  const auto b0 = Router::initial_backoff;
  reply(1, 1, main);
  auto c = r.route(1, main, connect, now);
  r.retire(r.index_of(c.get()), now);
  EXPECT_TRUE(r.is_paused(now + b0 - 1s));
  EXPECT_FALSE(r.is_paused(now + b0));

  now += b0;
  c = r.route(1, main, connect, now);
  ASSERT_NE(c, main);
  r.retire(r.index_of(c.get()), now);
  EXPECT_TRUE(r.is_paused(now + 2 * b0 - 1s));   // second failure: doubled
  EXPECT_FALSE(r.is_paused(now + 2 * b0));

  now += 2 * b0;
  c = r.route(1, main, connect, now);
  ASSERT_NE(c, main);
  reply(1, 1, c);   // a reply arrived on it: proven
  r.unroute(1, c);
  auto ret = r.retire(r.index_of(c.get()), now);
  EXPECT_TRUE(ret.was_proven);
  EXPECT_FALSE(r.is_paused(now));   // a proven connection does not pause
}

TEST_F(RouterTest, ProvenReplyResetsBackoff)
{
  const auto b0 = Router::initial_backoff;
  reply(1, 1, main);
  for (int i = 0; i < 3; ++i) {
    auto c = r.route(1, main, connect, now);
    r.retire(r.index_of(c.get()), now);
    now += 2h;
  }
  auto c = r.route(1, main, connect, now);
  reply(1, 1, c);
  r.unroute(1, c);
  r.retire(r.index_of(c.get()), now);   // proven: no pause
  reply(2, 1, main);
  c = r.route(2, main, connect, now);
  r.retire(r.index_of(c.get()), now);   // unproven again: back to the start
  EXPECT_TRUE(r.is_paused(now + b0 - 1s));
  EXPECT_FALSE(r.is_paused(now + b0));
}

TEST_F(RouterTest, BackoffIsCapped)
{
  reply(1, 1, main);
  for (int i = 0; i < 20; ++i) {
    auto c = r.route(1, main, connect, now);
    r.retire(r.index_of(c.get()), now);
    now += 2 * Router::max_backoff;
  }
  auto c = r.route(1, main, connect, now);
  r.retire(r.index_of(c.get()), now);
  EXPECT_TRUE(r.is_paused(now + Router::max_backoff - 1s));
  EXPECT_FALSE(r.is_paused(now + Router::max_backoff));
}

TEST_F(RouterTest, ExternalPauseStopsRouting)
{
  reply(1, 1, main);
  r.pause(now);
  EXPECT_TRUE(r.is_paused(now));
  EXPECT_EQ(r.route(1, main, connect, now), main);
  EXPECT_TRUE(dialled.empty());
}

TEST_F(RouterTest, PauseUntilKeepsTheLaterDeadline)
{
  reply(1, 1, main);
  r.pause_until(now + 10min);
  r.pause_until(now + 1min);   // an earlier deadline does not shorten it
  EXPECT_TRUE(r.is_paused(now + 10min - 1s));
  EXPECT_FALSE(r.is_paused(now + 10min));
  // and it does not count as a failure: the backoff is untouched
  now += 10min;
  auto c = r.route(1, main, connect, now);
  r.retire(r.index_of(c.get()), now);
  EXPECT_FALSE(r.is_paused(now + Router::initial_backoff));
}

TEST_F(RouterTest, LearnReportsTheFirstReplyOnAConnection)
{
  reply(1, 1, main);
  EXPECT_FALSE(r.learn(1, 1, 6801, main, now));   // main is never "proven"
  auto c = r.route(1, main, connect, now);
  EXPECT_EQ(r.conn_at(1), c);
  EXPECT_TRUE(r.learn(1, 1, 6801, c, now));
  EXPECT_FALSE(r.learn(1, 1, 6801, c, now));      // only the first time
}

TEST_F(RouterTest, ResetForgetsEverything)
{
  reply(1, 1, main);
  reply(2, 2, main);
  auto c1 = r.route(1, main, connect, now);
  r.route(2, main, connect, now);
  auto old = r.reset();
  EXPECT_EQ(old.size(), 2u);
  EXPECT_FALSE(r.has_conns());
  EXPECT_FALSE(r.owns(c1));
  EXPECT_FALSE(r.core_of(1));
  EXPECT_EQ(r.route(1, main, connect, now), main);
}

TEST_F(RouterTest, FailedConnectFallsBackToMain)
{
  reply(1, 1, main);
  Router::connect_fn_t refuse = [](uint16_t) { return Conn{}; };
  EXPECT_EQ(r.route(1, main, refuse, now), main);
  EXPECT_FALSE(r.has_conns());
}

TEST_F(RouterTest, ReplyNamingAnotherReactorMovesIdlePG)
{
  reply(1, 1, main);
  auto c1 = r.route(1, main, connect, now);
  // the reply on c1 says the PG runs elsewhere (e.g. after a split)
  reply(1, 6, c1);
  r.unroute(1, c1);
  auto c6 = r.route(1, main, connect, now);
  EXPECT_EQ(*c6, 6806);
  EXPECT_NE(c1, c6);
}

TEST_F(RouterTest, RetireUnknownIndexIsHarmless)
{
  EXPECT_TRUE(r.retire(-1, now).pgs.empty());
  EXPECT_TRUE(r.retire(42, now).pgs.empty());
  EXPECT_FALSE(r.is_paused(now));
}

TEST_F(RouterTest, StaleFindsBusySilentConnectionsOnly)
{
  reply(1, 1, main);
  reply(2, 2, main);
  auto c1 = r.route(1, main, connect, now);   // busy, will stay silent
  auto c2 = r.route(2, main, connect, now);   // busy, replies
  now += 30s;
  reply(2, 2, c2);
  EXPECT_TRUE(r.stale(now - 60s).empty());    // nothing silent for 60s yet
  now += 40s;
  EXPECT_EQ(r.stale(now - 60s), std::vector<int>{1});
  // an idle connection is not stale, however long it has been silent
  r.unroute(1, c1);
  now += 1h;
  r.unroute(2, c2);
  EXPECT_TRUE(r.stale(now - 60s).empty());
}

TEST_F(RouterTest, ReplyRefreshesStaleness)
{
  reply(1, 1, main);
  auto c = r.route(1, main, connect, now);
  now += 50s;
  reply(1, 1, c);
  now += 50s;
  EXPECT_TRUE(r.stale(now - 60s).empty());
  now += 20s;
  EXPECT_EQ(r.stale(now - 60s), std::vector<int>{1});
}

} // anonymous namespace
