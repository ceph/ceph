// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <future>
#include <thread>

#include "gtest/gtest.h"
#include "global/global_context.h"
#include "mon/DispatchWatchdog.h"

class DispatchWatchdogTest : public ::testing::Test {
protected:
  ceph::HeartbeatMap heartbeat_map{g_ceph_context};
  DispatchWatchdog watchdog{heartbeat_map, "test dispatch"};
  using Token = DispatchWatchdog::Token;
  using Clock = ceph::coarse_mono_clock;

  static ceph::heartbeat_handle_d* worker(const Token& token) {
    return token->heartbeat;
  }

  static void expire(const Token& token) {
    auto past = Clock::now() - std::chrono::seconds(1);
    worker(token)->timeout.store(past);
    worker(token)->suicide_timeout.store(past);
  }
};

TEST_F(DispatchWatchdogTest, OverlappingThreadsHaveIndependentDeadlines) {
  auto a = watchdog.start(120);
  auto* a_worker = worker(a);
  for (int i = 0; i < 20; ++i) {
    std::promise<Token> started;
    std::promise<void> release;
    auto released = release.get_future();
    std::thread b_thread([&] {
      auto b = watchdog.start(120);
      started.set_value(b);
      released.wait();
      watchdog.finish(b);
    });
    auto b = started.get_future().get();
    auto* b_worker = worker(b);
    auto b_deadline = b_worker->suicide_timeout.load();
    EXPECT_NE(a_worker, b_worker);
    EXPECT_NE(Clock::zero(), b_deadline);

    watchdog.finish(a);
    EXPECT_EQ(Clock::zero(), a_worker->suicide_timeout.load());
    EXPECT_EQ(b_deadline, b_worker->suicide_timeout.load());

    // B is still active when the next A starts: the monitor never goes idle.
    a = watchdog.start(120);
    EXPECT_EQ(a_worker, worker(a));
    EXPECT_NE(Clock::zero(), a_worker->suicide_timeout.load());
    auto a_deadline = a_worker->suicide_timeout.load();
    release.set_value();
    b_thread.join();
    EXPECT_EQ(Clock::zero(), b_worker->suicide_timeout.load());
    EXPECT_EQ(a_deadline, a_worker->suicide_timeout.load());
  }
  watchdog.finish(a);
  EXPECT_TRUE(heartbeat_map.is_healthy());
}

TEST_F(DispatchWatchdogTest, ProgressOnAnotherThreadCannotHideExpiredWorker) {
  auto a = watchdog.start(120);
  expire(a);
  auto expired = worker(a)->suicide_timeout.load();
  std::thread b([&] {
    for (int i = 0; i < 20; ++i) {
      auto token = watchdog.start(120);
      watchdog.finish(token);
    }
  });
  b.join();
  EXPECT_EQ(expired, worker(a)->suicide_timeout.load());
  EXPECT_DEATH(heartbeat_map.is_healthy(), ".*");
  // Disarm the parent's expired worker without invoking finish's timeout check.
  watchdog.shutdown();
  watchdog.finish(a);
}

TEST_F(DispatchWatchdogTest, NestedEntryDoesNotExtendOrClearOuterDeadline) {
  auto outer = watchdog.start(120);
  auto deadline = worker(outer)->suicide_timeout.load();
  // A configuration change must not disable an already active outer call.
  auto inner = watchdog.start(0);
  EXPECT_EQ(outer, inner);
  EXPECT_EQ(deadline, worker(inner)->suicide_timeout.load());
  watchdog.finish(inner);
  EXPECT_EQ(deadline, worker(outer)->suicide_timeout.load());
  watchdog.finish(outer);
  EXPECT_EQ(Clock::zero(), worker(outer)->suicide_timeout.load());
}

TEST_F(DispatchWatchdogTest, FinishStillDetectsExpiredCall) {
  auto token = watchdog.start(120);
  expire(token);
  EXPECT_DEATH(watchdog.finish(token), ".*");
  watchdog.shutdown();
}

TEST_F(DispatchWatchdogTest, ConfigurationAppliesAtNextOutermostEntry) {
  EXPECT_FALSE(watchdog.start(0));
  auto token = watchdog.start(120);
  watchdog.finish(token);
  auto next = watchdog.start(240);
  EXPECT_EQ(token, next);
  EXPECT_EQ(ceph::make_timespan(240), worker(next)->suicide_grace);
  watchdog.finish(next);
  EXPECT_FALSE(watchdog.start(0));
  EXPECT_EQ(nullptr, worker(token));
  EXPECT_TRUE(heartbeat_map.is_healthy());
  EXPECT_EQ(0, heartbeat_map.get_total_workers());
}

TEST_F(DispatchWatchdogTest, ShutdownInvalidatesActiveGuardsAndPreventsRearming) {
  auto outer = watchdog.start(120);
  auto inner = watchdog.start(120);
  std::promise<Token> started;
  std::promise<void> release;
  auto released = release.get_future();
  std::thread other([&] {
    auto token = watchdog.start(120);
    started.set_value(token);
    released.wait();
    watchdog.finish(token);
    EXPECT_FALSE(watchdog.start(120));
  });
  auto other_token = started.get_future().get();
  watchdog.shutdown();
  EXPECT_EQ(nullptr, worker(outer));
  EXPECT_EQ(nullptr, worker(other_token));
  release.set_value();
  other.join();
  watchdog.finish(inner);
  watchdog.finish(outer);
  watchdog.shutdown();
  EXPECT_FALSE(watchdog.start(120));
  EXPECT_TRUE(heartbeat_map.is_healthy());
  EXPECT_EQ(0, heartbeat_map.get_total_workers());
}
