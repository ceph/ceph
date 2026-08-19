// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <atomic>
#include <chrono>
#include <future>
#include <functional>
#include <thread>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/intrusive_ptr.hpp>
#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "include/scope_guard.h"
#include "mon/MonClient.h"
#include "osdc/Objecter.h"

namespace asio = boost::asio;
namespace bs = boost::system;
using namespace std::chrono_literals;

class LatestOsdmapTimeoutTest : public testing::Test {
 protected:
  boost::intrusive_ptr<CephContext> cct{
    new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};
  asio::io_context service;
  asio::executor_work_guard<asio::io_context::executor_type> work{
    service.get_executor()};
  std::thread service_thread;

  void SetUp() override {
    service_thread = std::thread([this] { service.run(); });
  }
  void TearDown() override {
    work.reset();
    service.stop();
    if (service_thread.joinable()) {
      service_thread.join();
    }
  }
};

class MonClientLatestOsdmapTimeoutTest : public LatestOsdmapTimeoutTest {
 protected:
  static void start_timer(MonClient& monc) {
    std::scoped_lock l(monc.monc_lock);
    monc.timer.init();
  }

  static void stop_timer(MonClient& monc) {
    std::scoped_lock l(monc.monc_lock);
    monc.timer.shutdown();
  }

  static ceph_tid_t first_request(MonClient& monc) {
    std::scoped_lock l(monc.monc_lock);
    ceph_assert(!monc.version_requests.empty());
    return monc.version_requests.begin()->first;
  }

  static size_t request_count(MonClient& monc) {
    std::scoped_lock l(monc.monc_lock);
    return monc.version_requests.size();
  }

  static void finish_request(
    MonClient& monc, ceph_tid_t tid, bs::error_code ec,
    version_t newest = 0, version_t oldest = 0) {
    std::scoped_lock l(monc.monc_lock);
    monc._finish_version_request(tid, ec, newest, oldest);
  }
};

TEST_F(MonClientLatestOsdmapTimeoutTest, VersionRequestTimesOut)
{
  MonClient monc(cct.get(), service);
  Objecter objecter(cct.get(), nullptr, &monc, service);
  start_timer(monc);
  auto stop = make_scope_guard([&] { stop_timer(monc); });

  std::promise<bs::error_code> result;
  auto future = result.get_future();
  objecter.wait_for_latest_osdmap(
    ceph::mono_clock::now() + 40ms,
    [&result](bs::error_code ec) { result.set_value(ec); });

  ASSERT_EQ(std::future_status::ready, future.wait_for(5s));
  EXPECT_EQ(bs::errc::timed_out, future.get().default_error_condition());
  EXPECT_EQ(0u, request_count(monc));
}

TEST_F(MonClientLatestOsdmapTimeoutTest, ZeroTimeoutRemainsUnlimited)
{
  MonClient monc(cct.get(), service);
  Objecter objecter(cct.get(), nullptr, &monc, service);
  start_timer(monc);
  auto stop = make_scope_guard([&] { stop_timer(monc); });

  std::promise<bs::error_code> result;
  auto future = result.get_future();
  objecter.wait_for_latest_osdmap(
    [&result](bs::error_code ec) { result.set_value(ec); });

  EXPECT_EQ(std::future_status::timeout, future.wait_for(80ms));
  auto tid = first_request(monc);
  finish_request(monc, tid, {}, 0, 0);

  ASSERT_EQ(std::future_status::ready, future.wait_for(5s));
  EXPECT_FALSE(future.get());
  EXPECT_EQ(0u, request_count(monc));
}

TEST_F(MonClientLatestOsdmapTimeoutTest, SessionResetPreservesDeadline)
{
  work.reset();
  service.stop();
  service_thread.join();
  service.restart();

  MonClient monc(cct.get(), service);
  Objecter objecter(cct.get(), nullptr, &monc, service);
  start_timer(monc);
  auto stop = make_scope_guard([&] { stop_timer(monc); });

  std::promise<bs::error_code> result;
  auto future = result.get_future();
  const auto deadline = ceph::mono_clock::now() + 80ms;
  objecter.wait_for_latest_osdmap(
    deadline, [&result](bs::error_code ec) { result.set_value(ec); });

  finish_request(
    monc, first_request(monc),
    make_error_code(monc_errc::session_reset));
  std::this_thread::sleep_for(120ms);

  service.run();
  ASSERT_EQ(std::future_status::ready, future.wait_for(0s));
  EXPECT_EQ(bs::errc::timed_out, future.get().default_error_condition());
  EXPECT_EQ(0u, request_count(monc));
}

TEST_F(MonClientLatestOsdmapTimeoutTest, ReplyTimeoutRaceCompletesOnce)
{
  MonClient monc(cct.get(), service);
  start_timer(monc);
  auto stop = make_scope_guard([&] { stop_timer(monc); });

  for (unsigned i = 0; i < 16; ++i) {
    std::promise<void> done;
    auto future = done.get_future();
    std::atomic<unsigned> completions{0};
    monc.get_version(
      "osdmap", ceph::mono_clock::now() + 5ms,
      [&done, &completions](bs::error_code, version_t, version_t) {
        if (completions.fetch_add(1) == 0) {
          done.set_value();
        }
      });
    auto tid = first_request(monc);

    std::thread reply([&monc, tid] {
      std::this_thread::sleep_for(5ms);
      finish_request(monc, tid, {}, 1, 1);
    });

    ASSERT_EQ(std::future_status::ready, future.wait_for(5s));
    reply.join();
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(1u, completions.load());
    EXPECT_EQ(0u, request_count(monc));
  }
}

class ObjecterLatestOsdmapTimeoutTest : public LatestOsdmapTimeoutTest {
 protected:
  template <typename Handler>
  static void wait_for_version(
    Objecter& objecter, std::optional<ceph::mono_time> deadline,
    Handler&& handler) {
    std::unique_lock l(objecter.rwlock);
    objecter._get_latest_version(
      0, 1, Objecter::OpCompletion(std::forward<Handler>(handler)),
      deadline, std::move(l));
  }

  static void finish_map_waiters(Objecter& objecter, epoch_t epoch) {
    std::unique_lock l(objecter.rwlock);
    objecter._finish_map_waiters(epoch);
  }

  template <typename Callable>
  static void schedule_timer(Objecter& objecter, Callable&& callback) {
    objecter.timer.add_event(0ms, std::forward<Callable>(callback));
  }

  static size_t waiter_count(Objecter& objecter) {
    std::shared_lock l(objecter.rwlock);
    size_t count = 0;
    for (const auto& [epoch, waiters] : objecter.waiting_for_map) {
      count += waiters.size();
    }
    return count;
  }
};

TEST_F(ObjecterLatestOsdmapTimeoutTest, MapWaiterTimesOutAndIsRemoved)
{
  MonClient monc(cct.get(), service);
  ASSERT_TRUE(monc.sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME));
  Objecter objecter(cct.get(), nullptr, &monc, service);

  std::promise<bs::error_code> result;
  auto future = result.get_future();
  wait_for_version(
    objecter, ceph::mono_clock::now() + 40ms,
    [&result](bs::error_code ec) { result.set_value(ec); });

  ASSERT_EQ(std::future_status::ready, future.wait_for(5s));
  EXPECT_EQ(bs::errc::timed_out, future.get().default_error_condition());
  EXPECT_EQ(0u, waiter_count(objecter));
}

TEST_F(ObjecterLatestOsdmapTimeoutTest, ZeroTimeoutMapWaiterRemainsUnlimited)
{
  MonClient monc(cct.get(), service);
  ASSERT_TRUE(monc.sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME));
  Objecter objecter(cct.get(), nullptr, &monc, service);

  std::promise<bs::error_code> result;
  auto future = result.get_future();
  wait_for_version(
    objecter, std::nullopt,
    [&result](bs::error_code ec) { result.set_value(ec); });

  EXPECT_EQ(std::future_status::timeout, future.wait_for(80ms));
  EXPECT_EQ(1u, waiter_count(objecter));
  finish_map_waiters(objecter, 1);

  ASSERT_EQ(std::future_status::ready, future.wait_for(5s));
  EXPECT_FALSE(future.get());
  EXPECT_EQ(0u, waiter_count(objecter));
}

TEST_F(ObjecterLatestOsdmapTimeoutTest, MapArrivalTimeoutRaceCompletesOnce)
{
  MonClient monc(cct.get(), service);
  ASSERT_TRUE(monc.sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME));
  Objecter objecter(cct.get(), nullptr, &monc, service);

  for (unsigned i = 0; i < 16; ++i) {
    std::promise<void> done;
    auto future = done.get_future();
    std::atomic<unsigned> completions{0};
    wait_for_version(
      objecter, ceph::mono_clock::now() + 5ms,
      [&done, &completions](bs::error_code) {
        if (completions.fetch_add(1) == 0) {
          done.set_value();
        }
      });

    std::thread map_arrival([&objecter] {
      std::this_thread::sleep_for(5ms);
      finish_map_waiters(objecter, 1);
    });

    ASSERT_EQ(std::future_status::ready, future.wait_for(5s));
    map_arrival.join();
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(1u, completions.load());
    EXPECT_EQ(0u, waiter_count(objecter));
  }
}

TEST_F(ObjecterLatestOsdmapTimeoutTest, ShutdownCancelsMapWaiters)
{
  MonClient monc(cct.get(), service);
  ASSERT_TRUE(monc.sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME));
  Objecter objecter(cct.get(), nullptr, &monc, service);
  objecter.init();

  std::promise<bs::error_code> result;
  auto future = result.get_future();
  wait_for_version(
    objecter, ceph::mono_clock::now() + 5s,
    [&result](bs::error_code ec) { result.set_value(ec); });
  EXPECT_EQ(1u, waiter_count(objecter));

  objecter.shutdown();

  ASSERT_EQ(std::future_status::ready, future.wait_for(5s));
  EXPECT_EQ(
    bs::errc::operation_canceled, future.get().default_error_condition());
  EXPECT_EQ(0u, waiter_count(objecter));
}

TEST_F(ObjecterLatestOsdmapTimeoutTest, ShutdownJoinsRunningTimerCallback)
{
  MonClient monc(cct.get(), service);
  Objecter objecter(cct.get(), nullptr, &monc, service);
  objecter.init();

  std::promise<void> started;
  auto started_future = started.get_future();
  std::promise<void> release;
  auto release_future = release.get_future().share();
  schedule_timer(objecter, [&started, release_future] {
    started.set_value();
    release_future.wait();
  });

  if (started_future.wait_for(5s) != std::future_status::ready) {
    release.set_value();
    objecter.shutdown();
    FAIL() << "timer callback did not start";
  }

  auto shutdown = std::async(
    std::launch::async, [&objecter] { objecter.shutdown(); });
  EXPECT_EQ(std::future_status::timeout, shutdown.wait_for(40ms));

  release.set_value();
  ASSERT_EQ(std::future_status::ready, shutdown.wait_for(5s));
  shutdown.get();
}

// Instantiates the wait_for_map(epoch, token) template so its
// CB_Objecter_GetVersion construction stays compilable; Client.cc and the
// MDS call it in builds this test's slim configuration does not cover.
TEST_F(ObjecterLatestOsdmapTimeoutTest, WaitForMapWithCurrentEpoch)
{
  MonClient monc(cct.get(), service);
  Objecter objecter(cct.get(), nullptr, &monc, service);

  std::promise<bs::error_code> result;
  auto future = result.get_future();
  objecter.wait_for_map(
    0, [&result](bs::error_code ec) { result.set_value(ec); });

  ASSERT_EQ(std::future_status::ready, future.wait_for(5s));
  EXPECT_FALSE(future.get());
}
