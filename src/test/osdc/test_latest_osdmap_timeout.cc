// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <chrono>
#include <memory>
#include <thread>

#include <boost/asio/io_context.hpp>
#include <boost/intrusive_ptr.hpp>
#include <gtest/gtest.h>

#include "common/ceph_context.h"
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
  MonClient monc{cct.get(), service};
  Objecter objecter{cct.get(), nullptr, &monc, service};
  struct Result {
    unsigned calls = 0;
    bs::error_code ec;
  };

  void SetUp() override {
    objecter.init();
    monc.sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
    std::scoped_lock l(monc.monc_lock);
    monc.timer.init();
  }
  void TearDown() override {
    if (objecter.initialized) {
      objecter.shutdown();
    }
    monc.shutdown();
    arrive(); // release legacy unbounded waiters even after a failed assertion
    drain();
  }
  auto wait(std::optional<ceph::mono_time> deadline = std::nullopt) {
    auto result = std::make_shared<Result>();
    objecter.wait_for_latest_osdmap(deadline, [result](bs::error_code ec) {
      ++result->calls;
      result->ec = ec;
    });
    return result;
  }
  ceph_tid_t last_request() {
    std::scoped_lock l(monc.monc_lock);
    return monc.version_req_id; // valid even if the request already timed out
  }
  void reply(ceph_tid_t tid, version_t epoch = 1, bs::error_code ec = {}) {
    std::scoped_lock l(monc.monc_lock);
    monc._finish_version_request(tid, ec, epoch, 0);
  }
  void expect_no_requests() {
    std::scoped_lock l(monc.monc_lock);
    EXPECT_TRUE(monc.version_requests.empty());
  }
  size_t queued_messages() {
    std::scoped_lock l(monc.monc_lock);
    return monc.waiting_for_session.size();
  }
  size_t waiters() {
    std::shared_lock l(objecter.rwlock);
    size_t count = 0;
    for (const auto& [epoch, pending] : objecter.waiting_for_map) {
      count += pending.size();
    }
    return count;
  }
  uint64_t last_waiter() {
    std::shared_lock l(objecter.rwlock);
    return objecter.last_map_waiter_id;
  }
  void expire_waiter(uint64_t id) {
    objecter._timeout_map_waiter(1, id);
  }
  void arrive() {
    std::unique_lock l(objecter.rwlock);
    objecter._finish_map_waiters(1);
  }
  void drain() {
    service.restart();
    service.poll();
  }
  void run(const std::shared_ptr<Result>& result) {
    service.restart();
    const auto until = std::chrono::steady_clock::now() + 2s;
    while (!result->calls && std::chrono::steady_clock::now() < until) {
      service.run_one_until(until);
    }
  }
  void expect_result(const std::shared_ptr<Result>& result,
                     bs::error_condition expected = {}) {
    EXPECT_EQ(1u, result->calls);
    EXPECT_EQ(expected, result->ec.default_error_condition());
  }
};

TEST_F(LatestOsdmapTimeoutTest, MonTimeoutRemovesUnsentRequest)
{
  const auto queued = queued_messages();
  auto result = wait(ceph::mono_clock::now() + 20ms);
  auto tid = last_request();
  run(result);
  reply(tid); // a late reply must not complete the operation again
  drain();
  expect_result(result, make_error_condition(bs::errc::timed_out));
  expect_no_requests();
  EXPECT_EQ(queued, queued_messages());
}

TEST_F(LatestOsdmapTimeoutTest, ExpiredDeadlineDoesNotSend)
{
  const auto queued = queued_messages();
  auto result = wait(ceph::mono_clock::now() - 1ms);
  drain();
  expect_result(result, make_error_condition(bs::errc::timed_out));
  expect_no_requests();
  EXPECT_EQ(queued, queued_messages());
}

TEST_F(LatestOsdmapTimeoutTest, ResetDoesNotExtendDeadline)
{
  auto deadline = ceph::mono_clock::now() + 20ms;
  auto result = wait(deadline);
  reply(last_request(), 0, make_error_code(monc_errc::session_reset));
  std::this_thread::sleep_until(deadline + 1ms);
  drain(); // run the queued retry only after the original deadline
  expect_result(result, make_error_condition(bs::errc::timed_out));
  expect_no_requests();
}

TEST_F(LatestOsdmapTimeoutTest, HandoffDoesNotExtendDeadline)
{
  auto deadline = ceph::mono_clock::now() + 20ms;
  auto result = wait(deadline);
  reply(last_request());
  std::this_thread::sleep_until(deadline + 1ms);
  drain(); // a queued success must not give map catch-up a fresh timeout
  expect_result(result, make_error_condition(bs::errc::timed_out));
  EXPECT_EQ(0u, waiters());
}

TEST_F(LatestOsdmapTimeoutTest, MapTimeoutRemovesOnlyItsWaiter)
{
  auto unlimited = wait();
  reply(last_request());
  drain();
  auto result = wait(ceph::mono_clock::now() + 20ms);
  reply(last_request());
  drain();
  run(result);
  expect_result(result, make_error_condition(bs::errc::timed_out));
  EXPECT_EQ(0u, unlimited->calls);
  EXPECT_EQ(1u, waiters());
  arrive();
  drain();
  expect_result(unlimited);
  expect_result(result, make_error_condition(bs::errc::timed_out));
}

TEST_F(LatestOsdmapTimeoutTest, MapArrivalWinsAgainstRunningTimeout)
{
  auto result = wait(ceph::mono_clock::now() + 1h);
  reply(last_request());
  drain();
  auto id = last_waiter();
  arrive();
  // Model a timer callback that was already running but acquired rwlock later.
  expire_waiter(id);
  drain();
  expect_result(result);
  EXPECT_EQ(0u, waiters());
}

TEST_F(LatestOsdmapTimeoutTest, NoDeadlineWaitsForBothPhases)
{
  auto result = wait();
  service.run_for(30ms);
  EXPECT_EQ(0u, result->calls);
  reply(last_request());
  drain();
  service.run_for(30ms);
  EXPECT_EQ(0u, result->calls);
  arrive();
  drain();
  expect_result(result);
}

TEST_F(LatestOsdmapTimeoutTest, ShutdownCancelsOnlyBoundedMapWaiters)
{
  auto unlimited = wait();
  reply(last_request());
  auto bounded = wait(ceph::mono_clock::now() + 1h);
  reply(last_request());
  drain();
  objecter.shutdown();
  drain();
  expect_result(bounded, make_error_condition(bs::errc::operation_canceled));
  EXPECT_EQ(0u, unlimited->calls);
  EXPECT_EQ(1u, waiters());
  arrive();
  drain();
  expect_result(unlimited);
}

TEST_F(LatestOsdmapTimeoutTest, QueuedReplyCannotRecreateWaiterAfterShutdown)
{
  auto result = wait(ceph::mono_clock::now() + 1h);
  reply(last_request());
  objecter.shutdown();
  drain();
  expect_result(result, make_error_condition(bs::errc::operation_canceled));
  EXPECT_EQ(0u, waiters());
}

TEST_F(LatestOsdmapTimeoutTest, QueuedResetCannotRetryAfterMonShutdown)
{
  auto result = wait(ceph::mono_clock::now() + 1h);
  reply(last_request(), 0, make_error_code(monc_errc::session_reset));
  objecter.shutdown();
  monc.shutdown();
  drain();
  EXPECT_EQ(1u, result->calls);
  EXPECT_EQ(make_error_code(monc_errc::shutting_down).default_error_condition(),
            result->ec.default_error_condition());
  expect_no_requests();
  EXPECT_EQ(0u, queued_messages());
}
