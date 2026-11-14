// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "adopt/adoption_queue.h"

#include <future>
#include <thread>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/strand.hpp>
#include <gtest/gtest.h>

using namespace std::chrono_literals;
using rgw::sal::nsfs::AdoptionQueue;

namespace {

/* A gate an adoption blocks on until the test opens it. */
struct Latch {
  std::mutex m;
  std::condition_variable cv;
  bool open{false};
  bool held{false};
  /* until an adoption is blocked on the latch */
  void await_held() {
    std::unique_lock l{m};
    cv.wait(l, [this] { return held; });
  }
  void release() {
    std::lock_guard l{m};
    open = true;
    cv.notify_all();
  }
  void wait() {
    std::unique_lock l{m};
    held = true;
    cv.notify_all();
    cv.wait(l, [this] { return open; });
  }
};

class AdoptionQueueTest : public ::testing::Test {
protected:
  boost::asio::io_context ioc;
  std::optional<boost::asio::executor_work_guard<
    boost::asio::io_context::executor_type>> guard;
  std::vector<std::thread> threads;

  std::mutex m;
  std::condition_variable cv;
  std::vector<std::string> order;
  int drains{0};

  void SetUp() override {
    guard.emplace(boost::asio::make_work_guard(ioc));
    for (int i = 0; i < 2; ++i) {
      threads.emplace_back([this] { ioc.run(); });
    }
  }
  void TearDown() override {
    guard.reset();
    for (auto& t : threads) {
      t.join();
    }
  }

  void adopted(const std::string& b) {
    std::lock_guard l{m};
    order.push_back(b);
    cv.notify_all();
  }
  void drained() {
    std::lock_guard l{m};
    ++drains;
    cv.notify_all();
  }
  bool wait_for(std::function<bool()> pred) {
    std::unique_lock l{m};
    return cv.wait_for(l, 10s, pred);
  }

  /* promote() from a request's coroutine, on a strand as a connection
   * is */
  std::future<int> promote_from_request(AdoptionQueue& q,
                                        const std::string& b,
                                        std::chrono::milliseconds quantum) {
    auto p = std::make_shared<std::promise<int>>();
    auto f = p->get_future();
    boost::asio::spawn(boost::asio::make_strand(ioc),
      [&q, b, quantum, p] (boost::asio::yield_context yield) {
        p->set_value(q.promote(b, quantum, optional_yield{yield}));
      },
      [] (std::exception_ptr eptr) {
        if (eptr) {
          std::rethrow_exception(eptr);
        }
      });
    return f;
  }
};

} // anonymous namespace

TEST_F(AdoptionQueueTest, IntentsForOneBucketCoalesce)
{
  Latch hold;
  std::atomic<int> calls{0};
  AdoptionQueue q(ioc, 1,
    [&] (const std::string& b, const std::atomic<bool>&) {
      ++calls;
      hold.wait();
      adopted(b);
      return 0;
    },
    [this] { drained(); });
  q.enqueue("first");
  q.start();
  hold.await_held();
  /* `first` is taken;  everything for `b` lands on one intent */
  q.enqueue("b");
  q.enqueue("b");
  EXPECT_EQ(-ETIMEDOUT, q.promote("b", 0ms, null_yield));
  EXPECT_EQ(-ETIMEDOUT, q.promote("b", 0ms, null_yield));
  hold.release();
  ASSERT_TRUE(wait_for([this] { return drains == 1; }));
  EXPECT_EQ(2, calls.load());
  EXPECT_EQ((std::vector<std::string>{"first", "b"}), order);
  q.shutdown();
}

TEST_F(AdoptionQueueTest, APromotedBucketGoesAheadOfBackfill)
{
  Latch hold;
  AdoptionQueue q(ioc, 1,
    [&] (const std::string& b, const std::atomic<bool>&) {
      if (b == "b0") {
        hold.wait();
      }
      adopted(b);
      return 0;
    },
    [this] { drained(); });
  for (auto b : { "b0", "b1", "b2", "b3" }) {
    q.enqueue(b);
  }
  q.start();
  hold.await_held();
  q.promote("b3", 0ms, null_yield);
  hold.release();
  ASSERT_TRUE(wait_for([this] { return drains == 1; }));
  EXPECT_EQ((std::vector<std::string>{"b0", "b3", "b1", "b2"}), order);
  auto s = q.stats();
  EXPECT_EQ(4u, s.adopted);
  EXPECT_EQ(1u, s.promotions);
  q.shutdown();
}

TEST_F(AdoptionQueueTest, ARequestWaitsForItsBucketWithinTheQuantum)
{
  AdoptionQueue q(ioc, 1,
    [&] (const std::string& b, const std::atomic<bool>&) {
      std::this_thread::sleep_for(50ms);
      adopted(b);
      return 0;
    },
    [this] { drained(); });
  q.start();
  auto f = promote_from_request(q, "b", 5s);
  ASSERT_EQ(std::future_status::ready, f.wait_for(10s));
  EXPECT_EQ(0, f.get());
  q.shutdown();
}

TEST_F(AdoptionQueueTest, ARequestThatWaitsItsQuantumTimesOut)
{
  Latch hold;
  AdoptionQueue q(ioc, 1,
    [&] (const std::string& b, const std::atomic<bool>&) {
      hold.wait();
      adopted(b);
      return 0;
    },
    [this] { drained(); });
  q.start();
  auto f = promote_from_request(q, "b", 50ms);
  ASSERT_EQ(std::future_status::ready, f.wait_for(10s));
  EXPECT_EQ(-ETIMEDOUT, f.get());
  EXPECT_EQ(1u, q.stats().waits_timed_out);
  /* the intent outlives the wait */
  hold.release();
  ASSERT_TRUE(wait_for([this] { return order.size() == 1; }));
  EXPECT_EQ("b", order[0]);
  q.shutdown();
}

TEST_F(AdoptionQueueTest, AWaiterSeesItsBucketsFailure)
{
  AdoptionQueue q(ioc, 1,
    [&] (const std::string&, const std::atomic<bool>&) { return -EIO; },
    [this] { drained(); });
  q.start();
  auto f = promote_from_request(q, "b", 5s);
  ASSERT_EQ(std::future_status::ready, f.wait_for(10s));
  EXPECT_EQ(-EIO, f.get());
  EXPECT_EQ(1u, q.stats().failed);
  q.shutdown();
}

TEST_F(AdoptionQueueTest, AFailedBucketIsNotTriedAgain)
{
  std::atomic<int> calls{0};
  AdoptionQueue q(ioc, 1,
    [&] (const std::string&, const std::atomic<bool>&) {
      ++calls;
      return -EIO;
    },
    [this] { drained(); });
  q.start();
  auto f = promote_from_request(q, "b", 5s);
  ASSERT_EQ(std::future_status::ready, f.wait_for(10s));
  EXPECT_EQ(-EIO, f.get());
  ASSERT_TRUE(wait_for([this] { return drains == 1; }));
  /* a later request returns the failure at once, and the backfill
   * passes the bucket over */
  EXPECT_EQ(-EIO, q.promote("b", 0ms, null_yield));
  auto g = promote_from_request(q, "b", 5s);
  ASSERT_EQ(std::future_status::ready, g.wait_for(10s));
  EXPECT_EQ(-EIO, g.get());
  q.enqueue("b");
  q.recheck();
  ASSERT_TRUE(wait_for([this] { return drains == 2; }));
  EXPECT_EQ(1, calls.load());
  EXPECT_EQ(1u, q.stats().failed);
  EXPECT_EQ(1u, q.stats().promotions);
  q.shutdown();
}

TEST_F(AdoptionQueueTest, ARecheckDrainsWithNothingToAdopt)
{
  std::atomic<int> calls{0};
  AdoptionQueue q(ioc, 1,
    [&] (const std::string&, const std::atomic<bool>&) {
      ++calls;
      return 0;
    },
    [this] { drained(); });
  q.start();
  q.recheck();
  ASSERT_TRUE(wait_for([this] { return drains == 1; }));
  EXPECT_EQ(0, calls.load());
  q.shutdown();
}

TEST_F(AdoptionQueueTest, ARecheckDuringAnAdoptionDrainsOnceAfterIt)
{
  Latch hold;
  AdoptionQueue q(ioc, 1,
    [&] (const std::string& b, const std::atomic<bool>&) {
      hold.wait();
      adopted(b);
      return 0;
    },
    [this] { drained(); });
  q.enqueue("b");
  q.start();
  hold.await_held();
  q.recheck();
  hold.release();
  ASSERT_TRUE(wait_for([this] { return drains == 1; }));
  EXPECT_EQ((std::vector<std::string>{"b"}), order);
  q.shutdown();
  EXPECT_EQ(1, drains);
}

TEST_F(AdoptionQueueTest, ShutdownStopsAWalkAndTheWorkers)
{
  std::atomic<bool> walking{false};
  std::atomic<int> result{0};
  AdoptionQueue q(ioc, 2,
    [&] (const std::string&, const std::atomic<bool>& stopping) {
      walking = true;
      while (!stopping) {
        std::this_thread::sleep_for(1ms);
      }
      result = -ECANCELED;
      return -ECANCELED;
    },
    [this] { drained(); });
  q.enqueue("b");
  q.start();
  while (!walking) {
    std::this_thread::sleep_for(1ms);
  }
  q.shutdown();
  EXPECT_EQ(-ECANCELED, result.load());
  EXPECT_EQ(-ESHUTDOWN, q.promote("c", 0ms, null_yield));
}

/* Completion stops the queue from its drain, where shutdown() cannot
 * run:  the workers leave without being waited for. */
TEST_F(AdoptionQueueTest, StopFromTheDrainEndsTheWorkers)
{
  AdoptionQueue* self = nullptr;
  AdoptionQueue q(ioc, 2,
    [this] (const std::string& b, const std::atomic<bool>&) {
      adopted(b);
      return 0;
    },
    [this, &self] {
      self->stop();
      drained();
    });
  self = &q;
  q.enqueue("b");
  q.start();
  ASSERT_TRUE(wait_for([this] { return drains == 1; }));
  for (int i = 0; i < 10000 && q.stats().workers != 0; ++i) {
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(0u, q.stats().workers);
  EXPECT_EQ(-ESHUTDOWN, q.promote("c", 0ms, null_yield));
}
