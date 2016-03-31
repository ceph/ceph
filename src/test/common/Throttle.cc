// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>
#include <signal.h>
#include "common/Mutex.h"
#include "common/Thread.h"
#include "common/Throttle.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include <gtest/gtest.h>

#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <list>
#include <random>

class ThrottleTest : public ::testing::Test {
protected:

  class Thread_get : public Thread {
  public:
    Throttle &throttle;
    int64_t count;
    bool waited;

    Thread_get(Throttle& _throttle, int64_t _count) :
      throttle(_throttle),
      count(_count),
      waited(false)
    {
    }

    virtual void *entry() {
      waited = throttle.get(count);
      throttle.put(count);
      return NULL;
    }
  };

};

TEST_F(ThrottleTest, Throttle) {
  uint64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle", throttle_max);
  ASSERT_EQ(throttle.get_max(), throttle_max);
  ASSERT_EQ(throttle.get_current(), (uint64_t)0);
}

TEST_F(ThrottleTest, take) {
  uint64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle", throttle_max);
  ASSERT_EQ(throttle.take(throttle_max), throttle_max);
  ASSERT_EQ(throttle.take(throttle_max), throttle_max * 2);
}

TEST_F(ThrottleTest, get) {
  uint64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle");

  // test increasing max from 0 to throttle_max
  {
    ASSERT_FALSE(throttle.get(throttle_max, throttle_max));
    ASSERT_EQ(throttle.get_max(), throttle_max);
    ASSERT_EQ(throttle.put(throttle_max), (uint64_t)0);
  }

  ASSERT_FALSE(throttle.get(5));
  ASSERT_EQ(throttle.put(5), (uint64_t)0);

  ASSERT_FALSE(throttle.get(throttle_max));
  ASSERT_FALSE(throttle.get_or_fail(1));
  ASSERT_FALSE(throttle.get(1, throttle_max + 1));
  ASSERT_EQ(throttle.put(throttle_max + 1), (uint64_t)0);
  ASSERT_FALSE(throttle.get(0, throttle_max));
  ASSERT_FALSE(throttle.get(throttle_max));
  ASSERT_FALSE(throttle.get_or_fail(1));
  ASSERT_EQ(throttle.put(throttle_max), (uint64_t)0);

  useconds_t delay = 1;

  bool waited;

  do {
    cout << "Trying (1) with delay " << delay << "us\n";

    ASSERT_FALSE(throttle.get(throttle_max));
    ASSERT_FALSE(throttle.get_or_fail(throttle_max));

    Thread_get t(throttle, 7);
    t.create("t_throttle_1");
    usleep(delay);
    ASSERT_EQ(throttle.put(throttle_max), (uint64_t)0);
    t.join();

    if (!(waited = t.waited))
      delay *= 2;
  } while(!waited);

  do {
    cout << "Trying (2) with delay " << delay << "us\n";

    ASSERT_FALSE(throttle.get(throttle_max / 2));
    ASSERT_FALSE(throttle.get_or_fail(throttle_max));

    Thread_get t(throttle, throttle_max);
    t.create("t_throttle_2");
    usleep(delay);

    Thread_get u(throttle, 1);
    u.create("u_throttle_2");
    usleep(delay);

    throttle.put(throttle_max / 2);

    t.join();
    u.join();

    if (!(waited = t.waited && u.waited))
      delay *= 2;
  } while(!waited);

}

TEST_F(ThrottleTest, get_or_fail) {
  {
    Throttle throttle(g_ceph_context, "throttle");

    ASSERT_TRUE(throttle.get_or_fail(5));
    ASSERT_TRUE(throttle.get_or_fail(5));
  }

  {
    uint64_t throttle_max = 10;
    Throttle throttle(g_ceph_context, "throttle", throttle_max);

    ASSERT_TRUE(throttle.get_or_fail(throttle_max));
    ASSERT_EQ(throttle.put(throttle_max), (uint64_t)0);

    ASSERT_TRUE(throttle.get_or_fail(throttle_max * 2));
    ASSERT_FALSE(throttle.get_or_fail(1));
    ASSERT_FALSE(throttle.get_or_fail(throttle_max * 2));
    ASSERT_EQ(throttle.put(throttle_max * 2), (uint64_t)0);

    ASSERT_TRUE(throttle.get_or_fail(throttle_max));
    ASSERT_FALSE(throttle.get_or_fail(1));
    ASSERT_EQ(throttle.put(throttle_max), (uint64_t)0);
  }
}

TEST_F(ThrottleTest, wait) {
  uint64_t throttle_max = 10;
  Throttle throttle(g_ceph_context, "throttle");

  // test increasing max from 0 to throttle_max
  {
    ASSERT_FALSE(throttle.wait(throttle_max));
    ASSERT_EQ(throttle.get_max(), throttle_max);
  }

  useconds_t delay = 1;

  bool waited;

  do {
    cout << "Trying (3) with delay " << delay << "us\n";

    ASSERT_FALSE(throttle.get(throttle_max / 2));
    ASSERT_FALSE(throttle.get_or_fail(throttle_max));

    Thread_get t(throttle, throttle_max);
    t.create("t_throttle_3");
    usleep(delay);

    //
    // Throttle::_reset_max(int64_t m) used to contain a test
    // that blocked the following statement, only if
    // the argument was greater than throttle_max.
    // Although a value lower than throttle_max would cover
    // the same code in _reset_max, the throttle_max * 100
    // value is left here to demonstrate that the problem
    // has been solved.
    //
    throttle.wait(throttle_max * 100);
    usleep(delay);
    t.join();
    ASSERT_EQ(throttle.get_current(), throttle_max / 2);

    if (!(waited = t.waited)) {
      delay *= 2;
      // undo the changes we made
      throttle.put(throttle_max / 2);
      throttle.wait(throttle_max);
    }
  } while(!waited);
}

TEST_F(ThrottleTest, destructor) {
  Thread_get *t;
  {
    uint64_t throttle_max = 10;
    Throttle *throttle = new Throttle(g_ceph_context, "throttle", throttle_max);

    ASSERT_FALSE(throttle->get(5));

    t = new Thread_get(*throttle, 7);
    t->create("t_throttle");
    bool blocked;
    useconds_t delay = 1;
    do {
      usleep(delay);
      if (throttle->get_or_fail(1)) {
	throttle->put(1);
	blocked = false;
      } else {
	blocked = true;
      }
      delay *= 2;
    } while(!blocked);
    delete throttle;
  }

  { //
    // The thread is left hanging, otherwise it will abort().
    // Deleting the Throttle on which it is waiting creates a
    // inconsistency that will be detected: the Throttle object that
    // it references no longer exists.
    //
    pthread_t id = t->get_thread_id();
    ASSERT_EQ(pthread_kill(id, 0), 0);
    delete t;
    ASSERT_EQ(pthread_kill(id, 0), 0);
  }
}

std::pair<double, std::chrono::duration<double> > test_backoff(
  double low_threshhold,
  double high_threshhold,
  double expected_throughput,
  double high_multiple,
  double max_multiple,
  uint64_t max,
  double put_delay_per_count,
  unsigned getters,
  unsigned putters)
{
  std::mutex l;
  std::condition_variable c;
  uint64_t total = 0;
  std::list<uint64_t> in_queue;
  bool stop = false;

  auto wait_time = std::chrono::duration<double>(0);
  uint64_t waits = 0;

  uint64_t total_observed_total = 0;
  uint64_t total_observations = 0;

  BackoffThrottle throttle(5);
  bool valid = throttle.set_params(
    low_threshhold,
    high_threshhold,
    expected_throughput,
    high_multiple,
    max_multiple,
    max,
    0);
  assert(valid);

  auto getter = [&]() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 10);

    std::unique_lock<std::mutex> g(l);
    while (!stop) {
      g.unlock();

      uint64_t to_get = dis(gen);
      auto waited = throttle.get(to_get);

      g.lock();
      wait_time += waited;
      waits += to_get;
      total += to_get;
      in_queue.push_back(to_get);
      c.notify_one();
    }
  };

  auto putter = [&]() {
    std::unique_lock<std::mutex> g(l);
    while (!stop) {
      while (in_queue.empty())
	c.wait(g);

      uint64_t c = in_queue.front();

      total_observed_total += total;
      total_observations++;
      in_queue.pop_front();
      assert(total <= max);

      g.unlock();
      std::this_thread::sleep_for(
	c * std::chrono::duration<double>(put_delay_per_count*putters));
      g.lock();

      total -= c;
      throttle.put(c);
    }
  };

  vector<std::thread> gts(getters);
  for (auto &&i: gts) i = std::thread(getter);

  vector<std::thread> pts(putters);
  for (auto &&i: pts) i = std::thread(putter);

  std::this_thread::sleep_for(std::chrono::duration<double>(5));
  {
    std::unique_lock<std::mutex> g(l);
    stop = true;
  }
  for (auto &&i: gts) i.join();
  gts.clear();
  for (auto &&i: pts) i.join();
  pts.clear();

  return make_pair(
    ((double)total_observed_total)/((double)total_observations),
    wait_time / waits);
}

TEST(BackoffThrottle, undersaturated)
{
  auto results = test_backoff(
    0.4,
    0.6,
    1000,
    2,
    10,
    100,
    0.0001,
    3,
    6);
  ASSERT_LT(results.first, 45);
  ASSERT_GT(results.first, 35);
  ASSERT_LT(results.second.count(), 0.0002);
  ASSERT_GT(results.second.count(), 0.00005);
}

TEST(BackoffThrottle, balanced)
{
  auto results = test_backoff(
    0.4,
    0.6,
    1000,
    2,
    10,
    100,
    0.001,
    7,
    2);
  ASSERT_LT(results.first, 60);
  ASSERT_GT(results.first, 40);
  ASSERT_LT(results.second.count(), 0.002);
  ASSERT_GT(results.second.count(), 0.0005);
}

TEST(BackoffThrottle, oversaturated)
{
  auto results = test_backoff(
    0.4,
    0.6,
    10000000,
    2,
    10,
    100,
    0.001,
    1,
    3);
  ASSERT_LT(results.first, 101);
  ASSERT_GT(results.first, 85);
  ASSERT_LT(results.second.count(), 0.002);
  ASSERT_GT(results.second.count(), 0.0005);
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make unittest_throttle ;
 *   ./unittest_throttle # --gtest_filter=ThrottleTest.destructor \
 *       --log-to-stderr=true --debug-filestore=20
 * "
 * End:
 */

