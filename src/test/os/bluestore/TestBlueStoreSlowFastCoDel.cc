// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <filesystem>
#include <iostream>
#include <unistd.h>
#include <mutex>
#include <cmath>
#include <vector>
#include <condition_variable>
#include <cmath>
#include <cstdlib>

#include "gtest/gtest.h"
#include "include/Context.h"

#include "common/ceph_time.h"
#include "os/bluestore/BlueStoreSlowFastCoDel.h"


static int64_t milliseconds_to_nanoseconds(int64_t ms) {
  return ms * 1000.0 * 1000.0;
}

static double nanoseconds_to_milliseconds(int64_t ms) {
  return ms / (1000.0 * 1000.0);
}

class BlueStoreSlowFastCoDelMock : public BlueStoreSlowFastCoDel {
public:
  BlueStoreSlowFastCoDelMock(
    CephContext *_cct,
    std::function<void(int64_t)> _bluestore_budget_reset_callback,
    std::function<int64_t()> _get_kv_throttle_current,
    std::mutex &_iteration_mutex,
    std::condition_variable &_iteration_cond,
    int64_t _target_latency,
    int64_t _fast_interval,
    int64_t _slow_interval,
    double _target_slope
  ) : BlueStoreSlowFastCoDel(_cct, _bluestore_budget_reset_callback,
                             _get_kv_throttle_current),
      iteration_mutex(_iteration_mutex), iteration_cond(_iteration_cond),
      test_target_latency(_target_latency), test_fast_interval(_fast_interval),
      test_slow_interval(_slow_interval), test_target_slope(_target_slope) {
    init_test();
  }

  void init_test() {
    std::lock_guard l(register_lock);
    activated = true;
    target_slope = test_target_slope;
    slow_interval = test_slow_interval;
    initial_fast_interval = test_fast_interval;
    min_target_latency = milliseconds_to_nanoseconds(1);
    initial_target_latency = test_target_latency;
    max_target_latency = milliseconds_to_nanoseconds(500);
    initial_bluestore_budget = 100 * 1024;
    min_bluestore_budget = 10 * 1024;
    bluestore_budget_increment = 1024;
    regression_history_size = 5;
    bluestore_budget = initial_bluestore_budget;
    min_bluestore_budget = initial_bluestore_budget;
    max_queue_length = min_bluestore_budget;
    fast_interval = initial_fast_interval;
    target_latency = initial_target_latency;
    min_latency = INITIAL_LATENCY_VALUE;
    slow_interval_registered_bytes = 0;
    regression_throughput_history.clear();
    regression_target_latency_history.clear();
    slow_interval_start = ceph::mono_clock::zero();
  }

  std::vector <int64_t> target_latency_vector;

protected:
  std::mutex &iteration_mutex;
  std::condition_variable &iteration_cond;
  int64_t test_target_latency;
  int64_t test_fast_interval;
  int64_t test_slow_interval;
  double test_target_slope;

  void on_fast_interval_finished() override {
    std::unique_lock <std::mutex> locker(iteration_mutex);
    iteration_cond.notify_one();
  }

  void on_slow_interval_finished() override {
    target_latency_vector.push_back(target_latency);
  }
};

class TestSlowFastCoDel : public ::testing::Test {
public:
  CephContext *ceph_context = nullptr;
  BlueStoreSlowFastCoDelMock *slow_fast_codel = nullptr;
  int64_t test_throttle_budget = 0;
  std::mutex iteration_mutex;
  std::condition_variable iteration_cond;
  int64_t target_latency = milliseconds_to_nanoseconds(50);
  int64_t fast_interval = milliseconds_to_nanoseconds(100);
  int64_t slow_interval = milliseconds_to_nanoseconds(400);
  double target_slope = 1;

  std::vector <int64_t> target_latency_vector;
  std::vector <int64_t> txc_size_vector;

  TestSlowFastCoDel() {}

  ~TestSlowFastCoDel() {}

  static void SetUpTestCase() {}

  static void TearDownTestCase() {}

  void SetUp() override {
    ceph_context = (new CephContext(CEPH_ENTITY_TYPE_ANY))->get();
  }

  void create_bluestore_slow_fast_codel() {
    slow_fast_codel = new BlueStoreSlowFastCoDelMock(
            ceph_context,
            [this](int64_t x) mutable {
                this->test_throttle_budget = x;
            },
            [this]() mutable {
                return this->test_throttle_budget;
            },
            iteration_mutex,
            iteration_cond,
            target_latency,
            fast_interval,
            slow_interval,
            target_slope);
  }

  void TearDown() override {
    if (slow_fast_codel)
      delete slow_fast_codel;
  }

  void test_codel() {
    int64_t max_iterations = 50;
    int iteration_timeout = 1; // 1 sec
    int txc_num = 4;
    for (int iteration = 0; iteration < max_iterations; iteration++) {
      std::unique_lock <std::mutex> locker(iteration_mutex);
      bool violation = iteration % 2 == 1;
      auto budget_tmp = test_throttle_budget;
      auto target = slow_fast_codel->get_target_latency();
      double target_throughput =
        (target_slope * nanoseconds_to_milliseconds(target_latency)) *
        std::log(nanoseconds_to_milliseconds(target) * 1.0);
      int64_t txc_size =
              (nanoseconds_to_milliseconds(slow_interval) *
               target_throughput) /
              (1000 * txc_num * (slow_interval / fast_interval));
      txc_size *= 1024 * 1024;
      txc_size_vector.push_back(txc_size);
      target_latency_vector.push_back(target);
      for (int i = 0; i < txc_num; i++) {
        auto time = ceph::mono_clock::now();
        if (violation) {
          int rand_ms = std::rand() % 1000 + 1000;
          int64_t time_diff = milliseconds_to_nanoseconds(rand_ms);
          time = time - std::chrono::nanoseconds(target + time_diff);
        }
        slow_fast_codel->update_from_txc_info(time, txc_size);
      }
      if (iteration_cond.wait_for(
              locker, std::chrono::seconds(iteration_timeout)) ==
          std::cv_status::timeout) {
        ASSERT_TRUE(false) << "Test timeout.";
        return;
      }
      if (violation) {
        ASSERT_LT(test_throttle_budget, budget_tmp);
      } else {
        ASSERT_GT(test_throttle_budget, budget_tmp);
      }
    }

    ASSERT_TRUE(slow_fast_codel->target_latency_vector.size() > 0);
  }
};

TEST_F(TestSlowFastCoDel, test1) {
  create_bluestore_slow_fast_codel();
  test_codel();
}
