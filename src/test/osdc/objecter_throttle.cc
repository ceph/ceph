// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Regression test: Objecter balanced-budget must not block the shared asio
 * pool that runs put_op_budget_bytes / completions (Option A).
 *
 * With keep_balanced_budget, if every pool thread blocked in Throttle::get
 * waiting for budget, puts posted to the same pool never ran → deadlock.
 * _throttle_op now defers on the service executor instead of blocking.
 */

#include <atomic>
#include <chrono>
#include <future>
#include <string>

#include <boost/asio/post.hpp>

#include "gtest/gtest.h"

#include "common/async/context_pool.h"
#include "common/options.h"
#include "global/global_context.h"
#include "msg/Messenger.h"
#include "osdc/Objecter.h"

namespace {

constexpr int budget_bytes = 4096;
constexpr int pool_threads = 2;
constexpr auto hang_timeout = std::chrono::seconds(3);

} // namespace

TEST(ObjecterThrottle, BalancedBudgetDoesNotDeadlockSharedAsioPool) {
  const auto prev_bytes = g_ceph_context->_conf.get_val<Option::size_t>(
      "objecter_inflight_op_bytes");
  ASSERT_EQ(0, g_ceph_context->_conf.set_val("objecter_inflight_op_bytes",
                                             std::to_string(budget_bytes)));
  g_ceph_context->_conf.apply_changes(nullptr);

  ceph::async::io_context_pool pool(pool_threads);

  Messenger *msgr = Messenger::create_client_messenger(
      g_ceph_context, "objecter_throttle_test");
  ASSERT_NE(nullptr, msgr);
  ASSERT_EQ(0, msgr->start());

  Objecter objecter(g_ceph_context, msgr, nullptr, pool);
  objecter.set_balanced_budget();

  // Occupy the entire byte budget from this (non-pool) thread.
  objecter.throttle_op_budget_for_test(budget_bytes);

  std::atomic<bool> held_released{false};
  auto release_held = [&] {
    bool expected = false;
    if (held_released.compare_exchange_strong(expected, true)) {
      objecter.put_op_budget_for_test(budget_bytes);
    }
  };

  std::promise<void> all_acquired;
  auto all_acquired_fut = all_acquired.get_future();
  std::atomic<int> acquired{0};

  // From the shared pool: request budget without blocking the worker.
  // Previously this deadlocked; Option A defers until put runs on the pool.
  for (int i = 0; i < pool_threads; ++i) {
    boost::asio::post(pool, [&] {
      objecter.throttle_op_budget_for_test(budget_bytes, [&] {
        if (acquired.fetch_add(1) + 1 == pool_threads) {
          all_acquired.set_value();
        }
        objecter.put_op_budget_for_test(budget_bytes);
      });
    });
  }

  // Completion that frees the held budget — same pool as the requesters.
  boost::asio::post(pool, [&] { release_held(); });

  const auto status = all_acquired_fut.wait_for(hang_timeout);
  const bool deadlocked = (status != std::future_status::ready);

  if (deadlocked) {
    release_held();
    (void)all_acquired_fut.wait_for(std::chrono::seconds(2));
  }

  pool.stop();
  msgr->shutdown();
  msgr->wait();
  delete msgr;

  g_ceph_context->_conf.set_val("objecter_inflight_op_bytes",
                                std::to_string(prev_bytes));
  g_ceph_context->_conf.apply_changes(nullptr);

  ASSERT_FALSE(deadlocked)
      << "Objecter balanced-budget throttle blocked the asio pool; "
         "deferred waiters were not resumed after put_op_budget";
}
