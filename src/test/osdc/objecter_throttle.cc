// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Reproducer for Objecter balanced-budget deadlock on a shared asio pool.
 *
 * With keep_balanced_budget, _throttle_op falls back to blocking
 * Throttle::get when get_or_fail fails. Completions that call
 * put_op_budget_bytes are posted to the same io_context_pool. If every
 * pool thread is blocked in get(), those puts never run → deadlock.
 *
 * This matches the fio/RBD hang under OSDC (io_context_pool threads stuck
 * in Objecter::_throttle_op → Throttle::_wait).
 */

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>

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

TEST(ObjecterThrottle, BalancedBudgetDeadlocksSharedAsioPool) {
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

  std::promise<void> acquired;
  auto acquired_fut = acquired.get_future();
  std::atomic<int> blockers_started{0};

  // Queue one blocking throttle per pool thread so every worker sits in
  // Throttle::get waiting for budget that only a pool-posted put can free.
  for (int i = 0; i < pool_threads; ++i) {
    boost::asio::post(pool, [&, i] {
      blockers_started.fetch_add(1);
      objecter.throttle_op_budget_for_test(budget_bytes);
      if (i == 0) {
        acquired.set_value();
      }
      objecter.put_op_budget_for_test(budget_bytes);
    });
  }

  // Give workers a moment to enter Throttle::_wait so the releaser cannot
  // sneak onto a free thread.
  for (int i = 0; i < 50 && blockers_started.load() < pool_threads; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Completion that frees the held budget — same pool as the blockers.
  boost::asio::post(pool, [&] { release_held(); });

  const auto status = acquired_fut.wait_for(hang_timeout);
  const bool deadlocked = (status != std::future_status::ready);

  if (deadlocked) {
    // Unstick workers so pool.stop() can join; then fail the assertion.
    release_held();
    (void)acquired_fut.wait_for(std::chrono::seconds(2));
  }

  pool.stop();
  msgr->shutdown();
  msgr->wait();
  delete msgr;

  g_ceph_context->_conf.set_val("objecter_inflight_op_bytes",
                                std::to_string(prev_bytes));
  g_ceph_context->_conf.apply_changes(nullptr);

  ASSERT_FALSE(deadlocked)
      << "Objecter balanced-budget throttle blocked all " << pool_threads
      << " asio pool threads; put_op_budget posted to the same pool never ran";
}
