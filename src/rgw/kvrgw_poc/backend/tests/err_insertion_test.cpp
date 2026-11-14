// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "err_insertion.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

using namespace kvrgw;

static void test_disabled_by_default()
{
  ErrInsertion ei;
  for (int i = 0; i < 100; ++i) {
    assert(!ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
    assert(!ei.is_error_active(FaultType::kAbortAfterSinglePhase2));
    assert(!ei.is_error_active(FaultType::kAbortSweeperAfterPutGo));
    assert(!ei.is_error_active(FaultType::kAbortGcWorkerMidGroup));
  }
  std::printf("  disabled_by_default: passed\n");
}

static void test_fixed_mode()
{
  ErrInsertion ei;
  ei.set_fixed(FaultType::kAbortAfterBatchPhase2);
  for (int i = 0; i < 50; ++i) {
    assert(ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  }
  assert(!ei.is_error_active(FaultType::kAbortAfterSinglePhase2));
  ei.clear(FaultType::kAbortAfterBatchPhase2);
  for (int i = 0; i < 50; ++i) {
    assert(!ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  }
  std::printf("  fixed_mode: passed\n");
}

static void test_counter_mode()
{
  ErrInsertion ei;
  ei.set_counter(FaultType::kAbortAfterBatchPhase2, 5);

  int triggers = 0;
  for (int i = 1; i <= 20; ++i) {
    if (ei.is_error_active(FaultType::kAbortAfterBatchPhase2)) {
      assert(i % 5 == 0);
      ++triggers;
    }
  }
  assert(triggers == 4);
  std::printf("  counter_mode: passed\n");
}

static void test_counter_with_burst()
{
  ErrInsertion ei;
  ei.set_counter(FaultType::kAbortAfterBatchPhase2, 5, 3);

  std::vector<int> trigger_positions;
  for (int i = 1; i <= 30; ++i) {
    if (ei.is_error_active(FaultType::kAbortAfterBatchPhase2)) {
      trigger_positions.push_back(i);
    }
  }
  // period=5, burst=3: trigger at call 5, then burst calls 6,7
  // Then resume counting from where we left off.
  // call_count was 5 when triggered. Next trigger at call_count=10.
  // But burst calls (6,7) don't increment call_count.
  // So after burst: call_count=5. Next calls increment: 6,7,8,9,10 -> trigger
  // at 10 (count%5==0) Actual calls: 5=trigger, 6=burst, 7=burst, 8(count=6),
  // 9(count=7), 10(count=8), 11(count=9), 12(count=10)=trigger 12=trigger,
  // 13=burst, 14=burst, ... Expected trigger positions: 5,6,7, 12,13,14,
  // 19,20,21, 26,27,28
  assert(trigger_positions.size() == 12);
  // First burst: 5, 6, 7
  assert(trigger_positions[0] == 5);
  assert(trigger_positions[1] == 6);
  assert(trigger_positions[2] == 7);
  // Second burst starts at call 12 (call_count reaches 10 at call position 12)
  assert(trigger_positions[3] == 12);
  assert(trigger_positions[4] == 13);
  assert(trigger_positions[5] == 14);
  std::printf("  counter_with_burst: passed\n");
}

static void test_time_mode()
{
  ErrInsertion ei;
  ei.set_time(FaultType::kAbortAfterBatchPhase2, 10000); // 10ms

  assert(!ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  std::this_thread::sleep_for(std::chrono::milliseconds(12));
  assert(ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  assert(!ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  std::this_thread::sleep_for(std::chrono::milliseconds(12));
  assert(ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  std::printf("  time_mode: passed\n");
}

static void test_time_with_burst()
{
  ErrInsertion ei;
  ei.set_time(FaultType::kAbortAfterBatchPhase2, 10000, 3); // 10ms, burst=3

  assert(!ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  std::this_thread::sleep_for(std::chrono::milliseconds(12));

  // Trigger fires, burst=3: first call triggers, then 2 more burst calls
  assert(ei.is_error_active(
      FaultType::kAbortAfterBatchPhase2)); // trigger (remaining_burst=2)
  assert(ei.is_error_active(
      FaultType::kAbortAfterBatchPhase2)); // burst (remaining_burst=1)
  assert(ei.is_error_active(
      FaultType::kAbortAfterBatchPhase2)); // burst (remaining_burst=0)
  // Burst done. Next trigger after interval_us from now.
  assert(!ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  std::this_thread::sleep_for(std::chrono::milliseconds(12));
  assert(
      ei.is_error_active(FaultType::kAbortAfterBatchPhase2)); // second trigger
  assert(ei.is_error_active(FaultType::kAbortAfterBatchPhase2)); // burst
  assert(ei.is_error_active(FaultType::kAbortAfterBatchPhase2)); // burst
  assert(!ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  std::printf("  time_with_burst: passed\n");
}

static void test_burst_does_not_increment_counter()
{
  ErrInsertion ei;
  ei.set_counter(FaultType::kAbortAfterBatchPhase2, 3, 2);

  // call_count increments: 1, 2, 3(trigger+burst=1)
  // burst call (no increment)
  // call_count increments: 4, 5, 6(trigger+burst=1)
  int triggers = 0;
  int total_true = 0;
  for (int i = 0; i < 12; ++i) {
    if (ei.is_error_active(FaultType::kAbortAfterBatchPhase2)) {
      ++total_true;
    }
  }
  // 12 calls: period=3, burst=2
  // Pattern: F,F,T(trigger),T(burst) | F,F,T(trigger),T(burst) |
  // F,F,T(trigger),T(burst) Wait - 12 calls but some are burst (don't
  // increment). Let me trace: call 1: count=1, no trigger -> false call 2:
  // count=2, no trigger -> false call 3: count=3, trigger! remaining_burst=1 ->
  // true call 4: remaining_burst=1>0, dec->0 -> true call 5: count=4, no
  // trigger -> false call 6: count=5, no trigger -> false call 7: count=6,
  // trigger! remaining_burst=1 -> true call 8: remaining_burst=1>0, dec->0 ->
  // true call 9: count=7, no trigger -> false call 10: count=8, no trigger ->
  // false call 11: count=9, trigger! remaining_burst=1 -> true call 12:
  // remaining_burst=1>0, dec->0 -> true
  assert(total_true == 6);
  std::printf("  burst_does_not_increment_counter: passed\n");
}

static void test_clear_resets_all()
{
  ErrInsertion ei;
  ei.set_counter(FaultType::kAbortAfterBatchPhase2, 2, 3);

  // Trigger once to get burst going
  ei.is_error_active(FaultType::kAbortAfterBatchPhase2); // count=1
  ei.is_error_active(FaultType::kAbortAfterBatchPhase2); // count=2, trigger

  ei.clear(FaultType::kAbortAfterBatchPhase2);

  for (int i = 0; i < 20; ++i) {
    assert(!ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  }
  std::printf("  clear_resets_all: passed\n");
}

static void test_independent_fault_types()
{
  ErrInsertion ei;
  ei.set_fixed(FaultType::kAbortAfterBatchPhase2);
  ei.set_counter(FaultType::kAbortAfterSinglePhase2, 3);

  assert(ei.is_error_active(FaultType::kAbortAfterBatchPhase2));
  assert(!ei.is_error_active(FaultType::kAbortAfterSinglePhase2)); // count=1
  assert(!ei.is_error_active(FaultType::kAbortAfterSinglePhase2)); // count=2
  assert(ei.is_error_active(
      FaultType::kAbortAfterSinglePhase2)); // count=3, trigger

  assert(!ei.is_error_active(FaultType::kAbortSweeperAfterPutGo));
  assert(!ei.is_error_active(FaultType::kAbortGcWorkerMidGroup));
  std::printf("  independent_fault_types: passed\n");
}

static void test_concurrent_counter()
{
  ErrInsertion ei;
  ei.set_counter(FaultType::kAbortAfterBatchPhase2, 10);

  constexpr int kThreads = 4;
  constexpr int kCallsPerThread = 1000;
  std::atomic<int> total_triggers{0};

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      int local = 0;
      for (int i = 0; i < kCallsPerThread; ++i) {
        if (ei.is_error_active(FaultType::kAbortAfterBatchPhase2)) {
          ++local;
        }
      }
      total_triggers.fetch_add(local, std::memory_order_relaxed);
    });
  }
  for (auto &th : threads) {
    th.join();
  }

  int expected = (kThreads * kCallsPerThread) / 10;
  int actual = total_triggers.load();
  // With concurrent fetch_add, we may have slight variation due to races
  // but total_triggers should be close to expected (±threads due to race on
  // modulo check)
  assert(actual >= expected - kThreads && actual <= expected + kThreads);
  std::printf("  concurrent_counter: passed (triggers=%d, expected~%d)\n",
              actual, expected);
}

int main()
{
  std::printf("err_insertion_test:\n");
  test_disabled_by_default();
  test_fixed_mode();
  test_counter_mode();
  test_counter_with_burst();
  test_time_mode();
  test_time_with_burst();
  test_burst_does_not_increment_counter();
  test_clear_resets_all();
  test_independent_fault_types();
  test_concurrent_counter();
  std::printf("err_insertion_test: all passed\n");
  return 0;
}
