// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <sstream>
#include <string>
#include <variant>

#include <seastar/core/sleep.hh>

#include "common/JSONFormatter.h"
#include "crimson/common/coroutine.h"
#include "crimson/osd/scheduler/mclock_scheduler.h"
#include "test/crimson/gtest_seastar.h"

using namespace crimson::osd::scheduler;
using MClockSchedulerTest = seastar_test_suite_t;

TEST_F(MClockSchedulerTest, cleanup_continues_after_timeout)
{
  run_scl([]() -> seastar::future<> {
    using namespace std::chrono_literals;
    CephContext cct;
    mClockScheduler queue(&cct, 0, 1, 0, false, 20ms, 40ms, 10ms, false);
    queue.enqueue(item_t{
      params_t{1, 0, 0, SchedulerClass::client},
      throttle_level_t::foreground, seastar::promise<>()});
    EXPECT_TRUE(std::holds_alternative<item_t>(
      queue.dequeue(throttle_level_t::foreground)));

    auto clients_cleaned = [&queue] {
      ceph::JSONFormatter formatter;
      queue.dump(formatter);
      std::ostringstream out;
      formatter.flush(out);
      return out.str().find("\"client_count\":0") != std::string::npos;
    };
    // The request has finished, but its client record is still retained.
    EXPECT_FALSE(clients_cleaned());
    const auto deadline = seastar::lowres_clock::now() + 5s;
    while (!clients_cleaned() && seastar::lowres_clock::now() < deadline) {
      co_await seastar::sleep(10ms);
    }
    EXPECT_TRUE(clients_cleaned());
  });
}
