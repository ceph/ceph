// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <list>
#include <optional>
#include <ranges>

#include <seastar/core/with_timeout.hh>

#include "crimson/common/config_proxy.h"
#include "crimson/common/coroutine.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/request_orderer.h"
#include "crimson/osd/scheduler/mclock_scheduler.h"
#include "test/crimson/gtest_seastar.h"

namespace crimson::osd {
namespace {
using scheduler::item_t;
using scheduler::params_t;
using scheduler::throttle_level_t;

class TestScheduler final : public scheduler::Scheduler {
  std::list<item_t> items;
public:
  std::array<unsigned, 3> dispatched = {};
  std::optional<uint64_t> get_strict_priority(const params_t &params) const {
    if (params.klass == SchedulerClass::immediate) {
      return std::numeric_limits<uint64_t>::max();
    }
    if (params.priority >= 100) {
      return params.priority;
    }
    return std::nullopt;
  }
  void enqueue(item_t &&item) final { items.push_back(std::move(item)); }
  void enqueue_front(item_t &&item) final { items.push_front(std::move(item)); }
  bool empty() const final { return items.empty(); }
  scheduler::WorkItem dequeue(throttle_level_t max_level) final {
    auto eligible = items | std::views::filter([max_level](const auto &item) {
      return item.level <= max_level;
    });
    auto next = std::ranges::max_element(eligible, {}, [this](const auto &item) {
      return get_strict_priority(item.params);
    });
    if (next == eligible.end()) {
      return std::monostate{};
    }
    auto item = std::move(*next);
    items.erase(next.base());
    ++dispatched[std::to_underlying(item.level)];
    return item;
  }
  void abort(std::exception_ptr error) final {
    for (auto &item : items) {
      item.wake.set_exception(error);
    }
    items.clear();
  }
  void dump(Formatter&) const final {}
  void print(std::ostream&) const final {}
};
}

class OperationThrottlerTest : public seastar_test_suite_t {
protected:
  std::unique_ptr<OperationThrottler> throttler;
  using Guard = OperationThrottler::ThrottleReleaser;
  std::string previous_scheduler;
  std::string previous_concurrency;

  seastar::future<> set_up_fut() final {
    auto &conf = crimson::common::local_conf();
    previous_scheduler = conf.get_val<std::string>("osd_op_queue");
    ceph_assert(conf.get_val("crimson_osd_scheduler_concurrency",
                             &previous_concurrency) == 0);
    throttler = std::make_unique<OperationThrottler>(conf);
    throttler->scheduler = std::make_unique<TestScheduler>();
    co_await conf.set_val("crimson_osd_scheduler_concurrency", "3");
    throttler->update_from_config(conf);
    throttler->start();
  }
  seastar::future<> tear_down_fut() final {
    co_await throttler->stop();
    EXPECT_TRUE(throttler->scheduler->empty());
    EXPECT_EQ(throttler->count_from(throttler->in_progress), 0);
    EXPECT_EQ(throttler->count_from(throttler->pending), 0);
    auto &conf = crimson::common::local_conf();
    conf.remove_observer(throttler.get());
    throttler.reset();
    co_await conf.set_val("osd_op_queue", previous_scheduler);
    co_await conf.set_val("crimson_osd_scheduler_concurrency", previous_concurrency);
  }
  void set_scheduler(scheduler::SchedulerRef scheduler) {
    throttler->scheduler = std::move(scheduler);
  }
  TestScheduler *test_scheduler() {
    return static_cast<TestScheduler*>(throttler->scheduler.get());
  }
  auto request(throttle_level_t level, unsigned priority = 0,
               scheduler::client_t owner = 0) {
    return seastar::with_timeout(
      seastar::lowres_clock::now() + std::chrono::seconds(5),
      throttler->get_throttle(
        params_t{1, priority, owner, SchedulerClass::client}, level));
  }
  seastar::future<> wait_for_worker() {
    co_await seastar::yield();
    co_await seastar::yield();
  }
};

TEST_F(OperationThrottlerTest, high_priority_bypasses_blocked_backlog)
{
  run_scl([this]() -> seastar::future<> {
    std::optional<Guard> running;
    running.emplace(co_await request(throttle_level_t::foreground));
    std::vector<seastar::future<Guard>> normal;
    for (unsigned i = 0; i < 20; ++i) {
      normal.push_back(request(throttle_level_t::foreground));
    }
    co_await wait_for_worker();
    auto high = request(throttle_level_t::foreground, 100);
    co_await wait_for_worker();
    running.reset();
    {
      auto admitted = co_await std::move(high);
      EXPECT_FALSE(normal.front().available());
    }
    for (auto &future : normal) {
      auto admitted = co_await std::move(future);
    }
  });
}

TEST_F(OperationThrottlerTest, earlier_client_keeps_its_place_among_equal_priorities)
{
  run_scl([this]() -> seastar::future<> {
    std::optional<Guard> first, second;
    first.emplace(co_await request(throttle_level_t::recovery));
    second.emplace(co_await request(throttle_level_t::recovery));
    auto client = request(throttle_level_t::foreground);
    co_await wait_for_worker();
    auto recovery = request(throttle_level_t::recovery);
    co_await wait_for_worker();
    first.reset();
    {
      auto admitted = co_await std::move(client);
      EXPECT_FALSE(recovery.available());
    }
    auto admitted = co_await std::move(recovery);
  });
}

TEST_F(OperationThrottlerTest, same_object_successor_waits_before_taking_a_slot)
{
  run_scl([this]() -> seastar::future<> {
    RequestOrderer<int> objects;
    std::optional<Guard> running, earlier;
    running.emplace(co_await request(throttle_level_t::foreground));
    auto first_turn = co_await objects.get(1);
    auto first = request(throttle_level_t::foreground);
    auto later = objects.get(1).then([this](auto turn) {
      return request(throttle_level_t::foreground, 100).then(
        [turn = std::move(turn)](auto slot) mutable {
          turn.reset();
          return slot;
        });
    });
    // Another object can register without waiting for object 1.
    auto other_object = co_await objects.get(2);
    co_await wait_for_worker();
    running.reset();
    earlier.emplace(co_await std::move(first));
    EXPECT_FALSE(later.available());
    // Model entering wait_pg_ready before releasing the admission turn.
    first_turn.reset();
    earlier.reset();
    auto admitted = co_await std::move(later);
  });
}

TEST_F(OperationThrottlerTest, leaf_uses_capacity_reserved_from_foreground)
{
  run_scl([this]() -> seastar::future<> {
    auto foreground = co_await request(throttle_level_t::foreground);
    auto recovery = co_await request(throttle_level_t::recovery);
    auto leaf = co_await request(throttle_level_t::leaf);
  });
}

TEST_F(OperationThrottlerTest, blocked_requests_do_not_consume_qos_service)
{
  run_scl([this]() -> seastar::future<> {
    auto *scheduler = test_scheduler();
    std::optional<Guard> running;
    running.emplace(co_await request(throttle_level_t::foreground));
    auto blocked = request(throttle_level_t::foreground);
    // A leaf request can run while foreground is full. Selecting it must
    // not count the blocked foreground request as QoS service.
    {
      auto leaf = co_await request(throttle_level_t::leaf);
      EXPECT_EQ(scheduler->dispatched[std::to_underlying(
        throttle_level_t::foreground)], 1);
    }
    running.reset();
    auto admitted = co_await std::move(blocked);
  });
}

TEST_F(OperationThrottlerTest, blocked_strict_foreground_does_not_hide_strict_recovery)
{
  run_scl([this]() -> seastar::future<> {
    std::optional<Guard> foreground, recovery, leaf;
    foreground.emplace(co_await request(throttle_level_t::foreground));
    recovery.emplace(co_await request(throttle_level_t::recovery));
    auto ordinary = request(throttle_level_t::recovery);
    co_await wait_for_worker();
    leaf.emplace(co_await request(throttle_level_t::leaf));
    auto strict_foreground = request(throttle_level_t::foreground, 200);
    auto strict_recovery = request(throttle_level_t::recovery, 100);
    recovery.reset();
    {
      auto admitted = co_await std::move(strict_recovery);
      EXPECT_FALSE(ordinary.available());
      EXPECT_FALSE(strict_foreground.available());
    }
    {
      auto admitted = co_await std::move(ordinary);
    }
    foreground.reset();
    leaf.reset();
    auto admitted = co_await std::move(strict_foreground);
  });
}

TEST_F(OperationThrottlerTest, wpq_keeps_other_owners_visible_under_saturation)
{
  run_scl([this]() -> seastar::future<> {
    auto &conf = crimson::common::local_conf();
    co_await conf.set_val("osd_op_queue", "wpq");
    set_scheduler(scheduler::make_scheduler(
      nullptr, conf, 0, 1, 0, false, false));
    std::optional<Guard> running;
    running.emplace(co_await request(throttle_level_t::foreground, 0, 1));
    std::vector<seastar::future<Guard>> busy_client;
    for (unsigned i = 0; i < 20; ++i) {
      busy_client.push_back(request(throttle_level_t::foreground, 0, 1));
    }
    co_await wait_for_worker();
    auto other_client = request(throttle_level_t::foreground, 0, 2);
    co_await wait_for_worker();
    running.reset();
    {
      auto admitted = co_await std::move(busy_client.front());
    }
    {
      auto admitted = co_await std::move(other_client);
      EXPECT_FALSE(busy_client[1].available());
    }
    for (auto &future : busy_client | std::views::drop(1)) {
      auto admitted = co_await std::move(future);
    }
  });
}

TEST_F(OperationThrottlerTest, stop_fails_queued_and_new_requests)
{
  run_scl([this]() -> seastar::future<> {
    auto running = co_await request(throttle_level_t::foreground);
    auto blocked = request(throttle_level_t::foreground);
    co_await wait_for_worker();
    co_await throttler->stop();
    unsigned stopped = 0;
    co_await std::move(blocked).discard_result().handle_exception_type(
      [&stopped](const crimson::common::system_shutdown_exception&) {
        ++stopped;
      });
    co_await request(throttle_level_t::leaf).discard_result().handle_exception_type(
      [&stopped](const crimson::common::system_shutdown_exception&) {
        ++stopped;
      });
    EXPECT_EQ(stopped, 2);
  });
}

TEST_F(OperationThrottlerTest, small_limits_preserve_dependencies_and_zero_is_unlimited)
{
  run_scl([this]() -> seastar::future<> {
    auto &conf = crimson::common::local_conf();
    co_await conf.set_val("crimson_osd_scheduler_concurrency", "1");
    auto foreground = co_await request(throttle_level_t::foreground);
    auto recovery = co_await request(throttle_level_t::recovery);
    auto leaf = co_await request(throttle_level_t::leaf);
    auto blocked = request(throttle_level_t::foreground);
    co_await wait_for_worker();
    EXPECT_FALSE(blocked.available());
    co_await conf.set_val("crimson_osd_scheduler_concurrency", "0");
    auto admitted = co_await std::move(blocked);
  });
}

TEST_F(OperationThrottlerTest, lower_limit_preserves_dependency_capacity)
{
  run_scl([this]() -> seastar::future<> {
    auto &conf = crimson::common::local_conf();
    co_await conf.set_val("crimson_osd_scheduler_concurrency", "5");
    std::vector<Guard> running;
    for (unsigned i = 0; i < 3; ++i) {
      running.push_back(co_await request(throttle_level_t::foreground));
    }
    co_await conf.set_val("crimson_osd_scheduler_concurrency", "3");
    auto blocked = request(throttle_level_t::foreground);
    while (!running.empty()) {
      // Each running foreground operation needs recovery and leaf work
      // before it can release its slot under the reduced limit.
      {
        auto recovery = co_await request(throttle_level_t::recovery);
        auto leaf = co_await request(throttle_level_t::leaf);
        EXPECT_FALSE(blocked.available());
      }
      running.pop_back();
    }
    auto admitted = co_await std::move(blocked);
  });
}

class SchedulerTest : public seastar_test_suite_t {
protected:
  std::unique_ptr<CephContext> cct;
  std::map<std::string, std::string> saved_config;

  seastar::future<> set_config(std::string key, std::string value) {
    auto &conf = crimson::common::local_conf();
    if (!saved_config.contains(key)) {
      std::string previous;
      ceph_assert(conf.get_val(key, &previous) == 0);
      saved_config.emplace(key, std::move(previous));
    }
    co_await conf.set_val(key, value);
  }

  seastar::future<> set_up_fut() final {
    cct = std::make_unique<CephContext>();
    co_await set_config("osd_op_queue_cut_off", "low");
    co_await set_config("osd_mclock_profile", "custom");
    co_await set_config("osd_mclock_max_sequential_bandwidth_ssd", "1000");
    co_await set_config("osd_mclock_max_capacity_iops_ssd", "1000");
    co_await set_config("osd_mclock_scheduler_client_res", "0");
    co_await set_config("osd_mclock_scheduler_client_wgt", "1");
    co_await set_config("osd_mclock_scheduler_client_lim", "0");
    co_await set_config("osd_mclock_scheduler_background_best_effort_res", "0");
    co_await set_config("osd_mclock_scheduler_background_best_effort_wgt", "1");
    co_await set_config("osd_mclock_scheduler_background_best_effort_lim", "0");
  }

  seastar::future<> tear_down_fut() final {
    for (const auto &[key, value] : saved_config) {
      co_await crimson::common::local_conf().set_val(key, value);
    }
    cct.reset();
  }

  auto make_mclock() {
    return std::make_unique<scheduler::mClockScheduler>(cct.get(), 0, 1, 0, false);
  }

  static item_t item(throttle_level_t level, uint64_t owner = 0,
                     int cost = 1, unsigned priority = 0,
                     SchedulerClass klass = SchedulerClass::client) {
    return {params_t{cost, priority, owner, klass}, level, seastar::promise<>()};
  }

  static void expect_item(scheduler::WorkItem work, throttle_level_t level,
                          uint64_t owner = 0) {
    auto *selected = std::get_if<item_t>(&work);
    ASSERT_NE(selected, nullptr);
    EXPECT_EQ(selected->level, level);
    EXPECT_EQ(selected->params.owner, owner);
  }
};

TEST_F(SchedulerTest, dependency_bypasses_blocked_work_in_the_same_qos_domain)
{
  run_scl([this]() -> seastar::future<> {
    for (const auto *type : {"wpq", "mclock_scheduler"}) {
      SCOPED_TRACE(type);
      co_await set_config("osd_op_queue", type);
      auto queue = scheduler::make_scheduler(
        cct.get(), crimson::common::local_conf(), 0, 1, 0, false, false);
      queue->enqueue(item(throttle_level_t::foreground));
      queue->enqueue(item(throttle_level_t::recovery));
      queue->enqueue(item(throttle_level_t::leaf));
      expect_item(queue->dequeue(throttle_level_t::leaf), throttle_level_t::leaf);
      EXPECT_TRUE(std::holds_alternative<std::monostate>(
        queue->dequeue(throttle_level_t::leaf)));
      expect_item(queue->dequeue(throttle_level_t::recovery),
                  throttle_level_t::recovery);
      expect_item(queue->dequeue(throttle_level_t::foreground),
                  throttle_level_t::foreground);
      EXPECT_TRUE(queue->empty());
    }
  });
}

TEST_F(SchedulerTest, eligible_levels_keep_fifo_order_within_a_qos_domain)
{
  run_scl([this]() -> seastar::future<> {
    for (const auto *type : {"wpq", "mclock_scheduler"}) {
      SCOPED_TRACE(type);
      co_await set_config("osd_op_queue", type);
      auto queue = scheduler::make_scheduler(
        cct.get(), crimson::common::local_conf(), 0, 1, 0, false, false);
      queue->enqueue(item(throttle_level_t::foreground));
      queue->enqueue(item(throttle_level_t::leaf));
      queue->enqueue(item(throttle_level_t::recovery));
      for (auto level : {throttle_level_t::foreground, throttle_level_t::leaf,
                         throttle_level_t::recovery}) {
        expect_item(queue->dequeue(throttle_level_t::foreground), level);
      }
      EXPECT_TRUE(queue->empty());
    }
  });
}

TEST_F(SchedulerTest, blocked_strict_priority_does_not_hide_runnable_priority)
{
  run_scl([this]() -> seastar::future<> {
    for (const auto *type : {"wpq", "mclock_scheduler"}) {
      SCOPED_TRACE(type);
      co_await set_config("osd_op_queue", type);
      auto queue = scheduler::make_scheduler(
        cct.get(), crimson::common::local_conf(), 0, 1, 0, false, false);
      auto klass = std::string_view(type) == "wpq"
        ? SchedulerClass::immediate : SchedulerClass::client;
      queue->enqueue(item(throttle_level_t::leaf));
      queue->enqueue(item(throttle_level_t::foreground, 0, 1, 200, klass));
      queue->enqueue(item(throttle_level_t::recovery, 0, 1, 100, klass));
      expect_item(queue->dequeue(throttle_level_t::recovery),
                  throttle_level_t::recovery);
      expect_item(queue->dequeue(throttle_level_t::recovery), throttle_level_t::leaf);
      expect_item(queue->dequeue(throttle_level_t::foreground),
                  throttle_level_t::foreground);
      EXPECT_TRUE(queue->empty());
    }
  });
}

TEST_F(SchedulerTest, wpq_levels_share_an_owners_turn)
{
  run_scl([this]() -> seastar::future<> {
    co_await set_config("osd_op_queue", "wpq");
    auto queue = scheduler::make_scheduler(
      cct.get(), crimson::common::local_conf(), 0, 1, 0, false, false);
    for (unsigned i = 0; i < 12; ++i) {
      queue->enqueue(item(static_cast<throttle_level_t>(i % 3), 1));
      queue->enqueue(item(throttle_level_t::foreground, 2));
    }
    for (unsigned i = 0; i < 12; ++i) {
      expect_item(queue->dequeue(throttle_level_t::foreground),
                  static_cast<throttle_level_t>(i % 3), 1);
      expect_item(queue->dequeue(throttle_level_t::foreground),
                  throttle_level_t::foreground, 2);
    }
    EXPECT_TRUE(queue->empty());
  });
}

TEST_F(SchedulerTest, mclock_levels_share_one_rate_limit)
{
  run_scl([this]() -> seastar::future<> {
    // One cost unit per second, with no reservation bypassing the limit.
    co_await set_config("osd_mclock_scheduler_client_lim", "0.001");
    auto queue = make_mclock();
    // Large costs keep deadlines ahead of wall time even on a slow builder.
    // Advance the scheduler's explicit clock instead of waiting for them.
    queue->enqueue(item(throttle_level_t::foreground, 0, 1000));
    queue->enqueue(item(throttle_level_t::recovery, 0, 1000));
    queue->enqueue(item(throttle_level_t::leaf, 0, 1000));
    double now = crimson::dmclock::get_time();
    expect_item(queue->dequeue(throttle_level_t::foreground, now),
                throttle_level_t::foreground);
    std::optional<double> previous_due;
    for (auto level : {throttle_level_t::recovery, throttle_level_t::leaf}) {
      auto work = queue->dequeue(throttle_level_t::foreground, now);
      EXPECT_TRUE(std::holds_alternative<double>(work));
      auto due = std::get<double>(work);
      EXPECT_GT(due, now);
      if (previous_due) {
        EXPECT_DOUBLE_EQ(due - *previous_due, 1000.0);
      }
      expect_item(queue->dequeue(throttle_level_t::foreground, due), level);
      previous_due = due;
      now = due;
    }
    EXPECT_TRUE(queue->empty());
  });
}

TEST_F(SchedulerTest, mclock_replacing_an_unserved_head_does_not_charge_it)
{
  run_scl([this]() -> seastar::future<> {
    co_await set_config("osd_mclock_scheduler_client_lim", "0.001");
    auto queue = make_mclock();
    queue->enqueue(item(throttle_level_t::foreground));
    double now = crimson::dmclock::get_time();
    expect_item(queue->dequeue(throttle_level_t::foreground, now),
                throttle_level_t::foreground);
    queue->enqueue(item(throttle_level_t::foreground, 1, 100000));
    auto first = queue->dequeue(throttle_level_t::foreground, now);
    EXPECT_TRUE(std::holds_alternative<double>(first));
    const auto blocked_due = std::get<double>(first);
    EXPECT_TRUE(std::holds_alternative<std::monostate>(
      queue->dequeue(throttle_level_t::leaf, now)));
    queue->enqueue(item(throttle_level_t::leaf, 2, 1000));
    double due = 0;
    for (unsigned i = 0; i < 3; ++i) {
      auto blocked = queue->dequeue(throttle_level_t::foreground, now);
      EXPECT_TRUE(std::holds_alternative<double>(blocked));
      EXPECT_DOUBLE_EQ(std::get<double>(blocked), blocked_due);
      auto leaf = queue->dequeue(throttle_level_t::leaf, now);
      EXPECT_TRUE(std::holds_alternative<double>(leaf));
      due = std::get<double>(leaf);
      EXPECT_DOUBLE_EQ(blocked_due - due, 99000.0);
    }
    expect_item(queue->dequeue(throttle_level_t::leaf, due), throttle_level_t::leaf, 2);
    auto remaining = queue->dequeue(throttle_level_t::foreground, due);
    EXPECT_TRUE(std::holds_alternative<double>(remaining));
    EXPECT_DOUBLE_EQ(std::get<double>(remaining) - due, 100000.0);
    expect_item(queue->dequeue(throttle_level_t::foreground, std::get<double>(remaining)),
                throttle_level_t::foreground, 1);
    EXPECT_TRUE(queue->empty());
  });
}

TEST_F(SchedulerTest, mclock_new_class_does_not_inherit_a_blocked_head)
{
  run_scl([this]() -> seastar::future<> {
    co_await set_config("osd_mclock_scheduler_client_lim", "0.001");
    auto queue = make_mclock();
    queue->enqueue(item(throttle_level_t::foreground));
    const double now = crimson::dmclock::get_time();
    expect_item(queue->dequeue(throttle_level_t::foreground, now),
                throttle_level_t::foreground);
    queue->enqueue(item(throttle_level_t::foreground, 1, 100000));
    EXPECT_TRUE(std::holds_alternative<double>(
      queue->dequeue(throttle_level_t::foreground, now)));

    // The background class sorts before the client class. Shrinking
    // eligibility must remove the expensive client head before this
    // newly active class calculates its proportional offset.
    queue->enqueue(item(throttle_level_t::leaf, 2, 1, 0,
                        SchedulerClass::background_best_effort));
    queue->enqueue(item(throttle_level_t::leaf, 3, 1000));
    expect_item(queue->dequeue(throttle_level_t::leaf, now + 1000),
                throttle_level_t::leaf, 2);
    expect_item(queue->dequeue(throttle_level_t::leaf, now + 1000),
                throttle_level_t::leaf, 3);
    expect_item(queue->dequeue(throttle_level_t::foreground, now + 201000),
                throttle_level_t::foreground, 1);
    EXPECT_TRUE(queue->empty());
  });
}

TEST_F(SchedulerTest, mclock_levels_share_one_weight)
{
  run_scl([this]() -> seastar::future<> {
    co_await set_config("osd_mclock_scheduler_client_wgt", "2");
    auto queue = make_mclock();
    for (unsigned i = 0; i < 100; ++i) {
      queue->enqueue(item(static_cast<throttle_level_t>(i % 3)));
      queue->enqueue(item(throttle_level_t::leaf, 0, 1, 0,
                          SchedulerClass::background_best_effort));
    }
    unsigned clients = 0;
    for (unsigned i = 0; i < 90; ++i) {
      auto work = queue->dequeue(throttle_level_t::foreground);
      EXPECT_TRUE(std::holds_alternative<item_t>(work));
      clients += std::get<item_t>(work).params.klass == SchedulerClass::client;
    }
    EXPECT_NEAR(clients, 60, 1);
    while (!queue->empty()) {
      auto work = queue->dequeue(throttle_level_t::foreground);
      std::ignore = std::get<item_t>(work);
    }
  });
}

TEST_F(SchedulerTest, abort_fails_visible_and_blocked_requests)
{
  run_scl([this]() -> seastar::future<> {
    for (const auto *type : {"wpq", "mclock_scheduler"}) {
      SCOPED_TRACE(type);
      co_await set_config("osd_op_queue", type);
      auto queue = scheduler::make_scheduler(
        cct.get(), crimson::common::local_conf(), 0, 1, 0, false, false);
      std::vector<seastar::future<>> completions;
      for (auto level : {throttle_level_t::foreground, throttle_level_t::leaf}) {
        for (auto klass : {SchedulerClass::client,
                           SchedulerClass::background_best_effort}) {
          auto request = item(level, 0, 1, 0, klass);
          completions.push_back(request.wake.get_future());
          queue->enqueue(std::move(request));
        }
      }
      auto selected = queue->dequeue(throttle_level_t::leaf);
      std::get<item_t>(selected).wake.set_value();
      queue->abort(std::make_exception_ptr(
        crimson::common::system_shutdown_exception{}));
      unsigned aborted = 0;
      for (auto &completion : completions) {
        co_await seastar::with_timeout(
          seastar::lowres_clock::now() + std::chrono::seconds(5),
          std::move(completion)).handle_exception_type(
          [&aborted](const crimson::common::system_shutdown_exception&) {
            ++aborted;
          });
      }
      EXPECT_EQ(aborted, 3);
      EXPECT_TRUE(queue->empty());
    }
  });
}

struct RequestOrdererTest : seastar_test_suite_t {};

TEST_F(RequestOrdererTest, handoff_preserves_order_before_waiter_resumes)
{
  run_scl([]() -> seastar::future<> {
    RequestOrderer<int> orderer;
    auto first = co_await orderer.get(1);
    auto waiting = orderer.get(1);
    // A missing PG must not hold up another PG on the connection.
    auto other_pg = co_await orderer.get(2);
    EXPECT_FALSE(waiting.available());
    // Model publication: the old waiter becomes ready, but has not resumed.
    first.reset();
    auto lookup_after_publication = orderer.get(1);
    auto old_waiter = co_await std::move(waiting);
    EXPECT_FALSE(lookup_after_publication.available());
    old_waiter.reset();
    auto later = co_await std::move(lookup_after_publication);
  });
}

TEST_F(RequestOrdererTest, abandoned_turn_releases_successor)
{
  run_scl([]() -> seastar::future<> {
    RequestOrderer<int> orderer;
    auto first = co_await orderer.get(1);
    auto abandoned = orderer.get(1);
    auto next = orderer.get(1);
    first.reset();
    // An interrupted request may discard its acquisition result.
    co_await std::move(abandoned).discard_result();
    auto turn = co_await std::move(next);
  });
}

}
