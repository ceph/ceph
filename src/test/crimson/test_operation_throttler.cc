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
#include "test/crimson/gtest_seastar.h"

namespace crimson::osd {
namespace {
using scheduler::item_t;
using scheduler::params_t;
using scheduler::throttle_level_t;

class TestScheduler final : public scheduler::Scheduler {
  std::list<item_t> items;
public:
  scheduler::priority_t get_strict_priority(const params_t &params) const final {
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
  scheduler::WorkItem dequeue() final {
    auto next = std::max_element(items.begin(), items.end(),
      [this](const auto &a, const auto &b) {
        return get_strict_priority(a.params) < get_strict_priority(b.params);
      });
    auto item = std::move(*next);
    items.erase(next);
    return item;
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

  seastar::future<> set_up_fut() final {
    auto &conf = crimson::common::local_conf();
    previous_scheduler = conf.get_val<std::string>("osd_op_queue");
    throttler = std::make_unique<OperationThrottler>(conf);
    throttler->scheduler = std::make_unique<TestScheduler>();
    throttler->max_in_progress = 3;
    throttler->start();
    return seastar::now();
  }
  seastar::future<> tear_down_fut() final {
    co_await throttler->stop();
    EXPECT_TRUE(throttler->waiting.empty());
    EXPECT_TRUE(throttler->parked.empty());
    EXPECT_EQ(throttler->count_from(throttler->committed), 0);
    EXPECT_EQ(throttler->count_from(throttler->in_progress), 0);
    EXPECT_EQ(throttler->pending, 0);
    auto &conf = crimson::common::local_conf();
    conf.remove_observer(throttler.get());
    throttler.reset();
    co_await conf.set_val("osd_op_queue", previous_scheduler);
  }
  void set_scheduler(scheduler::SchedulerRef scheduler) {
    throttler->scheduler = std::move(scheduler);
  }
  auto request(throttle_level_t level, unsigned priority = 0,
               scheduler::client_t owner = 0) {
    return seastar::with_timeout(
      seastar::lowres_clock::now() + std::chrono::seconds(5),
      throttler->get_throttle(
        params_t{1, priority, owner, SchedulerClass::client}, level));
  }
  seastar::future<> wait_for_parked(size_t count) {
    for (unsigned i = 0; i < 1000; ++i) {
      if (throttler->parked.size() >= count) {
        co_return;
      }
      co_await seastar::yield();
    }
    throw std::runtime_error("throttler did not park expected requests");
  }
};

TEST_F(OperationThrottlerTest, high_priority_bypasses_unfed_backlog)
{
  run_scl([this]() -> seastar::future<> {
    std::optional<Guard> running;
    running.emplace(co_await request(throttle_level_t::foreground));
    std::vector<seastar::future<Guard>> normal;
    for (unsigned i = 0; i < 20; ++i) {
      normal.push_back(request(throttle_level_t::foreground));
    }
    co_await wait_for_parked(1);
    auto high = request(throttle_level_t::foreground, 100);
    co_await wait_for_parked(2);
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

TEST_F(OperationThrottlerTest, earlier_parked_client_precedes_replacement_recovery)
{
  run_scl([this]() -> seastar::future<> {
    std::optional<Guard> first, second;
    first.emplace(co_await request(throttle_level_t::recovery));
    second.emplace(co_await request(throttle_level_t::recovery));
    auto client = request(throttle_level_t::foreground);
    co_await wait_for_parked(1);
    auto recovery = request(throttle_level_t::recovery);
    co_await wait_for_parked(2);
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
    co_await wait_for_parked(1);
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

TEST_F(OperationThrottlerTest, blocked_strict_foreground_does_not_hide_strict_recovery)
{
  run_scl([this]() -> seastar::future<> {
    std::optional<Guard> foreground, recovery, leaf;
    foreground.emplace(co_await request(throttle_level_t::foreground));
    recovery.emplace(co_await request(throttle_level_t::recovery));
    auto ordinary = request(throttle_level_t::recovery);
    co_await wait_for_parked(1);
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
    co_await wait_for_parked(1);
    auto other_client = request(throttle_level_t::foreground, 0, 2);
    co_await wait_for_parked(2);
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
