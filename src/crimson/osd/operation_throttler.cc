// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "osd_operation.h"

#include <algorithm>
#include <numeric>
#include "common/Formatter.h"
#include "crimson/common/log.h"

using namespace std::string_literals;
SET_SUBSYS(osd);

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using namespace crimson::osd::scheduler;
namespace crimson::osd {

void OperationThrottler::start()
{
  LOG_PREFIX(OperationThrottler::start);
  if (started) {
    DEBUG("OperationThrottler background task is already started, skipping.");
    return;
  }

  started = true;
  stopped = false;

  INFO("Starting OperationThrottler background task");
  bg_future.emplace(background_task());
}

void OperationThrottler::register_metrics(const std::string &sched_type) {
  namespace sm = seastar::metrics;

  LOG_PREFIX(OperationThrottler::register_metrics);
  INFO("registering metrics for scheduler {}", sched_type);
  const std::string group_name =
    (sched_type == "mclock_scheduler") ? "osd_mclock" : "osd_wpq";

  for (auto& [op_class, name] : {
    std::pair{SchedulerClass::background_recovery,    "background_recovery"},
    std::pair{SchedulerClass::background_best_effort, "background_best_effort"},
    std::pair{SchedulerClass::client,                 "client"},
    std::pair{SchedulerClass::repop,                  "repop"},
    std::pair{SchedulerClass::immediate,              "immediate"},
  }) {
    auto label = sm::label("op_class")(name);
    metrics.add_group(group_name, {
      sm::make_counter("throttled_ops",
        [this, op_class] { return throttled_ops[op_class]; },
        sm::description("ops delayed by mClock scheduler"), {label}),
      sm::make_counter("total_wait_ms",
        [this, op_class] { return total_wait_ms[op_class]; },
        sm::description("total ms spent waiting in mClock"), {label}),
      sm::make_gauge("max_wait_ms",
        [this, op_class] { return max_wait_ms[op_class]; },
        sm::description("max wait ms in mClock by op class"), {label}),
      sm::make_histogram("throttle_wait_latency",
        [this, op_class]() -> seastar::metrics::histogram& {
          return wait_hist[op_class]; },
        sm::description("mClock throttle wait distribution"), {label}),
    });
  }
}


OperationThrottler::OperationThrottler(ConfigProxy &conf)
{
  conf.add_observer(this);
  for (auto op_class : {SchedulerClass::background_recovery,
                        SchedulerClass::background_best_effort,
                        SchedulerClass::client,
                        SchedulerClass::repop,
                        SchedulerClass::immediate}) {
    wait_hist[op_class].buckets = {
      {0, 1},
      {0, 5},
      {0, 10},
      {0, 50},
      {0, 100},
      {0, 500},
      {0, 1000},
    };
  }
  register_metrics(conf.get_val<std::string>("osd_op_queue"));

}

void OperationThrottler::initialize_scheduler(CephContext *cct, ConfigProxy &conf, bool is_rotational, int whoami)
{
  scheduler = crimson::osd::scheduler::make_scheduler(cct, conf, whoami, seastar::this_smp_shard_count(),
            seastar::this_shard_id(), is_rotational, true);
  update_from_config(conf);
}

uint64_t OperationThrottler::count_from(const level_counts_t &counts, size_t level)
{
  return std::accumulate(counts.begin() + level, counts.end(), uint64_t{0});
}

bool OperationThrottler::available(throttle_level_t level) const
{
  if (!max_in_progress) {
    return true;
  }
  // An admission counts against its own level and every level below it.
  for (size_t l = 0; l <= std::to_underlying(level); ++l) {
    // A lower live limit must still leave room for the dependencies of
    // running higher-level operations. Their own level stays capped, so
    // new work cannot replace them until they have drained.
    const auto limit = std::max(max_in_progress - reserved_per_level * l,
      count_from(in_progress, l + 1) + reserved_per_level);
    if (count_from(in_progress, l) >= limit) {
      return false;
    }
  }
  return true;
}

seastar::future<> OperationThrottler::background_task()
{
  LOG_PREFIX(OperationThrottler::background_task);
  while (!stopped) {
    if (!scheduler->empty() && available(throttle_level_t::leaf)) {
      // Eligibility is a prefix of the levels: if a level has room, all
      // its dependencies do too. Let QoS choose within that prefix.
      auto level = throttle_level_t::foreground;
      while (!available(level)) {
        level = static_cast<throttle_level_t>(std::to_underlying(level) - 1);
      }
      auto work = scheduler->dequeue(level);
      if (auto item = std::get_if<item_t>(&work)) {
        const auto index = std::to_underlying(item->level);
        ceph_assert(available(item->level) && pending[index] > 0);
        ++in_progress[index];
        --pending[index];
        DEBUG("Waking up a work item at level {}: in_progress={}, pending={}",
              index, count_from(in_progress), count_from(pending));
        item->wake.set_value();
        co_await seastar::maybe_yield();
        continue;
      }
      if (auto when_ready = std::get_if<double>(&work)) {
        const auto delay = ceph::real_clock::from_double(*when_ready) -
                           ceph::real_clock::now();
        if (delay > decltype(delay)::zero()) {
          try {
            co_await cv.wait(delay);
          } catch (const seastar::condition_variable_timed_out&) {
            // Retry when the next mClock tag becomes due.
          }
        }
        continue;
      }
    }
    // No admissible work. Enqueue, release, config changes and stop all
    // signal this wait; a nonempty queue alone must not make it spin.
    co_await cv.wait();
  }

  DEBUG("Background task exiting cleanly");
}

void OperationThrottler::wake() {
  // Attempt to wakeup pending operation if resources are available.
  // The mclock scheduler might return delay if item is not ready
  // to process
  cv.signal();
}

void OperationThrottler::release_throttle(throttle_level_t level)
{
  LOG_PREFIX(OperationThrottler::release_throttle);
  const auto index = std::to_underlying(level);
  ceph_assert(in_progress[index] > 0);
  --in_progress[index];
  DEBUG("Updated counters during release_throttle: level={}, in_progress={}, pending={}",
        static_cast<int>(level), count_from(in_progress), count_from(pending));
  wake();
}

seastar::future<> OperationThrottler::acquire_throttle(
  crimson::osd::scheduler::params_t params,
  throttle_level_t level)
{
  if (stopped) {
    return seastar::make_exception_future<>(
      crimson::common::system_shutdown_exception{});
  }
  crimson::osd::scheduler::item_t item{params, level, seastar::promise<>()};
  auto fut = item.wake.get_future();
  scheduler->enqueue(std::move(item));
  ++pending[std::to_underlying(level)];
  wake();
  return fut;
}

seastar::future<> OperationThrottler::stop()
{
  if (stopped) {
    co_return;
  }

  stopped = true;
  cv.broadcast();

  if (bg_future) {
    co_await std::move(*bg_future);
  }

  bg_future.reset();
  if (scheduler) {
    scheduler->abort(std::make_exception_ptr(
      crimson::common::system_shutdown_exception{}));
  }
  pending = {};
  started = false;
}

void OperationThrottler::record_throttle_wait(
  SchedulerClass op_class, uint64_t wait_ms)
{
  if (wait_ms == 0) {
    return;
  }
  throttled_ops[op_class]++;
  total_wait_ms[op_class] += wait_ms;
  max_wait_ms[op_class] = std::max(max_wait_ms[op_class], wait_ms);

  auto& h = wait_hist[op_class];
  h.sample_count++;
  h.sample_sum += wait_ms;
  for (auto& bucket : h.buckets) {
    if (wait_ms <= bucket.upper_bound) {
      bucket.count++;
    }
  }
}

void OperationThrottler::dump_detail(Formatter *f) const
{
  f->dump_unsigned("max_in_progress", max_in_progress);
  f->dump_unsigned("in_progress", count_from(in_progress));
  f->open_array_section("levels");
  for (size_t l = 0; l < num_levels; ++l) {
    f->open_object_section("level");
    f->dump_unsigned("level", l);
    f->dump_unsigned("in_progress", in_progress[l]);
    f->dump_unsigned("pending", pending[l]);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("pending", count_from(pending));
  f->dump_unsigned("background_task started", started);
  f->dump_unsigned("background_task stopped", stopped);

  f->open_object_section("scheduler");
  {
    scheduler->dump(*f);
  }
  f->close_section();
}

void OperationThrottler::update_from_config(const ConfigProxy &conf)
{
  max_in_progress = conf.get_val<uint64_t>("crimson_osd_scheduler_concurrency");
  if (max_in_progress && max_in_progress < min_in_progress) {
    logger().warn("crimson_osd_scheduler_concurrency={} leaves no slot for "
                  "foreground operations, using {}",
                  max_in_progress, min_in_progress);
    max_in_progress = min_in_progress;
  }
  wake();
}

std::vector<std::string> OperationThrottler::get_tracked_keys() const noexcept
{
  return {"crimson_osd_scheduler_concurrency"s};
}

void OperationThrottler::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed)
{
  update_from_config(conf);
}

}
