// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "osd_operation.h"

#include <algorithm>
#include <numeric>
#include <ranges>
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

OperationThrottler::QueueKey OperationThrottler::queue_key(const item_t &item) const
{
  return {
    .level = item.level,
    .klass = item.params.klass,
    .priority = scheduler->get_strict_priority(item.params),
    .owner = scheduler->get_queue_owner(item.params),
  };
}

uint64_t OperationThrottler::count_from(const level_counts_t &counts, size_t level)
{
  return std::accumulate(counts.begin() + level, counts.end(), uint64_t{0});
}

bool OperationThrottler::fits(throttle_level_t level, const level_counts_t &counts) const
{
  if (!max_in_progress) {
    return true;
  }
  // An admission counts against its own level and every level below it.
  for (size_t l = 0; l <= std::to_underlying(level); ++l) {
    if (count_from(counts, l) >= max_in_progress - reserved_per_level * l) {
      return false;
    }
  }
  return true;
}

bool OperationThrottler::available(throttle_level_t level) const
{
  return fits(level, in_progress);
}

bool OperationThrottler::can_feed(const QueueKey &key, const PendingQueue &queue) const
{
  return !queue.items.empty() &&
    (queue.candidates == 0 || fits(key.level, committed));
}

void OperationThrottler::feed()
{
  for (auto &[key, queue] : waiting) {
    while (can_feed(key, queue)) {
      scheduler->enqueue(std::move(queue.items.front()));
      queue.items.pop_front();
      ++queue.candidates;
      ++committed[std::to_underlying(key.level)];
    }
  }
}

bool OperationThrottler::has_parked_admissible() const
{
  return std::ranges::any_of(parked, [this](const auto &item) {
    return available(item.level);
  });
}

bool OperationThrottler::admit_parked()
{
  auto admissible = parked | std::views::filter([this](const auto &item) {
    return available(item.level);
  });
  // max_element keeps the first candidate when priorities are equal.
  auto next = std::ranges::max_element(admissible, {}, [this](const auto &item) {
    return scheduler->get_strict_priority(item.params);
  });
  if (next == admissible.end()) {
    return false;
  }
  auto item = std::move(*next);
  parked.erase(next.base());
  admit(item);
  return true;
}

void OperationThrottler::admit(crimson::osd::scheduler::item_t &item)
{
  LOG_PREFIX(OperationThrottler::admit);
  const auto level = std::to_underlying(item.level);
  if (item.level == throttle_level_t::leaf) {
    ++committed[level];
  } else {
    auto queue = waiting.find(queue_key(item));
    ceph_assert(queue != waiting.end() && queue->second.candidates > 0);
    --queue->second.candidates;
    if (queue->second.empty()) {
      waiting.erase(queue);
    }
  }
  ++in_progress[level];
  --pending;
  DEBUG("Waking up a work item at level {}: in_progress={}, pending={}",
        static_cast<int>(item.level), count_from(in_progress), pending);
  item.wake.set_value();
}

seastar::future<> OperationThrottler::background_task() {
  LOG_PREFIX(OperationThrottler::background_task);
  while (!stopped) {
    co_await cv.wait([this] {
      return stopped ||
        has_parked_admissible() ||
        std::ranges::any_of(waiting, [this](const auto &entry) {
          return can_feed(entry.first, entry.second);
        }) ||
        (!scheduler->empty() && available(throttle_level_t::leaf));
    });

    while (!stopped) {
      feed();
      std::optional<double> when_ready;
      // Give newly arrived strict-priority work a chance before choosing
      // among parked candidates. Ordinary candidates retain dequeue order.
      if (!scheduler->empty() && available(throttle_level_t::leaf)) {
        auto work = scheduler->dequeue();
        if (auto item = std::get_if<crimson::osd::scheduler::item_t>(&work)) {
          const bool admissible = available(item->level);
          parked.push_back(std::move(*item));
          if (!admissible) {
            // Keep looking: a blocked strict-priority candidate can hide a
            // runnable strict-priority candidate ahead of ordinary parked work.
            continue;
          }
        } else if (auto when = std::get_if<double>(&work)) {
          when_ready = *when;
        } else {
          ceph_abort("Unexpected scheduler result");
        }
      }
      if (admit_parked()) {
        continue;
      }
      if (when_ready) {
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
      break;
    }
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
  ceph_assert(in_progress[index] > 0 && committed[index] > 0);
  --in_progress[index];
  --committed[index];
  DEBUG("Updated counters during release_throttle: level={}, in_progress={}, pending={}",
        static_cast<int>(level), count_from(in_progress), pending);
  wake();
}

seastar::future<> OperationThrottler::acquire_throttle(
  crimson::osd::scheduler::params_t params,
  throttle_level_t level)
{
  crimson::osd::scheduler::item_t item{params, level, seastar::promise<>()};
  auto fut = item.wake.get_future();
  if (level == throttle_level_t::leaf) {
    scheduler->enqueue(std::move(item));
  } else {
    waiting[queue_key(item)].items.push_back(std::move(item));
  }
  ++pending;
  wake();
  return fut;
}

seastar::future<> OperationThrottler::stop()
{
  if (!started) {
    co_return;
  }

  stopped = true;
  cv.broadcast();

  if (bg_future && !bg_future->available()) {
    co_await std::move(*bg_future);
  }

  bg_future.reset();
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
    const auto level = static_cast<throttle_level_t>(l);
    const auto parked_count = std::ranges::count(parked, level, &item_t::level);
    uint64_t unfed_count = 0;
    for (const auto &[key, queue] : waiting) {
      if (key.level == level) {
        unfed_count += queue.items.size();
      }
    }
    f->dump_unsigned("parked", parked_count);
    f->dump_unsigned("in_scheduler", committed[l] - in_progress[l] - parked_count);
    f->dump_unsigned("unfed", unfed_count);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("pending", pending);
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
