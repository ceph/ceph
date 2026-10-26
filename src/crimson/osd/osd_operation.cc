// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "osd_operation.h"
#include "common/ceph_time_fmt.h"
#include "common/Formatter.h"
#include "crimson/common/log.h"
#include "crimson/osd/osd_operations/client_request.h"

using namespace std::string_literals;
SET_SUBSYS(osd);

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using namespace crimson::osd::scheduler;
namespace crimson::osd {

void OSDOperationRegistry::do_stop()
{
  logger().info("OSDOperationRegistry::{}", __func__);
  // we need to decouple visiting the registry from destructing
  // ops because of the auto-unlink feature of boost::intrusive.
  // the list shouldn't change while iterating due to constrains
  // on iterator's validity.
  constexpr auto historic_reg_index =
    static_cast<size_t>(OperationTypeCode::historic_client_request);
  auto& historic_registry = get_registry<historic_reg_index>();
  std::vector<ClientRequest::ICRef> to_ref_down;
  std::transform(std::begin(historic_registry), std::end(historic_registry),
		 std::back_inserter(to_ref_down),
		 [] (const Operation& op) {
		   return ClientRequest::ICRef{
		     static_cast<const ClientRequest*>(&op),
		     /* add_ref= */ false
		   };
		 });
  // to_ref_down is going off
}

OSDOperationRegistry::OSDOperationRegistry()
  : OperationRegistryT(seastar::this_shard_id()) {}

static auto get_duration(const ClientRequest& client_request)
{
  // TODO: consider enhancing `CompletionEvent` with computing duration
  // once -- when it's enetered.
  return client_request.get_completed() - client_request.get_started();
}

void OSDOperationRegistry::put_historic(const ClientRequest& op)
{
  using crimson::common::local_conf;
  // unlink the op from the client request registry. this is a part of
  // the re-link procedure. finally it will be in historic/historic_slow registry.
  constexpr auto historic_reg_index =
    static_cast<size_t>(OperationTypeCode::historic_client_request);
  constexpr auto slow_historic_reg_index = 
    static_cast<size_t>(OperationTypeCode::historic_slow_client_request);

  if (get_duration(op) > local_conf()->osd_op_complaint_time) {
    auto& slow_historic_registry = get_registry<slow_historic_reg_index>();
    _put_historic(slow_historic_registry,
      op,
      local_conf()->osd_op_history_slow_op_size);
  } else {
    auto& historic_registry = get_registry<historic_reg_index>();
    _put_historic(historic_registry,
      op,
      local_conf()->osd_op_history_size);
  }
}

void OSDOperationRegistry::_put_historic(
  op_list& list,
  const class ClientRequest& op,
  uint64_t max)
{
  constexpr auto client_reg_index =
    static_cast<size_t>(OperationTypeCode::client_request);
  auto& client_registry = get_registry<client_reg_index>();

  // we only save the newest op
  list.splice(std::end(list), client_registry, client_registry.iterator_to(op));
  ClientRequest::ICRef(
      &op, /* add_ref= */true
    ).detach(); // yes, "leak" it for now!

  if (list.size() >= max) {
    auto old_op_ptr = &list.front();
    list.pop_front();
    const auto& old_op =
      static_cast<const ClientRequest&>(*old_op_ptr);
    // clear a previously "leaked" op
    ClientRequest::ICRef(&old_op, /* add_ref= */false);
  }
}

size_t OSDOperationRegistry::dump_historic_client_requests(ceph::Formatter* f) const
{
  const auto& historic_client_registry =
    get_registry<static_cast<size_t>(OperationTypeCode::historic_client_request)>(); //ClientRequest::type)>();
  f->open_object_section("op_history");
  f->dump_int("size", historic_client_registry.size());
  // TODO: f->dump_int("duration", history_duration.load());
  // the intrusive list is configured to not store the size
  size_t ops_count = 0;
  {
    f->open_array_section("ops");
    for (const auto& op : historic_client_registry) {
      op.dump(f);
      ++ops_count;
    }
    f->close_section();
  }
  f->close_section();
  return ops_count;
}

size_t OSDOperationRegistry::dump_slowest_historic_client_requests(ceph::Formatter* f) const
{
  const auto& slow_historic_client_registry =
    get_registry<static_cast<size_t>(OperationTypeCode::historic_slow_client_request)>(); //ClientRequest::type)>();
  f->open_object_section("op_history");
  f->dump_int("size", slow_historic_client_registry.size());
  // TODO: f->dump_int("duration", history_duration.load());
  // the intrusive list is configured to not store the size
  size_t ops_count = 0;
  {
    f->open_array_section("ops");
    for (const auto& op : slow_historic_client_registry) {
      op.dump(f);
      ++ops_count;
    }
    f->close_section();
  }
  f->close_section();
  return ops_count;
}

void OSDOperationRegistry::visit_ops_in_flight(std::function<void(const ClientRequest&)>&& visit)
{
  const auto& client_registry =
    get_registry<static_cast<size_t>(OperationTypeCode::client_request)>();
  auto it = std::begin(client_registry);
  for (; it != std::end(client_registry); ++it) {
    const auto& fastest_historic_op = static_cast<const ClientRequest&>(*it);
    visit(fastest_historic_op);
  }
}

void OperationThrottler::start()
{
  LOG_PREFIX(OperationThrottler::start);
  if (started) {
    DEBUG("OperationThrottler background task is already started, skipping.");
    return;
  }

  started=true;
  stopped=false;

  INFO("Starting OperationThrottler background task");
  bg_future.emplace(background_task());
  return;
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

uint64_t OperationThrottler::in_progress_from(size_t level) const
{
  uint64_t n = 0;
  for (size_t l = level; l < num_levels; ++l) {
    n += in_progress[l];
  }
  return n;
}

bool OperationThrottler::available(throttle_level_t level) const
{
  if (!max_in_progress) {
    return true;
  }
  // admitting an operation at this level adds a slot to every level at or
  // below it, so each of those levels must still be within its limit: all
  // slots, minus the ones reserved for each level below it
  for (size_t l = 0; l <= static_cast<size_t>(level); ++l) {
    const uint64_t limit = max_in_progress - reserved_per_level * l;
    if (in_progress_from(l) + 1 > limit) {
      return false;
    }
  }
  return true;
}

bool OperationThrottler::has_parked_admissible() const
{
  for (size_t l = 0; l < num_levels; ++l) {
    if (!parked[l].empty() && available(static_cast<throttle_level_t>(l))) {
      return true;
    }
  }
  return false;
}

void OperationThrottler::admit(crimson::osd::scheduler::item_t &item)
{
  LOG_PREFIX(OperationThrottler::admit);
  ++in_progress[static_cast<size_t>(item.level)];
  --pending;
  DEBUG("Waking up a work item at level {}: in_progress={}, pending={}",
        static_cast<int>(item.level), in_progress_from(0), pending);
  item.wake.set_value();
}

seastar::future<> OperationThrottler::background_task() {
  LOG_PREFIX(OperationThrottler::background_task);
  while (!stopped) {
    co_await cv.wait([this] {
      return stopped ||
        has_parked_admissible() ||
        (!scheduler->empty() && available(throttle_level_t::leaf));
    });

    while (!stopped) {
      // items parked earlier were dequeued before anything still in the
      // scheduler, so they go first, lowest level first
      for (size_t l = 0; l < num_levels; ++l) {
        while (!parked[l].empty() &&
               available(static_cast<throttle_level_t>(l))) {
          admit(parked[l].front());
          parked[l].pop_front();
        }
      }
      // an item whose level is full is parked once dequeued, so dequeuing
      // only stops when nothing at all can be admitted
      if (scheduler->empty() || !available(throttle_level_t::leaf)) {
        break;
      }
      // It might be possible as mclock scheduler can return a timestamp in double means
      // the work item is scheduled in the future, so in that case wait until
      // the returned timestamp in the dequeue response before retrying.
      WorkItem work_item = scheduler->dequeue();
      if (auto when_ready = std::get_if<double>(&work_item)) {
        ceph::real_clock::time_point future_time = ceph::real_clock::from_double(*when_ready);
        auto now = ceph::real_clock::now();
        if (future_time <= now) {
          DEBUG("future_time={} already passed now={}, retrying immediately", future_time, now);
          continue;
        }  
        auto wait_duration = std::chrono::duration_cast<std::chrono::milliseconds>(future_time - now);
        DEBUG("No items ready. Retrying in {} ms", wait_duration.count());
        // Wait on cv rather than sleep: an op enqueued meanwhile (an
        // immediate one goes to the high-priority queue, which dequeue()
        // serves first), a released slot or stop() must not wait out the
        // head mClock item's tag.
        try {
          co_await cv.wait(wait_duration);
        } catch (const seastar::condition_variable_timed_out&) {
          // the head item's tag is due
        }
        continue;
      }
      if (auto *item = std::get_if<crimson::osd::scheduler::item_t>(&work_item)) {
        if (available(item->level)) {
          admit(*item);
        } else {
          const auto l = static_cast<size_t>(item->level);
          parked[l].push_back(std::move(*item));
          DEBUG("Parking a work item at level {}: in_progress={}, pending={}, "
                "parked={}",
                l, in_progress_from(0), pending, parked[l].size());
        }
      } else {
        DEBUG("Unexpected variant in WorkItem — neither time nor item");
      }
    }
  }

  DEBUG("Background task exiting cleanly");
  co_return;
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
  auto &n = in_progress[static_cast<size_t>(level)];
  ceph_assert(n > 0);
  --n;
  DEBUG("Updated counters during release_throttle: level={}, in_progress={}, pending={}",
        static_cast<int>(level), in_progress_from(0), pending);
  wake();
}

seastar::future<> OperationThrottler::acquire_throttle(
  crimson::osd::scheduler::params_t params,
  throttle_level_t level)
{
  crimson::osd::scheduler::item_t item{params, level, seastar::promise<>()};
  auto fut = item.wake.get_future();
  scheduler->enqueue(std::move(item));
  ++pending;
  wake();
  return fut;
}

seastar::future<> OperationThrottler::stop()
{
  if (!started)
    co_return;

  stopped = true;
  cv.broadcast();

  if (bg_future && !bg_future->available()) {
    co_await std::move(*bg_future);
  }

  bg_future.reset();
  started = false;

  co_return;
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
  f->dump_unsigned("in_progress", in_progress_from(0));
  f->open_array_section("levels");
  for (size_t l = 0; l < num_levels; ++l) {
    f->open_object_section("level");
    f->dump_unsigned("level", l);
    f->dump_unsigned("in_progress", in_progress[l]);
    f->dump_unsigned("parked", parked[l].size());
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("pending", pending);
  f->dump_unsigned("background_task started", started);
  f->dump_unsigned("background_task stopeed", stopped);

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
